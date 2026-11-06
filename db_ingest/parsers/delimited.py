"""Bounded CSV framing with exact captured-byte provenance.

The framing state machine establishes record boundaries and quote provenance;
csv.reader supplies standards-aware ordered field decoding after a size check.
Oversized records are scanned without retaining their payload. An unclosed quote
or decoding error leaves a preserved unresolved tail, never guessed row counts.
"""
import codecs
import csv
import hashlib
import io
import json
import re
from functools import lru_cache
from pathlib import Path
from ..errors import IngestError
from ..models import DialectDecision, FieldValue, RecordEnvelope

_BOMS = [(codecs.BOM_UTF32_LE, "utf-32-le"), (codecs.BOM_UTF32_BE, "utf-32-be"), (codecs.BOM_UTF8, "utf-8"), (codecs.BOM_UTF16_LE, "utf-16-le"), (codecs.BOM_UTF16_BE, "utf-16-be")]
_SUPPORTED_ENCODINGS = {"utf-8", "utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be", "ascii", "cp1252", "iso8859-1", "cp037", "cp500", "cp1140", "cp437", "cp850", "cp858", "cp1250", "cp1251", "cp1253", "cp1254", "cp1255", "cp1256", "cp1257", "cp1258", "iso8859-15", "koi8-r"}
_MULTIBYTE_ENCODINGS = {"shift_jis", "cp932", "gb18030", "gbk", "big5", "euc_jp", "euc_kr", "cp949"}
_SUPPORTED_ENCODINGS |= _MULTIBYTE_ENCODINGS


def _error(code, text, **context):
    raise IngestError(code, text, context=context)


def _encoding(raw, configured):
    bom_encoding, bom_size = None, 0
    for bom, name in _BOMS:
        if raw.startswith(bom):
            bom_encoding, bom_size = name, len(bom)
            break
    if configured == "auto":
        name = bom_encoding or "utf-8"
    else:
        try:
            name = codecs.lookup(configured).name
        except LookupError:
            _error("ENCODING_UNSUPPORTED", "Unknown encoding codec", encoding=configured)
        if name == "utf-8-sig":
            name = "utf-8"
        if name in {"utf-16", "utf-32"}:
            if not bom_encoding or not bom_encoding.startswith(name):
                _error("ENCODING_BOM_REQUIRED", "Specify byte order explicitly for a source without a matching BOM", encoding=name)
            name = bom_encoding
        if bom_encoding and name != bom_encoding:
            _error("ENCODING_BOM_CONFLICT", "Configured encoding conflicts with captured BOM", configured=name, bom=bom_encoding)
    if name not in _SUPPORTED_ENCODINGS:
        _error("ENCODING_UNSUPPORTED", "Encoding lacks a tested byte-span mapping; select a documented UTF, single-byte or reversible multibyte codec explicitly. Stateful codecs and binary mainframe layouts require a separate decoder", encoding=name)
    return name, bom_size


def decide(artifact, config):
    settings = config.get("parsing", {})
    limits = config.get("limits", {})
    sample_limit = min(settings.get("sample_bytes", 8388608), limits.get("max_in_memory_record_bytes", 8388608), 8 * 1024 * 1024)
    with open(artifact.path, "rb") as source:
        raw = source.read(sample_limit)
        complete = source.read(1) == b""
    if not raw:
        _error("EMPTY_SOURCE", "Captured source has no bytes; no schema can be inferred")
    for magic in (b"PK\x03\x04", b"\x1f\x8b", b"%PDF", b"\xd0\xcf\x11\xe0", b"\x89PNG", b"SQLite format 3"):
        if raw.startswith(magic):
            _error("UNSUPPORTED_FORMAT", "Captured source has an unsupported binary/compressed signature", signature=magic.hex())
    encoding, bom_size = _encoding(raw, settings.get("encoding", "auto"))
    try:
        sample = codecs.getincrementaldecoder(encoding)(errors="strict").decode(raw[bom_size:], final=complete)
    except UnicodeDecodeError as error:
        _error("ENCODING_DECISION", "Strict decoding failed; choose the actual encoding explicitly. Captured original remains intact.", encoding=encoding, byte_start=bom_size + error.start, byte_end=bom_size + error.end)
    if not sample:
        _error("EMPTY_SOURCE", "Captured source contains a BOM but no records")
    if "\x00" in sample:
        _error("UNSUPPORTED_FORMAT", "Embedded NUL in decoded text requires an explicit supported binary representation")
    stripped = sample.lstrip()
    source_format = config.get("source", {}).get("format", "auto")
    explicit_tokens = source_format == "delimited" and isinstance(settings.get("quotechar"),str) and settings["quotechar"].startswith("<") and stripped.startswith(settings["quotechar"])
    if source_format in {"xml", "html"} or not explicit_tokens and (stripped.startswith(("<?xml", "<!DOCTYPE", "<html", "<HTML")) or (stripped.startswith("<") and ">" in stripped[:200])):
        _error("STRUCTURED_MAPPING_REQUIRED", "XML/HTML requires an enabled declared scalar mapping; delimited interpretation is blocked", source_format=source_format)
    quote = settings.get("quotechar", "auto")
    quote = '"' if quote == "auto" else None if quote in {None, "none", ""} else quote
    escape = settings.get("escapechar", "none")
    escape = None if escape in {None, "none", ""} else escape
    doublequote = settings.get("doublequote", True)
    delimiter = settings.get("delimiter", "auto")
    candidates = []
    if delimiter == "auto" and settings.get("nel_is_newline", False):
        _error("DIALECT_AMBIGUOUS", "NEL record separators require an explicit delimiter", next_action="Set parsing.delimiter and parsing.nel_is_newline=true")
    if delimiter == "auto" and (len(quote or "")>1 or len(escape or "")>1 or settings.get("record_terminator","auto")!="auto"):
        _error("DIALECT_AMBIGUOUS", "Alternate token dialects require an explicit delimiter")
    if delimiter == "auto":
        for value in (",", "\t", "|", ";"):
            try:
                rows = []
                reader = csv.reader(io.StringIO(sample, newline=""), delimiter=value, quotechar=quote, escapechar=escape, doublequote=doublequote, quoting=csv.QUOTE_MINIMAL if quote else csv.QUOTE_NONE)
                for row in reader:
                    if row:
                        rows.append(row)
                    if len(rows) >= 25:
                        break
                widths = [len(row) for row in rows]
                if not complete and len(rows) > 1:
                    widths = widths[:-1]
                if len(widths) >= 2 and min(widths) == max(widths) and widths[0] > 1:
                    candidates.append({"delimiter": value, "width": widths[0], "sample_records": len(widths)})
            except csv.Error:
                pass
        if len(candidates) != 1:
            _error("DIALECT_AMBIGUOUS", "Delimiter inference is ambiguous; supply --delimiter", candidates=candidates)
        delimiter = candidates[0]["delimiter"]
    from .token_delimited import validate_tokens
    validate_tokens({**settings,"delimiter":delimiter,"quotechar":quote or "none","escapechar":escape or "none"})
    header = settings.get("header", "auto")
    if header == "auto":
        _error("HEADER_AMBIGUOUS", "Header inference is advisory; declare header=present or absent before a reproducible interpretation", delimiter=delimiter)
    if header not in {"present", "absent"}:
        _error("HEADER_INVALID", "Header must be auto, present, or absent")
    return DialectDecision(encoding, delimiter, quote, escape, doublequote, header, {"version": 1, "bom_bytes": bom_size, **({"nel_is_newline":True} if settings.get("nel_is_newline",False) else {}), "encoding_source": "BOM" if bom_size else "configured" if settings.get("encoding", "auto") != "auto" else "strict UTF-8 candidate; validated during full parse", "delimiter_candidates": candidates, "sample_bytes": len(raw), "sample_complete": complete, "header_source": "explicit override", "quote_source": "conventional double quote" if settings.get("quotechar", "auto") == "auto" else "explicit override"})


@lru_cache(maxsize=32)
def _controls(delimiter,quotechar,escapechar,nel=False):
    values={delimiter,"\r","\n"}
    if nel:values.add("\x85")
    if quotechar:values.add(quotechar)
    if escapechar:values.add(escapechar)
    return re.compile("["+re.escape("".join(sorted(values)))+"]")


def _tokens(text,decision):
    """Ordinary runs contain no framing controls; each decode chunk is bounded."""
    start=0
    for match in _controls(decision.delimiter,decision.quotechar,decision.escapechar,decision.evidence.get("nel_is_newline", False)).finditer(text):
        if match.start()>start:yield text[start:match.start()]
        yield match.group();start=match.end()
    if start<len(text):yield text[start:]


def _chars(artifact, decision, runs=False, chunk_bytes=65536):
    encoding = decision.encoding
    bom = decision.evidence.get("bom_bytes", 0)
    decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    position, consumed = bom, bom
    with open(artifact.path, "rb") as source:
        source.seek(bom)
        while True:
            from ..resources import checkpoint
            checkpoint()
            block = source.read(chunk_bytes)
            buffered_bytes = decoder.getstate()[0]
            buffered = len(buffered_bytes)
            try:
                text = decoder.decode(block, final=not block)
            except UnicodeDecodeError as error:
                raise IngestError("ENCODING_INVALID", "Strict decoding failed in captured bytes", context={"byte_start": consumed - buffered + error.start, "byte_end": consumed - buffered + error.end, "encoding": encoding}) from error
            if encoding in _MULTIBYTE_ENCODINGS:
                pending = decoder.getstate()[0]
                represented = buffered_bytes + block
                if pending:represented = represented[:-len(pending)]
                if text.encode(encoding, errors="strict") != represented:
                    raise IngestError("ENCODING_INVALID", "Noncanonical multibyte sequence cannot prove reversible byte spans; captured original retained", context={"encoding":encoding,"byte_start":consumed-buffered,"byte_end":consumed+len(block)-len(pending),"reason":"noncanonical byte mapping"})
            consumed += len(block)
            for char in (_tokens(text,decision) if runs else text):
                end = position + len(char.encode(encoding, errors="strict"))
                yield char, position, end
                position = end
            if not block:
                break


def _field_values(text, decision, settings, nel_terminated=False):
    # Only an already framed terminal NEL becomes a csv.reader terminator.
    # Embedded quoted or escaped NEL remains the original Unicode value.
    if nel_terminated:
        text = text[:-1] + "\n"
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=decision.delimiter, quotechar=decision.quotechar, escapechar=decision.escapechar, doublequote=decision.doublequote, strict=True, quoting=csv.QUOTE_MINIMAL if decision.quotechar else csv.QUOTE_NONE)
    row = next(reader)
    quoted, state, escaped = [], "start", False
    for char in _tokens(text,decision):
        if len(char)>1:
            if state=="start":quoted.append(False)
            if state!="quoted":state="unquoted"
            escaped=False
            continue
        if state == "start":
            quoted.append(bool(decision.quotechar and char == decision.quotechar))
            state = "quoted" if quoted[-1] else "unquoted"
            if state == "quoted":
                continue
        if escaped:
            escaped = False
            continue
        if decision.escapechar and char == decision.escapechar:
            escaped = True
            continue
        if state == "quoted":
            if char == decision.quotechar:
                state = "closed"
        elif state == "closed" and char == decision.quotechar and decision.doublequote:
            state = "quoted"
        elif char == decision.delimiter:
            state = "start"
        elif char in "\r\n":
            break
        else:
            state = "unquoted"
    if len(quoted) < len(row):
        quoted.extend([False] * (len(row) - len(quoted)))
    tokens = settings.get("null_tokens", [])
    sensitive = settings.get("null_tokens_case_sensitive", True)
    token_set = set(tokens if sensitive else [token.casefold() for token in tokens])
    fields = []
    for ordinal, value in enumerate(row):
        if settings.get("trim_whitespace", False):
            value = value.strip()
        is_quoted = quoted[ordinal]
        match = value if sensitive else value.casefold()
        null = (not is_quoted or settings.get("null_tokens_in_quoted_fields", False)) and match in token_set
        null = null or (value == "" and settings.get("empty_string_is_null", False))
        state = "NULL" if null else "EMPTY" if value == "" else "VALUE"
        fields.append(FieldValue(state, None if null and not settings.get("retain_null_lexical",False) else value, is_quoted))
    return fields


def parse(artifact, decision, config):
    from .token_delimited import alternate, parse as parse_tokens
    if alternate(decision,config.get("parsing",{})):
        yield from parse_tokens(artifact,decision,config)
        return
    settings, limits = dict(config.get("parsing", {})), config.get("limits", {})
    if any("null_tokens" in c for c in config.get("columns",[])):settings["retain_null_lexical"]=True
    record_limit = limits.get("max_in_memory_record_bytes", 8388608)
    field_limit = limits.get("max_in_memory_field_bytes", record_limit)
    # csv.reader has a global field limit; bounded framing enforces the local byte
    # limit before invoking it. This is a finite bound rather than unlimited CSV.
    csv.field_size_limit(max(csv.field_size_limit(), field_limit))
    from dataclasses import asdict
    identity_settings={k:v for k,v in settings.items() if not (k=="column_names" and not v or k=="nel_is_newline" and not v or k=="record_terminator" and v=="auto" or k=="allow_multiline_fields" and v is True)}
    policy_content={"decision":asdict(decision),"parsing":identity_settings}
    if config.get("columns"):policy_content["column_policies"]=config["columns"]
    policy=hashlib.sha256(json.dumps(policy_content,sort_keys=True,separators=(",",":")).encode()).hexdigest()
    start = decision.evidence.get("bom_bytes", 0)
    ordinal, line, line_start = 0, 1, 1
    state, escaped, pending_cr = "start", False, False
    previous_cr = False
    last_character = ""
    newlines = "\r\n" + ("\x85" if decision.evidence.get("nel_is_newline", False) else "")
    buffer = io.StringIO()
    oversized, field_bytes, field_count, expected = False, 0, 1, len(settings.get("column_names", [])) or None

    def envelope(end, unresolved=False, nel_terminated=False):
        nonlocal ordinal, expected
        ordinal += 1
        record_id = hashlib.sha256(f"{artifact.capture_id}:{artifact.sha256}:{ordinal}:{policy}".encode()).hexdigest()
        errors, fields = [], []
        raw = buffer.getvalue() if not oversized and field_count <= 1024 else ""
        if unresolved:
            kind = "unresolved"
            errors.append({"code": "FRAMING_UNRESOLVED", "message": "Unclosed quoted field; logical count for this tail is unknown"})
        elif oversized or field_count > 1024:
            kind = "rejected"
            errors.append({"code": "RECORD_LIMIT", "message": "Record/field/column-count budget exceeded; exact bytes retained in artifact", "byte_start": start, "byte_end": end})
        elif raw.rstrip(newlines) == "":
            kind = "blank"
            policy_blank = settings.get("blank_record_policy", "quarantine")
            if policy_blank == "explicitly_skip":
                kind = "excluded"
                errors.append({"code": "BLANK_EXPLICITLY_EXCLUDED", "message": "Blank record intentionally excluded under declared policy; exact bytes retained"})
            elif policy_blank == "preserve":
                kind = "rejected"
                errors.append({"code": "BLANK_PRESERVED_UNIMPORTED", "message": "Blank material preserved but has no relational row representation"})
            else:
                kind = "rejected"
                errors.append({"code": "BLANK_RECORD", "message": "Blank record quarantined under declared policy"})
        else:
            try:
                is_header = ordinal == 1 and decision.header == "present"
                fields = _field_values(raw, decision, {} if is_header else settings, nel_terminated=nel_terminated)
                kind = "header" if is_header else "data"
                if expected is None:
                    expected = len(fields)
                elif kind == "data" and len(fields) != expected:
                    kind = "rejected"
                    errors.append({"code": "ROW_WIDTH", "message": "Field count differs from declared header/first record", "expected": expected, "observed": len(fields)})
            except (csv.Error, StopIteration) as error:
                kind = "rejected"
                errors.append({"code": "CSV_INVALID", "message": str(error)})
        last_line = line - 1 if last_character in newlines else line
        return RecordEnvelope(record_id, ordinal, start, end, line_start, max(line_start, last_line), fields, kind, errors)

    try:
        for char, byte_start, byte_end in _chars(artifact, decision, runs=True):
            # Delay CR publication so a following LF belongs to the same span.
            if pending_cr and char != "\n":
                yield envelope(byte_start)
                start, line_start = byte_start, line
                buffer = io.StringIO()
                state, escaped, oversized, field_bytes, field_count = "start", False, False, 0, 1
                pending_cr = False
            if len(char)>1:
                # A run has no quote/delimiter/escape/newline. The first ordinary
                # character clears a pending escape; subsequent ones leave only
                # quoted/unquoted state. All byte caps are checked before retain.
                last_character=char[-1]
                field_bytes+=byte_end-byte_start
                if not oversized and (byte_end-start>record_limit or field_bytes>field_limit or field_count>1024):
                    oversized=True;buffer=io.StringIO()
                elif not oversized:buffer.write(char)
                if state!="quoted":state="unquoted"
                escaped=False;previous_cr=False
                continue
            last_character = char
            size = byte_end - start
            field_bytes += byte_end - byte_start
            if not oversized and (size > record_limit or field_bytes > field_limit or field_count > 1024):
                oversized = True
                buffer = io.StringIO()
            elif not oversized:
                buffer.write(char)
            if char == "\r":
                line += 1
            elif char == "\x85" and "\x85" in newlines:
                line += 1
            elif char == "\n" and not previous_cr:
                line += 1
            previous_cr = char == "\r"
            if pending_cr and char == "\n":
                yield envelope(byte_end)
                start, line_start = byte_end, line
                buffer = io.StringIO()
                state, escaped, pending_cr, oversized, field_bytes, field_count = "start", False, False, False, 0, 1
                continue
            if escaped:
                escaped = False
                continue
            if decision.escapechar and char == decision.escapechar:
                escaped = True
                continue
            if state == "start":
                if decision.quotechar and char == decision.quotechar:
                    state = "quoted"
                    continue
                state = "unquoted"
            if state == "quoted":
                if char == decision.quotechar:
                    state = "closed"
                continue
            if state == "closed" and char == decision.quotechar and decision.doublequote:
                state = "quoted"
                continue
            if char == decision.delimiter:
                state, field_bytes = "start", 0
                field_count += 1
            elif char == "\r":
                pending_cr = True
            elif char == "\n" or char == "\x85" and "\x85" in newlines:
                yield envelope(byte_end, nel_terminated=char == "\x85")
                start, line_start = byte_end, line
                buffer = io.StringIO()
                state, escaped, oversized, field_bytes, field_count = "start", False, False, 0, 1
            else:
                state = "unquoted"
    except IngestError as error:
        if error.code != "ENCODING_INVALID":
            raise
        record = envelope(artifact.size, unresolved=True)
        record.errors.append({"code": error.code, "message": str(error), **error.context})
        record.line_end = None  # Undecoded bytes cannot establish the tail physical-line total.
        yield record
        return
    if start < artifact.size:
        yield envelope(artifact.size, unresolved=(state == "quoted" or escaped))
