"""Shared bounded lexical preflight for narrowly declared XML/HTML adapters."""
import hashlib
import io
from dataclasses import asdict
from ..errors import IngestError
from ..models import FieldValue, RecordEnvelope
from .delimited import _chars


def guarded_chunks(artifact, decision, config):
    """Bound parser tokens before a library can buffer an enormous open token.

    This preflight occurs before feeding the XML/HTML parser. Its independent
    decode/framing is conservative; rejected markup stays in the byte archive.
    """
    limit = min(config.get("limits", {}).get("max_in_memory_record_bytes", 8388608), config.get("limits", {}).get("max_in_memory_field_bytes", 8388608))
    token_limit = min(limit, 65536)
    token, state, quote, size, tail = "", "text", None, 0, ""
    for char, start, end in _chars(artifact, decision):
        if state == "text":
            if char == "<":
                state, token, size, quote, tail = "markup", "<", end - start, None, "<"
            else:
                size += end - start
        else:
            size += end - start
            if len(token) < 16:
                token += char
            upper = token.upper()
            if (decision.evidence.get("adapter") == "xml" and upper.startswith("<!DOCTYPE")) or upper.startswith("<!ENTITY"):
                raise IngestError("XML_DTD_FORBIDDEN", "DTD/entity declarations are forbidden; captured markup remains intact", context={"byte_offset": start})
            tail = (tail + char)[-3:]
            if token.startswith("<!--"):
                ended = tail == "-->"
            elif token.startswith("<![CDATA["):
                ended = tail == "]]>"
            elif token.startswith("<?"):
                ended = tail.endswith("?>")
            else:
                if quote:
                    if char == quote:
                        quote = None
                elif char in "\"'":
                    quote = char
                ended = char == ">" and quote is None
            if ended:
                state, size, token, tail = "text", 0, "", ""
        if size > (token_limit if state == "markup" else limit):
            raise IngestError("MARKUP_TOKEN_LIMIT", "Markup token/text exceeds the bounded adapter limit; original preserved", context={"byte_offset": start, "limit_bytes": limit})
    if state != "text":
        raise IngestError("MARKUP_UNCLOSED", "Markup ends inside an unfinished token; original preserved")
    # A second bounded decode pass feeds library adapters only after preflight.
    text = io.StringIO()
    count = 0
    for char, _, _ in _chars(artifact, decision):
        text.write(char)
        count += 1
        if count >= 8192:
            yield text.getvalue()
            text, count = io.StringIO(), 0
    if count:
        yield text.getvalue()


def fields_from_values(values, config):
    settings = config.get("parsing", {})
    tokens = settings.get("null_tokens", [])
    sensitive = settings.get("null_tokens_case_sensitive", True)
    tokens = set(tokens if sensitive else [v.casefold() for v in tokens])
    result = []
    for value in values:
        if value is None:
            result.append(FieldValue("MISSING", None))
            continue
        if settings.get("trim_whitespace", False):
            value = value.strip()
        is_null = (value if sensitive else value.casefold()) in tokens or (value == "" and settings.get("empty_string_is_null", False))
        result.append(FieldValue("NULL" if is_null else "EMPTY" if value == "" else "VALUE", None if is_null and not any("null_tokens" in c for c in config.get("columns",[])) else value))
    return result


def envelope(artifact, decision, ordinal, fields, kind="data", errors=None):
    import json
    policy = hashlib.sha256(json.dumps(asdict(decision), sort_keys=True).encode()).hexdigest()
    identity = hashlib.sha256(f"{artifact.capture_id}:{artifact.sha256}:{ordinal}:{policy}".encode()).hexdigest()
    return RecordEnvelope(identity, ordinal, None, None, None, None, fields, kind, errors or [])


def declared_header(artifact, decision, names):
    # This is schema metadata, not a claimed source-byte header record.
    return envelope(artifact, decision, 0, [FieldValue("VALUE" if name else "EMPTY", name) for name in names], "header")
