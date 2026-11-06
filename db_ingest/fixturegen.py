"""Independent, authored fixture/oracle format. Never imports importer parsing or typing code."""
from __future__ import annotations

import base64
import hashlib
import json
import random
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable

from .errors import IngestError

CATEGORIES = (
    "csv", "tsv", "non_standard_delimiters", "csv_bad_format", "csv_no_header",
    "csv_duplicate_headers", "csv_multiline", "csv_unicode", "csv_with_blobs",
    "csv_no_blobs", "csv_nulls_defaults", "csv_type_boundaries", "csv_schema_drift",
    "csv_escaping", "csv_dirty", "csv_legacy_encodings", "mainframe_ebcdic",
    "csv_wide", "csv_large_record", "csv_large_file", "unknown_extension", "xml", "html", "vertical_slice",
)


def _hash(path: Path) -> tuple[int, str]:
    total, digest = 0, hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
            total += len(chunk)
    return total, digest.hexdigest()


def _safe(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise IngestError("FIXTURE_PATH_ESCAPE", "Manifest path escapes its fixture directory")
    return path


def _cell(value, kind="text"):
    return {"type": kind, "value": value}


def _row(values, ordinal):
    return {"ordinal": ordinal, "values": [v if isinstance(v, dict) else _cell(v) for v in values]}


def _physical_lines(path: Path):
    # Count CRLF once, bare CR/LF once, plus a final unterminated physical line.
    count, pending_cr, any_bytes, last_break = 0, False, False, False
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            for value in chunk:
                any_bytes = True
                if value == 10 and pending_cr:
                    pending_cr, last_break = False, True
                    continue
                if value in (10, 13):
                    count += 1
                    last_break = True
                else:
                    last_break = False
                pending_cr = value == 13
    return count + int(any_bytes and not last_break)


def create_test_files(output, types=("all",), destination="mssql", seed=20261029,
                      rows=1000, encoding="utf-8", delimiter=",", size_bytes=None):
    """Write a reproducible fixture set, with streaming expected-row sidecars.

    Types support repeated/comma-separated values, with hyphen and underscore aliases.
    Existing generated paths are never overwritten. size_bytes controls large fixtures;
    the default oversized record is 10 MiB and large file has `rows` data records.
    """
    if destination != "mssql":
        raise IngestError("UNAVAILABLE_ADAPTER", f"No fixture oracle for destination {destination!r}")
    if rows < 0 or size_bytes is not None and size_bytes < 1:
        raise IngestError("FIXTURE_SIZE", "rows must be nonnegative; size_bytes must be positive")
    if len(delimiter) != 1 or delimiter in '\r\n"':
        raise IngestError("FIXTURE_DELIMITER", "Use one non-newline/non-quote delimiter")
    names = []
    for item in ([types] if isinstance(types, str) else types):
        for part in item.split(","):
            name = part.strip().replace("-", "_")
            if name == "all":
                names.extend(CATEGORIES)
            elif name in CATEGORIES:
                names.append(name)
            else:
                raise IngestError("FIXTURE_TYPE", f"Unknown fixture category {part!r}", context={"available": CATEGORIES})
    names = list(dict.fromkeys(names))
    if not names:
        raise IngestError("FIXTURE_TYPE", "Select at least one fixture category")
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "manifest.json").exists():
        raise IngestError("FIXTURE_EXISTS", "Refusing to overwrite an existing fixture manifest")
    rng = random.Random(seed)
    fixtures = []
    all_created = []

    def reserve(name):
        path = _safe(root, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raise IngestError("FIXTURE_EXISTS", f"Refusing to overwrite fixture {name}")
        all_created.append(name)
        return path

    def add(name, content, expected=(), *, category, sep=",", header="present", enc=encoding,
            outcome="parsed", rejected=(), unknown=False, schema=None, notes=None, overrides=None, reason_codes=None):
        target = reserve(name)
        if callable(content):
            with target.open("wb") as f:
                content(f)
        else:
            target.write_bytes(content.encode(enc) if isinstance(content, str) else content)
        exp_name = name + ".expected.jsonl"
        n = 0
        authored_columns = None
        first_accepted = last_accepted = None
        with reserve(exp_name).open("w", encoding="utf-8", newline="\n") as f:
            for n, record in enumerate(expected, 1):
                if isinstance(record, dict):
                    value = record
                else:
                    value = _row(record, n + int(header == "present" and category not in ("xml","html")))
                if authored_columns is None:
                    authored_columns = value["values"]
                if first_accepted is None:first_accepted=value["ordinal"]
                last_accepted=value["ordinal"]
                f.write(json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n")
        # These declarations are derived only from authored cell tags, never inference.
        # Empty/invalid files have explicit authored widths below.
        width = len(authored_columns) if authored_columns is not None else 2 if name != "entity.xml" else 1
        types = {"integer":"bigint", "decimal":"decimal(38,4)", "date":"date", "datetime2":"datetime2(7)", "datetimeoffset":"datetimeoffset(7)", "guid":"uniqueidentifier", "binary_hex":"varbinary(max)"}
        complete_schema = []
        for ordinal in range(1, width+1):
            tag = authored_columns[ordinal-1]["type"] if authored_columns else "integer" if ordinal == 1 and name not in ("entity.xml","span.html") else "text"
            declaration = {"source_ordinal":ordinal,"target_name":"fixture_c"+str(ordinal),"target_type":types.get(tag,"nvarchar(max)"),"nullable":True}
            if tag == "binary_hex":declaration["binary_encoding"]="base64"
            declaration.update(next((c for c in schema or [] if c["source_ordinal"]==ordinal),{}))
            complete_schema.append(declaration)
        reasons = {"rejected":["ROW_WIDTH"],"type_failure":["VALUE_INVALID"],"oversized":["RECORD_LIMIT"],"unresolved":["FRAMING_UNRESOLVED"],"encoding_failure":["ENCODING_DECISION","ENCODING_INVALID"],"unsupported_unsafe_xml":["XML_DTD_FORBIDDEN"],"unsupported_html_span":["HTML_SPAN_UNSUPPORTED"],"header_only":["NO_DATA"]}
        if reason_codes is not None:reasons[outcome]=reason_codes
        outcomes = ([{"ordinal":0,"outcome":"mapping_header","reason":"declared ordered scalar mapping; not a source data record"}] if category in ("xml","html") else [{"ordinal":1,"outcome":"header","reason":"declared first header"}] if header=="present" else [])
        if n:outcomes.append({"ordinal_range":[first_accepted,last_accepted],"count":n,"outcome":"accepted","reason":"authored exact ordered values in streamed expected_path"})
        outcomes += [{"ordinal":o,"outcome":"rejected","reason_codes":reasons.get(outcome,[]),"reason":"authored invalid representation"} for o in rejected]
        if unknown:outcomes.append({"ordinal":None,"outcome":"unresolved" if outcome=="unresolved" else "file_failure","reason_codes":reasons.get(outcome,[]),"reason":"boundaries/count remain unknown; original bytes retained"})
        length, digest = _hash(target)
        fixtures.append({"path": name, "category": category, "size": length, "sha256": digest,
                         "adapter": "xml" if category=="xml" else "html" if category=="html" else "delimited", "ordinal_basis":"selected data records start at1; declared mapping header0" if category in ("xml","html") else "source logical records including declared header", "encoding": enc, "delimiter": sep, "quotechar": '"', "header": header,
                         "expected_path": exp_name, "expected_rows": n,
                         "expected_outcome": outcome, "rejected_ordinals": list(rejected),
                         "logical_records": None if unknown else n + len(rejected) + int(header == "present"),
                         "physical_lines": _physical_lines(target) if enc in ("utf-8", "ascii", "cp1252") else None,
                         "sql_schema": complete_schema, "expected_record_outcomes": outcomes, "expected_reason_codes": reasons.get(outcome,[]), "notes": notes or [], "overrides": overrides or {}})

    for category in names:
        if category in ("csv_escaping", "csv_dirty", "csv_legacy_encodings", "mainframe_ebcdic"):
            from .legacy_examples import examples
            for example in examples(category):
                name=example.pop("name"); content=example.pop("content"); expected=example.pop("expected")
                add(name, content, expected, category=category, **example)
        elif category == "vertical_slice":
            row = '001,"\U0001f600 trailing  ","one,\r\ntwo",NULL,"","NULL",123456789012345.1234,2024-02-29,2026-10-29T12:34:56.1234567,2026-10-29T12:34:56.1234567+05:30,AAEC/w==\r\n'
            content = 'Code,Text,Text,,Empty,Literal,Amount,OnDate,OccurredAt,OffsetAt,Attachment\r\n' + row + row
            values = ["001", "\U0001f600 trailing  ", "one,\r\ntwo", _cell(None,"null"), "", "NULL", _cell("123456789012345.1234","decimal"), _cell("2024-02-29","date"), _cell("2026-10-29T12:34:56.1234567","datetime2"), _cell("2026-10-29T12:34:56.1234567+05:30","datetimeoffset"), _cell("000102ff","binary_hex")]
            declarations=[("code","nvarchar(10)"),("text1","nvarchar(80)"),("text2","nvarchar(80)"),("null_value","nvarchar(20)"),("empty","nvarchar(10)"),("literal","nvarchar(10)"),("amount","decimal(19,4)"),("on_date","date"),("occurred_at","datetime2(7)"),("offset_at","datetimeoffset(7)"),("attachment","varbinary(max)")]
            schema=[{"source_ordinal":i+1,"target_name":name,"target_type":kind,"nullable":True,**({"binary_encoding":"base64"} if kind=="varbinary(max)" else {})} for i,(name,kind) in enumerate(declarations)]
            reserve("slice_blob.bin").write_bytes(b'\x00\x01\x02\xff')
            add("vertical_slice.csv",content,[values,values],category=category,schema=schema,overrides={"null_tokens":["NULL"],"null_tokens_in_quoted_fields":False},notes=["Two identical data rows have distinct source ordinals. Full SQL verification needs explicit mappings and destination/run."])
        elif category in ("csv", "csv_no_blobs"):
            line = f'id{delimiter}name\r\n1{delimiter}Alice\r\n2{delimiter}Bob\r\n'
            add(category + ".csv", line, [[_cell("1", "integer"), "Alice"], [_cell("2", "integer"), "Bob"]], category=category, sep=delimiter)
        elif category == "tsv":
            add("tabbed.tsv", 'id\ttext\n1\t"tab\tinside"\n', [[_cell("1", "integer"), "tab\tinside"]], category=category, sep="\t")
        elif category == "non_standard_delimiters":
            for sep, name in [("|", "pipe"), (";", "semicolon"), ("~", "custom")]:
                add(name + ".txt", f'id{sep}text\r1{sep}"has{sep}separator"\r', [[_cell("1", "integer"), f"has{sep}separator"]], category=category, sep=sep)
        elif category == "csv_bad_format":
            add("unclosed.csv", 'id,text\n1,good\n2,"unknown\nremainder', [[_cell("1", "integer"), "good"]], category=category, outcome="unresolved", unknown=True, notes=["Tail begins at byte 15; do not fabricate its logical row count."])
            add("ragged.csv", 'a,b\n1,ok\n2\n3,bad,extra\n', [[_cell("1", "integer"), "ok"]], category=category, outcome="rejected", rejected=[3, 4])
            add("invalid_utf8.csv", b'a,b\n1,\xff\n', [], category=category, enc="utf-8", outcome="encoding_failure", unknown=True)
        elif category == "csv_no_header":
            add("no_header.csv", 'alpha,beta\ngamma,delta\n', [["alpha", "beta"], ["gamma", "delta"]], category=category, header="absent")
        elif category == "csv_duplicate_headers":
            add("duplicate.csv", 'name,name,,_ingest_record_id\nA,B,C,D\n', [["A", "B", "C", "D"]], category=category, notes=["All four ordered fields survive duplicate/blank/reserved labels."])
        elif category == "csv_multiline":
            add("multiline.csv", 'id,text\r\n1,"one\r\ntwo,three"\r\n2,"He said ""hello"""\n', [[_cell("1", "integer"), "one\r\ntwo,three"], [_cell("2", "integer"), 'He said "hello"']], category=category)
        elif category == "csv_unicode":
            add("unicode.csv", 'id,text\n001,"\U0001f600 e\u0301 \u4e2d\u6587  "\n001,"\U0001f600 e\u0301 \u4e2d\u6587  "\n', [["001", "\U0001f600 e\u0301 \u4e2d\u6587  "], ["001", "\U0001f600 e\u0301 \u4e2d\u6587  "]], category=category)
            add("utf8_bom.csv", b'\xef\xbb\xbfa,b\r\n1,ok\r\n', [[_cell("1", "integer"), "ok"]], category=category, enc="utf-8-sig")
            add("utf16.csv", 'a,b\n1,\u03a9\n'.encode("utf-16"), [[_cell("1", "integer"), "\u03a9"]], category=category, enc="utf-16")
        elif category == "csv_with_blobs":
            binary = bytes(range(256))
            side = reserve("all_bytes.bin")
            side.write_bytes(binary)
            add("blobs.csv", 'id,payload\n1,' + base64.b64encode(binary).decode("ascii") + '\n2,\n', [[_cell("1", "integer"), _cell(binary.hex(), "binary_hex")], [_cell("2", "integer"), _cell("", "binary_hex")]], category=category, schema=[{"source_ordinal": 2, "target_type": "varbinary(max)", "binary_encoding": "base64"}], notes=["all_bytes.bin independently contains bytes 0..255."])
            add("invalid_blob.csv", 'id,payload\n1,%%%\n', [], category=category, outcome="type_failure", rejected=[2], schema=[{"source_ordinal": 2, "target_type": "varbinary(max)", "binary_encoding": "base64"}])
        elif category == "csv_nulls_defaults":
            add("nulls.csv", 'a,b,c,d\n,"",NULL,"NULL"\n', [["", "", _cell(None, "null"), "NULL"]], category=category, overrides={"null_tokens": ["NULL"], "null_tokens_in_quoted_fields": False}, notes=["Explicit SQL NULL differs from empty and quoted NULL; no default is inferred."])
        elif category == "csv_type_boundaries":
            values = ["9223372036854775807", "9223372036854775808", "9" * 38, "9" * 39]
            add("integer_boundaries.csv", 'value\n' + '\n'.join(values) + '\n', [[v] for v in values], category=category, notes=["Mixed beyond-38-digit column must remain text, without rounding."])
            add("exact_types.csv", 'amount,date,naive,offset,guid\n123456789012345.1234,2024-02-29,2026-10-29T12:34:56.1234567,2026-10-29T12:34:56.1234567+05:30,01234567-89ab-cdef-0123-456789abcdef\n', [[_cell("123456789012345.1234", "decimal"), _cell("2024-02-29", "date"), _cell("2026-10-29T12:34:56.1234567", "datetime2"), _cell("2026-10-29T12:34:56.1234567+05:30", "datetimeoffset"), _cell("01234567-89ab-cdef-0123-456789abcdef", "guid")]], category=category)
        elif category == "csv_schema_drift":
            add("drift_v1.csv", 'id,text\n1,short\n', [[_cell("1", "integer"), "short"]], category=category)
            add("drift_v2.csv", 'text,id,new\na much longer value,2,extra\n', [["a much longer value", _cell("2", "integer"), "extra"]], category=category, notes=["Requires explicit name mapping/schema evolution; do not append positionally."])
        elif category == "csv_wide":
            width = 256
            add("wide.csv", ','.join(f'c{i}' for i in range(width)) + '\n' + ','.join('x' for _ in range(width)) + '\n', [["x"] * width], category=category)
        elif category == "csv_large_record":
            count = size_bytes or 10 * 1024 * 1024
            def huge(f):
                f.write(b'id,text\n1,"')
                remaining = count
                while remaining:
                    take = min(remaining, 65536)
                    f.write(b'x' * take)
                    remaining -= take
                f.write(b'"\n')
            add("large_record.csv", huge, [] if count>8388600 else [[_cell("1","integer"),"x"*count]], category=category, outcome="oversized" if count>8388600 else "parsed", rejected=[2] if count>8388600 else [], notes=[f"Field has exactly {count} ASCII bytes. Expected bounded quarantine if over configured cap."])
        elif category == "csv_large_file":
            # Formula, not importer output, is the oracle. Expected sidecar is streamed too.
            salt = rng.randrange(1000000)
            count = rows
            if size_bytes:
                count = max(rows, (size_bytes + 63) // 64)
            def large(f):
                f.write(b'id,text\n')
                for i in range(count):
                    f.write((str(i) + ',' + f'row-{salt}-{i}-' + 'x' * 48 + '\n').encode("ascii"))
            expected = ((_cell(str(i), "integer"), f'row-{salt}-{i}-' + 'x' * 48) for i in range(count))
            add("large_file.csv", large, expected, category=category, enc="ascii",outcome="parsed" if count else "header_only",notes=["Zero generated data rows require explicit NO_DATA decision; no publication inferred."] if not count else [])
        elif category == "unknown_extension":
            add("legacy.xyz", 'id|text\n1|legacy\n', [[_cell("1", "integer"), "legacy"]], category=category, sep="|")
            add("extensionless", 'id;text\n1;plain\n', [[_cell("1", "integer"), "plain"]], category=category, sep=";")
        elif category == "xml":
            add("flat.xml", '<?xml version="1.0"?><rows><row id="1"><name>A &amp; B</name></row></rows>', [["1", "A & B"]], category=category, header="absent", overrides={"format":"xml","xml":{"enabled":True,"record_selector":"row","fields":[{"name":"id","path":"@id"},{"name":"name","path":"name"}]}}, notes=["Record selector row; id attribute and name child are scalar mappings."])
            add("entity.xml", '<!DOCTYPE x [<!ENTITY leak SYSTEM "file:///unavailable">]><rows><row>&leak;</row></rows>', [], category=category, header="absent", outcome="unsupported_unsafe_xml", unknown=True, overrides={"format":"xml","xml":{"enabled":True,"record_selector":"row","fields":[{"name":"value","path":"value"}]}})
        elif category == "html":
            add("simple.html", '<table id="data"><tr><th>id</th><th>text</th></tr><tr><td>1</td><td>A &amp; B</td></tr></table>', [[_cell("1", "integer"), "A & B"]], category=category, overrides={"format":"html","html":{"enabled":True,"table_selector":"0","fields":["id","text"]}}, notes=["Table #data; textContent extraction; declared first row header."])
            add("span.html", '<table><tr><td colspan="2">ambiguous</td></tr></table>', [], category=category, header="absent", outcome="unsupported_html_span", unknown=True, overrides={"format":"html","html":{"enabled":True,"table_selector":"0","fields":["value","extra"]}})

    manifest = {"format": "db_ingest_fixture_manifest", "version": 1, "seed": seed,
                "destination": destination, "oracle": "authored/formula; independent of importer",
                "fixtures": fixtures, "sidecars": []}
    for name in all_created:
        if name.endswith(".bin"):
            length, digest = _hash(root / name)
            manifest["sidecars"].append({"path": name, "size": length, "sha256": digest})
    # UTF-8 byte hash/length alone is authoritative for source bytes; physical line count is auxiliary.
    temporary = root / "manifest.json.partial"
    temporary.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(root / "manifest.json")
    return manifest


def _equivalent(actual, expected):
    kind, value = expected["type"], expected["value"]
    if kind == "null":
        return actual is None
    if kind == "binary_hex":
        return isinstance(actual, (bytes, bytearray, memoryview)) and bytes(actual) == bytes.fromhex(value)
    if kind == "integer":
        return not isinstance(actual, (float, bool)) and isinstance(actual, (int, Decimal)) and Decimal(actual) == Decimal(value)
    if kind == "decimal":
        return isinstance(actual, Decimal) and actual == Decimal(value)
    if kind == "guid":
        return str(actual).lower() == value.lower()
    if kind in ("datetime2", "datetimeoffset"):
        # Read these from SQL as exact style-126/127 text. Python datetime loses digit 7.
        return isinstance(actual, str) and actual.replace(" ", "T", 1) == value
    if kind == "date":
        return (actual.isoformat() if isinstance(actual, date) and not isinstance(actual, datetime) else actual) == value
    return isinstance(actual, str) and actual == value


def verify_fixture_outcome(fixture, item, rows, file_codes=()):
    """Compare retained envelopes/dispositions to authored outcomes, never parse bytes.

    rows streams (ordinal, disposition, envelope_dict); accepted SQL values are checked
    separately against expected_path. No successful publication is invented for a file
    whose framing/encoding/structure is intentionally unresolved.
    """
    supported={'parsed','rejected','type_failure','oversized','unresolved','encoding_failure','unsupported_unsafe_xml','unsupported_html_span','unsupported_format','header_only'}
    if fixture.get('expected_outcome') not in supported or not fixture.get('expected_record_outcomes') or not fixture.get('sql_schema') or (fixture['expected_outcome']!='parsed' and not fixture.get('expected_reason_codes')):
        return {'path':fixture['path'],'outcome':fixture.get('expected_outcome'),'errors':[{'code':'FIXTURE_EXPECTATION_UNSUPPORTED','message':'Complete authored schema/outcomes/reasons are required; unknown outcomes are never certified'}]}
    expected_rejects=set(fixture.get("rejected_ordinals",[]));seen_rejects=set()
    accepted=unresolved=headers=0;codes=set(file_codes);errors=[]
    outcome=fixture["expected_outcome"]
    blocked=outcome in ("unresolved","encoding_failure","unsupported_unsafe_xml","unsupported_html_span","unsupported_format","header_only")
    structured=fixture.get("adapter") in ("xml","html")
    first=1+int(fixture['header']=='present' and not structured);next_accepted=first
    for ordinal,disposition,payload in rows:
        record_codes={e.get('code') for e in payload.get('errors',[])};codes.update(record_codes)
        if payload.get('ordinal')!=ordinal:
            errors.append({'code':'FIXTURE_RECORD_IDENTITY','ordinal':ordinal});continue
        if disposition=='header':
            headers+=1
            if (ordinal!=0 if structured else fixture['header']!='present' or ordinal!=1):errors.append({'code':'FIXTURE_HEADER_OUTCOME','ordinal':ordinal})
        elif ordinal in expected_rejects:
            seen_rejects.add(ordinal)
            if disposition!='rejected' or not record_codes.intersection(fixture.get('expected_reason_codes',[])):
                errors.append({'code':'FIXTURE_REJECT_OUTCOME','ordinal':ordinal,'observed_disposition':disposition,'observed_codes':sorted(record_codes)})
        elif disposition=='unresolved':
            unresolved+=1
            if outcome!='unresolved' or 'FRAMING_UNRESOLVED' not in record_codes:errors.append({'code':'FIXTURE_UNRESOLVED_OUTCOME','ordinal':ordinal})
        elif disposition in ('pending','committed'):
            while next_accepted in expected_rejects:next_accepted+=1
            accepted+=1
            if ordinal!=next_accepted:errors.append({'code':'FIXTURE_ACCEPTED_ORDINAL','ordinal':ordinal,'expected':next_accepted})
            next_accepted=ordinal+1
            if blocked and disposition=='committed':errors.append({'code':'FIXTURE_UNSAFE_PUBLICATION','ordinal':ordinal})
        else:errors.append({'code':'FIXTURE_UNEXPECTED_RECORD','ordinal':ordinal,'disposition':disposition})
    if accepted!=fixture['expected_rows'] or seen_rejects!=expected_rejects:
        errors.append({'code':'FIXTURE_OUTCOME_COUNTS','accepted':accepted,'expected_accepted':fixture['expected_rows'],'rejected_ordinals':sorted(seen_rejects),'expected_rejected_ordinals':sorted(expected_rejects)})
    if outcome=='unresolved' and unresolved!=1:errors.append({'code':'FIXTURE_UNRESOLVED_MISSING'})
    if blocked and item.get('receipt'):errors.append({'code':'FIXTURE_UNSAFE_RECEIPT'})
    if fixture.get('expected_reason_codes') and not codes.intersection(fixture['expected_reason_codes']):
        errors.append({'code':'FIXTURE_REASON_MISSING','expected_any':fixture['expected_reason_codes'],'observed':sorted(c for c in codes if c)})
    # Encoding/unsafe-structure failure may happen before a header can be extracted.
    if outcome not in ('encoding_failure','unsupported_unsafe_xml','unsupported_html_span','unsupported_format') and headers!=int(structured or fixture['header']=='present'):
        errors.append({'code':'FIXTURE_HEADER_COUNT','observed':headers})
    return {'path':fixture['path'],'outcome':outcome,'accepted_records':accepted,'rejected_records':len(seen_rejects),'unresolved_spans':unresolved,'errors':errors}


def verify_test_files(manifest_path, captures=None, destination_reader: Callable | None = None,
                      run_id=None, database=None):
    """Independently compare archive bytes and, only explicitly, actual SQL values.

    captures maps fixture-relative source names to retained artifact paths. Reader must
    accept (fixture, run_id, database) and yield {'ordinal': source_ordinal, 'values': []}
    in ordinal order, read from that explicit SQL destination/run. It must not parse source
    CSV to synthesize destination rows. Receipts/dispositions remain separate run checks.
    """
    manifest_file = Path(manifest_path).resolve()
    root = manifest_file.parent
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    if manifest.get("format") != "db_ingest_fixture_manifest" or manifest.get("version") != 1:
        raise IngestError("FIXTURE_MANIFEST", "Unsupported fixture manifest version")
    if not manifest.get("fixtures") or len({f.get("path") for f in manifest["fixtures"]}) != len(manifest["fixtures"]):
        raise IngestError("FIXTURE_MANIFEST_EMPTY", "Fixture manifest must contain nonempty, distinct source paths")
    if destination_reader is not None and (not run_id or not database):
        raise IngestError("FIXTURE_DESTINATION_REQUIRED", "SQL fixture verification needs explicit run_id and database")
    if captures is not None and not isinstance(captures, dict):
        raise IngestError("FIXTURE_CAPTURES", "captures must map fixture-relative paths to artifact paths")
    checked, errors, sql_checked = [], [], 0
    for entry in [*manifest["fixtures"], *manifest.get("sidecars", [])]:
        relative = entry["path"]
        original = _safe(root, relative)
        path = Path(captures[relative]) if captures is not None and relative in captures else original
        if captures is not None and relative not in captures and "expected_path" in entry:
            errors.append({"path": relative, "code": "CAPTURE_MISSING"})
            continue
        try:
            size, digest = _hash(path)
            if (size, digest) != (entry["size"], entry["sha256"]):
                errors.append({"path": relative, "code": "SOURCE_BYTE_MISMATCH", "observed_size": size, "observed_sha256": digest})
            else:
                checked.append(relative)
        except OSError as exc:
            errors.append({"path": relative, "code": "SOURCE_UNREADABLE", "diagnostic": str(exc)})
            continue
        if destination_reader is not None and (entry.get("expected_outcome")=="parsed" or entry.get("expected_outcome")=="rejected" and entry.get("expected_rows",0)>0):
            actual = iter(destination_reader(entry, run_id, database))
            with _safe(root, entry["expected_path"]).open(encoding="utf-8") as expected_file:
                for line in expected_file:
                    expected = json.loads(line)
                    got = next(actual, None)
                    if got is None:
                        errors.append({"path": relative, "code": "DESTINATION_ROW_MISSING", "ordinal": expected["ordinal"]})
                        break
                    if got.get("ordinal") != expected["ordinal"] or len(got.get("values", [])) != len(expected["values"]) or not all(_equivalent(a, b) for a, b in zip(got.get("values", []), expected["values"])):
                        errors.append({"path": relative, "code": "DESTINATION_VALUE_MISMATCH", "ordinal": expected["ordinal"]})
                if next(actual, None) is not None:
                    errors.append({"path": relative, "code": "DESTINATION_EXTRA_ROWS"})
            sql_checked += 1
    return {"state": "MISMATCH" if errors else "VALUES_CHECKED_RECEIPTS_REQUIRED" if destination_reader else "SOURCE_ONLY_NOT_SQL_VERIFIED",
            "exit_code": 5 if errors else 3, "source_files_checked": len(checked),
            "destination_files_checked": sql_checked, "run_id": run_id, "database": database,
            "errors": errors, "sql_integration": "reader supplied; verify its connection provenance" if destination_reader else "NOT_EXECUTED",
            "note": "Source fixture checks are not import completion; receipts/dispositions require independent reconciliation."}



def verify_imported_fixtures(manifest_path, config, run_id, database, username=None, password=None):
    """Explicit disposable SQL verification using authored fixture values.

    This independent reader does not call importer conversion, inference, digest,
    publication readback or reconciliation functions. It reads exact temporal text
    directly to avoid Python's six-digit datetime conversion.
    """
    if not run_id or not database or not database.lower().startswith(("ingestfixture_", "dbingesttest_")):
        raise IngestError("FIXTURE_DESTINATION_REQUIRED", "Supply explicit run_id and a disposable database beginning IngestFixture_ or DbIngestTest_; production defaults are never used")
    from .journal import Journal
    from .adapters.mssql.connect import connect
    manifest_file = Path(manifest_path).resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    if manifest.get("format") != "db_ingest_fixture_manifest" or manifest.get("version") != 1:
        raise IngestError("FIXTURE_MANIFEST", "Unsupported fixture manifest version")
    if not manifest.get("fixtures") or len({f.get("path") for f in manifest["fixtures"]}) != len(manifest["fixtures"]):
        raise IngestError("FIXTURE_MANIFEST_EMPTY", "Fixture manifest must contain nonempty, distinct source paths")
    journal = Journal(config["preservation"]["work_directory"])
    captures, entries, accounting, pending_audit = {}, {}, [], False
    reduced_assurance = config.get("verification",{}).get("level","full")!="full"
    connection = None
    # Identifier encoding here is intentionally small; only parsed metadata names are used.
    def ident(value):
        if not isinstance(value,str) or not value or len(value)>128 or "\x00" in value:
            raise IngestError("FIXTURE_IDENTIFIER", "Invalid destination identifier in stored fixture plan")
        return "["+value.replace("]", "]]")+"]"
    try:
        journal.run(run_id)
        for item in journal.files(run_id):
            relative = item["artifact"]["relative_path"].replace("\\", "/")
            if relative in entries:
                raise IngestError("FIXTURE_RUN_AMBIGUOUS", "Run contains several captures for the same fixture path")
            if item["plan"] is not None and item["plan"]["database"] != database:
                raise IngestError("FIXTURE_RUN_DESTINATION", "Stored import plan does not match the explicitly selected disposable database")
            entries[relative] = item
            captures[relative] = item["artifact"]["path"]
        expected_paths = {entry["path"] for entry in manifest["fixtures"]}
        if set(entries) - expected_paths:
            raise IngestError("FIXTURE_RUN_SOURCE", "Selected run contains sources outside this fixture manifest")
        source_report = verify_test_files(manifest_path, captures=captures)
        if source_report["errors"]:
            return source_report
        outcome_checks=[]
        for fixture in manifest['fixtures']:
            item=entries[fixture['path']]
            rows=((row[0],row[1],json.loads(row[2])) for row in journal.db.execute('SELECT ordinal,disposition,payload FROM records WHERE run_id=? AND file_id=? ORDER BY ordinal',(run_id,item['file_id'])))
            codes=(row[0] for row in journal.db.execute('SELECT code FROM incidents WHERE run_id=? AND file_id=?',(run_id,item['file_id'])))
            checked=verify_fixture_outcome(fixture,item,rows,codes);outcome_checks.append(checked)
        outcome_errors=[{'path':check['path'],**error} for check in outcome_checks for error in check['errors']]
        if outcome_errors:return {**source_report,'state':'MISMATCH','exit_code':5,'outcome_checks':outcome_checks,'errors':outcome_errors}
        # No SQL connection is opened until local run/destination and capture checks pass.
        connection = connect(config, username, password, database=database)
        cursor = connection.cursor()
        observed = cursor.execute("SELECT DB_NAME()").fetchone()[0]
        if observed != database:
            raise IngestError("FIXTURE_RUN_DESTINATION", "Connected database differs from explicit fixture destination", 5)
        receipt_table = ident(config["audit"]["control_schema"])+".[__db_ingest_receipts]"
        def reader(fixture, selected_run, selected_database):
            nonlocal pending_audit,reduced_assurance
            item = entries[fixture["path"]]
            plan = item["plan"]
            receipt_row = cursor.execute("SELECT [payload],[target_object_id],CONVERT(nvarchar(36),[database_guid]) FROM "+receipt_table+" WHERE [intent_id]=?", item["intent_id"]).fetchone()
            if receipt_row is None:
                raise IngestError("FIXTURE_RECEIPT_MISSING", "Destination has no receipt for the fixture intent",5)
            receipt = json.loads(receipt_row[0])
            reduced_assurance = reduced_assurance or receipt.get("assurance")!="full"
            live_identity = cursor.execute("SELECT OBJECT_ID(?),CONVERT(nvarchar(36),database_guid),CONVERT(nvarchar(36),recovery_fork_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID()",ident(plan["schema"])+"."+ident(plan["table"])).fetchone()
            if live_identity is None or live_identity[0]!=receipt_row[1] or str(live_identity[1]).lower()!=str(receipt_row[2]).lower() or str(live_identity[2]).lower()!=str(receipt.get("details",{}).get("recovery_fork_guid")).lower():
                raise IngestError("FIXTURE_DESTINATION_IDENTITY", "Receipt destination object/database identity differs from live metadata",5)
            expected_receipt = {"run_id":run_id,"intent_id":item["intent_id"],"database":database,"schema":plan["schema"],"table":plan["table"],"source_hash":fixture["sha256"],"interpretation_hash":plan["interpretation_hash"],"mapping_hash":plan["mapping_hash"],"committed_records":fixture["expected_rows"]}
            if any(receipt.get(key)!=value for key,value in expected_receipt.items()):
                raise IngestError("FIXTURE_RECEIPT_MISMATCH", "Fixture receipt identity/count/hash does not match the authored source and selected run",5)
            counts = journal.counts(run_id,item["file_id"])
            accounting.append({"path":fixture["path"],"counts":counts})
            if counts["committed"] != fixture["expected_rows"] or counts["rejected"]!=len(fixture.get("rejected_ordinals",[])) or any(counts[key] for key in ("pending","excluded","unresolved")):
                raise IngestError("FIXTURE_ACCOUNTING_MISMATCH", "Fixture source dispositions are incomplete or differ from expected rows",5)
            details = (item.get("receipt") or receipt).get("details",{})
            if any(details.get(key) in ("pending","FAILED") for key in ("audit_mirror_state","control_mirror_state","failure_table_state")):
                pending_audit = True
            authored_schema=fixture.get('sql_schema')
            if not authored_schema or len(authored_schema)!=len(plan['columns']):
                raise IngestError('FIXTURE_SCHEMA_MISMATCH','Stored fixture plan lacks the complete authored column schema',5)
            metadata={row[0]:row for row in cursor.execute("SELECT c.name,t.name,c.max_length,c.precision,c.scale,c.is_nullable FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE c.object_id=OBJECT_ID(?)",ident(plan['schema'])+'.'+ident(plan['table']))}
            for declaration,column in zip(authored_schema,plan['columns'],strict=True):
                row=metadata.get(declaration['target_name'])
                if row is None:raise IngestError('FIXTURE_SCHEMA_MISMATCH','Authored destination column missing',5)
                base,length,precision,scale=row[1:5]
                actual_type=base
                if base in ('nvarchar','varchar','varbinary'):actual_type+='('+str('max' if length==-1 else length//2 if base=='nvarchar' else length)+')'
                elif base in ('decimal','numeric'):actual_type='decimal('+str(precision)+','+str(scale)+')'
                elif base in ('time','datetime2','datetimeoffset'):actual_type+='('+str(scale)+')'
                if (column['ordinal'],column['target_name'],actual_type,bool(row[5]))!=(declaration['source_ordinal'],declaration['target_name'],declaration['target_type'].lower(),declaration.get('nullable',True)):
                    raise IngestError('FIXTURE_SCHEMA_MISMATCH','Destination schema differs from authored fixture declarations',5,{'ordinal':declaration['source_ordinal'],'expected_type':declaration['target_type'],'observed_type':actual_type})
            expressions=[]
            for column in plan["columns"]:
                name = ident(column["target_name"])
                base = column["sql_type"].lower().split("(")[0]
                expressions.append("CONVERT(nvarchar(64),"+name+",126)" if base in ("date","time","datetime2","datetimeoffset") else "CONVERT(nvarchar(36),"+name+")" if base=="uniqueidentifier" else name)
            table=ident(plan["schema"])+"."+ident(plan["table"])
            cursor.execute("SELECT [__ingest_record],[__ingest_ordinal],"+",".join(expressions)+" FROM "+table+" WHERE [__ingest_intent]=? ORDER BY [__ingest_ordinal]",item["intent_id"])
            # SQL's unique ingestion key distinguishes equal rows; compare exact IDs to
            # the retained journal rather than relying on generated-ID return order.
            while True:
                row=cursor.fetchone()
                if row is None:break
                local=journal.db.execute("SELECT record_id FROM records WHERE run_id=? AND file_id=? AND ordinal=? AND disposition='committed'",(run_id,item["file_id"],row[1])).fetchone()
                if local is None or local[0]!=row[0]:
                    raise IngestError("FIXTURE_LINEAGE_MISMATCH", "Destination source-record identity differs from retained provenance",5)
                yield {"ordinal":row[1],"values":list(row[2:])}
        result=verify_test_files(manifest_path,captures=captures,destination_reader=reader,run_id=run_id,database=database)
        for fixture in manifest['fixtures']:
            if fixture['expected_outcome']=='parsed' or fixture['expected_outcome']=='rejected' and fixture['expected_rows']>0:continue
            item=entries[fixture['path']]
            if cursor.execute('SELECT OBJECT_ID(?)',receipt_table).fetchone()[0] is not None:
                if cursor.execute('SELECT COUNT_BIG(*) FROM '+receipt_table+' WHERE intent_id=?',item['intent_id']).fetchone()[0]:
                    raise IngestError('FIXTURE_UNEXPECTED_SQL_RECEIPT','Expected rejected/unresolved fixture has a destination receipt',5)
            if item.get('plan'):
                target=ident(item['plan']['schema'])+'.'+ident(item['plan']['table'])
                if cursor.execute('SELECT OBJECT_ID(?)',target).fetchone()[0] is not None:
                    if cursor.execute('SELECT COUNT_BIG(*) FROM '+target+' WHERE __ingest_intent=?',item['intent_id']).fetchone()[0]:
                        raise IngestError('FIXTURE_UNEXPECTED_SQL_ROWS','Expected rejected/unresolved fixture has published rows',5)
        unsupported=[]
        expected_nonclean=any(f['expected_outcome']!='parsed' for f in manifest['fixtures'])
        if not result["errors"]:
            result.update(state=("FIXTURE_EXPECTED_FAILURES_VERIFIED" if expected_nonclean else "FIXTURE_VERIFIED") if not unsupported and not pending_audit and not reduced_assurance else "FIXTURE_PARTIAL",exit_code=0 if not unsupported and not pending_audit and not reduced_assurance and not expected_nonclean else 3,expected_nonclean=expected_nonclean,outcome_checks=outcome_checks,sql_integration="EXECUTED_EXPLICIT_DISPOSABLE_DESTINATION",accounting=accounting,unverified_outcomes=unsupported,pending_audit=pending_audit,reduced_assurance=reduced_assurance)
        return result
    except IngestError:
        raise
    except Exception as exc:
        message=str(exc)
        if password:message=message.replace(password,"<redacted>")
        raise IngestError("FIXTURE_SQL_READ_FAILED",message,5) from None
    finally:
        if connection is not None:
            try:connection.rollback()
            finally:connection.close()
        journal.close()
