# Messy exports, encodings and missing schemas

The checked-in [example corpus](../examples/messy-exports/manifest.json) contains synthetic,
authored examples of real export conventions, including deliberately invalid data. These
are not operational files. Each source has a SHA256, explicit interpretation settings, expected
ordered values in `.expected.jsonl`, and expected reject/unresolved reasons. A negative
fixture passing means the importer retained and correctly refused it; it does not mean
malformed bytes were silently repaired.

Run the supplied parsing-only configurations from the project root:

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config examples/incomplete-header.toml plan
.\.venv\Scripts\python.exe -m db_ingest --config examples/messy-exports/ingest.examples.toml plan
```

The first profiles three inferred columns from a two-name header. The second intentionally
reports rejected/unresolved examples; its per-file rules show each dialect/encoding.
Both commands capture/profile locally without publishing to SQL.

## Choose the format explicitly

| Source | Settings or action |
| --- | --- |
| Ordinary CSV, escaped double quotes | `delimiter=","`, `quotechar='"'`, `doublequote=true` |
| Backslash escaping | `escapechar='\'`, `doublequote=false`; choose only when the producer uses it |
| Unquoted escaped delimiters | `quotechar="none"`, `escapechar='\'` |
| Excel-style semicolon/TSV/pipe | `delimiter=";"`, `"\t"` or `"|"` |
| UTF-8/16/32 with BOM | `encoding="auto"`; strict decoding continues through the whole file |
| UTF-16/32 without BOM | Specify `utf-16-le`, `utf-16-be`, `utf-32-le` or `utf-32-be` |
| Windows/DOS exports | Choose the actual code page; never assume Latin-1 just because it decodes |
| EBCDIC text exports | `encoding="cp037"`, `"cp500"` or `"cp1140"`, with the actual delimiter |
| EBCDIC NEL separators | Additionally set `nel_is_newline=true`; delimiter must be explicit |
| No header | `header="absent"`; generated column names retain the first data row |
| Blank/duplicate header cells | Retained in source; generated SQL names are stable and unique |
| Too few header names | Supply an ordered `column_names` list covering every data field |
| Unequal row widths | Quarantine with byte spans; no padding or dropping fields |
| Unclosed quoted tail | Preserve as unresolved; do not guess where the next row begins |

Literal backslashes, padding, Unicode combining marks, CR/LF inside quoted fields, quoted
empty strings and quoted NULL markers retain their configured meanings. Choosing the wrong
valid single-byte encoding can produce plausible but incorrect text: the importer cannot
prove which code page the producer intended. Use the export's declared CCSID/code page.

`encoding="auto"` detects UTF BOMs, otherwise it tries strict UTF-8. It does not
statistically guess legacy code pages. Set a known encoding globally, in a matching
file rule, or with `--encoding`; the [configuration reference](configuration-reference.md#encoding-what-auto-means)
lists every supported canonical codec and the override precedence.

Header auto mode currently pauses for an explicit `present`/`absent` choice; it does
not automatically classify missing headers. With `header="absent"` and no name override,
logical labels are `column_1`, `column_2`, etc.; physical SQL names also have ordinal/hash
suffixes. Use per-column `target_name` for exact SQL names. See the
[header settings reference](configuration-reference.md#headers-and-generated-names).

## Mainframe text example

```toml
[parsing]
encoding = "cp037"
delimiter = ";"
header = "present"
nel_is_newline = true
trim_whitespace = false
```

EBCDIC LF (`0x25`) and CRLF work without enabling NEL. With the explicit NEL policy,
`0x15` establishes record boundaries outside quoted/escaped fields. A quoted embedded NEL
remains U+0085 in its field; original bytes and spans remain intact. CP500 punctuation and
CP1140 Euro values have separate fixtures. Padded DISPLAY text and leading-zero identifiers
stay exact. Textual signed decimals can use an explicit decimal column override.

Supported explicit single-byte codecs: ASCII, CP037/500/1140, CP437/850/858,
CP1250 through CP1258, Latin-1, ISO-8859-15 and KOI8-R. Japanese Shift-JIS/CP932/EUC-JP,
Chinese GB18030/GBK/Big5 and Korean EUC-KR/CP949 are also supported for strict reversible
byte mappings. Noncanonical multibyte aliases that cannot round-trip are retained with an
encoding error and unresolved span. Stateful encodings such as ISO-2022-JP/HZ are refused.
All conversions use Python's standard codecs; no new runtime dependency was added.
See the [Python codec table](https://docs.python.org/3.13/library/codecs.html#standard-encodings).

A mainframe file is not necessarily delimited text. Fixed-length FB records, VB/RDW/BDW,
COBOL copybooks, COMP/COMP-3 packed decimal, overpunched/zoned signs and mixed text/binary
records are not decoded by selecting `cp037`. They require explicit record layouts and
numeric representations; this build does not infer or flatten them. Overpunch text remains
text. Obtain a delimited DISPLAY export, or use a separately validated layout decoder and
retain its original file and conversion manifest. IBM documents [record formats](https://www.ibm.com/docs/en/zos-basic-skills?topic=set-data-record-formats)
and [COBOL numeric representations](https://www.ibm.com/docs/en/i/7.5.0?topic=items-computational-data-representation-usage-clause).

## Missing names, no supplied schema

For a physical header `id,label` followed by data `001,alpha,12.50`:

```toml
[parsing]
encoding = "utf-8"
delimiter = ","
header = "present"
column_names = ["id", "label", "amount"]
```

The original two-cell header is retained unchanged. The explicit ordered list names all
three data columns, and full-file inference chooses their SQL types. For an entirely
headerless file, use `header="absent"`; `column_names` is optional. The list can rename
columns but never rearranges source fields. Without it, a short header causes width
rejections. Changing the interpretation of an existing capture requires linked `reimport`.

Disabled new policies preserve the previous build's interpretation and record IDs; enabling
NEL or changing column names changes interpretation and requires linked reimport for an
existing capture.

A per-file rule can apply to one export family:

```toml
[[file_rules]]
glob = "mainframe/*.ebc"
encoding = "cp037"
delimiter = ";"
header = "absent"
column_names = ["account", "name", "amount"]
nel_is_newline = true
```

If extensionless or `.ebc` files are selected, include them explicitly in `source.include`.

## Large CSVs and late type changes

Planning profiles every accepted row before publication, with bounded record/batch memory
and a disk-backed journal. `sample_bytes` and `sample_records` do not authorize sampled-only
schema inference. Late text changes a numeric candidate to Unicode text; combined decimal
precision/scale is checked across the full column. Beyond38 digits, leading-zero identifiers
and invalid/ambiguous dates stay text. Later supplementary Unicode can require NVARCHAR(MAX).
All-null/empty columns use conservative text; binary representations require explicit rules.
A supplied schema is optional, but ambiguous header/encoding/dialect choices remain explicit.

Generate a scalable example with final-row outliers and a deliberately short header:

```powershell
.\.venv\Scripts\python.exe scripts/create_large_inference_example.py --output D:\TestExports\large --rows 1000000 --padding 2048
```

This creates roughly2GB of ordinary bounded records, plus an independently authored manifest.
Use `header="present"`, `delimiter=","`, `encoding="utf-8"` and
`column_names=["code","mixed","amount","label"]`. The final row changes mixed numeric text,
decimal scale and Unicode width. The generator refuses to overwrite existing files.
Use fewer rows for a quick trial; choose disk space before generating a large fixture.

Both row and byte limits apply to loading. Start with `batch_rows=5000`,
`batch_bytes=8388608`; file workers parallelize separate files. One large file is streamed
by one file worker. Increasing the batch is not required for correctness. The configured
record/field cap still applies: an oversized single row is retained/quarantined rather
than materialized without a bound. Keep the durable work directory for capture and replay.

Regenerate the smaller examples anywhere new:

```powershell
.\.venv\Scripts\python.exe -m db_ingest create-test-files --output D:\TestExports\messy --types csv_escaping,csv_dirty,csv_legacy_encodings,mainframe_ebcdic
```

Each manifest's `overrides`, encoding, delimiter and header settings are authoritative.
Generating/verifying source hashes alone is not SQL fidelity certification. Actual live SQL
checks compare every authored value, destination type and source identity; their current
reports are linked in [test evidence](testing.md).
