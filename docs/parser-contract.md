# Source interpretation implemented in this build

Delimited parsing is independent of file suffix. Discovery uses a temporary SQLite
inventory for stable relative-path ordering; `*` matches one component and `**`
matches zero or more components. The work directory is excluded even when callers
replace include/exclude lists. Default traversal excludes symlinks and junctions.

Windows capture opens the source with FILE_SHARE_READ only, excluding concurrent
write/delete opens. Capture hashes in bounded blocks, flushes/fsyncs contents, uses
MoveFileExW WRITE_THROUGH finalization, and records an artifact sidecar. Same-API
before/after identity, size, mtime and ctime checks detect observed changes. Python
3.13 on this Windows host produced different stat/fstat ctime values, so cross-API
checks compare identity/size/mtime while each API checks its own ctime stability.
Non-Windows capture has only observed-stat checks and requires a stable source;
non-Windows operation has not been certified.

Supported span mappings include UTF-8/BOM, explicit UTF-16/32 byte order or BOM,
ASCII, Latin-1/15, CP1250..1258, CP437/850/858, KOI8-R and EBCDIC CP037/500/1140.
Explicit reversible Shift-JIS/CP932/EUC-JP, GB18030/GBK/Big5 and EUC-KR/CP949 mappings
also retain exact spans; noncanonical aliases fail closed. Stateful encodings are refused.
Auto detection uses BOM or strict UTF-8, without guessing a permissive legacy codec.
Header auto requires a present/absent choice. Delimiter auto accepts exactly one consistent
comma/tab/pipe/semicolon candidate and otherwise pauses. Every full parse decodes strictly.

`nel_is_newline=true` explicitly adds NEL record separators, requiring an explicit delimiter;
quoted/escaped NEL remains field data. Default CR/LF framing is unchanged. An ordered
`column_names` list can name headerless data or override an incomplete header. The actual
header fields/bytes stay retained. Blank/duplicate names generate stable unique SQL names;
unequal data widths still reject. See [examples and exact policies](messy-data-and-encodings.md).

Delimited logical ordinals include the declared header. Byte spans include record
terminators and exclude the BOM. Physical spans are one-based and preserve CR,
LF, CRLF and multiline distinctions. Blank, ragged, malformed and oversized
records retain original spans. An unterminated quote has one unresolved tail with
unknown logical count, not guessed rows. Raw data is never reconstructed from CSV
values. Lexical/field memory limits are checked before csv.reader materializes a
record. More than 1024 fields is a representation-limit reject. Explicit schema
conversion still occurs in staging before SQL publication.

Full-stream profiling proposes exact integers/decimals, dates and GUIDs when all
accepted observations fit. Leading-zero identifiers, precision beyond 38, mixed
values and uncertain temporal representations stay Unicode text. Supplementary
characters count as two UTF-16 storage units. NULL and empty observations are
separate; no default constraint is inferred. Per-column overrides declare schema
but do not excuse failed conversion validation.

## Optional flat XML

Install the xml extra. Declare a record name or expanded `{namespace}name`, and
ordered direct-child or attribute mappings. No XPath, nested flattening, repeated
children, missing-to-NULL conversion, DTD/entity declarations or network access is
permitted. defusedxml SAX releases each extracted record without retaining a DOM.
Configured depth, field and record bounds apply; markup tokens additionally have
a conservative 64 KiB bound before library parsing.

```toml
[source]
format = "xml"
include = ["**/*.xml"]
[xml]
enabled = true
record_selector = "row"
fields = [{name="id", path="@id"}, {name="label", path="label"}]
```

## Optional simple HTML table

Declare one zero-based *top-level* table index and ordered field names. Set header
present or absent explicitly. A declared physical header must equal those ordered
names. Text/entity extraction preserves decoded cell text and whitespace. Nested
tables, rowspan/colspan expansion, script/style/object/image/linebreak cell
content and unclosed table/row/cell structures are rejected. Inline formatting
tags contribute their text only, under the declared text-preservation policy.

```toml
[source]
format = "html"
include = ["**/*.html", "**/*.htm"]
[parsing]
header = "present"
[html]
enabled = true
table_selector = "0"
fields = ["id", "label"]
text_policy = "preserve"
```

XML/HTML adapters preserve the exact original archive but report row byte/line
spans as unavailable (`null`). Their ordinal-zero header envelope is explicitly
schema metadata from the declared mapping, not an invented source record. No
SQL Server integration claim follows from parser tests.

Source changes during capture: see [changed-export recovery](smb-outage-recovery.md).
Nonempty incomplete captures cannot be automatically recaptured under the same intent.
