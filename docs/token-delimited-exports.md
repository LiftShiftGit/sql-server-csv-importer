# Explicit delimited export tokens

The default dialect keeps the existing CSV parser and record identities. It accepts
one-character delimiters, ordinary CSV qualifiers/escapes and automatic CR, LF or
CRLF endings. New settings at their defaults do not change interpretation hashes:

```toml
[parsing]
record_terminator = "auto"
allow_multiline_fields = true
```

For explicitly specified export formats, delimiters, qualifiers, escapes and a
record terminator can each contain 1 through 32 Unicode characters. Their limits
are characters; record and field preservation limits remain captured **bytes**.
Use actual TOML escapes, for example `"\r\n"`, for CRLF.

```toml
[source]
format = "delimited"

[parsing]
encoding = "utf-8"
header = "present"
delimiter = "||"
quotechar = "@@"
escapechar = "!!"
doublequote = true
record_terminator = "<END>"
allow_multiline_fields = true
```

`left||right<END>@@A||B@@||@@x!!@@y@@@@z@@<END>` yields a header and
one row with values `A||B` and `x@@y@@z`. An escape quotes the next **whole
recognized control token**, or the next Unicode scalar when no token follows.
Doubling a qualifier inside a quoted field emits one literal qualifier when
`doublequote=true`. Qualifiers open a quoted field only at the beginning of that
field. Content following a closing qualifier, other than a delimiter, record
ending or doubled qualifier, rejects that complete record. A qualifier occurring
inside an unquoted field is literal.

The token branch is selected only for a multi-character field control, an explicit
record terminator, or `allow_multiline_fields=false`. Its escaped CRLF is one whole
token; default CSV retains its existing single-character escape behavior. Automatic
delimiter inference requires single-character field controls and automatic record
endings. Set `delimiter` explicitly for an alternate dialect.

Distinct field-control and record-ending tokens must not equal or prefix one
another. For example delimiter `|` with qualifier `||`, or delimiter `END` with
record ending `ENDROW`, is refused before parsing. Common prefixes which are not
whole-token prefixes, such as qualifier `<<` and ending `<END>`, are supported.
Self-overlapping tokens are matched exactly, from left to right. Automatic CRLF
is the documented exception to the CR/LF prefix rule. Delimiters, qualifiers and
escapes cannot contain CR or LF, NUL, or opt-in NEL. Disabled quote and escape
sentinels remain `none`; the conventional quote sentinel remains `auto`.

An explicit record terminator defines logical boundaries. Other CR/LF sequences
are field data when multiline fields are allowed; they do not silently become
alternate record endings. Physical line spans always count the actual captured
CR/LF sequences (CRLF counts once), and NEL only with `nel_is_newline=true`.
An arbitrary `<END>` separator therefore need not advance the physical line.
For NEL-delimited EBCDIC text, the existing `nel_is_newline=true` option also adds
NEL to automatic record endings; an explicit terminator takes precedence.

For line-oriented exports set `allow_multiline_fields=false`. A physical newline
inside a quoted field, an escaped physical newline, or a physical newline not
recognized as the explicit record ending leaves the entire remaining source tail
**unresolved**. The parser never guesses a new row from the following line.
Unclosed qualifiers and dangling escapes at EOF likewise produce one unresolved
tail. Choose this option when a malformed quote must not absorb later physical
lines as a single apparently valid field.

Framing scans strict decoded chunks of at most 64 KiB with at most 32 characters of
token lookahead. Tokens may cross byte, decoder and read boundaries. Exact original
byte spans, quoted/null provenance, empty strings and row-width checks are retained.
There is no missing-field padding. After a record/field/1024-column limit is
exceeded, field payloads are discarded from memory while quote/escape framing
continues to a provable record ending; original bytes stay in the capture archive.
A known oversized record is rejected; ambiguous framing still blocks that tail.

Explicit `format="delimited"` with a configured angle qualifier beginning the
source is honored as that qualifier, rather than guessed XML. Other leading XML
or HTML signatures retain the existing structured-format conflict checks.
Structured XML/HTML scalar mappings do not use delimited token boundaries.

Verification is in `tests/test_parser_tokens.py`: authored exact-value/byte-span
oracles across eight codecs and forced 1/2/3/7/31-byte reads, supplementary Unicode
tokens, CR/LF and NEL boundaries, malformed tails, escape/qualifier overlap,
1024-column and 32-character bounds, quoted NULLs, randomized round trips and a
4 MiB oversized-record allocation bound. These are local parser proofs. Independent multi-GB SQL publication evidence is
linked below and covers both loaders.

EOF column-budget correction: the unfinished final field counts toward the 1024
column limit even when its only evidence is a trailing delimiter and the source
has no record terminator. Both parsers check that total before decoding/finalizing
fields. A 1024-field record remains valid; 1025 fields reject with RECORD_LIMIT,
including header records and all-empty rows. The authored EOF boundary matrix
covers both parser branches, headers present/absent, trailing empty/nonempty fields
and terminated/unterminated records. Valid-record identities are unchanged.

## Completed multi-GB verification

The [eleven-case matrix](testing.md) includes a 2,153,023,131-byte export
with `||`, `@@`, `!!` and `<EOR>` through both loaders. All accepted cells, inferred
SQL types, source IDs, full captures, rejects, receipts and chunk hashes were checked.
The exact EOF-fixed code was rerun through both providers; a final starter-comment-only
package revision preserves the other 66 runtime/default files and parsed settings.
Earlier checkpoint statements above describe their executed scope at the time.
