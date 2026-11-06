# Configuration and command examples

Use `python -m db_ingest --config ingest.toml <command>`; the config option precedes the command.
The installed `.venv\Scripts\db-ingest.exe` entry point is equivalent. Normal operation needs
only pyodbc and Microsoft ODBC Driver 18. Development tests and optional XML dependencies are
separate extras: `.\.venv\Scripts\python.exe -m pip install -e ".[dev,xml]"`.

For EBCDIC/legacy exports, escaping, missing column names and large schema-free CSVs,
see [the format guide and checked-in examples](messy-data-and-encodings.md).

For multi-character delimiter/qualifier/escape/record-ending tokens and a no-multiline
row-bleed guard, see [token exports](token-delimited-exports.md). From the project root:

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config examples/token-delimited.toml plan
```

See the [complete field/value configuration reference](configuration-reference.md) for every global field,
allowed values, encoding behavior, credential sources and fixed capability settings.

## One configuration file

`init-config` writes the short [example](../ingest.example.toml), refusing to overwrite unless
`--force` is supplied. Complete documented defaults are in
[db_ingest/defaults.toml](../db_ingest/defaults.toml); operators need not copy every key.
Precedence is CLI > named environment > matching file rule > global TOML > defaults.
Later matching rules win; lists replace. Plans report resolved schema, policy hashes and provenance.

```toml
[destination]
server = "sqlhost,1433"
database = "LegacyLanding"
control_database = "IngestControl"
encrypt = "yes"
trust_server_certificate = "no"

[source]
root = 'D:\Exports'
path = 'D:\Exports'
include = ["**/*.csv", "**/*.xyz"]

[parsing]
header = "present"
delimiter = ","
encoding = "utf-8"
null_tokens = ["\\N"]

[preservation]
work_directory = 'D:\ingest-work'
```

The work directory stores immutable captures, the SQLite journal, diagnostics and transport
spools; it is always excluded from discovery. The minimum-free-space reserve is explicit;
the importer does not delete the only retained copy to recover space.

## Per-file and per-column interpretation

```toml
[[file_rules]]
glob = "folder_a/**/*.csv"
delimiter = "|"
encoding = "cp1252"
header = "present"

[[file_rules.columns]]
source_name = "CaseNumber"
source_ordinal = 1
target_name = "case_number"
target_type = "nvarchar"
length = 40
null_tokens = []

[[file_rules.columns]]
source_ordinal = 4
target_type = "decimal"
precision = 19
scale = 4

[[file_rules.columns]]
source_ordinal = 5
target_type = "datetimeoffset(7)"
date_format = "%d/%m/%Y"

[[file_rules.columns]]
source_ordinal = 6
target_type = "varbinary(max)"
binary_encoding = "base64"
```

Ordinals are 1-based. Duplicate names require ordinals; a supplied name and ordinal must agree.
Column null_tokens replace the global list, including an empty list that preserves a global
marker as literal text. Quoted markers remain literal unless the declared quoting policy says
otherwise. Empty text, SQL NULL and a missing field are different; short records are rejected.
Length/precision/scale aliases resolve to the explicit SQL type shown in the plan. Decimal
exponents in exact mappings are accepted only when the declared result fits; no binary float intermediate,
rounding, silent truncation or failed-value-to-NULL substitution is used.

Explicit numeric calendar layouts: `%Y-%m-%d`, `%Y/%m/%d`, `%Y%m%d`, `%d/%m/%Y`, `%m/%d/%Y`,
`%d-%m-%Y`, `%m-%d-%Y`, or `iso8601`. The calendar prefix may be followed by an ISO clock/offset
for datetime columns. Day/month order is never guessed. Seven fractional digits and original
stored offsets are retained; source lexical forms remain in the capture independently.

For an explicit sidecar reference, use `target_type="varbinary(max)"`,
`binary_encoding="attachment_path"`, and `attachment_root='D:\Attachments'` on that column.
The referenced bytes are captured and hashed; missing files, root escapes and symlinks are
reported rather than becoming NULL. Ordinary cell text never causes filesystem access.
Base64 and hex mappings are also strict. Larger unsupported values remain captured/quarantined;
this is not a promise of arbitrary streaming database LOB support.

## Credentials and TLS

SQL passwords are prompted without echo when missing in an interactive terminal.
Usernames are **not prompted**: use `--username` or `DB_USERNAME`. Username/password
TOML keys are not accepted by the current loader. Non-interactive automatic prompting
is disabled; use `DB_PASSWORD` or `--password-stdin`. Integrated authentication requires
no SQL credentials. See the [exact precedence and prompt behavior](configuration-reference.md#login-and-prompts).


Prefer `--password-prompt`, `--password-stdin`, or DB_PASSWORD. The requested argv form works,
but passwords may be visible in process inspection or shell history:

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml run --username loader --password 'YOUR_PASSWORD' --header present
```

PowerShell environment override:

```powershell
$env:DB_ENCRYPT = 'yes'
$env:DB_TRUST_SERVICE_CERTIFICATE = 'no'
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml test-connection --username loader --password-prompt
```

cmd.exe equivalent:

```bat
set "DB_ENCRYPT=yes"
set "DB_TRUST_SERVICE_CERTIFICATE=no"
.venv\Scripts\python.exe -m db_ingest --config ingest.toml test-connection --username loader --password-prompt
```

DB_TRUST_SERVER_CERTIFICATE is canonical; SERVICE is a supported spelling alias. Conflicting
values stop. Other names: DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD, DB_DRIVER,
DB_CONTROL_DATABASE and DB_CONNECT_TIMEOUT. Integrated authentication is configurable; exact
observed authentication/TLS combinations are recorded in evidence. A test-only self-signed
loopback policy is not a recommendation to bypass real-server certificate validation.

`test-connection` is read-only. `--write-probe` deliberately creates a uniquely named temporary
scratch object and reports only the capabilities actually observed. TLS negotiation/certificate
observations are not inferred merely from a successful login.

## Loaders and resource settings

```toml
[load]
engine = "auto"
batch_rows = 5000
batch_bytes = 8388608
fast_executemany = true

[parallel]
enabled = true
max_workers = 4

[limits]
total_process_tree_memory_bytes = 1073741824
worker_memory_bytes = 167772160
```

The defaults request two admitted workers, guarded arrays and bcp preference when compatible;
SQL-auth no-argv policy commonly selects ODBC instead. Both batch row and byte bounds apply.
Native ODBC array admission uses largest parameter stride,
not source-byte sums. Long canonical rows use bounded ordinary executemany and report the actual
fallback counts. bcp first loads a reversible encoded raw stage, checks exact source IDs/payloads,
then validates typed staging and target values; it never treats arbitrary CSV as bcp framing.
Wide layouts may use an explicitly recorded bounded client typed-decode route after raw bcp
verification. Memory-heavy server decode is serialized separately from file workers.

Native bcp selection requires actual tool capabilities matching the configured TLS policy.
The tested SDK180 executable reports 17.0.4055.5 and advertises -Y/-u; version-label assumptions
are insufficient. Configure your installed tool path explicitly where needed:

```toml
[tools]
bcp_path = 'C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe'

[credentials]
allow_password_in_child_args = true
```

SQL-auth bcp uses its documented argv password path only after that intentional opt-in. It does
not consume SQLCMDPASSWORD. With the default no-argv policy, auto can choose ODBC. Explicit
`--engine bcp` stops if prerequisites fail. Tool flags for bcp and sqlcmd are not interchangeable.

Worker admission can reduce requested concurrency to fit the global reservation. Each worker
owns its journal/ODBC connection. Requested/effective workers and observed process-tree memory
are reported. Windows ancestry checks reject stale ParentPID links after PID reuse. Sampling
and cooperative checkpoints can miss short peaks; they are not an OS hard allocation sandbox.

## Select and map files

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml run --root 'D:\Exports' --source 'D:\Exports\folder_a' --include '**/*.xyz' --delimiter '|' --encoding cp1252 --header present --mode append --no-parallel --username loader --password-prompt
```

Root and nested files match `**/*.csv`; include/exclude flags replace configured lists.
Content is checked independently of suffix. `--recursive/--no-recursive` controls traversal;
symlinks/junctions are not followed by default. The selection root never changes stable names.
For root `D:\Exports` and `folder_a/Archive/cases.xyz`, base labels are:

| database_strategy | Database | Table |
| --- | --- | --- |
| single | configured database | folder_a__Archive__cases |
| per_top_level_directory | Import_folder_a + identity hash | Archive__cases + identity hash |
| per_directory | Import_folder_a__Archive + identity hash | cases + identity hash |
| per_file | Import_folder_a__Archive__cases + identity hash | configured data table |

All generated names use stable collision protection and proper quoting; original labels and
ordinals remain queryable. Root-file behavior is explicit. Per-directory merged-table publication
has separate contributor/schema requirements; consult current boundaries before selecting it.

## Append, replace and recovery

Append publishes explicit columns. Replace validates first, retains the previous generation,
and changes usable data only inside the checked publication transaction. Empty/incomplete
replacement is blocked by default. Existing non-owned business tables, generated/identity
mappings or unsupported trigger/dependency behavior are not silently treated as landing tables.

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml plan --output plan.json --header present
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml export-failures --run-id RUN_ID --output rejects.jsonl
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml resume --run-id RUN_ID --plan-only
.\.venv\Scripts\python.exe -m db_ingest --config corrected.toml reimport --run-id RUN_ID --mode replace --username loader --password-prompt
```

Resume reuses captured sources and the same intent. Missing receipts authorize no replay by
themselves: transaction resolution, current source lineage, database/object/recovery-fork identity,
local and central acknowledged history are checked under locking. A current or historical
acknowledged receipt missing from the destination blocks replay. Corrected semantics create a
linked reimport; intentional repeats require `--new-intent`. Do not edit journal states to force
success. Normalization/conversion is a derived projection; original bytes remain available.

## Independent fixtures and integration

```powershell
.\.venv\Scripts\python.exe -m db_ingest create-test-files --output .\fixtures\smoke --type csv --seed 20261029
.\.venv\Scripts\python.exe -m db_ingest --config docs\local-smoke.toml plan --output local-plan.json
.\.venv\Scripts\python.exe -m db_ingest verify-test-files --manifest .\fixtures\smoke\manifest.json
```

Source-only verification deliberately returns 3/SOURCE_ONLY_NOT_SQL_VERIFIED. Fixture generation
refuses to overwrite an existing set. Type options are repeatable/comma-separated and accept
hyphen/underscore aliases. Actual fixture SQL verification requires explicit manifest/run and a
disposable database prefix; it never selects a production destination implicitly.

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config ingest.integration.toml verify-test-files --manifest .\fixtures\vertical\manifest.json --run-id RUN_ID --database DbIngestTest_Landing --username loader --password-prompt
.\.venv\Scripts\python.exe -m pytest -q
```

The tracked [integration runner](../scripts/run_integration.py) requires explicit disposable
config, credentials mechanism and actual engine major. It exposes separate bcp/batching/parallel/
native-resource/receipt-resolution/affected-file/restore test gates. See
[local SQL test infrastructure](testing.md) for the development containers; none is a
runtime requirement. Test reports distinguish real runs, simulations, skips and remaining work.

## Explicit VARCHAR byte storage

Unicode fallback remains NVARCHAR. VARCHAR requires an explicit certified collation/encoder
pair; bounds count encoded bytes, including trailing spaces. Input decoding remains separately
configured by parsing.encoding. The supported pairs are Latin1_General_100_BIN2_UTF8/utf-8 and
Latin1_General_100_BIN2/cp1252; actual SQL codepage is verified before target mutation.

```toml
[[file_rules]]
glob = "western.csv"
[[file_rules.columns]]
source_name = "description"
target_type = "varchar(200)"
collation = "Latin1_General_100_BIN2"
text_encoding = "cp1252"
```

CP1252 cannot represent emoji; those values reject rather than become question marks. An emoji
uses four UTF8 bytes; VARCHAR(8) accepts `é🙂  ` exactly. Length overflow defaults to a required
decision; `schema.on_length_overflow="quarantine"` explicitly permits retaining/rejecting that
record while independent valid records publish. For broader text, choose NVARCHAR or change
the explicit approved size/type and use linked reimport. This policy never silently trims.

## Authorized owned-table changes

```toml
[schema]
allow_add_columns_on_append = true
allow_widen_on_append = true
allow_retype_on_append = true
```

These flags request concrete compatible changes for importer-owned tables; they do not approve
arbitrary SQL. Currently tested examples are nullable additions, longer Unicode text and integer
widening, with all existing values and historical receipt projections verified. Defaults leave
all three false. Narrowing, dropping published columns, external schema drift or dependencies
that cannot be proven safe stop before target changes. Use the reported concrete diff when
reviewing a schema change. Explicit source mappings preserve named values when input columns reorder.

## Existing business table append

```toml
[destination]
schema = "dbo"
[mapping]
existing_target_strategy = "mapped_business"
target_table = "ExistingOrders"
omit_target_columns = ["OrderId", "CreatedAt", "ComputedTotal", "Version"]
identity_policy = "generate"
allow_target_triggers = false
```

Map each supplied source field explicitly with file_rules.columns. The target must already
exist and have a usable nonnullable unique key. List every omitted target column exactly;
only declared generated/default/nullable columns may be omitted. Explicit source NULL remains
NULL and does not request a column default. identity_policy="preserve" permits a supplied
identity column; otherwise omit it and retain its generated key in the durable source sidecar.

Every selected file maps to the exact target_table in destination.schema. Review the source
selection and resolved plan before importing. This route appends atomically per file; it does
not replace or merge directory groups. Approved AFTER triggers must leave supplied values
unchanged; a transform rolls back. Generated/default values, computed values and rowversion
are captured/reverified along with explicit source-to-key identity. Arbitrary generated-always
semantics and unsupported dependencies fail with concrete diagnostics.

## Merge one directory into one table

```toml
[mapping]
table_strategy = "per_directory"
name_collision = "path_error" # readable relative folder names, e.g. north_2026
directory_merge_schema = "strict"
```

Strict requires equivalent ordered source/declared labels. Explicit union_by_name merges by
unique declared names, preserving missing-column provenance separately from source NULLs and
projecting absent nullable cells to SQL NULL. Duplicate/blank names need ordinal-disambiguated
unique target_name overrides. Real files keep independent counters/archives/record IDs; one
group intent and atomic receipt publish the combined table. Manifest JSONL preserves source
hashes, paths, byte sizes, column mappings and original/global ordinals.

The default load.replace_scope="complete_group" checks ALL known acknowledged contributors,
including older runs and central/destination history after local cache loss. Missing, unreadable,
excluded or unselected files block replacement and are named. Explicit selected_subset means
whole-target replacement by that selected subset, retaining a proven previous generation;
union preserves the acknowledged nullable column superset. A directory across tables/databases
is not a global transaction.

## Deliberate lossy destination projection

schema.allow_lossy_projection=true must accompany a precise per-column truncation="right"
for sized Unicode/qualified VARCHAR/binary or rounding="half_even" for DECIMAL. Defaults forbid
these projections. Truncation preserves whole Unicode scalars within actual storage units/bytes.
Every changed cell retains original/result states, rule and source reference in durable evidence;
export-projections --run-id RUN_ID --output projections.jsonl writes that verified evidence.
A lossy receipt stays assurance="lossy" and exit3 for run/resume/reconcile, even when all intended
projected values are verified. Original source bytes stay exact. Corrupted/missing projection
proof blocks recovery/reconciliation/export; it cannot become ordinary clean success.

## Lossless length overflow decisions

schema.on_length_overflow accepts ask (default), quarantine, abort, widen or retype_text.
Widen measures NVARCHAR in UTF16 storage units, qualified VARCHAR in its declared encoded bytes,
and VARBINARY in decoded bytes; text_length_headroom_percent supplies explicit headroom.
MAX is selected only when schema.allow_max_types allows it. Retype_text preserves source lexical
text, including Base64/hex spellings, in NVARCHAR; malformed original binary/encoding still rejects
after the fallback. Captured attachment content uses its recorded canonical Base64 representation.
These are reviewed schema choices, with source hashes/rules/change events retained, not silent
substring or conversion-to-NULL repairs. Existing owned targets separately require explicit
compatible ALTER authorization; a new-file overflow policy alone cannot change an existing table.

## Terminal decisions and corrected interpretations

With ask, an interactive terminal can choose widen, retype_text, quarantine or stop for a pure
length-overflow decision before any publication was acknowledged. The chooser shows bounded
column/value evidence and current/proposed types, then creates a NEW linked interpretation
from ALL durable captures. The old run remains unchanged; the live source is not read again.
Existing-target ALTER still requires its separately configured authorization.

JSON output, redirected stdin and --non-interactive never prompt. Ask returns NEEDS_DECISION2.
Correct it explicitly, for example:

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml reimport --run-id OLD_RUN --on-length-overflow widen --password-prompt
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml resume --run-id NEW_RUN --on-length-overflow widen --password-prompt
```

Keep the selected semantic policy in the configuration or repeat the same override on resume.
Other schema/dialect decisions need their reported explicit configuration correction; a terminal
choice never invents permission to alter an existing target. See the
[diagnostic catalog](diagnostic-catalog.md) for observed errors, uncertainty and recovery actions.


## Queryable summaries and generated fixture expectations

Control schema version5 keeps the current per-file operational report in `Files.summary_json`
(next to `Runs.summary_json`). It includes captured source identity, actual destination/format,
configuration hashes, separate row disposition measures, failure-table and archive references,
phase timings, the explicitly run-level sampled RSS basis and next commands. Unknown total
logical/staged/target counts remain SQL NULL/JSON null; source-only candidate profiles never
invent publication chunks or receipts. Immutable source/schema/receipt evidence remains separate.

On a real terminal the CLI prints a compact operator summary. `--json` and redirected stdout
preserve bounded structured JSON for scripts. The diagnostic catalog explains error recovery.

`create-test-files` emits complete ordered `sql_schema` declarations, explicit parser/adapter
settings, known or unknown logical counts, streamed authored typed values and expected
accepted/rejected/unresolved outcomes with reasons. Apply those declarations explicitly when
importing a fixture; do not infer the expected schema with the importer. Structured mappings
use a declared metadata header ordinal0 and data-record ordinals starting1; these are separate
from a physical source header. The manifest states that ordinal convention.

`verify-test-files --manifest ...` checks golden source bytes only and returns3; it does not
claim SQL completion. An explicit `--run-id` and disposable `--database DbIngestTest_...` or
`IngestFixture_...` enable actual independent SQL value/schema/ID/receipt checks plus retained
negative-outcome/reason accounting. Expected failure fixtures return3 with
`FIXTURE_EXPECTED_FAILURES_VERIFIED`, never a clean import. Unknown/incomplete authored
expectations fail5. Header-only generated data expects an explicit NO_DATA decision with no publication receipt.

Per-directory group append requires complete observable central contributor history (control schema version4 or later). Unavailable/incomplete central history blocks5 with captures pending; it cannot establish that no earlier contributor/publication exists. Ordinary per-file new imports retain the documented audit-fallback partial3 policy. Recovery and complete-group replacement remain stricter than that fallback.

Native version/help probes use `limits.output_buffer_bytes` (32,768 combined raw stdout/stderr bytes by default, maximum1MiB), then a separately bounded diagnostic preview. Excess output or timeout stops the owned tool process tree and fails capability detection; explicit bcp never silently substitutes. Publication stdout/stderr remain streamed artifacts with disk/resource checks.
