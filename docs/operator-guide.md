# db_ingest - Windows operator guide

Import delimited exports into SQL Server while retaining originals, rejected rows and
publication receipts. Start with the sample below, then plan a representative export.
This walkthrough uses importer-owned landing tables and append mode. Existing business
tables, replacement and lossy conversions are separate advanced workflows.

## 1. Install once (PowerShell)

Prerequisites: Windows x64, [Python 3.13 x64](https://www.python.org/downloads/windows/),
[Microsoft ODBC Driver 18 x64](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server),
and a reachable SQL Server 2019+. Python 3.12+ is accepted; the tested client is Windows 11,
Python 3.13.2 x64. Docker is optional test infrastructure, not a runtime requirement.

Run in a normal writable project folder, outside protected Documents. The examples use
`C:\Projects\db-ingest`; if you extracted the release elsewhere, replace that path in
the commands and sample configuration. Start in a fresh release folder with no `.venv-run`
or `ingest.toml`; keep earlier releases and their work directories for recovery:

```powershell
Set-Location 'C:\Projects\db-ingest' -ErrorAction Stop
if (Test-Path -LiteralPath '.\.venv-run') { throw 'Stop: existing environment; choose a fresh release folder.' }
if (Test-Path -LiteralPath '.\ingest.toml') { throw 'Stop: existing configuration; keep it with its original release/work directory.' }
py -3.13 -m venv .venv-run
if ($LASTEXITCODE -ne 0) { throw 'Stop: virtual environment creation failed.' }
$IngestPython = (Resolve-Path '.\.venv-run\Scripts\python.exe' -ErrorAction Stop).Path
& $IngestPython -m pip install .
if ($LASTEXITCODE -ne 0) { throw 'Stop: package installation failed.' }
New-Item -ItemType Directory -Force '.\.local' -ErrorAction Stop | Out-Null
& $IngestPython -I -m db_ingest init-config --output '.\ingest.toml'
if ($LASTEXITCODE -ne 0) { throw 'Stop: review the existing configuration or init error.' }
```

Use the same PowerShell window. In a new window repeat `Set-Location` and the
`$IngestPython = ...` line. No activation or execution-policy change is required.
If `ingest.toml` exists, review it; do not overwrite it using `--force`.
The `pip install .` step needs package-index access, including build dependencies.
If that is unavailable, ask the release administrator for a wheelhouse containing the
approved `db_ingest-0.1.0` wheel and the matching Python/Windows `pyodbc==5.3.0` wheel.
In the install block, replace only `& $IngestPython -m pip install .` with:
`& $IngestPython -m pip install --no-index --find-links 'D:\approved-wheelhouse' db-ingest==0.1.0`.
Keep the exit check. Do not install test/dev requirements into the operator environment.

For a new release, use a fresh virtual-environment directory; do not recreate or reinstall
an environment used by an active run.

This installs a **non-editable package**. `-I` uses that installed package even inside
the source checkout. Do not use `pip install -e .` for operator imports, and keep the
installed environment/configuration unchanged during a run. CSV runtime needs only
pyodbc, plus the separately installed ODBC driver; no test libraries are required.

### Windows path support

This is a **Windows-only** importer release. Local/relative paths, UNC sources, mapped-drive
sources and extended Windows paths are supported as described in [Windows paths](windows-paths.md).
Use single-quoted TOML paths. Relative paths resolve from the working directory, not source.root
or the config file. Network sources require the current Windows account's share permissions.

Keep `preservation.work_directory` on local durable storage: UNC/mapped-network work directories
are rejected because the recovery journal uses SQLite WAL. Long source paths were tested with
ODBC and bcp; bcp still requires a short work path so generated staging filenames stay within
its 255-character limit. The guide includes exact spellings, tested limits and recovery steps.

### If a source share disconnects

Keep the original run and local work directory. A verified complete capture continues
using its original bytes, even if the export changes. A nonempty incomplete capture is
blocked with `CAPTURE_RETRY_UNVERIFIED` (exit 5): automatic recapture cannot prove that
unread bytes are unchanged. A failure before any bytes were retained can be retried.
See [share outage and changed-export recovery](smb-outage-recovery.md) for exact
same-run recovery steps and the reviewed separate-import procedure for blocked captures.

## One-off directory imports

For many CSVs loaded once into new tables, use the [directory presets](directory-import.md).
Choose filename-only, folder-prefixed names (`north_orders`), or merge each folder into
one table. The presets use `create_only`, refuse existing targets, preserve same-run
recovery, and include a complete file-to-table mapping export. The general sample below
retains append mode for existing workflows.

## 2. Configure the sample

Get a SQL server address, account, and **two dedicated databases** from the DBA: a
landing DB and a control/audit DB. The account needs permissions to create/use the
required schemas/tables there; it need not be a server administrator. Ask the DBA to
review production privileges. See [SQL permissions/support](sql-support.md).

Open `ingest.toml` in Notepad. Replace its contents with this first-sample configuration,
then replace **SQLHOST, LandingDB and ControlDB** with the DBA's values. The single-file
quickstart below is just a rehearsal; for your normal folder of exports, use
[Importing multiple files: root versus path](#importing-multiple-files-root-versus-path). Save as UTF-8,
with the exact name `ingest.toml` (not `ingest.toml.txt`). Unspecified fields use validated
defaults. TOML sections are the lines in square brackets; put each field under its section.

```toml
config_version = 1
[destination]
server = "SQLHOST,1433"
database = "LandingDB"
control_database = "ControlDB"
authentication = "sql"
create_databases = false
create_schemas = true
create_tables = true
encrypt = "yes"
trust_server_certificate = "no"

[source]
root = 'C:\Projects\db-ingest\examples'
path = 'C:\Projects\db-ingest\examples\quickstart.csv'
include = ["quickstart.csv"]
recursive = false
format = "delimited"

[mapping]
name_collision = "filename_error" # Default: quickstart.csv -> quickstart; conflicts stop

[parsing]
header = "present"
delimiter = ","
quotechar = '"'          # Text qualifier: a literal double quote
doublequote = true       # Within quoted text, "" represents one literal "
escapechar = "none"      # Set explicitly only if the producer uses escapes
record_terminator = "auto" # CR, LF or CRLF
encoding = "utf-8"

[load]
engine = "odbc"
mode = "append"

[parallel]
enabled = false

[preservation]
work_directory = 'C:\Projects\db-ingest\.local\work'
```

### What these settings mean

| Setting | Meaning and accepted values |
| --- | --- |
| `destination.server` | One SQL endpoint for both data and audit connections: `hostname,port`, not a web URL. |
| `destination.database` | Landing database for imported data under the default single-database mapping. |
| `destination.control_database` | Database holding central run/error/audit metadata, on **the same server and using the same credentials**. No separate control-server setting exists. Destination-local receipts also remain in the landing DB. |
| `destination.create_databases` | `false`: databases must already exist and be accessible. `true`: attempt to create missing configured/mapped databases, subject to permissions. Default is `true`, so this sample deliberately overrides it. It does not create an RDS instance. An access failure is not proof that a database is missing. |
| `destination.create_schemas`, `create_tables` | `true` allows required target/audit schema and table creation. `false` requires the corresponding objects to be provisioned already; these flags do not grant SQL permissions. |
| `load.mode` | `create_only` refuses existing targets except original-intent recovery; see [directory presets](directory-import.md). `append` adds rows without clearing existing target rows. `replace` is an explicit whole-target replacement workflow, including retained prior-generation handling; use only after review. |
| `mapping.name_collision` | `"filename_error"` by default: `orders.csv` becomes `orders`, and collisions stop. Set `"stable_hash_suffix"` explicitly for hashed names or existing hashed imports. |
| `source.root` | Stable base directory used for relative file names, include/exclude patterns and target naming. Keep it fixed between related runs. |
| `source.path` | Actual file or directory to read, inside `root`. To import the whole root, set `path` equal to `root`. Omit `path` or set `path = ""` to use the final configured root. An explicit path still selects a narrower file/folder. |
| `source.include` | List of filename/path glob filters, relative to root. Applies even when path is a single file. Optional, but omission keeps defaults `**/*.csv`, `**/*.tsv`, `**/*.txt`, `**/*.xyz`; it does not mean every file. `[]` selects no files. |
| `source.exclude` | List of globs to omit; exclusions win. The work directory is always excluded. Defaults omit `**/ingest-work/**` and `**/*.tmp`. |
| `source.recursive` | `true` walks subdirectories when path is a directory; `false` examines only that directory's immediate files. It has no effect on a single selected file. The default is `true`; the sample sets `false`. |
| `source.format` | `delimited` for CSV/TSV/custom-delimited text; `auto` for content-based adapter selection; `xml`/`html` only with explicitly enabled scalar adapters and mappings. Extension alone does not establish format. No Excel, ZIP, Parquet or binary mainframe/copybook decoder. |
| `parsing.delimiter` | Field separator, such as `","`, `"\t"`, `"|"` or `"||"`; `auto` considers comma/tab/pipe/semicolon. |
| `parsing.quotechar` | Text qualifier: `'"'` for a double quote, `"'"` for a single quote, `"@@"` for a custom token, or `"none"` to disable quoting. `auto` means the conventional double quote; it does not discover arbitrary qualifiers. |
| `parsing.doublequote` | `true` decodes doubled qualifiers inside quoted fields; `false` disables that convention. |
| `parsing.escapechar` | `"none"` by default, or an explicit escape token. For a literal backslash, use TOML `escapechar = '\'`. Do not enable escapes merely because values contain backslashes. |
| `parsing.record_terminator` | `auto` recognizes CR/LF/CRLF; otherwise an explicit token, e.g. `"\r\n"` or `"<END>"`. Embedded quoted newlines are allowed by default. |

Field separator, qualifier, escape and explicit record-ending tokens support 1..32 Unicode
characters, subject to unambiguous token/prefix rules. Invalid combinations are rejected.
For example, `1,"Smith, Jane","said ""hello"""` has three fields, with the comma inside
`Smith, Jane` preserved and the last value decoded to `said "hello"`.
Ordinary quoting/multiline behavior is tested in `tests/test_independent_parser.py`;
custom qualifiers/escapes and token boundaries are tested in `tests/test_parser_tokens.py`.
Actual SQL bcp/ODBC token cases and multi-GB token loads are recorded in
[the format guide](token-delimited-exports.md) and [test evidence](testing.md).

Glob examples: `"*.csv"` selects root-level CSV names; `"**/*.csv"` selects CSV names at
root and below; `"folder_a/**/*.csv"` limits selection to that branch; `"**/*"` includes
all filenames, including extensionless files (unsupported content still fails inspection).
Globs are **case-sensitive even on Windows**: add `"**/*.CSV"` for uppercase extensions.
Use `/` within globs. Lists replace earlier/default lists; they do not extend them.

### Importing multiple files: root versus path

**For the normal case, set `root` and omit `path` (or leave it empty).** `path` accepts a
folder containing many files; it does not mean you must name each file individually.
The initial quickstart selects one bundled file only to make the first run easy to verify.

`root` answers **"What is the stable base folder for naming these exports?"**
`path` answers **"Which file or folder under that base should this run read?"**
Keeping root fixed lets you import one branch later without changing the relative names
used to map those files to SQL tables.

| What you want to import | `root` | `path` |
| --- | --- | --- |
| All CSVs under Exports | `D:\Exports` | `D:\Exports` |
| Only the folder_a folder and its CSVs | `D:\Exports` | `D:\Exports\folder_a` |
| Only one CSV | `D:\Exports` | `D:\Exports\folder_a\records.csv` |

**You can now omit `path`.** An omitted path or `path = ""` inherits the final root,
including a `--root` override. TOML has no `null` literal: use omission or an empty string,
not `path = null`. A nonempty path never silently falls back if it is missing or outside root.
For the normal folder import, just configure root; explicit `path = root` also works.

An existing configuration with an explicit path keeps that selection. To adopt inheritance,
remove that line or set it empty. Do not change a saved run's selection during recovery:
resume compares the resolved paths and refuses changed selections. For a legacy run whose
omitted path resolved differently, retain its original explicit path when resuming.

Copy this `[source]` section for a multiple-file directory import:

```toml
[source]
root = 'D:\Exports'
# path omitted: import this root folder
include = ["**/*.csv", "**/*.CSV"]
exclude = ["**/archive/**", "**/*.tmp"]
recursive = true
format = "delimited"
```

To select just one file, keep that root, set `path = 'D:\Exports\folder_a\records.csv'`
and use `include = ["folder_a/records.csv"]` (or another matching glob).
`include` filters the selection; it never replaces `path` or enables recursion by itself.
The [complete parameter reference](configuration-reference.md) documents every
remaining field, default, accepted value and capability limit.

### AWS RDS for SQL Server example

Use [examples/ingest.rds-sqlserver.toml](../examples/ingest.rds-sqlserver.toml) as a complete
starting configuration, or replace the sample's `[destination]` section with:

```toml
[destination]
# Illustrative endpoint: replace with your RDS instance's actual Endpoint and Port.
server = "imports-sql.abcdefghijkl.us-east-1.rds.amazonaws.com,1433"
database = "LegacyLanding"
control_database = "IngestControl"
authentication = "sql"
create_databases = false
create_schemas = true
create_tables = true
encrypt = "yes"
trust_server_certificate = "no"
```

Have the DBA provision both databases and the required account permissions. The client
must have network access to the instance and trust the appropriate RDS CA certificate;
use the actual endpoint so certificate-name validation can work. Follow AWS's
[endpoint/port instructions](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ConnectToMicrosoftSQLServerInstance.html)
and [SQL Server certificate guidance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/ssl-certificate-rotation-sqlserver.html).
This is a configuration example, **not an executed RDS integration test**. Keep passwords
out of TOML. With RDS as destination, imported values and audit/reject data go to AWS;
captures remain in the configured local work directory.

**The append sample does not truncate a database or clear existing target tables.**
Creating missing databases/schemas/tables does not authorize replacement. The importer
writes its metadata and staging objects as part of normal operation. Explicit replace,
advanced schema-evolution or mapped-business policies are separate reviewed workflows;
this assurance applies to the sample's append/owned-landing defaults.

Use a durable work location with enough free space for captures, journal and staging;
do not put it in a temporary/automatically cleaned folder. The default minimum free-space
reserve is 2 GiB, which is **not** a complete capacity estimate. Keep this directory and
its configuration together when backing up or recovering a run.

Keep `encrypt="yes"` and `trust_server_certificate="no"` for a trusted server certificate.
For validation errors, ask the DBA to fix the server name/trust chain. Our disposable
localhost tests explicitly trust their self-signed certificate; that is a separate lab setting.

Named `DB_*` environment variables override TOML (for example `DB_SERVER`,
`DB_DATABASE`, `DB_ENCRYPT`). Before starting, check names without exposing their values:
`Get-ChildItem Env:DB_* | Select-Object Name`. Remove unintended overrides in this session
with `Remove-Item Env:DB_SERVER` (substitute the actual name), or start a clean session.
Keep intentionally supplied credential variables. The reference lists every recognized name.

Use `--username loader` or `DB_USERNAME`; **there is no username prompt**.
`--password-prompt` reads a password without echo. Automation can use `DB_PASSWORD` or
`--password-stdin`. Username/password TOML keys are not accepted. Avoid password arguments.

## 3. Check, plan, review, import

Replace `loader` below with your SQL username. Stop on any nonzero exit code.
The commands print JSON or a terminal summary; `--json` forces JSON for saved reports:

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest.toml' doctor
if ($LASTEXITCODE -ne 0) { throw 'Stop: review doctor output.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' test-connection --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: fix the connection before continuing.' }
# Give every plan its own file so an older plan cannot be mistaken for this attempt.
$PlanPath = '.\.local\plan-' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff') + '.json'
$PlannedRun = $null
$PlanReport = $null
& $IngestPython -I -m db_ingest --config '.\ingest.toml' plan --output $PlanPath
$PlanExit = $LASTEXITCODE
if (Test-Path -LiteralPath $PlanPath) {
    $PlanReport = Get-Content -LiteralPath $PlanPath -Raw | ConvertFrom-Json
    $PlannedRun = $PlanReport.run_id
}
if ($PlanExit -ne 0) { throw 'Stop: inspect the plan/error output; do not import this attempt.' }
if (-not $PlannedRun) { throw 'Stop: no run ID was saved.' }
if (@($PlanReport.files).Count -eq 0 -or $PlanReport.files_report_truncated) {
    throw 'Stop: no complete file preview; inspect the retained run.'
}
if (@($PlanReport.files | Where-Object { $_.counts.rejected -gt 0 -or $_.counts.unresolved -gt 0 }).Count -gt 0) {
    throw 'Stop: this plan contains rejected/unresolved records; inspect show-errors.'
}
Write-Output "Plan: $PlanPath ; run: $PlannedRun"
```

Open the file printed as `Plan` (or `notepad $PlanPath`). Check `files[0].artifact` for
the source, `files[0].plan` for destination and ordered column types, and `files[0].counts`
for accounting. This sample configuration fixes append mode; do not change it after review.
A plan can exit 0 with quarantined rows: it successfully planned those rejections. The
PowerShell guard above stops that case; never treat plan exit 0 as proof of clean input.
Expect one header and **two data rows**, no rejects/unresolved
spans, and IDs `001`/`002` kept as text. Planning captures/profiles the source without
importing into SQL. Doctor is read-only; it does not prove every SQL write privilege.

Execute that reviewed run:

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest.toml' resume --run-id $PlannedRun --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: retain work and review this run.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' status --run-id $PlannedRun
if ($LASTEXITCODE -ne 0) { throw 'Stop: inspect the reported run state.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' reconcile --run-id $PlannedRun --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: reconciliation needs review.' }
```

Expect `CLEAN_VERIFIED`, two committed rows and zero rejected/pending/unresolved rows.
Keep the run ID and complete work directory. A directory import is not globally atomic:
one file may commit before another fails. Never interpret a partial result as full success.

## 4. Use your exports

Change `source.root` and `source.include` to select your exports; omit/empty `source.path`
to use root, or set it to select a particular file/subfolder; keep root
stable between related runs. Set the producer's actual format, then repeat the small-file
check/plan/review/resume process before a large import. Types are inferred from the full
accepted stream. Use column rules when a known schema should reject invalid values rather
than accept a broader inferred text type.

| Situation | Setting/behavior |
| --- | --- |
| No header | `header="absent"`; first row retained. Labels `column_1`, `column_2`, etc.; default SQL names include collision-protection suffixes. |
| Header unknown | `header="auto"` currently pauses for a choice; it does not classify headers automatically. |
| Incomplete names | Supply ordered `column_names`; the raw header remains. Missing positions are not guessed. |
| Encoding unknown | `encoding="auto"`: UTF BOM detection, otherwise strict UTF-8. No legacy-codepage guessing. |
| Legacy/EBCDIC | Set the producer's codec, e.g. `cp1252`, `cp037`, `cp500`, `cp1140`; see NEL/binary-layout limits in the format guide. |
| Delimiter unknown | `delimiter="auto"` considers comma/tab/pipe/semicolon; ambiguity requires a choice. |
| Row endings | `record_terminator="auto"` handles CR/LF/CRLF. Custom literal endings and field controls support unambiguous 1..32-character tokens. |
| One physical line per record | `allow_multiline_fields=false` only when the producer forbids multiline fields; unexpected quoted newlines leave an unresolved tail. |
| Unequal widths | Rows quarantined without padding/truncating columns. Same-width semantic shifts need schema/business validation. |

## 5. Stop and recover correctly

| Exit | Meaning / action |
| --- | --- |
| 0 | Command succeeded. For import/resume/reconcile, check `CLEAN_VERIFIED` and counts; plan/doctor/status-list do not import data. |
| 2 | Configuration/decision needed; review before retrying. |
| 3 | Partial or reduced assurance; inspect rejects and verification mode. |
| 4 | Failed/interrupted; preserve work and inspect the incident. |
| 5 | Unresolved framing/reconciliation/commit/accounting; investigate before replay. |

If the PowerShell window closed, restore the project and installed interpreter first.
List retained runs; choose the ID matching the source/time of the failed attempt, then
paste that ID in place of `PASTE_RUN_ID`. Do not choose a different run simply because it
has a clean state. Use the original configuration/work directory for that run.

```powershell
Set-Location 'C:\Projects\db-ingest' -ErrorAction Stop
$IngestPython = (Resolve-Path '.\.venv-run\Scripts\python.exe' -ErrorAction Stop).Path
& $IngestPython -I -m db_ingest --config '.\ingest.toml' status
$PlannedRun = 'PASTE_RUN_ID'
& $IngestPython -I -m db_ingest --config '.\ingest.toml' status --run-id $PlannedRun
& $IngestPython -I -m db_ingest --config '.\ingest.toml' show-errors --run-id $PlannedRun
```

`status --run-id` can return the **stored run's nonzero exit code while successfully
showing its status**. Read its report. `show-errors` returns incident codes, affected
records and byte spans; its exit 0 means the inspection succeeded, not that the load is clean.
If configuration validation failed before a run was created, fix that error first; there
may be no new run ID or plan file. Retain any older plan files as separate attempts.

| Observed problem | Next action |
| --- | --- |
| `CONFIG_*`, unknown setting, TOML syntax | Check section/key spelling, quoted strings and UTF-8 file name. Never put passwords in TOML. |
| Missing Python/module/ODBC driver | Verify `$IngestPython`, reinstall into a fresh environment if needed, and install the x64 ODBC 18 driver. Run `doctor` again. |
| Missing source or insufficient disk | Fix the named path/storage issue; keep source root/selection and all retained evidence. Run `doctor` again. |
| `AUTHENTICATION`, `CREDENTIALS_REQUIRED` | Correct the username/password, test the connection, then resume the **same** run. |
| `TLS`, database access, permission denied | Ask the DBA to fix the reported certificate/database/permission issue. Keep the original security policy; rerun the connection check. |
| SQL resource errors (such as [17300](https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-17300-database-engine-error) in the server log), unresponsive server, or login/query timeout | Ask the DBA to inspect SQL logs, memory/session limits and storage. Repair server health, test the connection, then resume the original run using the steps below. A timeout alone does not identify the cause. |
| `PLANNING_INCOMPLETE` / interrupted capture | See [capture recovery](smb-outage-recovery.md). Nonempty incomplete captures block with `CAPTURE_RETRY_UNVERIFIED` (exit 5); complete snapshots retain original bytes. |
| Rejects, `HEADER_AMBIGUOUS`, `DIALECT_AMBIGUOUS`, unknown row boundaries | Stop and inspect producer format/schema and byte spans. Resuming unchanged parsing does not repair bad input. See the correction rules below. |
| Missing receipts, changed target, unknown commit, integrity mismatch | Preserve everything and escalate with run ID/artifacts. Do not create a new intent or edit journal/receipt data. |

Importer memory limits do not allocate or limit SQL Server memory. Batch settings bound
transfer chunks; SQL staging, transaction logs and ordered verification still process the
full file. Ask the DBA to size memory, log and tempdb space for that workload. A successful
connection check does not prove capacity for the full import.

For a transient login/server/storage problem, after fixing the cause **without changing
source selection or interpretation**, use the original configuration and run ID:

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest.toml' test-connection --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: connection still needs repair.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' resume --run-id $PlannedRun --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: retain work; inspect this run again.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' reconcile --run-id $PlannedRun --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: reconciliation needs investigation.' }
```

Resume resolves receipts before replay; it must not add a second copy of an already
acknowledged import. Never delete captures/journal or use `--new-intent` to bypass an
error. Changing parsing/types/NULL rules requires a reviewed, linked `reimport`, which
**can publish immediately**; involve an experienced engineer before that correction.
Changing source bytes creates a different source, not a repair to the retained capture.
Do not edit captured files. Do not switch append to replace as a troubleshooting step.

For known rejected rows, export the complete retained rejects to a **new** file:
`& $IngestPython -I -m db_ingest --config '.\ingest.toml' export-failures --run-id $PlannedRun --output '.\.local\rejects-UNIQUE.jsonl'`.
The export contains source values; protect it like the source. It does not make an
unresolved tail's row count known. Escalate with configuration (no secrets), run ID,
`status`/`show-errors` output, and the complete work directory kept in place; share source
or reject payloads only through approved channels.

## Creation flags and destination names

`create_databases`, `create_schemas` and `create_tables` allow creating **missing**
objects; they do not recreate or empty existing ones. Existing table rows follow
`load.mode`: `append` preserves earlier rows; `replace` explicitly replaces target data.
Names come from `destination.*` and `mapping.*`, not from the creation flags. See
[creation behavior, exact naming examples and live verification](creation-and-naming.md).
Review the plan before publishing to see the actual database, schema and table names.

### Table names: filename-only by default

`orders.csv` now becomes `orders` in your configured database/schema. The example config
states the default explicitly:

```toml
[mapping]
name_collision = "filename_error"
```

**Two files that map to the same table cause an error, not a merge or overwrite.** This
includes duplicate basenames in different subfolders, case variants and names that
normalize identically. Persistent claims also protect separate and concurrent runs.
For example, `north/orders.csv` and `south/orders.csv` cannot both use `orders`.
Omitting `name_collision` also selects filename-only; it does not disable collision checks.

For `TABLE_NAME_COLLISION`, inspect `show-errors` and the conflicting paths. Keep the failed
run/configuration and any already published tables. Choose distinct source basenames, a
separate destination schema/database, or the hash option below. Save that choice in a
**separate configuration file**, then repeat check/plan/review with `--config` pointing to
that file before importing. This creates a reviewed mapping for different targets; it does
not repair or rename an existing table. Do not change mapping settings while resuming an
old run, delete claims, or use `--new-intent` to bypass protection. If a directory run partly
published, reconcile those original intents before deciding what to import next.
`TABLE_NAME_CLAIM_MISSING` needs investigation of table ownership; do not adopt it by guessing.

To keep path-derived names with stable hash suffixes, change the value in your existing
`[mapping]` section:

```toml
name_collision = "stable_hash_suffix"
```

**Upgrading an older hashed import:** set that hash option explicitly before planning,
resuming or starting another import against its existing tables. An omitted option now
means filename-only. Existing tables are not renamed or migrated; a resume with a changed
mapping is blocked. Do not use a new intent to bypass that error.

Directory merging, any multi-database strategy and mapped-business targets require
`stable_hash_suffix` explicitly. See [naming rules, examples and migration limits](filename-table-names.md).

## Logs and statistics

Use [logging and statistics](logging-and-statistics.md) for copy/paste CLI commands
and tested SQL queries: run/file counts, rejected records, errors, events, phase timings,
column profiles and publication receipts. It explains which database to select, custom
audit schemas, unknown counts and audit-mirror delays.

## Optional configuration and expansion

- [Full field/value reference](configuration-reference.md): defaults, allowed values,
  restrictions, per-file/column rules, credential sources and precedence.
- [75 messy examples/encodings](messy-data-and-encodings.md),
  [custom tokens and row boundaries](token-delimited-exports.md).
- [bcp, batches and file parallelism](usage.md#loaders-and-resource-settings).
  bcp needs Microsoft's optional tools and intentional SQL-auth child-argument permission.
  Row/byte caps both apply; memory admission can reduce worker count.
- [Business tables, groups, replacement and projections](usage.md): advanced workflows
  requiring review, not prerequisites for a landing-table import.
- [Data locality](data-locality.md): no AI/cloud-upload client; configured SQL receives
  values and audit/reject data. Local paths/local SQL keep that workflow local.
- [Configuration test matrix and deployment checks](testing.md), [evidence](testing.md).

Before handing off an installed release, run the read-only deployment gate. It checks
Windows/Python, installed source parity, configuration, driver, filesystem observations
and explicit bcp prerequisites. It creates only its output report:

```powershell
& $IngestPython -I '.\scripts\check_deployment.py' --config '.\ingest.toml' --source-root '.' --output '.\.local\deployment.json'
if ($LASTEXITCODE -ne 0) { throw 'Stop: review .local/deployment.json before loading.' }
```

Run the earlier `test-connection --username loader --password-prompt` step with the actual
operator account too. A preflight cannot establish workload capacity or recovery behavior.
See [testing](testing.md) for regression and disposable-SQL checks.
