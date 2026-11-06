# SQL Server CSV Importer

Import a folder of CSV files into SQL Server on Windows, with inferred column types,
bounded bulk loading, audit logs and recovery for interrupted imports.
CSV and other delimited text are the main workflow; XML requires explicit mappings and
an optional dependency. The Python package and command remain `db_ingest`.

**[Start a one-off import](#import-a-directory-once)** ·
**[Choose an example](docs/directory-import.md)** ·
**[Every setting and accepted value](docs/configuration-reference.md)** ·
**[Troubleshoot and recover](docs/recovery.md)**

With the recommended preset:

```text
Exports/orders.csv        -> dbo.orders
Exports/north/orders.csv  -> dbo.north_orders
Exports/south/orders.csv  -> dbo.south_orders
```

Existing tables and naming collisions stop for review. Original bytes and rejected
records remain available for investigation. See [verification results and limits](docs/testing.md)
for the tested multi-GB and recovery scenarios.

## Requirements

- Windows x64 and standard CPython 3.13 x64 (tested with 3.13.2).
- Microsoft ODBC Driver 18 for SQL Server and a reachable SQL Server 2019 or later.
- A local, durable work directory with space for captures and staging.

The Python package accepts Python 3.12+, but the tested Windows/offline distribution targets
standard Python 3.13 x64. Newer, ARM64 and free-threaded interpreters need separate validation.
Use `py -3.13` even if a newer Python is installed; install standard Python 3.13 x64
alongside it if that command is unavailable. Python and the ODBC driver are installed separately.
CSV runtime has one Python dependency:
`pyodbc`. XML support optionally adds `defusedxml`. Native bcp is optional.

SQL Server needs its own memory and database/log/tempdb capacity; importer batch limits
do not cap server resource use. Ask your DBA to size the workload before large imports.
See [tested SQL capacity and recovery steps](docs/recovery.md).

## Install from source

In PowerShell, change to the checked-out repository directory, then run:

```powershell
py -3.13 -m venv .venv-run
if ($LASTEXITCODE -ne 0) { throw 'Virtual environment creation failed.' }
$IngestPython = (Resolve-Path '.\.venv-run\Scripts\python.exe').Path
& $IngestPython -m pip install .
if ($LASTEXITCODE -ne 0) { throw 'Installation failed.' }
```

Use a fresh environment for a new release; do not update an environment used by an active
import. Source installation needs package-index access. For disconnected machines, use a
maintainer-provided offline bundle and its packaged README; native installers remain separate.
Continue below for one-off imports. The [operator guide](docs/operator-guide.md) covers
expanded setup and recurring-feed workflows; its general sample uses append mode.

## Import a directory once

**Start with `examples/directory-paths.toml` for a one-time directory import.**
The commands below copy this preset to `ingest.toml` and always use it explicitly.

| Your goal | Configuration to copy | Table names |
| --- | --- | --- |
| One table per CSV, with folders distinguishing names (recommended) | `examples/directory-paths.toml` | `north/orders.csv` becomes `north_orders` |
| One table per CSV, using only its filename | `examples/directory-filenames.toml` | `orders.csv` becomes `orders`; duplicate names stop |
| Combine the immediate CSV files in each folder | `examples/directory-merge.toml` | `north/part1.csv` + `north/part2.csv` become `north` |

**All three directory presets use `load.mode = "create_only"`.** They refuse existing
target tables; they do not append to or replace them. Recovery of the original run is
allowed. Naming collisions stop for review; nested folders stay separate.

**Do not use `ingest.example.toml` for this one-off workflow.** It uses `append` for
recurring feeds. The built-in load mode is also `append`, so keep the preset's explicit
`mode = "create_only"` line. Append can add new imports to an existing compatible table;
it does not deduplicate arbitrary source rows.

### What the recommended preset does

| Setting | Starting behavior |
| --- | --- |
| File selection | All `.csv` files under `source.root`, including subfolders; omitted/empty `path` uses that root |
| CSV format | UTF-8, comma separator, double-quote text qualifier (`"`), header row present; CR/LF/CRLF row endings detected automatically |
| Database setup | Uses your existing landing/control databases; creates missing schemas and tables when permitted |
| Loading | ODBC; up to two file workers; batches capped at 5,000 rows or 8 MiB, whichever comes first |
| Types and verification | Full-file type inference and full verification |
| Bad data | Malformed rows are quarantined; unknown row boundaries block the affected import; review all rejects |
| Recovery | Original captures and journals retained indefinitely in your local work directory |

These are the **recommended preset's effective settings**, including inherited defaults.
The general example instead enables format detection, database creation and automatic
loader selection, and uses filename-only naming and append mode.
For different exports, edit the parsing settings using the
[configuration reference](docs/configuration-reference.md); ambiguous detection requires review.

```powershell
if (Test-Path '.\ingest.toml') { throw 'Keep the existing configuration; choose another filename.' }
Copy-Item '.\examples\directory-paths.toml' '.\ingest.toml' -ErrorAction Stop
notepad '.\ingest.toml'
```

Set the SQL server, existing landing/control database names, source root and local work
directory. These presets do not create databases; ask your DBA to create them and grant
the [required permissions](docs/sql-support.md). Confirm
the header, encoding, delimiter and quote settings match your exports. Do not store passwords
in the config. The control database uses the same server and credentials as the landing database.
Replace `loader` in the commands below with your SQL login; passwords are prompted securely.

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest.toml' doctor
if ($LASTEXITCODE -ne 0) { throw 'Fix the reported prerequisite or configuration problem first.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' test-connection --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Ask the DBA to resolve the connection or permission error.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' plan
if ($LASTEXITCODE -ne 0) { throw 'Review planning errors before loading.' }
$RunId = 'PASTE_RUN_ID_FROM_PLAN'
& $IngestPython -I -m db_ingest --config '.\ingest.toml' export-mapping --run-id $RunId --output '.\file-table-map.csv'
if ($LASTEXITCODE -ne 0) { throw 'Mapping export failed; choose a new output filename if it exists.' }
```

Before loading, confirm the plan reports `create_only`. If it reports `append` or `replace`,
stop and correct the configuration, then plan again. Review the mapping CSV, inferred types,
row counts and rejects. Then load the saved run
(replace `loader` with your SQL login):

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest.toml' resume --run-id $RunId --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Keep the original run and inspect status/show-errors.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' reconcile --run-id $RunId --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Reconciliation needs investigation.' }
```

A directory import is not one global transaction: each file or merged folder publishes
independently. Keep the source root, configuration, work directory and original run ID for
recovery. Do not use a new intent or switch to replace mode to clear an error.

## If an import stops

Keep the original configuration, run ID and entire work directory. Do not switch to
append/replace, delete recovery files or create a new intent to clear an error.
Follow [diagnose, repair and resume](docs/recovery.md) for copyable commands, SQL capacity
checks and escalation steps. Source-share failures have a separate [recovery guide](docs/smb-outage-recovery.md).

## Documentation

- [Directory presets and sample files](docs/directory-import.md)
- [Every configuration field and allowed value](docs/configuration-reference.md)
- [CSV formats, encodings and messy data](docs/messy-data-and-encodings.md)
- [Windows local, UNC and long paths](docs/windows-paths.md)
- [Share outages and changed exports](docs/smb-outage-recovery.md)
- [Logging, statistics and sample SQL queries](docs/logging-and-statistics.md)
- [SQL permissions and supported types](docs/sql-support.md)
- [Diagnostics and recovery](docs/diagnostic-catalog.md)
- [Data locality](docs/data-locality.md)
- [Testing and limitations](docs/testing.md)

## Contributing and licensing

See [CONTRIBUTING.md](CONTRIBUTING.md) for tests and source packaging, and
[SECURITY.md](SECURITY.md) for vulnerability reports. Licensed under [MIT](LICENSE).
