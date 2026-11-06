# db_ingest

A Windows command-line tool for importing CSV and other delimited exports into SQL Server.
It preserves original bytes, validates inferred or configured types, records rejected data,
and uses publication receipts to recover interrupted imports without blindly replaying rows.

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

## SQL Server capacity

The importer and SQL Server have separate memory needs. Small batch sizes limit data
sent at once; they do not cap SQL Server memory, transaction-log growth or tempdb use.
Staging, publication and verification still process the full file.

Our 2.15 GB synthetic-file test hit a SQL resource failure with a 2 GiB SQL allocation.
The original import recovered successfully with a 6 GiB SQL allocation inside an 8 GiB
container, without duplicate rows. These are observed test settings, **not a universal
minimum**; the exact server failure cause was not isolated. See [test results](docs/testing.md).

Before a large import, have your DBA confirm available SQL memory, database/log/tempdb
space and operating-system headroom for the expected file size, row width and concurrent
workload. Have IT provide the Python/ODBC installers, SQL endpoint, authentication and
TLS requirements. The directory presets expect the DBA to create `LandingDB` and
`ControlDB` (or your chosen names) and grant the [required permissions](docs/sql-support.md).
Start with a small representative export, then validate a full-size export before
scheduling unattended imports. A successful connection check alone does not prove capacity.

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
See the [full setup and operator guide](docs/operator-guide.md) for prerequisites and recovery.

## Import a directory once

Choose one configuration from `examples/`:

| Configuration | Behavior | Example |
| --- | --- | --- |
| `directory-filenames.toml` | One table per CSV; duplicate basenames stop | orders.csv â†’ orders |
| `directory-paths.toml` | Folders distinguish tables using one underscore | north/orders.csv â†’ north_orders |
| `directory-merge.toml` | Merge immediate CSV files in each folder | north/part1.csv + north/part2.csv â†’ north |

These presets use `create_only`: existing tables are refused, while recovery of the original
run remains supported. The general `ingest.example.toml` uses append mode for recurring feeds.
Collision checks never silently merge unrelated sources. Nested folders stay separate.

```powershell
if (Test-Path '.\ingest.toml') { throw 'Keep the existing configuration; choose another filename.' }
Copy-Item '.\examples\directory-paths.toml' '.\ingest.toml' -ErrorAction Stop
notepad '.\ingest.toml'
```

Set the SQL server, landing/control databases, source root and local work directory. Confirm
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

Review the mapping CSV, inferred types, row counts and rejects. Then load the saved run
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

## Recover a failed import

1. Keep the original run ID, configuration, source selection and entire work directory.
   Do not delete captures, journals, staging tables or receipts, start a new intent, or
   switch to replace mode to clear an error.
2. In a new PowerShell window, return to the repository directory and set the variables
   below. Inspect status and errors; `status` can return the saved failure code while
   successfully displaying its report.

```powershell
$IngestPython = (Resolve-Path '.\.venv-run\Scripts\python.exe').Path
$RunId = 'PASTE_ORIGINAL_RUN_ID'
& $IngestPython -I -m db_ingest --config '.\ingest.toml' status --run-id $RunId
& $IngestPython -I -m db_ingest --config '.\ingest.toml' show-errors --run-id $RunId
```

3. For SQL resource errors, an unresponsive server or timeouts, give the DBA the run ID,
   error codes and time of failure. Ask them to check server logs, memory and storage
   capacity and restore server health while preserving the databases. A timeout by itself
   does not establish a memory problem. Do not change server memory settings blindly.
4. Once the cause is fixed, use the original configuration and run ID:

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest.toml' test-connection --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: the SQL connection still needs repair.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' resume --run-id $RunId --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: retain the work directory and inspect status/show-errors again.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' reconcile --run-id $RunId --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: reconciliation needs investigation.' }
```

Resume checks publication receipts before replaying work. Confirm `CLEAN_VERIFIED` and
expected counts; rejected rows require review even when accepted rows were published.
If recovery still fails, preserve the evidence and involve an experienced engineer.
Changed exports, incomplete captures and parsing corrections need the specific
[operator recovery steps](docs/operator-guide.md) or [share recovery guide](docs/smb-outage-recovery.md).
Never edit retained source captures to make recovery pass.

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
