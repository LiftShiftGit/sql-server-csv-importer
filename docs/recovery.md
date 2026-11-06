# SQL capacity and import recovery

[Back to the quick start](../README.md)

Use this guide when a server or storage problem interrupts an import. For file-format
corrections, see the [operator guide](operator-guide.md); for network shares, see
[share recovery](smb-outage-recovery.md).

## SQL Server capacity

The importer and SQL Server have separate memory needs. Small batch sizes limit data
sent at once; they do not cap SQL Server memory, transaction-log growth or tempdb use.
Staging, publication and verification still process the full file.

Our 2.15 GB synthetic-file test hit a SQL resource failure with a 2 GiB SQL allocation.
The original import recovered successfully with a 6 GiB SQL allocation inside an 8 GiB
container, without duplicate rows. These are observed test settings, **not a universal
minimum**; the exact server failure cause was not isolated. See [test results](testing.md).

Before a large import, have your DBA confirm available SQL memory, database/log/tempdb
space and operating-system headroom for the expected file size, row width and concurrent
workload. Have IT provide the Python/ODBC installers, SQL endpoint, authentication and
TLS requirements. The directory presets expect the DBA to create `LandingDB` and
`ControlDB` (or your chosen names) and grant the [required permissions](sql-support.md).
Start with a small representative export, then validate a full-size export before
scheduling unattended imports. A successful connection check alone does not prove capacity.

## Recover the original run

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
[operator recovery steps](operator-guide.md) or [share recovery guide](smb-outage-recovery.md).
Never edit retained source captures to make recovery pass.

