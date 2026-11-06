# Inspect logs, statistics and failures

Start with the CLI when you have the original work directory. Use SQL for shared reporting
or when inspecting the server from SSMS. SQL audit data is a mirror and can lag during an
outage; a missing audit row is not proof that nothing was committed.

## Local status and errors

From the project folder, using your installed interpreter and original configuration:

```powershell
$IngestPython = (Resolve-Path '.\.venv-run\Scripts\python.exe').Path
& $IngestPython -I -m db_ingest --config '.\ingest.toml' status
$RunId = 'PASTE_RUN_ID'
& $IngestPython -I -m db_ingest --config '.\ingest.toml' status --run-id $RunId --json
& $IngestPython -I -m db_ingest --config '.\ingest.toml' show-errors --run-id $RunId --json
```

`status --run-id` returns the stored run's exit code, which can be nonzero even when
inspection succeeds. `show-errors` returning 0 means the inspection worked, not that the
import succeeded. To export all retained rejects to a **new** output file:

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest.toml' export-failures --run-id $RunId --output '.\.local\rejects-UNIQUE.jsonl'
```

Keep the output protected like the source; it contains source values. For recovery use
the README's same-run `resume`/`reconcile` steps. Do not repair logs, receipts or the local
`journal.sqlite3` manually.

## SQL examples: choose the right database

1. Connect SSMS to `destination.server` with an account that can SELECT the audit tables.
2. Choose **`destination.control_database`** in the database dropdown.
3. Open [logging-queries.sql](../examples/logging-queries.sql). If `audit.control_schema`
   is not `audit`, replace every `[audit]` with your bracket-quoted schema name.
4. Set `@RunId = N'your-run-id'`, or leave NULL to inspect the most recently updated
   mirrored run. Keep the whole script together: its variables apply to all ten queries.
5. Execute. Each query produces a separate Results grid. `@Limit` defaults to 100;
   detail grids are previews. The grouped error counts cover the whole selected run.

The control database uses the same configured server as the destination. These examples
perform reads only and do not create tables or databases. They require control schema
version 5 and SQL database compatibility level 130 or higher for OPENJSON.

| Grid | Information |
| --- | --- |
| 1 | Recent run IDs, states and last mirror-update timestamps |
| 2 | Selected run: files, source bytes, physical lines, logical records, committed/rejected/pending records, unresolved spans, error events and sampled peak memory |
| 3 | Per-file counts, state, intent, target, capture path/hash, assurance and generated failure-table reference |
| 4 | Error codes ranked by event count, with distinct affected-record counts |
| 5 | Rejected records, source ordinal/byte spans, ordered field values and column diagnostics |
| 6 | File/run incidents, including failures unrelated to one record |
| 7 | Recent events and complete event details |
| 8 | Per-file phase duration, attempts and successful attempts |
| 9 | Mirrored publication receipts, committed counts, verification assurance and digests |
| 10 | Original/target column names, SQL types and profile JSON, keyed by interpretation |

For the destination's own receipts, switch the SSMS database dropdown to
**`destination.database`**, then run
[destination-receipt-queries.sql](../examples/destination-receipt-queries.sql).
Set its `@RunId` to the same ID. Both databases use `audit.control_schema` for these
objects. A receipt lookup helps inspection; CLI `reconcile` also checks identities and
integrity and is required before resolving uncertain commits.

## Reading the numbers correctly

- NULL means unknown or unobserved. Do not replace it with zero in dashboards. An
  unresolved span can contain an unknown number of rows.
- Physical lines differ from logical CSV records when quoted values contain newlines.
  Headers/blanks are not committed data records.
- One rejected record can have several errors. Do not sum distinct-record counts across
  error categories or count the joined error-detail rows as rejected records.
- Committed counts describe a publication. They are not the current number of rows in a
  target after later appends/replacements. Grouped imports have one publication receipt;
  joining that receipt to every contributing file can multiply totals.
- Phase times include retries and failed attempts; concurrent file phases overlap. Their
  sum is not run wall-clock duration. No reliable run rows/second is inferred here.
  The memory number is a sampled run/process-tree peak, not an absolute or per-file bound.
- `updated_at_utc` is the last mirror update, not the run's start/end time. A clean state
  in an old snapshot does not establish the current health of the destination.
- Profiles describe an interpretation/source candidate; shared targets may use a wider
  canonical schema. Keep `interpretation_hash` when comparing profiles.

## If a query fails or shows nothing

| Symptom | Check |
| --- | --- |
| Invalid object `audit.Runs` | Control database selected? Custom `audit.control_schema`? Has mirroring created the tables? |
| Invalid object `__db_ingest_receipts` | Destination database selected? Correct schema? Has a publication initialized the receipt table? |
| Permission denied | Ask the DBA for SELECT on the required audit objects; do not change loader permissions to sysadmin. |
| OPENJSON unavailable | Check database compatibility level with the DBA; it must be at least 130. |
| No matching mirrored run / missing incidents | Check the run ID and local `status`/`show-errors`. Mirroring may be disabled, failed or pending; database connectivity alone does not backfill it. |
| No rejected rows in SQL | There may be none, or failure-table materialization is disabled/failed/pending. Check local errors and export-failures. |

