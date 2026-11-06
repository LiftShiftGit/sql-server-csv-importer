# Diagnose a run

Start with the retained run ID. These commands read local evidence; `reconcile` also checks SQL Server.

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml status --run-id RUN_ID
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml show-errors --run-id RUN_ID
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml reconcile --run-id RUN_ID --password-prompt
```

An error contains a stable code, observed message and context. Provider errors additionally retain SQLSTATE, native code when supplied, operation and remediation. A timeout does not establish whether a publication committed. Keep the work directory, captures and journal together.

| Code or family | Meaning and next action |
| --- | --- |
| `CONFIG_*`, `COLUMN_SELECTOR`, `COLUMN_AMBIGUOUS` | Correct the named setting or source ordinal. Unknown options never become ignored defaults. |
| `CREDENTIALS_REQUIRED`, `AUTHENTICATION` (18456) | Supply prompt/stdin credentials; check the login failure with the DBA. Credentials are not retried as transient errors. |
| `TLS` | Check the certificate chain, validity and server name. Connection failures do not relax TLS settings. |
| `DATABASE_ACCESS` (4060), `PERMISSION` (229/262) | Check database visibility/state and the specific permission. An inaccessible database is not proof of absence. |
| `TIMEOUT`, `DATABASE` | Inspect the supplied endpoint/provider diagnostic. Resume resolves receipts before any replay. |
| `SOURCE_ACCESS`, `SOURCE_CHANGED`, `CAPTURE_IO` | Stabilize or restore access to the source. Incomplete captures remain retained; resume allocates a new capture attempt under the same intent. |
| `DISK_CAPACITY`, `STORAGE_CAPACITY` (1105), `LOG_CAPACITY` (9002) | Repair the named storage condition. Do not delete the only capture. SQL log/filegroup changes belong to the DBA. |
| `RESOURCE_BUDGET`, `RESOURCE_ADMISSION`, `QUEUE_BUDGET` | Reduce workers or batch rows/bytes and inspect the reported observation. Oversized individual records follow explicit preservation/quarantine limits. |
| `*_DECISION_REQUIRED`, `FILE_DECISION_REQUIRED` | Review the column/dialect evidence. Correct interpretation through a linked `reimport`; an existing intent cannot silently change source meaning. |
| `LENGTH_DECISION_REQUIRED`, `LENGTH_OVERFLOW_ABORT` | Choose `widen`, `retype_text`, `quarantine` or `abort` deliberately. Example: `reimport --run-id OLD_RUN --on-length-overflow widen --password-prompt`. |
| `UNRESOLVED_FRAMING` | The source contains an unknown record tail. Review the captured byte span and correct the format; a failed-row count cannot replace an unknown logical total. |
| `TYPE`, `RANGE`, `LENGTH`, `NULLABILITY`, `CONSTRAINT`, `UNIQUE` | Inspect the retained original and all column errors. Widen/correct the declared type through reimport, or explicitly quarantine. No default or NULL is invented. |
| `NEW_INTENT_REQUIRED` | Identical source meaning/mapping has acknowledged history. Resume the prior run, or use `--new-intent` only when a deliberate additional import is intended. |
| `DEADLOCK` (1205), `LOCK_TIMEOUT` (1222) | Bounded retry is available after outcome resolution. Inspect blockers if the configured attempts are exhausted. |
| `SERVER_MEMORY` (701), `SERVER_MEMORY_GRANT` (8645) | Inspect actual SQL resource pressure and staging concurrency. Client row/byte limits do not guarantee server query grants. |
| `*_UNSUPPORTED`, `*_UNAVAILABLE` | Follow the named capability boundary. Explicit bcp selection cannot silently substitute ODBC. |
| `ARCHIVE_*`, `LEDGER_*`, `PLAN_*`, `PROJECTION_*`, `*_MISMATCH`, `*_CONFLICT` | A retained artifact, record set or acknowledged value disagrees with its seal. Preserve all evidence and investigate; do not reseal edited data to force a pass. |
| `RECOVERY_*`, `GROUP_*HISTORY*`, `COMMIT_OUTCOME_UNKNOWN` | Required receipt, identity, baseline or contributor history is missing or changed. Preserve the original intent and repair/backfill authoritative evidence. Do not reset it to manufacture a retry. |
| `CANCELLED` | Admission stops and active units/subprocesses are resolved or terminated. Inspect status, then resume the same run. |

Exit codes: **0** clean import/successful command; **2** configuration or decision; **3** partial or explicitly reduced assurance; **4** failed/interrupted; **5** unresolved commit, integrity or reconciliation. A partial run may include valid committed files and separately retained rejects. `show-errors` shows a bounded recent incident preview; the journal retains complete events and record diagnostics.
