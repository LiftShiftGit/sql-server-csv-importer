# Recovering a disconnected or changed source share

Keep the original run ID, configuration and entire local work directory. A partial
capture cannot prove the identity of bytes that were never read. This release replaces
the earlier policy of automatically recapturing incomplete exports.

| Retained source state | Recovery behavior |
| --- | --- |
| Opening failed before any bytes were retained | Restore access, then resume the same run with `--plan-only` and review the plan. |
| Nonempty incomplete capture, without a verified complete snapshot | Exit 5, `CAPTURE_RETRY_UNVERIFIED`. No automatic recapture or publication under that intent. |
| Verified complete local capture | Resume uses the original local snapshot, even if the share is down or its export has changed. |
| SQL publication already committed | Same-run resume/reconcile checks receipts instead of appending those rows again. |

Unchanged size, timestamps and a matching captured prefix are insufficient evidence:
the unread tail may differ. Even restoring the original file does not automatically
clear an incomplete-capture block. Earlier nonempty attempts also remain relevant.
Invalid capture identifiers produce `CAPTURE_RETRY_EVIDENCE_INVALID`; preserve evidence
and ask support to investigate rather than editing the journal.

## Inspect and recover a verified snapshot

Restore the README's `$IngestPython` variable in the release folder. Keep the original
configuration and work directory. Use your original run ID:

```powershell
$RunId = 'PASTE_ORIGINAL_RUN_ID'
& $IngestPython -I -m db_ingest --config '.\ingest.toml' status --run-id $RunId
& $IngestPython -I -m db_ingest --config '.\ingest.toml' show-errors --run-id $RunId
& $IngestPython -I -m db_ingest --config '.\ingest.toml' resume --run-id $RunId --plan-only
if ($LASTEXITCODE -ne 0) { throw 'Stop: retain this run and follow the blocked-capture guidance below.' }
```

Status may return the stored failure code while correctly displaying the report.
Review counts, destination, types and rejected/unresolved records as in the README.
For a verified snapshot, publish and reconcile the same run (replace `loader` with
your account, or use the configured integrated-authentication workflow):

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest.toml' resume --run-id $RunId --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: inspect the original run.' }
& $IngestPython -I -m db_ingest --config '.\ingest.toml' reconcile --run-id $RunId --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: reconciliation needs investigation.' }
```

Expect CLEAN_VERIFIED and the intended committed count. A replacement export on the
share is not loaded by this recovery; importing it is a separate decision.

## If CAPTURE_RETRY_UNVERIFIED blocks recovery

1. Retain the failed run, configuration, partials, logs, journal and receipts. Do not
   delete/edit evidence, truncate tables or use `--new-intent` to clear the incident.
2. Ask the producer and DBA to review the complete export now available, its expected
   contents/counts and which files already committed. Directory imports can have
   committed sibling files. Unknown original bytes cannot be reconstructed from a partial.
3. Only after approving the available export as a **separate import**, copy the config
   to a new filename (do not overwrite an existing reviewed config):

```powershell
if (Test-Path '.\ingest-reviewed.toml') { throw 'Stop: reviewed config already exists.' }
Copy-Item -LiteralPath '.\ingest.toml' -Destination '.\ingest-reviewed.toml' -ErrorAction Stop
notepad '.\ingest-reviewed.toml'
```

Keep `source.root` and `preservation.work_directory` unchanged to retain local history.
Set `source.path` to only the affected approved file's full path; exclude already
committed siblings. Do not change naming/targets to bypass existing claims. Freeze the
producer's export while capturing. Verify share access using the same Windows account;
server startup alone does not prove the Windows client reconnected.

4. Follow the README's plan/review workflow using `--config '.\ingest-reviewed.toml'`:
   run `plan`, inspect its new run ID, counts/types/destination, then `resume` and
   `reconcile` that explicitly reviewed new run. Do not add `--new-intent`. Existing
   duplicate/target protections still apply; investigate any refusal with the DBA.
   This is a new reviewed import, not recovery of the unknown original content.
   The original incomplete run remains blocked and retained for audit.

## Verified behavior

Actual connections to an isolated authenticated SMB server were broken and restored
from a Windows client. Both ODBC and bcp were tested:

- Mid-capture: changed an unread tail while preserving file size and timestamp.
  Reconnection and repeated retries produced exit 5, no target table and no receipt.
  Retained original prefixes and intent IDs were unchanged. Restoring original bytes
  still required review.
- After complete capture: replaced the export while SMB was down. Loading and repeated
  resume/reconciliation, offline and after reconnect, retained exactly the original
  1,024 rows and one receipt per intent for each loader. Replacement data was not loaded.
- A source opening failure with zero captured bytes recovered under the original intent:
  exactly two rows and one receipt after repeated resume/reconciliation.
- Local regression tests cover changed prefixes, unread tails, replacement, append,
  truncation, legacy earlier partial attempts and complete orphan capture recovery.

This verifies these cases on the disposable SMB/SQL setup. It does not certify every
NAS/DFS implementation, timeout, simultaneous disk/SQL failure or power loss. Preserve
local storage and test the production share/account and operational restore procedure.
