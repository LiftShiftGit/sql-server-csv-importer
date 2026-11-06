# SQL adapter behavior and evidence scope

SQL Server is the implemented destination. The current client is Windows 11/Python 3.13.2 x64,
pyodbc 5.3.0 with explicitly selected Microsoft ODBC Driver 18. Actual SQL Server 2019/2022/2025 baseline
engines were tested separately; compatibility level was not substituted. Exact newer loader,
array, parallel, failure and restore scopes are in TEST_EVIDENCE.md and per-engine reports.
They are not a blanket platform/production certification.

## Transport and exact types

ODBC supports bounded row/byte groups. Guarded native arrays transport a small set of primitive
canonical columns; they do not entrust heterogeneous business-type descriptors with fidelity.
Long canonical rows are isolated into bounded ordinary executemany. Receipts report actual
native/fallback group/row counts and native stride estimates. The independent2,001-row MiB
outlier tests check every SQL value and observed process-tree memory; see native-fast-mib-resource.md.

Native Microsoft bcp uses an ASCII delimiter-safe versioned JSON/Base64 spool. Explicit state
vectors distinguish NULL/empty/value; encoded text is UTF16LE and binary retains exact bytes.
No arbitrary source CSV reaches bcp. Each isolated raw chunk is compared byte/ID-for-byte/ID
against its durable spool before typed staging. Wide layouts can use an explicitly reported
bounded client typed-decode route after raw verification. Complete typed stage and final target
readback both precede the receipt/publication commit. Partial child batches are reconciled from
actual staging, not tool row-count output; failure/retry rebuilds isolated attempts idempotently.

The tested SDK180 bcp reports17.0.4055.5 and advertises-Y/-u. Tool help/capabilities, not a numeric
major label alone, determine the adapter. Old bcp16 is not silently substituted for missing TLS
controls. SQL-auth argv delivery requires deliberate allow_password_in_child_args=true; integrated
-T has its own path. SQLCMDPASSWORD is for sqlcmd, not bcp. bcp-N/C and sqlcmd-N/C have different
meanings. No fallback is counted as an explicit bcp pass.

Supported exact families: bit, tinyint/smallint/int/bigint, decimal(1..38,scale), date,
time/datetime2/datetimeoffset precision 0..7, uniqueidentifier, NVARCHAR(n/MAX), and explicit
VARBINARY(n/MAX). Per-column formats and aliases resolve before validation. Default exact policies reject nonfinite,
overflowing, rounding, overlong or malformed values; no float intermediate, silent
truncation or conversion-to-NULL. Original lexical forms remain captured. SQL style126 preserves
local datetimeoffset clock/offset; observed style127 normalized UTC and is not the offset oracle.
Qualified VARCHAR requires one of the two explicit certified pairs: Latin1_General_100_BIN2_UTF8/utf-8 or Latin1_General_100_BIN2/cp1252. Actual codepage is checked; every scalar/native/bcp storage byte is independently verified in test_live_varchar_oracle.py. Unrepresentable values reject, byte overflow follows the explicit length policy, and unsupported pairs fail before capture. Deprecated/specialized types and arbitrary large streaming LOBs are not implicitly supported. The full advertised family oracle is tests/oracles/type_matrix.py.

## Publication and recovery

Importer-owned landing tables retain __ingest_intent/__ingest_record/__ingest_ordinal. Equal
source rows remain distinct. Columns map explicitly. Same-target application locks coordinate
importers; SERIALIZABLE plus target locks also protect the checked publication snapshot against
unrelated writers. Triggers, generated columns, IGNORE_DUP_KEY and unproven dependencies are
blocked rather than silently bypassed. Existing-target/evolution/group capabilities are tracked
separately in contract-deviations.md; per-file certification does not imply arbitrary business targets.

Append preserves prior rows. Replace stages/validates first and retains a previous-generation
data table, then deletes/inserts inside the target transaction. That copy retains data/lineage,
not a complete backup of every permission/index/default. Directory publication is not globally
atomic across files/databases. Per-directory merge/replacement needs complete contributor proof.

Final values are read in stable source ordinal order under the transaction, compared with a
versioned length/type/state-framed digest, and a unique destination-local receipt is inserted
before COMMIT. This is not COUNT-difference, SQL CHECKSUM or generated-ID return-order matching.
Independent tests separately read authored actual values; they do not use the app digest as oracle.

An interrupted intent resolves receipts under the same publication lock. If the receipt exists,
current values/identity are verified. If absent, replay needs zero current-intent rows plus live
owned object/schema/database/recovery-fork identity and complete local/central acknowledged
history agreement. A current or historical acknowledged receipt missing at the destination,
orphan rows, drift or an unobservable history blocks with5. The resolution proof is revalidated
under the new publication lock, with central history guarded through commit. No state reset or
new intent is used to hide a failed attempt.

Actual tests cover same-intent recovery of observed SQL701 failures, real128-row bcp partial
commit before injected129th-row collision, native1222 retry, postcommit/precommit process death,
and owned fresh-database backup/restore. In restore tests the old acknowledged fork mismatches
and the later acknowledged receipt is missing; neither is replayed. Shared databases were not
restored. More failover/log/disk/cancellation scenarios remain individually gated.

## Control, probes and diagnostics

Local SQLite/captures remain the recovery basis during SQL outage. Versioned central metadata
mirrors receipts, interpretations, profiles, schemas, chunks, artifacts, current rejects/errors,
incidents/events and summaries. Current control version3 adds normalized group/contributor metadata; future migration versions fail before mutation. Group behavior needs its separate publication evidence. Missing control
connectivity remains pending and idempotently backfills when access returns; no unavailable
SQL failure table is described as created. One rejected source record can have multiple errors.

Connection properties use braced escaping and parameterized metadata. Defaults are Encrypt=yes,
TrustServerCertificate=no. Explicit test-only loopback trust is recorded separately. Read-only
connectivity checks cover bootstrap/destination/control; write probes use unique scratch objects.
An inaccessible/hidden database is not labeled absent merely from4060/DB_ID(NULL). Native codes,
SQLSTATE, observed context and corrective actions remain distinct from uncertain interpretations.

Actual negative probes include certificate-chain rejection,18456 nonexistent-login/no retry,
low-privilege262 and hidden existing4060; they do not certify every TLS/permission/provider case.
See sql-bootstrap.sql, sql-reconciliation.sql, local-sql-tests.md and the maintained requirement index.

## Explicit owned schema evolution

Importer-owned targets can authorize compatible changes with allow_add_columns_on_append,
allow_widen_on_append and allow_retype_on_append. The actual independent SQL Server 2022/2025 matrix
checks named source reorder, int-to-bigint, NVARCHAR widening and nullable added columns. Old
rows gain NULL only for the declared nullable addition; old strings/IDs remain exact. Original
schema receipt digests still reconcile and original intents resume against the evolved table.
Every prior value is verified under its acknowledged old projection before/after the ALTER
transaction. Concrete schema diff/history is retained; a failed speculative plan is not published.

Narrowing, dropped published columns, index/check/default/FK dependencies and externally edited
schema were independently refused with the old values/IDs unchanged. These checks do not certify
arbitrary business-target generated mappings, trigger transformations, lossy projection or every
permission/dependency variation. See test_live_evolution_oracle.py and independent-evolution-2022-
repeatable / independent2025-evolution reports. Ordinary execution still needs explicit flags;
defaults do not automatically authorize ALTER.

Additional actual SQL Server 2022 cases preserve decimal integer range while increasing scale and all
three temporal families from precision2 to7, then reconcile the original receipt under its old
projection. Replacement retains exact old rows/IDs in a proven prior-generation table; original
receipts can reconcile against that acknowledged backup. The independent oracle also edits only
its fixture backup and requires subsequent original-receipt verification to fail while leaving
the current target unchanged. Per-engine executed results remain explicit in the test reports.

## Explicit mapped business append

A separate opt-in mapped_business strategy has actual SQL Server 2019/2022/2025 independent tests for
existing identity/default/computed/rowversion tables, explicit unique generated-key sidecars,
duplicate source rows, identity preservation, NULL versus omitted defaults, approved audit
AFTER triggers, source-transform trigger rollback, constraint rollback and lost-ACK resume.
All supplied cells are re-read after synchronous triggers; defaults/generated values are
captured and reverified. OUTPUT order is never treated as source order. Append and guarded replacement require a usable nonnullable unique key, complete metadata visibility and an exact
explicit list of every omitted target column. It does not silently omit generated inputs.

INSTEAD OF triggers, unapproved enabled triggers, IGNORE_DUP_KEY, missing stable keys and an
incomplete omitted-column list were independently refused without target mutation. Tests are
in test_live_business_oracle.py. mapping.target_table selects an exact existing table name within destination.schema; it is accepted only for mapped_business. Every selected per-file source maps to that table with independent intents and serialized publication. Independent tests exercise Unicode, a closing bracket, spaces and shell-like characters in the table name under both loaders. Native bcp/ODBC load only verified staging; final business inserts obtain one explicit key per source record.

## Mapped business replacement and prepared recovery

Explicit mapped_business replacement stages/validates the complete new source, snapshots ALL
old target rows (including unimported rows), and retains a verified generation before DELETE
and checked INSERT in the same transaction. Immutable old receipts/sidecars resolve their
original retained generation. The original object, constraints/defaults/computed/identity and
rowversion behavior remain inspected. FK and DELETE/UPDATE triggers block; approved INSERT
AFTER audit triggers are allowed only with exact supplied-value verification. Empty/incomplete
replacement needs explicit supported approval and never silently clears a useful table.

Actual independent2019/2022/2025 tests cover generated/preserved keys, explicit NULL vs defaults,
all-row backups/rowversion bytes, late constraint rollback, mid-delete hard crash and same-intent
resume, lost ACK, changed unimported backup cells and missing generation membership. New F09
replay authority requires an fsynced BEFORE artifact bound to the original independently
recorded SHA/run/intent/target. Original-event-only pointer repair is accepted and logged;
both local seals absent refuses5 even if a self-consistent file exists. Legacy unsealed
fixtures stay unknown; no current-state snapshot is retroactively promoted into authority.

Per-directory group append requires complete observable central contributor history (control schema version4 or later). Unavailable/incomplete central history blocks5 with captures pending; it cannot establish that no earlier contributor/publication exists. Ordinary per-file new imports retain the documented audit-fallback partial3 policy. Recovery and complete-group replacement remain stricter than that fallback.

## Windows operator startup and permissions

The README first-run path uses DBA-provisioned landing/control databases and
create_databases=false. A SQL login needs connection, schema/table creation and the
read/write/metadata/locking permissions used by the selected workflow. The Windows
test account used db_owner limited to two disposable databases. That role is not a
production least-privilege prescription. The DBA should review permissions for the chosen
features rather than granting server sysadmin to suppress errors. See [testing](testing.md).
