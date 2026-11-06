# Data locality and scaling audit

The importer reads configured source paths, retains local captures and sends accepted data
and audit records to the configured SQL Server. There is no application telemetry or cloud
upload client. Drivers, native tools and the SQL server have their own storage/network behavior.

## Where the bytes go

| Location | Data and actual implementation |
| --- | --- |
| Configured source/attachment paths | Read selected input bytes; attachment files are read only for explicit attachment_path mappings. [discovery.py](../db_ingest/discovery.py), [capture.py](../db_ingest/capture.py), [attachments.py](../db_ingest/attachments.py). |
| preservation.work_directory | Exact source/attachment captures, artifact metadata, SQLite journal/WAL with ordered fields, profiles, decisions, rejects, projection evidence, target reviews and recovery proofs. Originals remain available independently of SQL. [journal.py](../db_ingest/journal.py), [pipeline.py](../db_ingest/pipeline.py), [projections.py](../db_ingest/projections.py). |
| Work-directory transport subfolders | Reversible D3 JSON/Base64 chunks, chunk ledger/manifests and bcp stdout/stderr/error artifacts. Guarded native ODBC uses the canonical spool too. Base64 is encoding, not encryption. [bcp.py](../db_ingest/transports/bcp.py), [odbc.py](../db_ingest/transports/odbc.py). |
| OS temporary directory | Discovery's temporary SQLite inventory stores paths/selection/reasons outside work_directory by default; TemporaryDirectory removes it on normal cleanup, not necessarily after forced death. Native SQLite/driver scratch behavior is not exhaustively audited. [discovery.py](../db_ingest/discovery.py). |
| Configured SQL destination | Accepted typed business values, ingestion IDs/ordinals, raw canonical staging payloads, typed temporary staging, receipts and metadata. Replacement can retain complete prior target generations, including preexisting rows. SQL tempdb/logs/backups remain server-managed storage. [publication.py](../db_ingest/publication.py), [business_generations.py](../db_ingest/adapters/mssql/business_generations.py). |
| SQL control database on the configured server | Paths, hashes, original column names, schema/policies, data-derived profile statistics (including numeric min/max), summaries/events/diagnostics, contribution history and ordered reject field payloads. It is not merely aggregate counts; reject payloads can contain sensitive source values. Oversized/unresolved fields can use archive/span references. [control.py](../db_ingest/adapters/mssql/control.py), [source_audit.py](../db_ingest/adapters/mssql/source_audit.py). |
| Explicit exports/output and terminal | Operator-selected failure/projection exports, plans/reports and diagnostics can contain original or projected values, headers and paths. Credential redaction is not source-data anonymization. [cli.py](../db_ingest/cli.py), [projections.py](../db_ingest/projections.py). |

Raw originals are not automatically uploaded as complete files to an HTTP service. Accepted
values and reversible staging records are transferred to SQL through pyodbc/bcp; control and
failure records also transfer data. Configured target triggers, replication, jobs, backups
and server operators can make additional copies outside this application's control.

## Network and credentials

- [connect.py](../db_ingest/adapters/mssql/connect.py) uses explicit Driver18 connection strings
  and the configured destination.server; database mappings/control names do not establish
  another hidden HTTP endpoint. bcp uses that server with `-S`; sqlcmd diagnostic arguments
  use it too. Encrypt=yes and TrustServerCertificate=no are defaults, not proof that a
  destination is local or that every negotiated state was independently observed.
- `127.0.0.1` test SQL containers are local to this workstation, but arbitrary configured
  servers can be remote. UNC/mapped/synchronized source, work, attachment or export locations
  can involve other systems. There is no enforced loopback-only/local-disk operating mode.
- Prompt/stdin/environment credentials avoid the wrapper's password argv option. Explicit
  `--password` can appear in process inspection/history. SQL-auth bcp requires deliberate
  allow_password_in_child_args=true for `-P`; otherwise auto can use ODBC and explicit bcp
  refuses. sqlcmd uses SQLCMDPASSWORD in its child environment. In-memory/child-environment
  credentials are not a secret enclave from privileged local inspection.
- Standard preparation stores a redacted configuration in SQLite and redacted fingerprints
  ([config.py](../db_ingest/config.py), [pipeline.py](../db_ingest/pipeline.py)). Diagnostic
  redaction and streamed bcp log redaction are implemented. Source archives/fields intentionally
  preserve original data, including any secrets contained in the source itself. Work artifacts
  have inherited filesystem permissions; this application does not add at-rest encryption or
  a managed ACL boundary. A tool may write its log before post-process redaction completes.
- Executables are configured/PATH-resolved programs, invoked without a shell. Their trust and
  native driver/OS behavior are outside this source-only review. Version/help capture strips
  DB_PASSWORD/SQLCMD variables, bounds output and cleans owned descendants; it is not a general
  sandbox. [process_output.py](../db_ingest/transports/process_output.py).

The only core Python dependency is pyodbc; defusedxml is optional. XML forbids DTD/entities/
external fetching, and HTML scalar extraction does not render pages or fetch links. Literal
URLs in cells are data, not download requests. Python/package/tool installation can use network
services. The separately invoked development reference checks
performs HTTP GETs; integration helpers connect to their explicit disposable SQL endpoint.
Those are separate from normal CSV import. This report does not cover separately sharing
files with an editor/chat, operating-system synchronization, security tooling or server egress.

## Large files

The importer streams capture and parsing, enforces configured row/byte batch limits and
uses disk-backed journals and staging. Large exports require space for the original capture,
transport data and SQL staging. Input size alone does not predict throughput; benchmark
representative column widths, formats, driver settings and storage on the intended host.
See [testing](testing.md) for opt-in large-file checks and verification limits.
