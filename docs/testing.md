# Testing

## Local regression

Follow [development setup](../CONTRIBUTING.md), then run:

```powershell
& $DevPython -m pytest tests --ignore=tests/integration -q
```

Tests cover configuration validation, parsing/encoding, preservation, type conversion,
resource limits, naming/collisions, journaling and recovery guards. Optional UNC tests require
`DB_INGEST_TEST_UNC_ROOT` pointing to a writable disposable share directory. Without that
setting those tests are skipped; do not interpret a skip as a passed share test.

## Disposable SQL integration

Prepare a dedicated SQL Server instance and a private config whose landing and control
names both begin `DbIngestTest_`. Keep config/credentials under ignored `.integration/`.
The integration account must be allowed to create the test schemas and tables. Some opt-in
restore/failure suites need broader permissions on the disposable server.

```powershell
& $DevPython scripts/run_integration.py --config .integration/ingest.toml --credentials-json .integration/credentials.json --expected-major 16 --label local-sql2022 --test-path tests/integration/test_live_folder_presets.py --engines odbc,bcp --bcp-path 'C:\Tools\bcp.exe'
if ($LASTEXITCODE -ne 0) { throw 'Integration tests failed; retain the reports and work directories.' }
```

The private credentials file contains `username` and `password`; alternatively set
`DB_USERNAME` / `DB_PASSWORD` in the test process environment and omit the file argument.
Never commit either. Reports are written under `.integration/reports/`. Native bcp must be
installed separately for tests parametrized for both engines. Inspect the selected suite's
prerequisites; the runner's engine option does not filter every parametrized test.

Upgrade tests additionally accept `DB_INGEST_PREVIOUS_PYTHON`, pointing to a separately
installed previous release. Keep that environment unchanged. Large-file and resource tests
are opt-in and need substantial disk/time; the synthetic generators live in scripts/ and
 tests/oracles/. Retain failure evidence for diagnosis rather than editing journal states.

## Multi-GB acceptance

Use a separately installed release that will remain unchanged for the whole test.
The controller requires a disposable SQL Server at `127.0.0.1`, a private config with
permission to create test databases, private SQL credentials, and native bcp. Allow
substantial disk space for retained captures, journals, transport files and SQL copies;
the four cases can take hours. Never run this against production.

Set `$ReleasePython` to that installed environment's Python and `$BcpPath` to `bcp.exe`.
Choose a new matrix folder for each build:

```powershell
$Matrix = '.\.integration\matrix-001'
& $DevPython scripts/run_multigb_acceptance.py --folder $Matrix --interpreter $ReleasePython --config '.\.integration\ingest.toml' --credentials-json '.\.integration\credentials.json' --bcp-path $BcpPath
if ($LASTEXITCODE -ne 0) { throw 'Retain the failed case and investigate; do not replay with a new intent.' }
foreach ($Case in 'bcp-128', 'odbc-128', 'bcp-4096', 'odbc-4096') {
    & $DevPython scripts/verify_multigb_attestation.py --folder "$Matrix\$Case" --source-manifest "$Matrix\source\independent-source.json" --credentials-json '.\.integration\credentials.json'
    if ($LASTEXITCODE -ne 0) { throw "Attestation failed: $Case" }
}
```

The synthetic source exceeds 2 GiB. Checks compare every accepted SQL cell and row
identity to an independent oracle, preserve three intentional wrong-width rejects,
and verify original bytes, late type changes, receipts, actual loader and batch limits.
The incomplete header uses explicit `column_names`; this case does not test automatic
header inference. Memory is sampled every 0.2 seconds for the importer and observed
children, excluding SQL Server and the verifier; short-lived peaks can be missed.

To repeat readback of an existing completed case, rerun the controller with the same
arguments plus `--only bcp-128 --verify-only` (substitute the desired case). This performs
fresh verification even when a saved result exists, without importing again. It requires
the original installed runtime and retained expected-partial completion (exit 3).
An interrupted or failed import instead needs its original recovery workflow; do not
use `--verify-only` to label an unfinished import successful.

## Verification scope

Windows x64 / CPython 3.13.2 with ODBC Driver 18 and disposable SQL Server 2022:

- 1,782 local tests and 1,191 subtests passed, including 29 new adversarial cases.
- 108 live SQL cases passed across naming/collisions, grouping, replacement, schema
  evolution, transport recovery, and real ODBC native-array/LOB fallback transitions.
- 1,248 naming combinations matched the pre-refactor implementation. Separate review
  checked transaction/receipt ordering, saved plans and recovery evidence. No runtime
  regression was found in that scope.
- The acceptance controller's cached `--verify-only` behavior was fixed. Regression
  tests require fresh readback, forbid reimport, and retain the prior certificate on failure.
- All 69 installed runtime files matched the source throughout the live tests.

Four serial imports used the same 2,150,139,421-byte synthetic CSV, exceeding 2 GiB.
Each independently verified 131,070 accepted rows and preserved three intentional
wrong-width rejects. All accepted cells, schema, ordinals, original bytes, receipts,
actual loaders and retained transport batches were checked. Results matched across
loaders and batch settings.

| Loader | Row ceiling | Byte ceiling | Largest observed batch (rows) | Sampled peak importer tree RSS |
| --- | ---: | ---: | ---: | ---: |
| bcp | 128 | 8 MiB | 128 | 82.3 MiB |
| odbc | 128 | 8 MiB | 128 | 58.7 MiB |
| bcp | 4,096 | 16 MiB | 380 | 93.4 MiB |
| odbc | 4,096 | 16 MiB | 380 | 71.2 MiB |

The table shows successful attempts, with 128 MiB worker / 256 MiB process-tree budgets.
The ODBC cases used bounded ordinary `executemany` for this wide `nvarchar(max)` fixture.
Three additional live tests proved actual native arrays plus an isolated LOB fallback,
using 2,001 rows and MiB-scale text/binary outliers placed first, middle and last.
These are acceptance measurements, not throughput guarantees or universal memory bounds.

The first ODBC-128 attempt failed when the SQL lab reported resource exhaustion under
its 2 GiB SQL allocation. The failed attempt was retained; there was no destination
receipt or committed row. After restarting the same container/database files with a
6 GiB SQL allocation inside an 8 GiB container, the original run/intent recovered and
passed complete readback and duplicate checks. Importer limits stayed unchanged.
The precise server failure cause was not isolated. Subsequent samples showed no sustained
user-session growth or low-memory flags; they do not prove absence of all leaks.
A separate Docker-engine interruption after bcp-128 completion was followed by fresh
read-only verification of the existing result, without replaying that import.

Five real Windows/SMB checks passed: changed unread tails after interrupted captures
were blocked for both loaders, while complete captured originals survived replacement
exports and repeated offline/reconnected recovery with 1,024 original rows and one
receipt each. A zero-byte opening failure recovered two rows under its original intent.
See [share recovery](smb-outage-recovery.md) for the required operator steps.

The multi-GB cases use explicit names for the incomplete header and ordinary UTF-8 CSV.
They do not certify every encoding, automatic header inference, custom token format,
parallel large-file workload, NAS/DFS implementation, or simultaneous failure at that
scale. Smaller regression fixtures cover the supported parsing/configuration behaviors.
Production validation still needs the intended account, TLS settings, SQL memory/log/
tempdb capacity, storage, network share and representative exports. Raw machine-specific
reports, credentials and test environments are excluded from public source.
