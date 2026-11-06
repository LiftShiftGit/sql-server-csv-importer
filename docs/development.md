# Code structure and recovery boundaries

The importer is organized around durable artifacts and explicit publication phases.
Start with the CLI command, then follow the run through planning and publication.

| Module | Responsibility |
| --- | --- |
| `cli.py` | Arguments, credentials and operator-facing reports |
| `config.py` | Configuration validation, precedence and semantic hashes |
| `discovery.py`, `capture.py` | Source selection and immutable byte capture |
| `parsers/`, `profile.py` | Record framing and full-stream column inference |
| `planning.py` | Stable database/table names and readable-name claim policy |
| `models.py` | Data structures and strict reconstruction of serialized plans |
| `pipeline.py`, `group_execution.py` | Durable state transitions and worker coordination |
| `publication.py` | Verified SQL staging, target locking and atomic publication |
| `recovery.py`, `reconciliation.py` | Commit resolution and original-value verification |
| `journal.py`, `adapters/mssql/control.py` | Local evidence and SQL audit mirrors |

## Publication flow

`publication.publish()` owns the connections, transaction boundaries and transport cleanup.
Its helpers perform specific work without committing the target transaction:

1. `_stage_records()` checks record order, field states and memory bounds while streaming
   converted values to the chosen loader. It does not retain the whole input.
2. The caller finishes transport and independently reads back the session-local stage.
3. Under the destination lock, the caller checks the current intent receipt, source history,
   recovery proof, naming claims and create-only policy before changing target data.
4. `_prepare_landing_target()` verifies ownership/schema or creates the missing landing table.
   Mapped business tables retain their separate adapter and baseline rules.
5. After inserts and exact value readback, `_store_receipt()` writes the receipt in the same
   transaction. Only the caller commits, with the unknown-commit flag and crash hooks visible.

Keep native-loader cleanup and recovery/control connections owned by the outer publication
function. Moving cleanup into a staging helper can lose resources when a partial stage fails.

## Compatibility rules

- Naming hash inputs, separators and semantic configuration hashes identify saved work.
  A cosmetic refactor must not change them. Readable names still require SQL ownership claims.
- `models.schema_plan_from_dict()` deep-copies serialized columns and statistics. Mutating an
  in-memory plan must not change the retained dictionary. Unknown fields remain errors.
- Missing receipts do not prove that a transaction failed. Preserve the existing resolver and
  its destination/local/control evidence checks before allowing replay.
- Keep fault hooks at their original durability boundaries. Test failure before commit and
  after destination commit but before the local checkpoint.
- Partial captures are not complete snapshots. Retain the changed-source retry guard.

## Refactoring checks

Run the [local suite and disposable SQL tests](testing.md). For publication work, include
folder presets, filename claims, merged groups, transport recovery, business replacement and
schema evolution. Use both ODBC and bcp with exact row/receipt oracles. Also resume a run
created by the previous installed release to check saved-plan and intent compatibility.

The parser state machines, configuration validator and publication coordinator remain
substantial because they enforce distinct failure policies. Prefer small changes with
specific contracts over a generic abstraction that hides transaction or recovery ordering.
