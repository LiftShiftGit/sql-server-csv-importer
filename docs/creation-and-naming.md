# Creation flags, existing objects and names

**`create_* = true` means create the required object if missing. It does not mean
recreate, overwrite or empty an existing database/schema/table.** The three flags do
not choose names or grant SQL permissions. They default to true in the application;
the README starter explicitly sets `create_databases = false`.

| Setting | Missing object | Existing object |
| --- | --- | --- |
| `destination.create_databases = true` | May create the configured/mapped destination and control databases, using `bootstrap_database` (normally master) for the initial connection. | Connects to the existing database. It does not drop, recreate or empty it. A permission/visibility/offline problem is not treated as permission to reset it. |
| `destination.create_schemas = true` | May create required target/audit/failure schemas. | Reuses the schema; existing tables in it remain. |
| `destination.create_tables = true` | May create required owned landing, audit/control, registry and failure tables. | Reuses compatible importer objects. An unowned destination table is refused by default, not dropped to make room. |
| Any flag set to `false` | The required missing object is not created; the operation reports a creation/access error. | An already provisioned object can still be used, subject to permissions, ownership and compatibility. |

These are provisioning controls, not a read-only mode or blanket ban on all DDL. Loading
still uses staging tables. Versioned importer-owned audit metadata can be migrated when
authorized; target schema evolution has its own explicit policies. `create_tables=false`
does not prohibit inserts into existing tables.

## Existing table data is controlled separately

- `load.mode = "append"` adds accepted records to a compatible target and preserves existing
  rows. Resume uses receipt checks to avoid repeating an acknowledged publication.
- `load.mode = "replace"` explicitly requests replacement of the selected target's data,
  subject to its validation, retention and contributor guards. The creation flags do not
  make replace non-destructive. It does not replace an entire database or schema.
- Default `mapping.existing_target_strategy = "owned_landing"` requires importer ownership
  of an existing target. An ordinary existing business table requires the explicit
  `mapped_business` strategy and its mappings/guards. `create_tables=true` does not override
  ownership checks or automatically enable schema changes.

## What are the objects called?

Names are independent of the creation flags. With the default mapping strategies:

| Object | Name source | Example |
| --- | --- | --- |
| Destination database | `destination.database` when `mapping.database_strategy="single"` | `LegacyLanding` |
| Control database (same server) | `destination.control_database` | `IngestControl` |
| Target schema | `destination.schema` | `dbo` |
| Landing table | Filename without its last extension, normalized; collisions refuse publication | `orders.csv` becomes `orders`; `sales/orders.csv` also maps to `orders`, so selecting both is rejected |
| Audit schema, in both databases | `audit.control_schema` | `audit` |
| Central audit tables | Fixed importer names under that schema | `IngestControl.audit.Runs`, `Files`, `Events`, `RecordErrors`, `PublicationReceipts`, etc. |
| Destination receipts/ownership | Fixed importer names under that schema | `LegacyLanding.audit.__db_ingest_receipts`, `__db_ingest_targets`, plus feature-specific metadata |
| Failure schema in the control database | `audit.failure_schema` | `failures` |
| Per-run failure table | Generated from target name, UTC creation timestamp and run-ID hash | `<target-prefix>_fail_<UTC-timestamp>_<run-hash>`; use the report's exact failure-table reference |

Filename-only is the default. To use path-derived hashed names, explicitly set
`mapping.name_collision="stable_hash_suffix"`; then `orders.csv` becomes
`orders__bef94e2e5e84a227` and `sales/orders.csv` becomes `sales__orders__5c2ee55621b656b6`.
See [naming and upgrade guidance](filename-table-names.md). Keep the hash option explicit
for existing hashed imports; changing the default does not migrate their tables.

Other database strategies derive names from the top-level directory, full directory or
file path using `mapping.database_prefix` and a stable hash. Root-level files in the
applicable directory strategies use `mapping.root_database`. `table_strategy="per_directory"`
uses a directory-derived table name; per-file databases use `mapping.per_file_database_table`
(default `data`). `mapping.target_table` is an exact name override for **mapped business**
tables, not a general rename switch for automatically created landing tables.

Use the README's **plan/review step before publishing** to see each exact database,
schema and table name. Afterward, `status --run-id` or the
[logging queries](logging-and-statistics.md) show the target and failure-table references.


See [testing](testing.md) for validation scope and integration setup.
