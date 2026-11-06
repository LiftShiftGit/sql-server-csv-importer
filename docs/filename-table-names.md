# Filename-only table names

**Filename-only is now the default:** `orders.csv` becomes `orders`.
The starter templates state it explicitly; omitting the option has the same behavior:

```toml
[mapping]
name_collision = "filename_error"
```

This mode uses the default single-database, per-file, owned-landing mapping. Destination
database/schema still come from `destination.database` / `destination.schema`. Collision
protection is unchanged: ambiguous names stop instead of merging or overwriting.

To opt into the previous deterministic, path-derived hashed names:

```toml
[mapping]
name_collision = "stable_hash_suffix"
```

**For an existing hashed import, set `stable_hash_suffix` explicitly before upgrading or
resuming.** Old saved runs retain their mapping; attempting to resume with the new default
is rejected as `RESUME_SEMANTIC_CHANGE`. Selecting the hash option restores the old naming
and mapping identities. Never start a new intent merely to bypass this refusal.
Existing tables are not renamed/migrated. A new run with the new default selects plain
names, which may create different tables from an older hashed run. Review the plan.

Directory merging supports readable `path_error` naming or the explicit hash option.
Multi-database strategies and mapped-business targets require the explicit hash option. There is no automatic fallback from filename-only to hashing.
Column names, audit names and database-name configuration are unaffected.

## Names and collisions

| Source relative path | Filename-only table |
| --- | --- |
| `orders.csv` | `orders` |
| `north/orders.csv` | `orders` |
| `Order Items.csv` | `Order_Items` |
| `2026.csv` | `n_2026` |
| `orders.backup.csv` | `orders_backup` |

The last extension is removed. Characters other than ASCII letters, digits and `_` become
`_`; leading/trailing underscores are removed. A digit-leading name gets `n_`; an empty
result becomes `unnamed`. Names longer than 128 characters are rejected, not truncated.
This conservative naming rule can collapse Unicode-only names; use the optional hashed
mode when preserving readable Unicode names matters.

**A collision is an error, never permission to merge, overwrite or invent a suffix.**

- In one selection, duplicate basenames, case variants and names that sanitize identically
  are rejected before capture/publication. The selection manifest and error identify the
  conflicting source paths. Case variants are rejected conservatively even if the SQL
  database uses a case-sensitive collation. Inventory checks use SQLite storage, not an
  unbounded in-memory set of filenames.
- Across separate runs, the destination stores a claim tying the target to the normalized
  absolute `source.root` and the exact relative source path. `north/orders.csv` cannot
  take over the table claimed by `south/orders.csv`; another root cannot take it over
  either. Concurrent jobs use transactional SQL claim checks under the server's collation.
- A changed export at the **same root and relative path** can append/replace according to
  the normal load policy. This permits recurring `orders.csv` exports. Claim checks do
  not identify individual source producers beyond that root/path.
- An existing table without a claim is refused, including an ordinary business table.
  Missing or corrupt claims are not rebuilt by guessing ownership. `--new-intent` and
  `load.mode="replace"` do not bypass a different source's claim.

Resolve a collision by giving the source a distinct basename, selecting a different
configured destination database/schema, or using `stable_hash_suffix` and reviewing a
new plan. Switching to hashed names selects different targets; it does not migrate the
old table. Moving a claimed root/path requires a deliberate migration decision, not
manual edits to claims or receipts. Keep the original configuration to resume old runs.

## Persistent metadata and permissions

The destination database stores `audit.__db_ingest_name_claims` (using your configured
`audit.control_schema`). It is created only for filename-only mode, subject to
`destination.create_tables`. A new claim and its publication commit in the same SQL
transaction; a rolled-back publication does not reserve the name. Existing importer
ownership, schema validation, full-value verification and receipt recovery still apply.
Preprovision the claim table through the approved application/bootstrap process if normal
runs use `create_tables=false`; do not create a guessed schema or hand-edit claim rows.

No runtime dependency was added. Explicit hashed-mode naming and mapping identities remain unchanged. This is filename-only **table** naming for owned landing imports; directory
merging, per-file databases and mapped-business targets retain their existing naming
configuration and cannot be combined with this option.


For single-underscore folder prefixes and one-off imports, see [directory presets](directory-import.md).
