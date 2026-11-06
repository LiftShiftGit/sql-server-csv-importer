# Import a directory once

[Back to the quick start](../README.md) · [All settings](configuration-reference.md)

Start with `directory-paths.toml` unless you need one of the alternatives below. Install
the tool using the [README](../README.md#install-from-source); IT supplies Python 3.13 x64
and ODBC Driver 18 separately. These modes need no optional Python dependencies.
Choose one preset; do not run all three on the same exports.

| Preset file in examples/ | Meaning | Example table |
| --- | --- | --- |
| [directory-filenames.toml](../examples/directory-filenames.toml) | One table per CSV, filenames must be unique across the selection | north/orders.csv -> orders |
| [directory-paths.toml](../examples/directory-paths.toml) | One table per CSV, folders distinguish names | north/orders.csv -> north_orders |
| [directory-merge.toml](../examples/directory-merge.toml) | Combine the immediate CSV files in each folder | north/part1.csv + north/part2.csv -> north |

For a rehearsal, `examples/directory-samples/flat` contains unique filenames and
`examples/directory-samples/tree` demonstrates duplicate basenames, nested folders and
merge parts. The tree intentionally fails filename-only collision checks; use path names
or merge mode for it. Set root to the chosen sample directory's full Windows path.

These presets use `load.mode="create_only"`: existing targets are refused, never appended
or replaced. Recovering the original run checks its committed receipt before considering
creation, so repeated resume does not duplicate rows. `--new-intent` cannot bypass
create-only. Database/schema/audit objects may already exist and are reused normally.
The ordinary starter retains its prior append default for compatibility; choose a
one-off preset explicitly. Do not change naming or load mode while resuming an old run.

## Configure one preset

After the README's installation, copy your selected preset to a new config filename:

```powershell
if (Test-Path '.\ingest-directory.toml') { throw 'Stop: configuration already exists.' }
Copy-Item '.\examples\directory-paths.toml' '.\ingest-directory.toml' -ErrorAction Stop
notepad '.\ingest-directory.toml'
```

Set server, landing/control databases and Windows source/work paths to the DBA/IT values.
Keep the work directory local and durable. Credentials are prompted, not saved in this file.
Set `header="present"` only when each selected CSV has a header; otherwise configure the
parsing rules before import. The presets explicitly assume UTF-8, comma-separated files
with normal double-quote text qualifiers. Change these to match the actual exports;
`encoding="auto"` and `delimiter="auto"` are available but ambiguous input can require review.

The three presets differ only in these existing mapping controls:

| Choice | mapping.table_strategy | mapping.name_collision |
| --- | --- | --- |
| Filename per CSV | per_file | filename_error |
| Folder plus filename | per_file | path_error |
| Folder merged table | per_directory | path_error |

`source.root` is the stable base for naming. Omit `source.path` to import all of root.
`include=["**/*.csv"]` selects CSVs; `recursive=true` visits subfolders. To narrow a later
selection, set path inside root without changing root. Nested folders remain distinct:
`north/2026/orders.csv` becomes `north_2026_orders` in per-file mode; its folder becomes
`north_2026` in merge mode. Merge uses direct files in each folder, not all descendants
rolled up into one ancestor. Root-level files merge into `mapping.root_table="root_records"`.

One underscore separates path components. Existing underscores in names are retained.
Names are sanitized to ASCII letters/digits/underscores, with a prefix for leading digits.
Names over 128 characters fail instead of being truncated. Distinct sources which collapse
to one name (north/orders.csv and north_orders.csv, punctuation/case aliases, or a_b/ and
 a/b/) stop with TABLE_NAME_COLLISION. Claims also protect against collisions across runs.
The optional stable_hash_suffix policy preserves legacy hashed names and their separators.

Merged files must have equivalent ordered column labels under `directory_merge_schema="strict"`.
Different folder schemas are independent. `union_by_name` is an advanced explicit choice
for combining compatible named columns; it does not mean arbitrary incompatible files
can be combined safely. Every contributor retains its own original and accounting.

## Preview, import and recover

Run from the extracted release folder, using the README's `$IngestPython`:

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest-directory.toml' plan
if ($LASTEXITCODE -ne 0) { throw 'Stop: inspect the planning errors.' }
$RunId = 'PASTE_RUN_ID_FROM_PLAN'
& $IngestPython -I -m db_ingest --config '.\ingest-directory.toml' export-mapping --run-id $RunId --output '.\file-table-map.csv'
if ($LASTEXITCODE -ne 0) { throw 'Stop: mapping export failed; use a new output filename if it exists.' }
notepad '.\file-table-map.csv'
```

The mapping CSV streams every selected source without a fixed file-count cutoff or loading
the full list into memory. Blank targets mean the file has no completed plan. Multiple files
with the same destination are intentional only in merge mode. It does not certify SQL target
availability, group readiness or successful publication. Review the plan's column types,
counts, rejects and group errors too. An existing output CSV is never overwritten.
The default console preview is bounded; use export-mapping for a complete mapping list.

After review, replace `loader` with your SQL login and publish the saved run:

```powershell
& $IngestPython -I -m db_ingest --config '.\ingest-directory.toml' resume --run-id $RunId --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: retain the original run; inspect status and show-errors.' }
& $IngestPython -I -m db_ingest --config '.\ingest-directory.toml' reconcile --run-id $RunId --username loader --password-prompt
if ($LASTEXITCODE -ne 0) { throw 'Stop: reconciliation needs investigation.' }
```

Expect CLEAN_VERIFIED and the reviewed committed counts. A directory is not one global
transaction: successful files/folders can commit before another fails. Fix the cause and
resume the same run. TARGET_EXISTS means the target already exists for another import;
do not switch to append/replace to clear the error. Check the original run/receipts with
the DBA. Historical identical imports can instead report NEW_INTENT_REQUIRED; for this
one-off workflow recover the original run rather than forcing another intent.

Paths, source hashes, row counts, errors and receipts are retained locally and in the
configured SQL audit databases. See [logging queries](logging-and-statistics.md) and
[share recovery](smb-outage-recovery.md). Renaming exports or deleting the local journal
is not a duplicate-removal strategy. Keep the source root, work directory and run ID.
