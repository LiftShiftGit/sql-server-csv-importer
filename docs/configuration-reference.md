# Configuration reference

This reference lists **every global TOML field** in the current defaults, its default,
and supported values. Fixed values are capability limits, not configurable alternatives.
Unknown sections/keys are rejected; a type-correct value can still fail format, target,
resource or capability checks. Use TOML booleans `true`/`false`; boolean fields also accept
quoted `yes/no`, `true/false`, `1/0` case-insensitively. Numeric fields use whole numbers,
not quoted strings. Paths are strings; single quotes simplify Windows paths.

Precedence: **CLI > named environment variables > last matching file rule > global TOML > defaults**.
Lists replace earlier lists. CLI options follow the command; `--config` precedes it.
Changing interpretation/mapping on an existing intent requires `reimport`, not blind replay.
For a one-off import, start with a [directory preset](directory-import.md), which explicitly
uses `create_only`. The general [example](../ingest.example.toml) and built-in load default
use `append`. Defaults below are built-in values, not the directory preset overrides.
Use this page to look up individual fields; do not copy it as a complete configuration.

Documentation coverage is distinct from test coverage. See the
[parameter-test and deployment status](testing.md) for verified scope
and its field inventory, executed SQL cases and environment acceptance limits.

## Encoding: what auto means

`encoding="auto"` detects UTF-8, UTF-16 LE/BE and UTF-32 LE/BE **by BOM**. Without a BOM
it tries strict UTF-8. It does **not** statistically guess Windows/DOS/EBCDIC/Asian codecs.
Invalid decoding stops interpretation or leaves a preserved unresolved tail; bytes are never
silently replaced. A wrong single-byte codec can decode successfully into wrong characters,
so use the producer's code page/CCSID. Strict decoding continues through the entire file.

Canonical explicit encodings:

`ascii`, `big5`, `cp037`, `cp1140`, `cp1250`, `cp1251`, `cp1252`, `cp1253`, `cp1254`, `cp1255`, `cp1256`, `cp1257`, `cp1258`, `cp437`, `cp500`, `cp850`, `cp858`, `cp932`, `cp949`, `euc_jp`, `euc_kr`, `gb18030`, `gbk`, `iso8859-1`, `iso8859-15`, `koi8-r`, `shift_jis`, `utf-16-be`, `utf-16-le`, `utf-32-be`, `utf-32-le`, `utf-8`.

Python codec aliases resolving to these names are accepted (e.g.`latin-1`, `windows-1252`).
`utf-8-sig` maps to UTF-8. `utf-16`/`utf-32` require a matching BOM; specify byte order
explicitly otherwise. An explicit encoding conflicting with a BOM is refused. Stateful
and non-reversible mappings are unsupported. Mainframe packed/binary layouts require an
explicit decoder; selecting an EBCDIC text codec is not a copybook decoder.

```toml
[parsing]
encoding = "cp1252"

[[file_rules]]
glob = "Mainframe/**/*.csv"
encoding = "cp037"
delimiter = "|"
nel_is_newline = true
header = "absent"
```

CLI override: `python -m db_ingest --config ingest.toml inspect --encoding cp1252`.
It overrides matching per-file encoding rules too. There is no `DB_ENCODING` environment
variable; only the named environment mappings listed below are recognized.

## Headers and generated names

Current behavior: `header="auto"` pauses with `HEADER_AMBIGUOUS`; it does not automatically
classify even an apparently obvious headerless file. Select `present` or `absent` explicitly.

```toml
[parsing]
header = "absent"
column_names = []
```

This retains the first data row and generates logical labels `column_1`, `column_2`, etc.
Default physical SQL names include ordinal/hash collision protection, for example
`column_1_1_746a517bc1`. Square brackets are SQL identifier quoting, not part of the name.
For exactly `[column_1]`, `[column_2]`, etc., set per-column `target_name` overrides using
`source_ordinal`; supplying `column_names` alone does not disable the suffix policy.
Blank/duplicate cells in a declared header retain raw originals and receive distinct safe
SQL names. A header containing fewer fields than its data requires an ordered
`column_names` override; the importer does not guess where a missing name belongs.
See [the checked behavior](testing.md).

## Login and prompts

| Mode/source | Actual behavior |
| --- | --- |
| SQL username | `--username loader` or `DB_USERNAME`; **no username prompt currently**. |
| SQL password, interactive | Missing password prompts using `getpass`, without echo, when stdin is a terminal and non-interactive mode is off. |
| Explicit password prompt | `--password-prompt` requests a hidden prompt, even when an environment password exists; use in a real terminal. |
| Scripted password | `--password-stdin` reads one line, or use `DB_PASSWORD`. |
| Non-interactive, password missing | Returns `CREDENTIALS_REQUIRED`; does not silently open an automatic prompt. |
| Integrated authentication | `destination.authentication="integrated"`; no SQL username/password is required. Omit password options. |
| TOML credential keys | `destination.username` and `destination.password` are **not allowed TOML keys**. Credentials are supplied through the sources above. |

Explicit password CLI options are mutually exclusive. They override environment password;
`--username` overrides `DB_USERNAME`. `--password` exists but can expose the value in shell
history/process arguments. SQL-auth bcp additionally requires explicit
`credentials.allow_password_in_child_args=true`; otherwise auto can select ODBC and an
explicit bcp request refuses. `password_source="prompt"` does not add a username prompt.

```powershell
.\.venv\Scripts\python.exe -m db_ingest --config ingest.toml test-connection --username loader --password-prompt
```

## Global fields

`config_version`: integer **1** only.

### [destination]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `type` | `"mssql"` | `"mssql"`. |
| `server` | `"localhost,1433"` | SQL server/instance string; e.g. "localhost,1433" or "HOST\\INSTANCE". |
| `database` | `"LegacyLanding"` | Destination database name. |
| `schema` | `"dbo"` | Destination schema name. |
| `control_database` | `"IngestControl"` | Control/audit database name on the same server. |
| `bootstrap_database` | `"master"` | Existing database used to connect before creating a destination. |
| `driver` | `"ODBC Driver 18 for SQL Server"` | Installed ODBC driver name; supported/tested client is Microsoft ODBC Driver 18. |
| `authentication` | `"sql"` | `"sql"`, `"integrated"`. |
| `encrypt` | `"yes"` | "yes" or "no"; boolean aliases below are accepted. Default requires encryption. |
| `trust_server_certificate` | `"no"` | "yes" or "no"; boolean aliases accepted. "yes" bypasses certificate validation. |
| `connect_timeout_seconds` | `15` | Integer 1..2147483647 seconds. |
| `statement_timeout_seconds` | `120` | Integer 1..2147483647 seconds. |
| `create_databases` | `true` | Allow creation of missing configured/mapped databases; existing databases are reused, never reset. False requires them to exist and be accessible. |
| `create_schemas` | `true` | Allow creation of missing required schemas; existing schemas are reused, not recreated. |
| `create_tables` | `true` | Allow creation of missing required tables; existing targets follow ownership/schema and append/replace policies. This flag is not overwrite permission. |

### [source]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `root` | `"D:\\Exports"` | Nonempty stable source-root directory path string; blank/whitespace-only values are rejected. |
| `path` | `"D:\\Exports"` | Omitted or empty string inherits final source.root after CLI precedence. Explicit nonempty path selects a file/directory inside root. TOML has no null literal. Resume requires the same resolved selection. |
| `recursive` | `true` | `true` or `false`. |
| `include` | `["**/*.csv", "**/*.tsv", "**/*.txt", "**/*.xyz"]` | String list of include globs, e.g.["**/*.csv"]. |
| `exclude` | `["**/ingest-work/**", "**/*.tmp"]` | String list of exclude globs. |
| `follow_symlinks` | `false` | Fixed: `false` only. |
| `format` | `"auto"` | `"auto"`, `"delimited"`, `"xml"`, `"html"`. |

### [mapping]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `database_strategy` | `"single"` | `"single"`, `"per_top_level_directory"`, `"per_directory"`, `"per_file"`. |
| `table_strategy` | `"per_file"` | `"per_file"`, `"per_directory"`. |
| `existing_target_strategy` | `"owned_landing"` | `"owned_landing"`, `"mapped_business"`. |
| `target_table` | `""` | Empty string for automatic naming, or exact mapped_business table identifier (at most128 UTF16 units; no NUL). |
| `omit_target_columns` | `[]` | Unique target-name string list; each1..128 UTF16 units, no NUL; explicit business mapping only. |
| `identity_policy` | `"generate"` | `"generate"`, `"preserve"`. |
| `allow_target_triggers` | `false` | `true` or `false`. |
| `database_prefix` | `"Import_"` | String naming this database/schema/table/prefix; identifiers are validated before SQL use. |
| `root_files` | `"root_database"` | `"root_database"`, `"error"`. |
| `root_database` | `"LegacyLanding"` | String naming this database/schema/table/prefix; identifiers are validated before SQL use. |
| `root_table` | `"root_records"` | String naming this database/schema/table/prefix; identifiers are validated before SQL use. |
| `per_file_database_table` | `"data"` | String naming this database/schema/table/prefix; identifiers are validated before SQL use. |
| `directory_merge_schema` | `"strict"` | `"strict"`, `"union_by_name"`. |
| `name_collision` | `"filename_error"` | `"stable_hash_suffix"`, `"filename_error"`, `"path_error"`. filename_error (default): basename-only per-file tables. path_error: single-underscore relative path names, per-file or per-directory tables. Both refuse collisions and require single database/owned_landing; stable_hash_suffix retains legacy hashed naming. See [folder presets](directory-import.md). |

### [parsing]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `record_terminator` | `"auto"` | "auto" (CR/LF/CRLF; opt-in NEL) or literal 1..32-character ending, e.g. "\r\n" or "<END>". |
| `allow_multiline_fields` | `true` | true or false. False leaves quoted/escaped physical newline tails unresolved; no guessed resync. |
| `encoding` | `"auto"` | "auto" or a supported codec/alias; complete canonical list and BOM behavior below. |
| `delimiter` | `"auto"` | "auto" or literal 1..32-character token; auto considers comma, tab, pipe and semicolon. Multi-character dialects must be explicit. |
| `quotechar` | `"auto"` | "auto" (double quote), "none"/empty string (disabled), or literal 1..32-character qualifier. |
| `escapechar` | `"none"` | "none"/empty string (disabled), or literal 1..32-character escape. "auto" is not supported. |
| `doublequote` | `true` | `true` or `false`. |
| `column_names` | `[]` | Ordered string list; [] means no override. At most1024 names, each at most1024 characters, without NUL. Raw header remains retained. |
| `header` | `"auto"` | `"auto"`, `"present"`, `"absent"`. "auto" currently always requires an explicit present/absent decision; it does not perform automatic header classification. "absent" retains first data row and generates labels when column_names is empty. |
| `ambiguity_policy` | `"pause"` | Fixed: `"pause"` only. |
| `sample_records` | `10000` | Positive integer provisional inspection/sample count; full execution schema still scans the accepted stream. |
| `sample_bytes` | `8388608` | Positive integer bytes; sniff reads are additionally bounded by record limit and8MiB. Does not limit full-stream schema inference. |
| `nel_is_newline` | `false` | true or false; true adds NEL physical/automatic line breaks and requires explicit delimiter. |
| `strict_decoding` | `true` | Fixed: `true` only. |
| `null_tokens` | `[]` | String list; [] disables token-based NULL interpretation. |
| `null_tokens_case_sensitive` | `true` | `true` or `false`. |
| `null_tokens_in_quoted_fields` | `false` | `true` or `false`. |
| `empty_string_is_null` | `false` | `true` or `false`. |
| `trim_whitespace` | `false` | `true` or `false`. |
| `blank_record_policy` | `"quarantine"` | `"quarantine"`, `"preserve"`, `"explicitly_skip"`. quarantine: rejected BLANK_RECORD; preserve: rejected BLANK_PRESERVED_UNIMPORTED (no fabricated fields); explicitly_skip: excluded BLANK_EXPLICITLY_EXCLUDED. Bytes remain captured in every case. |
| `ragged_row_policy` | `"quarantine"` | Fixed: `"quarantine"` only. |
| `repeated_header_policy` | `"preserve_as_data"` | Fixed: `"preserve_as_data"` only. |
| `repair_policy` | `"none"` | Fixed: `"none"` only. |

### [schema]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `inference` | `"full"` | Fixed: `"full"` only. |
| `text_fallback` | `"nvarchar"` | Fixed: `"nvarchar"` only. |
| `text_length_headroom_percent` | `20` | Integer 0..10000 percent. |
| `allow_max_types` | `true` | `true` or `false`. |
| `new_columns_nullable` | `true` | `true` or `false`. |
| `infer_defaults` | `false` | Fixed: `false` only. |
| `numeric_policy` | `"exact"` | Fixed: `"exact"` only. |
| `leading_zero_policy` | `"text"` | `"text"`, `"numeric"`. |
| `date_policy` | `"iso_unambiguous"` | Fixed: `"iso_unambiguous"` only. |
| `timezone_policy` | `"preserve"` | Fixed: `"preserve"` only. |
| `fractional_second_policy` | `"preserve_exact"` | Fixed: `"preserve_exact"` only. |
| `on_length_overflow` | `"ask"` | `"ask"`, `"quarantine"`, `"abort"`, `"widen"`, `"retype_text"`. |
| `on_type_conflict` | `"retype_text"` | Fixed: `"retype_text"` only. |
| `on_append_schema_mismatch` | `"pause"` | Fixed: `"pause"` only. |
| `allow_add_columns_on_append` | `false` | `true` or `false`. |
| `allow_widen_on_append` | `false` | `true` or `false`. |
| `allow_retype_on_append` | `false` | `true` or `false`. |
| `allow_lossy_projection` | `false` | `true` or `false`. |

### [load]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `mode` | `"append"` | `"append"`, `"replace"`, `"create_only"`. create_only creates a new target, refusing any existing target under the publication lock except same-intent receipt recovery. Owned landing only; --new-intent does not bypass this refusal. append adds rows; replace is explicit replacement with retention. |
| `engine` | `"auto"` | `"auto"`, `"bcp"`, `"odbc"`. |
| `preferred_engine` | `"bcp"` | `"bcp"`, `"odbc"`. |
| `fallback_engine` | `"odbc"` | Fixed: `"odbc"` only. |
| `batch_rows` | `5000` | Positive integer maximum staging rows; actual byte limit may stop a batch sooner. |
| `batch_bytes` | `8388608` | Positive integer encoded/bound staging byte cap; both row and byte caps apply. |
| `fast_executemany` | `true` | `true` or `false`. |
| `partial_publish` | `"new_or_append"` | `"new_or_append"`, `"none"`. |
| `replace_requires_complete_input` | `true` | Fixed: `true` only. |
| `replace_scope` | `"complete_group"` | `"complete_group"`, `"selected_subset"`. |
| `allow_empty_replace` | `false` | `true` or `false`. |
| `retain_previous_generation` | `true` | Fixed: `true` only. |
| `target_lock_timeout_seconds` | `60` | Positive integer seconds. |

### [tools]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `bcp_path` | `"bcp"` | Path/executable string; see usage for prerequisites. |
| `sqlcmd_path` | `"sqlcmd"` | Path/executable string; see usage for prerequisites. |
| `sqlcmd_variant` | `"odbc"` | Fixed: `"odbc"` only. |
| `detect_versions` | `true` | `true` or `false`. |
| `capture_stdout_stderr` | `true` | Fixed: `true` only. |

### [credentials]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `password_source` | `"prompt"` | Fixed: `"prompt"` only. |
| `allow_password_in_child_args` | `false` | `true` or `false`. |
| `redact_secrets` | `true` | Fixed: `true` only. |

### [preservation]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `work_directory` | `"D:\\ingest-work"` | Path/executable string; see usage for prerequisites. |
| `capture_original_bytes` | `true` | Fixed: `true` only. |
| `hash` | `"sha256"` | Fixed: `"sha256"` only. |
| `source_change_policy` | `"abort"` | Fixed: `"abort"` only. |
| `retain_originals` | `"indefinitely"` | Fixed: `"indefinitely"` only. |
| `retain_journal` | `"indefinitely"` | Fixed: `"indefinitely"` only. |
| `retain_failure_artifacts` | `"indefinitely"` | Fixed: `"indefinitely"` only. |
| `cleanup_verified_transport_spools` | `false` | Fixed: `false` only. |
| `minimum_free_disk_bytes` | `2147483648` | Nonnegative integer reserve bytes; zero explicitly disables the reserve threshold, not capture/durability. |
| `flush_durable_checkpoints` | `true` | Fixed: `true` only. |

### [audit]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `control_schema` | `"audit"` | String naming this database/schema/table/prefix; identifiers are validated before SQL use. |
| `failure_schema` | `"failures"` | String naming this database/schema/table/prefix; identifiers are validated before SQL use. |
| `create_failure_tables` | `true` | `true` or `false`. |
| `failure_table_pattern` | `"{table}_fail_{utc_timestamp}_{run_short}"` | Fixed: `"{table}_fail_{utc_timestamp}_{run_short}"` only. |
| `local_failure_fallback` | `true` | Fixed: `true` only. |
| `mirror_receipts_after_commit` | `true` | `true` or `false`. |
| `event_timestamps` | `"utc"` | Fixed: `"utc"` only. |
| `json_log` | `true` | Fixed: `true` only. |
| `console_log_level` | `"INFO"` | Fixed: `"INFO"` only. |
| `show_raw_errors` | `false` | Fixed: `false` only. |
| `log_source_values` | `false` | Fixed: `false` only. |

### [verification]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `level` | `"full"` | `"full"`, `"counts"`, `"sample"`. |
| `sample_records` | `10000` | Positive integer row count for reduced-assurance sample verification. |
| `unresolved_policy` | `"block_affected_publication"` | Fixed: `"block_affected_publication"` only. |

### [parallel]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `enabled` | `true` | `true` or `false`. |
| `max_workers` | `2` | Positive integer requested file workers; memory admission can reduce effective workers. |
| `strategy` | `"files"` | Fixed: `"files"` only. |
| `serialize_same_target_publication` | `true` | Fixed: `true` only. |

### [limits]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `total_process_tree_memory_bytes` | `536870912` | Positive integer bytes; at least worker_memory_bytes +32MiB. Admission may reduce worker count. |
| `worker_memory_bytes` | `201326592` | Positive integer bytes; at least32MiB +8 × max_in_memory_record_bytes. |
| `max_in_memory_record_bytes` | `8388608` | Integer bytes, 1..8388608; larger records are preserved/quarantined. |
| `max_in_memory_field_bytes` | `8388608` | Integer bytes, 1..8388608. |
| `max_queue_bytes` | `16777216` | Integer bytes, at least4096. |
| `fetch_bytes` | `8388608` | Positive integer bytes. |
| `output_buffer_bytes` | `32768` | Integer bytes, 1..1048576. |
| `oversized_record_policy` | `"preserve_and_quarantine"` | Fixed: `"preserve_and_quarantine"` only. |

### [retry]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `max_attempts` | `3` | Positive integer. |
| `initial_backoff_seconds` | `1` | Positive integer seconds, no greater than maximum_backoff_seconds. |
| `maximum_backoff_seconds` | `15` | Positive integer retry-delay cap, at least initial_backoff_seconds. |
| `jitter` | `true` | `true` or `false`. |
| `require_commit_resolution` | `true` | Fixed: `true` only. |

### [interaction]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `non_interactive` | `false` | `true` or `false`. |
| `on_required_decision` | `"pause"` | Fixed: `"pause"` only. |

### [xml]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `enabled` | `false` | `true` or `false`. |
| `record_selector` | `""` | Empty while disabled; when enabled, one element name or {namespace}name, at most2048 characters; not arbitrary XPath. |
| `fields` | `[]` | [] while disabled; when enabled,1..1024 {name="...",path="..."} mappings. Name≤128 characters; path≤2048, direct scalar child or @attribute, optionally expanded namespace. |
| `allow_entities` | `false` | Fixed: `false` only. |
| `allow_network` | `false` | Fixed: `false` only. |
| `max_depth` | `64` | Positive integer maximum nesting depth. |

### [html]

| Field | Default | Supported values / constraints |
| --- | --- | --- |
| `enabled` | `false` | `true` or `false`. |
| `table_selector` | `""` | Empty while disabled; when enabled, zero-based table index as string, e.g."0". |
| `fields` | `[]` | [] while disabled; when enabled,1..1024 ordered field-name strings, each≤128 characters. |
| `text_policy` | `"preserve"` | Fixed: `"preserve"` only. |
| `allow_nested_tables` | `false` | Fixed: `false` only. |
| `allow_rowspan_colspan` | `false` | Fixed: `false` only. |

## Per-file and per-column fields

`[[file_rules]]` requires a nonempty `glob`. Allowed additional keys are:
`format`, `encoding`, `delimiter`, `quotechar`, `escapechar`, `header`, `null_tokens`,
`empty_string_is_null`, `trim_whitespace`, `doublequote`, `nel_is_newline`, `column_names`,
`record_terminator`, `allow_multiline_fields`, and nested `columns`. Their values use the
same constraints as global source/parsing fields. Later matching rules win.
There is no global `[columns]` TOML section; use `[[file_rules.columns]]`.

| Column key | Supported values |
| --- | --- |
| `source_name` | Exact source-header string; must resolve uniquely unless ordinal also supplied. |
| `source_ordinal` | Integer≥1; if name also supplied, both selectors must agree. At least one selector is required. |
| `target_name` | SQL identifier string, at most128 UTF16 units, no NUL. |
| `target_type` | `bit`, `tinyint`, `smallint`, `int`, `bigint`, `date`, `uniqueidentifier`; `decimal(p,s)`/`numeric(p,s)` with1≤p≤38 and0≤s≤p; `nvarchar(n)` with1≤n≤4000, `varchar(n)`/`varbinary(n)` with1≤n≤8000, or `max`; `time(p)`/`datetime2(p)`/`datetimeoffset(p)` with0≤p≤7 (default7). |
| `nullable` | Boolean; enforced before publication. |
| `length` | Positive integer or string `"max"`; family-specific limits above. Enables bare `nvarchar`/`varchar`/`varbinary`; must agree with a sized target type. |
| `precision`, `scale` | Integers within the target-type bounds; together enable bare `decimal`/`numeric`. |
| `binary_encoding` | `"base64"`, `"hex"`, `"attachment_path"`; requires explicit VARBINARY target. |
| `attachment_root` | Required path string for `attachment_path`; references cannot escape it or use symlinks. |
| `null_tokens` | String list replacing global tokens for this column; [] preserves global markers as text. |
| `date_format`, `temporal_format` | `"iso8601"`, `%Y-%m-%d`, `%Y/%m/%d`, `%Y%m%d`, `%d/%m/%Y`, `%m/%d/%Y`, `%d-%m-%Y`, `%m-%d-%Y`; requires temporal target. Time-only permits ISO only. |
| `timezone_policy` | `"preserve"` only. |
| `fractional_second_policy` | `"preserve_exact"` only. |
| `truncation` | `"none"`, `"error"`, `"reject"`, or explicit `"right"` for NVARCHAR/VARCHAR/VARBINARY with allow_lossy_projection=true. |
| `rounding` | `"none"`, `"error"`, `"reject"`, or explicit `"half_even"` for DECIMAL/NUMERIC with allow_lossy_projection=true. |
| `collation`, `text_encoding` | VARCHAR only: `Latin1_General_100_BIN2_UTF8` with `utf-8`, or `Latin1_General_100_BIN2` with `cp1252`. |

Explicit projections produce reduced/non-clean assurance. Defaults, arbitrary type coercions,
locale-dependent date guessing and missing-field padding are not inferred.

## Environment variables

| Variable | Destination setting |
| --- | --- |
| `DB_SERVER` | `destination.server` |
| `DB_DATABASE` | `destination.database` |
| `DB_USERNAME` | `destination.username` |
| `DB_PASSWORD` | `destination.password` |
| `DB_ENCRYPT` | `destination.encrypt` |
| `DB_TRUST_SERVER_CERTIFICATE` | `destination.trust_server_certificate` |
| `DB_DRIVER` | `destination.driver` |
| `DB_CONTROL_DATABASE` | `destination.control_database` |
| `DB_CONNECT_TIMEOUT` | `destination.connect_timeout_seconds` |

`DB_TRUST_SERVICE_CERTIFICATE` is an accepted alias of `DB_TRUST_SERVER_CERTIFICATE`;
conflicting values fail. Boolean environment values use yes/no, true/false or1/0;
`DB_CONNECT_TIMEOUT` is integer seconds. CLI credential options override environment values.

See [usage](usage.md) for CLI flags and examples, [token grammar](token-delimited-exports.md)
for control-prefix/newline constraints, [SQL support](sql-support.md) for target guards,
and [parser contract](parser-contract.md) for structured mapping limits. Enabling XML/HTML
requires the declared selectors/fields and explicit header policy where applicable.
