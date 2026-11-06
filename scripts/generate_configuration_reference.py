"""Generate exhaustive default-field reference from defaults and validation literals."""
import ast,json,tomllib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
module=ast.parse((ROOT/'db_ingest/config.py').read_text());validate=next(n for n in module.body if isinstance(n,ast.FunctionDef) and n.name=='_validate')
values={}
for n in validate.body:
 if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ('choices','fixed'):values[n.targets[0].id]=ast.literal_eval(n.value)
fixed=values['fixed'];choices=values['choices']
for a,b,c in [('preservation','capture_original_bytes',True),('preservation','flush_durable_checkpoints',True),('parsing','strict_decoding',True),('audit','local_failure_fallback',True),('retry','require_commit_resolution',True),('schema','infer_defaults',False),('credentials','redact_secrets',True),('xml','allow_entities',False),('xml','allow_network',False),('source','follow_symlinks',False),('html','allow_nested_tables',False),('html','allow_rowspan_colspan',False),('html','text_policy','preserve')]:fixed[a+'.'+b]=c
choices.update({'load.partial_publish':('new_or_append','none'),'schema.on_length_overflow':('ask','quarantine','abort','widen','retype_text')})
parser_ast=ast.parse((ROOT/'db_ingest/parsers/delimited.py').read_text());codecs=set()
for n in parser_ast.body:
 if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ('_SUPPORTED_ENCODINGS','_MULTIBYTE_ENCODINGS'):codecs.update(ast.literal_eval(n.value))
def literal(x):return json.dumps(x,ensure_ascii=False).replace('|','&#124;')
notes={
 'mapping.name_collision':'filename_error (default): basename-only per-file tables. path_error: single-underscore relative path names, per-file or per-directory tables. Both refuse collisions and require single database/owned_landing; stable_hash_suffix retains legacy hashed naming. See [folder presets](directory-import.md).',
 'load.mode':'create_only creates a new target, refusing any existing target under the publication lock except same-intent receipt recovery. Owned landing only; --new-intent does not bypass this refusal. append adds rows; replace is explicit replacement with retention.',
'destination.create_databases':'Allow creation of missing configured/mapped databases; existing databases are reused, never reset. False requires them to exist and be accessible.',
'destination.create_schemas':'Allow creation of missing required schemas; existing schemas are reused, not recreated.',
'destination.create_tables':'Allow creation of missing required tables; existing targets follow ownership/schema and append/replace policies. This flag is not overwrite permission.',
'source.path':'Omitted or empty string inherits final source.root after CLI precedence. Explicit nonempty path selects a file/directory inside root. TOML has no null literal. Resume requires the same resolved selection.',
'destination.server':'SQL server/instance string; e.g. "localhost,1433" or "HOST\\\\INSTANCE".',
'destination.database':'Destination database name.', 'destination.schema':'Destination schema name.', 'destination.control_database':'Control/audit database name on the same server.', 'destination.bootstrap_database':'Existing database used to connect before creating a destination.', 'destination.driver':'Installed ODBC driver name; supported/tested client is Microsoft ODBC Driver 18.',
'destination.encrypt':'"yes" or "no"; boolean aliases below are accepted. Default requires encryption.', 'destination.trust_server_certificate':'"yes" or "no"; boolean aliases accepted. "yes" bypasses certificate validation.',
'destination.connect_timeout_seconds':'Integer 1..2147483647 seconds.', 'destination.statement_timeout_seconds':'Integer 1..2147483647 seconds.',
'parsing.header':'"auto" currently always requires an explicit present/absent decision; it does not perform automatic header classification. "absent" retains first data row and generates labels when column_names is empty.',
'parsing.encoding':'"auto" or a supported codec/alias; complete canonical list and BOM behavior below.',
'parsing.delimiter':'"auto" or literal 1..32-character token; auto considers comma, tab, pipe and semicolon. Multi-character dialects must be explicit.',
'parsing.quotechar':'"auto" (double quote), "none"/empty string (disabled), or literal 1..32-character qualifier.',
'parsing.escapechar':'"none"/empty string (disabled), or literal 1..32-character escape. "auto" is not supported.',
'parsing.record_terminator':'"auto" (CR/LF/CRLF; opt-in NEL) or literal 1..32-character ending, e.g. "\\r\\n" or "<END>".',
'parsing.column_names':'Ordered string list; [] means no override. At most1024 names, each at most1024 characters, without NUL. Raw header remains retained.',
'parsing.null_tokens':'String list; [] disables token-based NULL interpretation.',
'parsing.allow_multiline_fields':'true or false. False leaves quoted/escaped physical newline tails unresolved; no guessed resync.',
'parsing.nel_is_newline':'true or false; true adds NEL physical/automatic line breaks and requires explicit delimiter.',
'source.root':'Nonempty stable source-root directory path string; blank/whitespace-only values are rejected.', 'source.include':'String list of include globs, e.g.["**/*.csv"].', 'source.exclude':'String list of exclude globs.',
'mapping.target_table':'Empty string for automatic naming, or exact mapped_business table identifier (at most128 UTF16 units; no NUL).',
'mapping.omit_target_columns':'Unique target-name string list; each1..128 UTF16 units, no NUL; explicit business mapping only.',
'schema.text_length_headroom_percent':'Integer 0..10000 percent.',
'preservation.minimum_free_disk_bytes':'Nonnegative integer bytes reserved on the work disk.',
'limits.max_queue_bytes':'Integer bytes, at least4096.', 'limits.output_buffer_bytes':'Integer bytes, 1..1048576.',
'limits.max_in_memory_record_bytes':'Integer bytes, 1..8388608; larger records are preserved/quarantined.',
'limits.max_in_memory_field_bytes':'Integer bytes, 1..8388608.',
'limits.worker_memory_bytes':'Positive integer bytes; at least32MiB +8 × max_in_memory_record_bytes.',
'limits.total_process_tree_memory_bytes':'Positive integer bytes; at least worker_memory_bytes +32MiB. Admission may reduce worker count.',
'xml.record_selector':'Empty while disabled; when enabled, one element name or {namespace}name, at most2048 characters; not arbitrary XPath.',
'xml.fields':'[] while disabled; when enabled,1..1024 {name="...",path="..."} mappings. Name≤128 characters; path≤2048, direct scalar child or @attribute, optionally expanded namespace.',
'xml.max_depth':'Positive integer maximum nesting depth.',
'html.table_selector':'Empty while disabled; when enabled, zero-based table index as string, e.g."0".',
'html.fields':'[] while disabled; when enabled,1..1024 ordered field-name strings, each≤128 characters.',
'parsing.sample_bytes':'Positive integer bytes; sniff reads are additionally bounded by record limit and8MiB. Does not limit full-stream schema inference.',
'parsing.sample_records':'Positive integer provisional inspection/sample count; full execution schema still scans the accepted stream.',
'verification.sample_records':'Positive integer row count for reduced-assurance sample verification.',
'load.batch_rows':'Positive integer maximum staging rows; actual byte limit may stop a batch sooner.',
'load.batch_bytes':'Positive integer encoded/bound staging byte cap; both row and byte caps apply.',
'parallel.max_workers':'Positive integer requested file workers; memory admission can reduce effective workers.',
'preservation.minimum_free_disk_bytes':'Nonnegative integer reserve bytes; zero explicitly disables the reserve threshold, not capture/durability.',
'retry.initial_backoff_seconds':'Positive integer seconds, no greater than maximum_backoff_seconds.',
'retry.maximum_backoff_seconds':'Positive integer retry-delay cap, at least initial_backoff_seconds.',
'parsing.blank_record_policy':'quarantine: rejected BLANK_RECORD; preserve: rejected BLANK_PRESERVED_UNIMPORTED (no fabricated fields); explicitly_skip: excluded BLANK_EXPLICITLY_EXCLUDED. Bytes remain captured in every case.',
}
text='''# Configuration reference

This reference lists **every global TOML field** in the current defaults, its default,
and supported values. Fixed values are capability limits, not configurable alternatives.
Unknown sections/keys are rejected; a type-correct value can still fail format, target,
resource or capability checks. Use TOML booleans `true`/`false`; boolean fields also accept
quoted `yes/no`, `true/false`, `1/0` case-insensitively. Numeric fields use whole numbers,
not quoted strings. Paths are strings; single quotes simplify Windows paths.

Precedence: **CLI > named environment variables > last matching file rule > global TOML > defaults**.
Lists replace earlier lists. CLI options follow the command; `--config` precedes it.
Changing interpretation/mapping on an existing intent requires `reimport`, not blind replay.
The short [starter](../ingest.example.toml) is enough for normal use; this page is the full reference.

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

'''+', '.join('`'+c+'`' for c in sorted(codecs))+'''.

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
.\\.venv\\Scripts\\python.exe -m db_ingest --config ingest.toml test-connection --username loader --password-prompt
```

## Global fields

'''
c=tomllib.loads((ROOT/'db_ingest/defaults.toml').read_text());text+='`config_version`: integer **1** only.\n\n'
for section,fields in c.items():
 if section=='config_version':continue
 text+='### ['+section+']\n\n| Field | Default | Supported values / constraints |\n| --- | --- | --- |\n'
 for key,default in fields.items():
  path=section+'.'+key
  if path in fixed:allowed='Fixed: `'+literal(fixed[path])+'` only.'
  elif path in choices:allowed=', '.join('`'+literal(x)+'`' for x in choices[path])+'.'
  elif path in notes:allowed=notes[path]
  elif isinstance(default,bool):allowed='`true` or `false`.'
  elif isinstance(default,int):allowed='Positive integer'+(' seconds.' if key.endswith('_seconds') else ' bytes.' if key.endswith('_bytes') else '.')
  elif isinstance(default,list):allowed='String list.'
  elif key.endswith('_path') or key.endswith('_directory'):allowed='Path/executable string; see usage for prerequisites.'
  else:allowed='String naming this database/schema/table/prefix; identifiers are validated before SQL use.'
  if path in notes and path in choices:allowed+=' '+notes[path]
  text+='| `'+key+'` | `'+literal(default)+'` | '+allowed.replace('|','&#124;').replace('\n',' ')+' |\n'
 text+='\n'
text+='''## Per-file and per-column fields

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

'''
env=ast.literal_eval(next(n.value for n in module.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='ENV'))
text+='| Variable | Destination setting |\n| --- | --- |\n'+''.join('| `DB_'+k+'` | `destination.'+v+'` |\n' for k,v in env.items())
text+='''
`DB_TRUST_SERVICE_CERTIFICATE` is an accepted alias of `DB_TRUST_SERVER_CERTIFICATE`;
conflicting values fail. Boolean environment values use yes/no, true/false or1/0;
`DB_CONNECT_TIMEOUT` is integer seconds. CLI credential options override environment values.

See [usage](usage.md) for CLI flags and examples, [token grammar](token-delimited-exports.md)
for control-prefix/newline constraints, [SQL support](sql-support.md) for target guards,
and [parser contract](parser-contract.md) for structured mapping limits. Enabling XML/HTML
requires the declared selectors/fields and explicit header policy where applicable.
'''
(ROOT/'docs/configuration-reference.md').write_text(text,encoding='utf-8')
print(json.dumps({'global_fields':sum(len(v) for v in c.values() if isinstance(v,dict))+1,'canonical_codecs':len(codecs),'reference':'docs/configuration-reference.md'}))
