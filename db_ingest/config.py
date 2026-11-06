"""Strict TOML configuration, provenance, precedence and nonsecret identities."""
import copy
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import tomllib
from .errors import IngestError
from .discovery import matches

EXAMPLE = Path(__file__).with_name("ingest.example.toml")
DEFAULTS = Path(__file__).with_name("defaults.toml")
ENV = {"SERVER":"server", "DATABASE":"database", "USERNAME":"username", "PASSWORD":"password", "ENCRYPT":"encrypt", "TRUST_SERVER_CERTIFICATE":"trust_server_certificate", "DRIVER":"driver", "CONTROL_DATABASE":"control_database", "CONNECT_TIMEOUT":"connect_timeout_seconds"}
BOOLEAN_STRINGS = {"true":True,"yes":True,"1":True,"false":False,"no":False,"0":False}
COLUMN_KEYS = set("source_name source_ordinal target_name target_type nullable binary_encoding temporal_format timezone_policy null_tokens length precision scale date_format fractional_second_policy truncation rounding attachment_root collation text_encoding".split())
RULE_KEYS = set("glob columns format encoding delimiter quotechar escapechar header null_tokens empty_string_is_null trim_whitespace doublequote nel_is_newline column_names record_terminator allow_multiline_fields".split())

def parse_bool(value):
    if isinstance(value,bool): return value
    key=str(value).lower()
    if key not in BOOLEAN_STRINGS: raise IngestError("CONFIG_BOOLEAN", "Expected yes/no, true/false or 1/0", context={"value":str(value)})
    return BOOLEAN_STRINGS[key]

def redact(value):
    if isinstance(value,dict): return {k:("<redacted>" if k.lower() in {"password","db_password","secret","access_token","api_key"} else redact(v)) for k,v in value.items() if k!="_provenance"}
    if isinstance(value,list): return [redact(v) for v in value]
    return value

def stable_hash(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=True,separators=(",",":"),default=str).encode()).hexdigest()

def hashes(config):
    semantic={k:config[k] for k in ("config_version","source","parsing","schema","xml","html")}
    # Disabled additive parsing policies preserve pre-extension intent identities.
    semantic["parsing"]={k:v for k,v in config["parsing"].items() if not (k=="column_names" and not v or k=="nel_is_newline" and not v or k=="record_terminator" and v=="auto" or k=="allow_multiline_fields" and v is True)}
    semantic["source"]={k:v for k,v in config["source"].items() if k in ("format",)}
    semantic["implementation_versions"]={"application":"0.1.0","parser":1,"adapter":1}
    semantic["columns"]=config.get("columns",[])
    semantic["file_rules"]=config.get("file_rules",[])
    semantic["framing_limits"]={k:config["limits"][k] for k in ("max_in_memory_record_bytes","max_in_memory_field_bytes","oversized_record_policy")}
    mapping={"mapping":config["mapping"],"destination":{k:config["destination"][k] for k in ("type","server","database","schema")},"publication":{k:config["load"][k] for k in ("mode","replace_scope","allow_empty_replace","partial_publish")}}
    return {"interpretation":stable_hash(semantic),"mapping":stable_hash(mapping),"operational":stable_hash(redact({k:v for k,v in config.items() if k not in ("_provenance","columns")}))}

def _validate(config):
    config["parsing"].setdefault("column_names",[])
    config["parsing"].setdefault("nel_is_newline",False)
    config["parsing"].setdefault("record_terminator","auto")
    config["parsing"].setdefault("allow_multiline_fields",True)
    # Validate every entry point, including CLI/environment and resolved rules.
    # bool is an int subclass in Python; accepting it as a count is unsafe.
    defaults=tomllib.loads(DEFAULTS.read_text(encoding="utf-8"))
    if type(config.get("config_version")) is not int or config["config_version"]!=1:
        raise IngestError("CONFIG_VERSION","Only integer config_version=1 is supported")
    for section,fields in defaults.items():
        if not isinstance(fields,dict):continue
        for key,expected in fields.items():
            value=config[section][key]
            if isinstance(expected,bool):config[section][key]=parse_bool(value)
            elif type(value) is not type(expected):
                raise IngestError("CONFIG_LIMIT" if (section,key) in (("limits","output_buffer_bytes"),("schema","text_length_headroom_percent"),("destination","connect_timeout_seconds"),("destination","statement_timeout_seconds")) else "CONFIG_TYPE",f"Invalid type for {section}.{key}")
    if not isinstance(config.get("file_rules",[]),list):
        raise IngestError("CONFIG_RULE","file_rules must be an array of tables")
    d=config["destination"]
    for key in ("connect_timeout_seconds","statement_timeout_seconds"):
        if not isinstance(d[key],int) or isinstance(d[key],bool) or not 1<=d[key]<=2147483647:raise IngestError("CONFIG_LIMIT","destination."+key+" must be a positive signed32-bit integer")
    for key in ("encrypt","trust_server_certificate"): d[key]="yes" if parse_bool(d[key]) else "no"
    choices={"mapping.name_collision":("stable_hash_suffix","filename_error","path_error"),"destination.type":("mssql",),"destination.authentication":("sql","integrated"),"source.format":("auto","delimited","xml","html"),"mapping.database_strategy":("single","per_top_level_directory","per_directory","per_file"),"mapping.table_strategy":("per_file","per_directory"),"mapping.existing_target_strategy":("owned_landing","mapped_business"),"mapping.identity_policy":("generate","preserve"),"mapping.directory_merge_schema":("strict","union_by_name"),"mapping.root_files":("root_database","error"),"parsing.header":("auto","present","absent"),"load.mode":("append","replace","create_only"),"load.engine":("auto","bcp","odbc"),"load.preferred_engine":("bcp","odbc"),"schema.leading_zero_policy":("text","numeric"),"verification.level":("full","counts","sample"),"parsing.blank_record_policy":("quarantine","preserve","explicitly_skip"),"load.replace_scope":("complete_group","selected_subset")}
    for path,allowed in choices.items():
        section,key=path.split(".")
        if config[section][key] not in allowed: raise IngestError("CONFIG_CHOICE",f"{path} must be one of {allowed}")
    for section,key,required in [("preservation","capture_original_bytes",True),("preservation","flush_durable_checkpoints",True),("parsing","strict_decoding",True),("audit","local_failure_fallback",True),("retry","require_commit_resolution",True),("schema","infer_defaults",False),("credentials","redact_secrets",True),("xml","allow_entities",False),("xml","allow_network",False)]:
        if config[section][key] is not required: raise IngestError("CONFIG_INVARIANT",f"{section}.{key} must be {required}")
    if config["mapping"]["name_collision"]=="filename_error" and any(config["mapping"][k]!=v for k,v in (("database_strategy","single"),("table_strategy","per_file"),("existing_target_strategy","owned_landing"))):
        raise IngestError("CONFIG_MAPPING","filename_error requires single database, per_file tables and owned_landing targets")
    if config["mapping"]["name_collision"]=="path_error" and (config["mapping"]["database_strategy"]!="single" or config["mapping"]["existing_target_strategy"]!="owned_landing"):
        raise IngestError("CONFIG_MAPPING","path_error requires single database and owned_landing targets")
    if config["load"]["mode"]=="create_only" and config["mapping"]["existing_target_strategy"]!="owned_landing":
        raise IngestError("CONFIG_MAPPING","create_only requires owned_landing targets")
    if config["mapping"]["target_table"]:
        name=config["mapping"]["target_table"]
        if config["mapping"]["existing_target_strategy"]!="mapped_business" or not isinstance(name,str) or len(name.encode("utf-16-le"))//2>128 or "\x00" in name:raise IngestError("CONFIG_MAPPING","target_table requires a legal explicit mapped_business table name")
    if config["mapping"]["existing_target_strategy"]=="mapped_business" and config["mapping"]["table_strategy"]!="per_file":raise IngestError("CONFIG_MAPPING","Mapped business targets currently use per-file append intents")
    if config["mapping"]["database_strategy"]=="per_file" and config["mapping"]["table_strategy"]=="per_directory": raise IngestError("CONFIG_MAPPING","per_file databases cannot merge per_directory tables")
    for section in ("load","limits","parallel","retry","parsing"):
        for key,value in config[section].items():
            if isinstance(value,int) and not isinstance(value,bool) and value<=0: raise IngestError("CONFIG_LIMIT",f"{section}.{key} must be positive")
    for section,key in (("verification","sample_records"),("xml","max_depth")):
        if config[section][key]<=0:raise IngestError("CONFIG_LIMIT",f"{section}.{key} must be positive")
    if config["preservation"]["minimum_free_disk_bytes"]<0:
        raise IngestError("CONFIG_LIMIT","preservation.minimum_free_disk_bytes must be nonnegative")
    if config["retry"]["initial_backoff_seconds"]>config["retry"]["maximum_backoff_seconds"]:
        raise IngestError("CONFIG_LIMIT","Initial retry backoff must not exceed maximum backoff")
    if config["load"]["partial_publish"] not in ("new_or_append","none"):
        raise IngestError("CONFIG_CHOICE","load.partial_publish must be new_or_append or none")
    from .parsers.token_delimited import validate_tokens
    validate_tokens(config["parsing"])
    fixed={
        "load.fallback_engine":"odbc", "preservation.hash":"sha256", "preservation.source_change_policy":"abort", "preservation.retain_originals":"indefinitely", "preservation.retain_journal":"indefinitely", "preservation.retain_failure_artifacts":"indefinitely", "preservation.cleanup_verified_transport_spools":False,
        "parsing.ambiguity_policy":"pause", "parsing.ragged_row_policy":"quarantine", "parsing.repeated_header_policy":"preserve_as_data", "parsing.repair_policy":"none",
        "schema.on_type_conflict":"retype_text", "schema.on_append_schema_mismatch":"pause", "schema.numeric_policy":"exact", "schema.date_policy":"iso_unambiguous", "schema.timezone_policy":"preserve", "schema.fractional_second_policy":"preserve_exact", "schema.text_fallback":"nvarchar", "schema.inference":"full",
        "verification.unresolved_policy":"block_affected_publication", "interaction.on_required_decision":"pause", "limits.oversized_record_policy":"preserve_and_quarantine",
        "audit.event_timestamps":"utc", "audit.failure_table_pattern":"{table}_fail_{utc_timestamp}_{run_short}", "audit.json_log":True, "audit.console_log_level":"INFO", "audit.show_raw_errors":False, "audit.log_source_values":False,
        "credentials.password_source":"prompt", "tools.capture_stdout_stderr":True, "tools.sqlcmd_variant":"odbc", "load.replace_requires_complete_input":True, "load.retain_previous_generation":True,
        "parallel.strategy":"files", "parallel.serialize_same_target_publication":True,
    }
    for path,required in fixed.items():
        section,key=path.split(".")
        if config[section][key]!=required:raise IngestError("CONFIG_CAPABILITY_UNAVAILABLE",f"This development build requires {path}={required!r}; alternate behavior is not implemented/certified. See docs/contract-deviations.md")
    output_buffer=config["limits"].get("output_buffer_bytes",32768)
    if not isinstance(output_buffer,int) or isinstance(output_buffer,bool) or not 1<=output_buffer<=1048576:raise IngestError("CONFIG_LIMIT","limits.output_buffer_bytes must be an integer from1 through1048576")
    headroom=config["schema"]["text_length_headroom_percent"]
    if not isinstance(headroom,int) or isinstance(headroom,bool) or not 0<=headroom<=10000:raise IngestError("CONFIG_LIMIT","schema.text_length_headroom_percent must be an integer from0 through10000")
    if config["limits"]["max_queue_bytes"]<4096:raise IngestError("CONFIG_LIMIT","limits.max_queue_bytes must be at least4096")
    minimum_worker=32*1024*1024+8*config["limits"]["max_in_memory_record_bytes"]
    if config["limits"]["worker_memory_bytes"]<minimum_worker:raise IngestError("CONFIG_LIMIT",f"Worker memory must allow baseline plus bounded record buffers (at least {minimum_worker} bytes for this record cap)")
    if config["limits"]["total_process_tree_memory_bytes"]<config["limits"]["worker_memory_bytes"]+32*1024*1024:raise IngestError("CONFIG_LIMIT","Total memory budget must allow the parent process and at least one worker")
    for key in ("max_in_memory_record_bytes","max_in_memory_field_bytes"):
        if config["limits"][key]>8388608:raise IngestError("CONFIG_CAPABILITY_UNAVAILABLE",f"limits.{key} above 8MiB requires a separately tested streaming LOB path; lower limits and preserve/quarantine larger records")
    if config["schema"]["on_length_overflow"] not in ("ask","quarantine","abort","widen","retype_text"):raise IngestError("CONFIG_CAPABILITY_UNAVAILABLE","Length overflow supports ask, quarantine, abort, widen or retype_text")
    for section,key in (("source","include"),("source","exclude"),("parsing","null_tokens"),("parsing","column_names"),("mapping","omit_target_columns")):
        value=config[section][key]
        if not isinstance(value,list) or any(not isinstance(item,str) for item in value):raise IngestError("CONFIG_TYPE",f"{section}.{key} must be a list of strings")
    names=config["parsing"]["column_names"]
    if len(names)>1024 or any(len(name)>1024 or "\x00" in name for name in names):raise IngestError("CONFIG_COLUMN_NAMES","Use at most1024 column names of at most1024 characters without NUL")
    omitted=config["mapping"]["omit_target_columns"]
    if len({x.casefold() for x in omitted})!=len(omitted) or any(not x or len(x.encode("utf-16-le"))//2>128 or "\x00" in x for x in omitted):raise IngestError("CONFIG_COLUMN","Omitted target names must be unique legal SQL identifiers")
    for rule in config.get("file_rules",[]):
        if not isinstance(rule,dict) or "glob" not in rule or set(rule)-RULE_KEYS: raise IngestError("CONFIG_RULE","Unknown per-file rule key or missing glob")
        if not isinstance(rule["glob"],str) or not rule["glob"]:raise IngestError("CONFIG_RULE","File rule glob must be a nonempty string")
        for key,value in rule.items():
            if key in ("glob","columns"):continue
            section="source" if key=="format" else "parsing"
            expected=config[section][key]
            if isinstance(expected,bool):rule[key]=parse_bool(value)
            elif not isinstance(value,type(expected)):raise IngestError("CONFIG_RULE",f"Invalid type for file rule {key}")
        if not isinstance(rule.get("columns",[]),list) or any(not isinstance(col,dict) for col in rule.get("columns",[])):
            raise IngestError("CONFIG_COLUMN","Rule columns must be an array of tables")
        seen=set()
        for col in rule.get("columns",[]):
            from .column_policy import validate_spec
            validate_spec(col,config["schema"]["allow_lossy_projection"])
            if set(col)-COLUMN_KEYS or not ({"source_name","source_ordinal"}&set(col)): raise IngestError("CONFIG_COLUMN","Unknown column key or missing source selector")
            if "source_ordinal" in col and (not isinstance(col["source_ordinal"],int) or isinstance(col["source_ordinal"],bool) or col["source_ordinal"]<1): raise IngestError("CONFIG_COLUMN","source_ordinal is 1-based")
            if "nullable" in col:col["nullable"]=parse_bool(col["nullable"])
            if "binary_encoding" in col:
                if col["binary_encoding"] not in ("base64","hex","attachment_path") or not str(col.get("target_type","")).lower().startswith("varbinary("):raise IngestError("CONFIG_COLUMN","binary_encoding requires an explicit varbinary target and base64/hex encoding")
            if str(col.get("target_type","")).lower().startswith("varbinary(") and "binary_encoding" not in col:raise IngestError("CONFIG_COLUMN","Text BLOB input requires binary_encoding=base64|hex")
            if col.get("binary_encoding")=="attachment_path" and not isinstance(col.get("attachment_root"),str):raise IngestError("CONFIG_COLUMN","attachment_path needs an explicit attachment_root")
            if "temporal_format" in col and not str(col.get("target_type","")).lower().startswith(("date","time")):raise IngestError("CONFIG_COLUMN","temporal_format requires an explicit temporal target_type")
            selector=(col.get("source_name"),col.get("source_ordinal"))
            if selector in seen: raise IngestError("CONFIG_COLUMN","Duplicate column selector within rule")
            seen.add(selector)
    from .windows_paths import validate_path_spelling, validate_local_work
    for key in ("root", "path"):
        validate_path_spelling(config["source"][key], "source." + key)
    validate_local_work(config["preservation"]["work_directory"])
    return config

def load_config(path=None, overrides=None, environ=None, relative_path=None):
    defaults=tomllib.loads(DEFAULTS.read_text(encoding="utf-8"))
    result=copy.deepcopy(defaults); provenance={}
    if path:
        try:
            with Path(path).open("rb") as source:raw=source.read(8388609)
            if len(raw)>8388608:raise IngestError("CONFIG_LIMIT","TOML configuration exceeds the supported8MiB input limit",2)
            supplied=tomllib.loads(raw.decode("utf-8-sig",errors="strict"))
        except (OSError,UnicodeError,tomllib.TOMLDecodeError) as exc: raise IngestError("CONFIG_READ",str(exc)) from exc
        if set(supplied)-set(defaults)-{"file_rules"}: raise IngestError("CONFIG_UNKNOWN","Unknown top-level configuration key")
        for section,value in supplied.items():
            if section in ("config_version","file_rules"): result[section]=value; continue
            if not isinstance(value,dict) or set(value)-set(defaults[section]): raise IngestError("CONFIG_UNKNOWN",f"Unknown key in {section}")
            for key,v in value.items():
                expected=defaults[section][key]
                if isinstance(expected,bool): v=parse_bool(v)
                elif not isinstance(v,type(expected)) or (isinstance(expected,int) and isinstance(v,bool)): raise IngestError("CONFIG_TYPE",f"Invalid type for {section}.{key}")
                result[section][key]=v; provenance[f"{section}.{key}"]="toml"
    result.setdefault("file_rules",[])
    _validate(result)
    if relative_path:
        columns=[]
        for i,rule in enumerate(result["file_rules"]):
            pattern=rule["glob"]
            if matches(relative_path,[pattern]):
                for key,v in rule.items():
                    if key in ("glob","columns"): continue
                    section="source" if key=="format" else "parsing"
                    result[section][key]=v;provenance[f"{section}.{key}"]=f"file_rules[{i}]"
                for col in rule.get("columns",[]):
                    selector=(col.get("source_name"),col.get("source_ordinal"))
                    columns=[c for c in columns if (c.get("source_name"),c.get("source_ordinal"))!=selector]
                    columns.append({**copy.deepcopy(col),"_rule_index":i})
        result["columns"]=columns
    env=os.environ if environ is None else environ
    canonical=env.get("DB_TRUST_SERVER_CERTIFICATE");alias=env.get("DB_TRUST_SERVICE_CERTIFICATE")
    if canonical is not None and alias is not None and parse_bool(canonical)!=parse_bool(alias): raise IngestError("CONFIG_ALIAS_CONFLICT","DB_TRUST_SERVER_CERTIFICATE and DB_TRUST_SERVICE_CERTIFICATE disagree")
    if alias is not None:
        result["destination"]["trust_server_certificate"]=alias;provenance["destination.trust_server_certificate"]="DB_TRUST_SERVICE_CERTIFICATE (supported alias)"
    for suffix,key in ENV.items():
        if "DB_"+suffix in env:
            v=env["DB_"+suffix]
            if key.endswith("_seconds"):
                try:v=int(v)
                except ValueError as exc:raise IngestError("CONFIG_TYPE",f"DB_{suffix} must be an integer") from exc
            result["destination"][key]=v;provenance[f"destination.{key}"]="DB_"+suffix
    for path,v in (overrides or {}).items():
        if v is None:continue
        parts=path.split(".")
        if len(parts)!=2 or parts[0] not in defaults or not isinstance(defaults[parts[0]],dict) or (parts[1] not in defaults[parts[0]] and not (parts[0]=="destination" and parts[1] in ("username","password"))):
            raise IngestError("CONFIG_UNKNOWN",f"Unknown override {path}")
        section,key=parts;result[section][key]=v;provenance[path]="cli"
    # Resolve only after TOML and CLI precedence. An explicit nonempty selection
    # must remain explicit, even when the root is overridden by the CLI.
    if not isinstance(result["source"]["root"],str) or not result["source"]["root"].strip():
        raise IngestError("CONFIG_SOURCE", "source.root must be a nonempty directory path")
    if "source.path" not in provenance or result["source"]["path"] == "":
        result["source"]["path"] = result["source"]["root"]
        provenance["source.path"] = "derived:source.root"
    result["_provenance"]=provenance
    if result["config_version"]!=1:raise IngestError("CONFIG_VERSION","Only config_version=1 is supported")
    return _validate(result)

def resolve_columns(config,headers):
    resolved={};seen=set()
    for spec in config.get("columns",[]):
        indices=[i for i,n in enumerate(headers,1) if n==spec.get("source_name")] if "source_name" in spec else []
        ordinal=spec.get("source_ordinal")
        if ordinal is None:
            if len(indices)!=1: raise IngestError("COLUMN_AMBIGUOUS","Column name must resolve uniquely; use source_ordinal")
            ordinal=indices[0]
        if ordinal>len(headers) or ("source_name" in spec and ordinal not in indices): raise IngestError("COLUMN_SELECTOR","Name and ordinal selectors do not agree")
        identity=(spec.get("_rule_index",-1),ordinal)
        if identity in seen:raise IngestError("CONFIG_COLUMN","Multiple selectors in one rule resolve to the same source column")
        seen.add(identity)
        resolved[ordinal]={**spec,"source_ordinal":ordinal}
    return list(resolved.values())


def effective_for_file(config,relative_path):
    """Apply file rules below already resolved environment/CLI settings."""
    config=_validate(copy.deepcopy(config))
    result=copy.deepcopy(config)
    provenance=result.setdefault("_provenance",{})
    columns=[]
    for i,rule in enumerate(config.get("file_rules",[])):
        pattern=rule["glob"]
        if not (matches(relative_path,[pattern])):continue
        for key,value in rule.items():
            if key in ("glob","columns"):continue
            section="source" if key=="format" else "parsing"
            path=f"{section}.{key}";origin=provenance.get(path,"")
            if origin=="cli" or origin.startswith("DB_"):continue
            result[section][key]=copy.deepcopy(value);provenance[path]=f"file_rules[{i}]"
        for col in rule.get("columns",[]):
            selector=(col.get("source_name"),col.get("source_ordinal"))
            columns=[c for c in columns if (c.get("source_name"),c.get("source_ordinal"))!=selector]
            columns.append({**copy.deepcopy(col),"_rule_index":i})
    result["columns"]=columns or copy.deepcopy(config.get("columns",[]))
    return _validate(result)
