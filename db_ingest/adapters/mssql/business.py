"""Explicit mapped business append with generated-key/source identity sidecars.

Native transports load only the verified typed stage. Each business INSERT returns
one explicit key; OUTPUT row order is never used as a source identity mapping.
"""
import base64,hashlib,json
from dataclasses import asdict
from db_ingest.errors import IngestError
from db_ingest.models import ColumnProfile,PublicationReceipt,SchemaPlan
from db_ingest.receipts import new_value_digest,update_value_digest
from db_ingest.resources import checkpoint
from .ddl import qualified,quote_identifier as q
from .types import parse_sql_type,parameter_value,canonical_value,read_expression
from .text import declaration,conversion,verify_plan as verify_text_plan
from .evolution import inspection,projection_compatible


def tables(config):
    schema=config.get("audit",{}).get("control_schema","audit")
    return qualified(schema,"__db_ingest_business_targets"),qualified(schema,"__db_ingest_business_rows")


def bootstrap(cur,config):
    targets,rows=tables(config)
    for name,ddl in ((targets,"target nvarchar(260) NOT NULL PRIMARY KEY,target_object_id int NOT NULL,contract_hash char(64) NOT NULL,metadata_json nvarchar(max) NOT NULL"),(rows,"intent_id nvarchar(64) NOT NULL,record_id nvarchar(128) NOT NULL,ordinal bigint NOT NULL,target nvarchar(260) NOT NULL,target_object_id int NOT NULL,key_json nvarchar(max) NOT NULL,generated_json nvarchar(max) NOT NULL,source_digest char(64) NOT NULL,PRIMARY KEY(intent_id,record_id)")):
        if cur.execute("SELECT OBJECT_ID(?)",name).fetchone()[0] is None:
            if not config.get("destination",{}).get("create_tables",False):raise IngestError("BUSINESS_METADATA_UNAVAILABLE","Mapped business sidecars are required and creation disabled",2)
            cur.execute(f"CREATE TABLE {name} ({ddl})")
    from .business_generations import bootstrap as bootstrap_generations
    bootstrap_generations(cur,config)


def _profile(row):
    name,_,base,length,precision,scale,nullable,identity,computed,generated,collation,*_=row
    if base in ("timestamp","rowversion"):kind="varbinary(8)"
    elif base in ("nvarchar","varchar","varbinary"):kind=f"{base}({('max' if length==-1 else length//2 if base=='nvarchar' else length)})"
    elif base in ("decimal","numeric"):kind=f"decimal({precision},{scale})"
    elif base in ("time","datetime2","datetimeoffset"):kind=f"{base}({scale})"
    else:kind=base
    spec=parse_sql_type(kind);stats={}
    if spec.base=="varchar":stats={"collation":collation,"text_encoding":"utf-8" if collation.upper().endswith("_UTF8") else "cp1252"}
    return ColumnProfile(0,name,name,kind,bool(nullable),stats)


def inspect_target(cur,config,plan,target,object_id,allow_register=True):
    if object_id is None:raise IngestError("BUSINESS_TARGET_MISSING","Mapped business strategy requires an existing explicit target",2)
    if config.get("_publication_group"):raise IngestError("BUSINESS_GROUP_UNSUPPORTED","Mapped business groups require a separately verified key/provenance strategy",2)
    kind=cur.execute("SELECT temporal_type,is_memory_optimized,is_filetable,is_node,is_edge FROM sys.tables WHERE object_id=OBJECT_ID(?)",target).fetchone()
    if kind is None or any(kind):raise IngestError("BUSINESS_TABLE_CAPABILITY","Mapped business currently requires an ordinary disk-based user table; temporal/memory-optimized/filetable/graph adapters are not verified",2)
    major=cur.execute("SELECT CONVERT(int,SERVERPROPERTY('ProductMajorVersion'))").fetchone()[0]
    if major>=16:
        ledger=cur.execute("SELECT ledger_type FROM sys.tables WHERE object_id=OBJECT_ID(?)",target).fetchone()[0]
        if ledger:raise IngestError("BUSINESS_TABLE_CAPABILITY","Ledger business publication requires a separately verified adapter",2)
    if cur.execute("SELECT HAS_PERMS_BY_NAME(?,'OBJECT','VIEW DEFINITION')",target).fetchone()[0]!=1:raise IngestError("BUSINESS_METADATA_VISIBILITY","Business mapping requires complete target definitions and dependencies",2)
    observed=inspection(cur,target)
    source={c.target_name:c for c in plan.columns};actual={r[0]:r for r in observed["columns"]}
    if len(actual)!=len(observed["columns"]):raise IngestError("BUSINESS_COLUMN_IDENTITY","Target column identities are ambiguous",2)
    omitted=set(config["mapping"].get("omit_target_columns",[]));unmapped=set(actual)-set(source)
    if omitted!=unmapped:raise IngestError("BUSINESS_OMISSION_REQUIRED","Every unmapped business column must be explicitly listed in mapping.omit_target_columns",2,{"unmapped":sorted(unmapped),"declared_omissions":sorted(omitted),"unknown_mapped":sorted(set(source)-set(actual))})
    if len(actual)>1024:raise IngestError("BUSINESS_COLUMN_LIMIT","Mapped business tables support at most1024 actual columns",2)
    profiles={name:_profile(row) for name,row in actual.items()}
    for name,column in source.items():
        if name not in actual:raise IngestError("BUSINESS_COLUMN_MISSING","Mapped source target column does not exist",2,{"column":name})
        row=actual[name]
        if row[8] or row[9] or row[2] in ("timestamp","rowversion"):raise IngestError("BUSINESS_GENERATED_INPUT","Computed/rowversion/generated-always columns must be explicitly omitted",2,{"column":name})
        if row[7] and config["mapping"]["identity_policy"]!="preserve":raise IngestError("BUSINESS_IDENTITY_POLICY","Supplying identity values requires identity_policy=preserve",2)
        a=ColumnProfile(column.ordinal,column.original_name,column.target_name,column.sql_type,profiles[name].nullable,column.stats)
        if not projection_compatible(a,profiles[name]):raise IngestError("BUSINESS_TYPE_MISMATCH","Source SQL type is not a preserving projection into the actual business column",2,{"column":name,"source":column.sql_type,"target":profiles[name].sql_type})
    if config["mapping"]["identity_policy"]=="preserve" and any(actual[name][7] for name in unmapped):raise IngestError("BUSINESS_IDENTITY_REQUIRED","identity_policy=preserve requires every identity column to be explicitly mapped from source",2)
    for name in unmapped:
        row=actual[name]
        if not(row[6] or row[7] or row[8] or row[9] or row[2] in ("timestamp","rowversion") or row[11]):raise IngestError("BUSINESS_DEFAULT_REQUIRED","Omitted nonnullable business column lacks a generated value/default",2,{"column":name})
    enabled=[r for r in observed["triggers"] if not r[1]]
    if any(r[2] for r in enabled):raise IngestError("BUSINESS_INSTEAD_OF_UNSUPPORTED","INSTEAD OF triggers lack a verified generated-key mapping",2)
    if enabled and not config["mapping"].get("allow_target_triggers",False):raise IngestError("BUSINESS_TRIGGER_POLICY","Enabled business AFTER triggers require explicit allow_target_triggers=true and unchanged supplied-value verification",2)
    if any(r[3] for r in observed["indexes"]):raise IngestError("IGNORE_DUP_KEY_UNSUPPORTED","Business IGNORE_DUP_KEY can suppress source rows",2)
    if config.get("load",{}).get("mode","append")=="replace":
        if not config.get("load",{}).get("retain_previous_generation",True):raise IngestError("BUSINESS_REPLACE_RETENTION","Mapped business replacement requires a retained complete previous generation",2)
        if observed["foreign_keys"]:raise IngestError("BUSINESS_REPLACE_DEPENDENCY","Business replacement with incoming/outgoing foreign keys requires an explicit manual migration",2,{"foreign_keys":observed["foreign_keys"]})
        events=cur.execute("SELECT t.name,e.type_desc FROM sys.triggers t JOIN sys.trigger_events e ON t.object_id=e.object_id WHERE t.parent_id=OBJECT_ID(?) AND t.is_disabled=0 AND e.type_desc IN ('DELETE','UPDATE')",target).fetchmany(129)
        if events:raise IngestError("BUSINESS_REPLACE_TRIGGER","Business replacement has active DELETE/UPDATE trigger semantics outside the verified replacement strategy",2,{"trigger_events":[list(r) for r in events]})
    # Choose actual stable key metadata, never infer identity or OUTPUT order.
    indexes=cur.execute("SELECT i.index_id,i.is_primary_key,c.name,c.is_nullable,c.is_computed,t.name FROM sys.indexes i JOIN sys.index_columns x ON i.object_id=x.object_id AND i.index_id=x.index_id JOIN sys.columns c ON x.object_id=c.object_id AND x.column_id=c.column_id JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE i.object_id=OBJECT_ID(?) AND i.is_unique=1 AND i.is_disabled=0 AND i.has_filter=0 AND i.ignore_dup_key=0 AND x.key_ordinal>0 ORDER BY i.is_primary_key DESC,i.index_id,x.key_ordinal",target).fetchmany(8193)
    choices={}
    for index,primary,name,nullable,computed,kind in indexes:
        choices.setdefault(index,[]).append((name,nullable,kind))
    key=next(([name for name,nullable,kind in columns] for columns in choices.values() if all(not nullable and kind not in ("timestamp","rowversion") for name,nullable,kind in columns)),None)
    if not key:raise IngestError("BUSINESS_STABLE_KEY_REQUIRED","Business publication requires an enabled unfiltered nonnullable unique key/primary key",2)
    generated=[profiles[name] for name in actual if name in omitted]
    key_profiles=[profiles[name] for name in key]
    verify_text_plan(cur,SchemaPlan(plan.database,plan.schema,plan.table,list(profiles.values()),plan.source_hash,plan.interpretation_hash,plan.mapping_hash))
    contract={"inspection":observed,"key":key,"version":3}
    encoded=json.dumps(contract,ensure_ascii=False,separators=(",",":"),sort_keys=True);fingerprint=hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    registry,_=tables(config)
    prior=None
    registry_exists=cur.execute("SELECT OBJECT_ID(?)",registry).fetchone()[0] is not None
    if registry_exists:prior=cur.execute(f"SELECT target_object_id,contract_hash FROM {registry} WITH (READCOMMITTEDLOCK) WHERE target=?",target).fetchone()
    if prior is not None and (prior[0]!=object_id or prior[1]!=fingerprint):raise IngestError("BUSINESS_SCHEMA_DRIFT","Existing business identity/schema/key/default/mapping contract changed; explicit migration required",2,{"published_contract_hash":prior[1],"proposed_contract_hash":fingerprint,"inspection":observed})
    if prior is None and allow_register is False:raise IngestError("BUSINESS_REGISTRY_MISSING","Acknowledged business ownership/key contract is missing; reconciliation cannot recreate it",5)
    if prior is None and allow_register is True:cur.execute(f"INSERT INTO {registry}(target,target_object_id,contract_hash,metadata_json) VALUES(?,?,?,?)",target,object_id,fingerprint,encoded)
    return key_profiles,generated,fingerprint,observed


def receipt_config(config,receipt):
    policies=receipt.get("details",{}).get("business_mapping")
    return {**config,"mapping":{**config["mapping"],**policies}} if policies else config


def _encode(values,columns):
    fields=[]
    for value,column in zip(values,columns,strict=True):
        raw=None if value is None else bytes(value) if parse_sql_type(column.sql_type).base=="varbinary" else str(parameter_value(value)).encode("utf-16-le")
        fields.append({"column":asdict(column),"value":None if raw is None else base64.b64encode(raw).decode("ascii")})
    return json.dumps(fields,ensure_ascii=True,separators=(",",":"))


def _decode(payload):
    from .types import normalize_value
    values=[];columns=[]
    for field in json.loads(payload):
        column=ColumnProfile(**field["column"]);columns.append(column);encoded=field["value"]
        if encoded is None:values.append(None);continue
        raw=base64.b64decode(encoded,validate=True)
        value=raw if parse_sql_type(column.sql_type).base=="varbinary" else raw.decode("utf-16-le")
        values.append(normalize_value(value,column.sql_type,text_encoding=column.stats.get("text_encoding")))
    return values,columns


def _lookup(cur,config,target,keys,key_columns,columns):
    # Prevent fetching malicious externally enlarged LOB cells before their bounds.
    predicates=" AND ".join(q(c.target_name)+"="+conversion(c,"?") for c in key_columns)
    params=[parameter_value(v) for v in keys]
    lengths=",".join("DATALENGTH("+read_expression(c)+")" for c in columns)
    row=cur.execute("SELECT "+lengths+" FROM "+target+" WITH (HOLDLOCK) WHERE "+predicates,*params).fetchone()
    if row is None:raise IngestError("BUSINESS_KEY_MISSING","Post-trigger generated key does not identify an existing business row",5)
    field_limit=config.get("limits",{}).get("max_in_memory_field_bytes",8388608);record_limit=config.get("limits",{}).get("max_in_memory_record_bytes",8388608)
    if any((n or 0)>field_limit for n in row) or sum(n or 0 for n in row)>record_limit:raise IngestError("BUSINESS_READBACK_BUDGET","Business row exceeds explicit bounded exact-readback policy",5)
    return list(cur.execute("SELECT "+",".join(read_expression(c) for c in columns)+" FROM "+target+" WITH (HOLDLOCK) WHERE "+predicates,*params).fetchone())


def readback(cur,config,plan,receipt,pending_receipt=None,include_generation=False):
    from .business_generations import read_target
    logical=qualified(plan.schema,plan.table);target,generation=read_target(cur,config,plan,receipt,pending_receipt)
    _,rows=tables(config);last=0;digest=new_value_digest();count=0
    bound=config.get("limits",{}).get("max_in_memory_record_bytes",8388608)*4+1048576
    sizes=cur.execute(f"SELECT MAX(DATALENGTH(key_json)),MAX(DATALENGTH(generated_json)) FROM {rows} WITH (READCOMMITTEDLOCK) WHERE intent_id=?",receipt["intent_id"]).fetchone()
    if any((n or 0)>bound for n in sizes):raise IngestError("BUSINESS_SIDECAR_BUDGET","Business sidecar exceeds explicit bounded metadata policy",5)
    while True:
        item=cur.execute(f"SELECT TOP(1) record_id,ordinal,key_json,generated_json,source_digest,target_object_id,target FROM {rows} WITH (READCOMMITTEDLOCK) WHERE intent_id=? AND ordinal>? ORDER BY ordinal",receipt["intent_id"],last).fetchone()
        if item is None:break
        checkpoint();record_id,last,key_json,generated_json,expected,object_id,sidecar_target=item
        if sidecar_target!=logical or object_id!=receipt["details"]["target_object_id"]:raise IngestError("BUSINESS_SIDECAR_IDENTITY","Business sidecar object identity differs from receipt",5)
        keys,key_columns=_decode(key_json);generated,generated_columns=_decode(generated_json)
        actual=_lookup(cur,config,target,keys,key_columns,[*plan.columns,*generated_columns]);provided=actual[:len(plan.columns)];observed_generated=actual[len(plan.columns):]
        row_digest=new_value_digest()
        try:update_value_digest(row_digest,record_id,last,provided,plan.columns)
        except IngestError as error:raise IngestError("BUSINESS_SOURCE_VALUES_CHANGED","Business values no longer fit their original typed projection",5,{"cause":error.code}) from None
        if row_digest.hexdigest()!=expected:raise IngestError("BUSINESS_SOURCE_VALUES_CHANGED","Business supplied values differ from their per-source sidecar proof",5)
        if any(canonical_value(a,c.sql_type,text_encoding=c.stats.get("text_encoding"))!=canonical_value(b,c.sql_type,text_encoding=c.stats.get("text_encoding")) for a,b,c in zip(generated,observed_generated,generated_columns,strict=True)):raise IngestError("BUSINESS_GENERATED_VALUES_CHANGED","Generated/default business values differ from their captured sidecar",5)
        update_value_digest(digest,record_id,last,provided,plan.columns);count+=1
    return (count,digest.hexdigest(),generation) if include_generation else (count,digest.hexdigest())



def verify_history(cur,config,plan,target,pending=None):
    """Verify every acknowledged typed projection; stream one receipt at a time."""
    from db_ingest.publication import _tables
    from .metadata import schema_hash
    _,receipts,_=_tables(config);last="";started=0
    while True:
        row=cur.execute(f"SELECT TOP(1) intent_id,payload FROM {receipts} WITH (READCOMMITTEDLOCK) WHERE target=? AND (?=0 OR intent_id>?) ORDER BY intent_id",target,started,last).fetchone()
        if row is None:break
        checkpoint();last=str(row[0]);started=1;r=json.loads(row[1])
        if r.get("details",{}).get("target_strategy")!="mapped_business":raise IngestError("BUSINESS_HISTORY_CONFLICT","Business target has incompatible publication history",5)
        saved=r.get("details",{}).get("business_source_columns")
        if saved is None:
            if r["details"].get("schema_hash")!=schema_hash(plan):raise IngestError("BUSINESS_HISTORY_PROJECTION_MISSING","Prior business receipt lacks its original typed source projection",5)
            historical=plan
        else:historical=SchemaPlan(plan.database,plan.schema,plan.table,[ColumnProfile(**c) for c in saved],r["source_hash"],r["interpretation_hash"],r["mapping_hash"])
        n,value=readback(cur,config,historical,r,pending_receipt=pending)
        if n!=r["committed_records"] or value!=r["value_digest"]:raise IngestError("BUSINESS_HISTORY_CHANGED","Prior business source/generated rows no longer match acknowledged receipts",5)

def publish_from_stage(cur,config,plan,target,object_id,intent_id,run_id,count,expected,transport,batches,text_evidence):
    from db_ingest.publication import _tables,_database_guid,_recovery_fork,utcnow
    from .metadata import schema_hash
    # Serialize external target writers while validating keys/defaults and sidecars.
    if object_id is None:raise IngestError("BUSINESS_TARGET_MISSING","Mapped business requires an existing target",2)
    cur.execute(f"SELECT COUNT_BIG(*) FROM {target} WITH (TABLOCKX,HOLDLOCK)").fetchone()
    keys,generated,contract,observed=inspect_target(cur,config,plan,target,object_id)
    registry,sidecars=tables(config);_,receipts,_=_tables(config)
    # Sidecar rows from all prior imports are reverified after AFTER triggers too.
    def verify_prior(pending=None):verify_history(cur,config,plan,target,pending)
    orphaned=cur.execute(f"SELECT COUNT_BIG(*) FROM {sidecars} s WITH (READCOMMITTEDLOCK) LEFT JOIN {receipts} r WITH (READCOMMITTEDLOCK) ON s.intent_id=r.intent_id WHERE s.target=? AND s.intent_id<>? AND (r.intent_id IS NULL OR r.target<>s.target OR JSON_VALUE(r.payload,'$.details.target_strategy')<>'mapped_business')",target,str(intent_id)).fetchone()[0]
    if orphaned:raise IngestError("BUSINESS_ORPHAN_SIDECARS","Prior source/key sidecars lack their matching destination receipt; publication blocked",5,{"orphan_count":orphaned})
    verify_prior()
    from .business_generations import prepare_intent
    prepared,before=prepare_intent(cur,config,plan,target,object_id,keys,observed,contract,intent_id,run_id)
    mode=config.get("load",{}).get("mode","append");retention=None
    if mode=="replace":
        if not count and not config.get("load",{}).get("allow_empty_replace",False):raise IngestError("EMPTY_REPLACE_BLOCKED","Empty business input cannot replace the usable target",2)
        from .business_generations import retain
        retention=retain(cur,config,plan,target,object_id,keys,observed,intent_id)
        cur.execute(f"DELETE FROM {target}")
        from db_ingest.transports.faults import test_fault
        test_fault(config,"REPLACE_AFTER_DELETE_BEFORE_INSERT")
    cur.execute("CREATE TABLE #db_ingest_generated_keys ("+",".join(declaration(c) for c in keys)+")")
    preserve=any(r[7] and r[0] in {c.target_name for c in plan.columns} for r in observed["columns"])
    supplied=",".join(q(c.target_name) for c in plan.columns);outputs=",".join("inserted."+q(c.target_name) for c in keys);last=0;written=0
    try:
        if preserve:cur.execute(f"SET IDENTITY_INSERT {target} ON")
        while True:
            record=cur.execute("SELECT TOP(1) __ingest_record,__ingest_ordinal FROM #db_ingest_stage WHERE __ingest_ordinal>? ORDER BY __ingest_ordinal",last).fetchone()
            if record is None:break
            checkpoint(force=True);record_id,last=record
            cur.execute("DELETE FROM #db_ingest_generated_keys")
            cur.execute(f"INSERT INTO {target} ({supplied}) OUTPUT {outputs} INTO #db_ingest_generated_keys SELECT {supplied} FROM #db_ingest_stage WHERE __ingest_record=?",record_id)
            returned=cur.execute("SELECT "+",".join(read_expression(c) for c in keys)+" FROM #db_ingest_generated_keys").fetchmany(2)
            if len(returned)!=1:raise IngestError("BUSINESS_KEY_CARDINALITY","Each source record must produce exactly one explicit generated-key mapping",5)
            key_values=list(returned[0]);actual=_lookup(cur,config,target,key_values,keys,[*plan.columns,*generated])
            staged=list(cur.execute("SELECT "+",".join(read_expression(c) for c in plan.columns)+" FROM #db_ingest_stage WHERE __ingest_record=?",record_id).fetchone())
            row_digest=new_value_digest();update_value_digest(row_digest,record_id,last,staged,plan.columns)
            observed_digest=new_value_digest()
            try:update_value_digest(observed_digest,record_id,last,actual[:len(plan.columns)],plan.columns)
            except IngestError as error:raise IngestError("BUSINESS_SOURCE_TRANSFORMED","Business target/trigger produced values outside the source's exact typed projection; rollback",5,{"record_id":record_id,"cause":error.code}) from None
            if row_digest.hexdigest()!=observed_digest.hexdigest():raise IngestError("BUSINESS_SOURCE_TRANSFORMED","Business target/trigger changed supplied values; transaction will roll back",5,{"record_id":record_id})
            cur.execute(f"INSERT INTO {sidecars}(intent_id,record_id,ordinal,target,target_object_id,key_json,generated_json,source_digest) VALUES(?,?,?,?,?,?,?,?)",str(intent_id),record_id,last,target,object_id,_encode(key_values,keys),_encode(actual[len(plan.columns):],generated),row_digest.hexdigest());written+=1
    finally:
        if preserve:
            try:cur.execute(f"SET IDENTITY_INSERT {target} OFF")
            except Exception:pass
    details={"target_strategy":"mapped_business","target_object_id":object_id,"database_guid":_database_guid(cur),"recovery_fork_guid":_recovery_fork(cur),"schema_hash":schema_hash(plan),"business_contract_hash":contract,"business_source_columns":[asdict(c) for c in plan.columns],"business_mapping":{key:config["mapping"][key] for key in ("omit_target_columns","identity_policy","allow_target_triggers")},"operation":mode,"transport":transport,"staging_batches":batches,"control_mirror_state":"pending","value_verification":"all supplied cells after synchronous AFTER triggers, generated/default values captured and reverified, all prior imported rows reverified","key_columns":[c.target_name for c in keys],"omitted_columns":[c.target_name for c in generated],"sidecar_table":sidecars,"business_prepared":prepared,"publication_attempt":config.get("_publication_attempt",{"number":1}),"varchar_representation":text_evidence}
    if retention is not None:
        details["business_previous_generation"]=retention
        details["previous_generation"]=retention["target"]
    receipt=PublicationReceipt(str(intent_id),str(run_id),plan.database,plan.schema,plan.table,plan.source_hash,plan.interpretation_hash,plan.mapping_hash,count,expected,config.get("verification",{}).get("level","full"),utcnow(),details)
    n,value=readback(cur,config,plan,asdict(receipt));verify_prior(asdict(receipt))
    if written!=count or n!=count or value!=expected:raise IngestError("BUSINESS_VALUE_VERIFICATION_FAILED","Business source identities/values differ before commit",5)
    return receipt
