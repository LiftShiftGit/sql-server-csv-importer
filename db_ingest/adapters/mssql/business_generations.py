"""Immutable retained business generations and original-intent lookup."""
import hashlib,json,uuid
from dataclasses import asdict
from db_ingest.errors import IngestError
from db_ingest.models import ColumnProfile
from db_ingest.receipts import new_value_digest,update_value_digest
from db_ingest.resources import checkpoint
from db_ingest.planning import truncate_utf16
from .ddl import qualified,quote_identifier as q
from .types import read_expression
from .generations import generation_target


def tables(config):
    schema=config.get("audit",{}).get("control_schema","audit")
    return qualified(schema,"__db_ingest_business_generations"),qualified(schema,"__db_ingest_business_intent_generations")


def bootstrap(cur,config):
    generations,mappings=tables(config)
    definitions=((generations,"generation_id char(32) NOT NULL PRIMARY KEY,logical_target nvarchar(260) NOT NULL,original_object_id int NOT NULL,generation_target nvarchar(517) NOT NULL,generation_object_id int NOT NULL,replacement_intent nvarchar(64) NOT NULL,proof_sha256 char(64) NOT NULL,proof_json nvarchar(max) NOT NULL"),(mappings,"intent_id nvarchar(64) NOT NULL PRIMARY KEY,generation_id char(32) NOT NULL REFERENCES "+generations+"(generation_id)"))
    for name,definition in definitions:
        if cur.execute("SELECT OBJECT_ID(?)",name).fetchone()[0] is None:
            if not config.get("destination",{}).get("create_tables",False):raise IngestError("BUSINESS_GENERATION_METADATA","Business generation metadata is absent and creation disabled",2)
            cur.execute(f"CREATE TABLE {name} ({definition})")


def shape(cur,target):
    return [list(row) for row in cur.execute("SELECT c.name,t.name,c.max_length,c.precision,c.scale,c.is_nullable,c.is_identity,c.is_computed,c.generated_always_type,c.collation_name FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id",target)]


def full_snapshot(cur,config,target,columns,keys):
    lengths=["COALESCE(DATALENGTH("+read_expression(c)+"),0)" for c in columns]
    aggregate=cur.execute("SELECT "+",".join("MAX("+value+")" for value in lengths)+",MAX("+"+".join("CONVERT(bigint,"+value+")" for value in lengths)+") FROM "+target+" WITH (HOLDLOCK)").fetchone()
    field_limit=config.get("limits",{}).get("max_in_memory_field_bytes",8388608);record_limit=config.get("limits",{}).get("max_in_memory_record_bytes",8388608)
    if any((n or 0)>field_limit for n in aggregate[:-1]) or (aggregate[-1] or 0)>record_limit:raise IngestError("BUSINESS_GENERATION_BUDGET","Existing business values exceed explicit full-copy readback bounds",5)
    checkpoint((aggregate[-1] or 0)*3,force=True)
    digest=new_value_digest();count=0
    cur.execute("SELECT "+",".join(read_expression(c) for c in columns)+" FROM "+target+" WITH (HOLDLOCK) ORDER BY "+",".join(q(c.target_name) for c in keys))
    while True:
        row=cur.fetchone()
        if row is None:break
        checkpoint();count+=1
        try:update_value_digest(digest,"business-generation-row",count,list(row),columns)
        except IngestError as error:raise IngestError("BUSINESS_GENERATION_VALUE","Business generation contains a value outside the exact supported type representation",5,{"cause":error.code}) from None
    return {"count":count,"sha256":digest.hexdigest(),"verification":"every column including generated/default/rowversion; stable actual unique-key order"}


def _mapping_digest(cur,config,generation):
    _,mappings=tables(config);digest=hashlib.sha256(b"db_ingest:business-generation-intents:v1\0");count=0
    cur.execute(f"SELECT intent_id FROM {mappings} WITH (READCOMMITTEDLOCK) WHERE generation_id=? ORDER BY intent_id",generation)
    while True:
        row=cur.fetchone()
        if row is None:break
        checkpoint();raw=str(row[0]).encode("utf-8");digest.update(len(raw).to_bytes(4,"big"));digest.update(raw);count+=1
    return {"count":count,"sha256":digest.hexdigest()}


def retain(cur,config,plan,target,object_id,keys,observed,intent_id):
    from .business import _profile
    from db_ingest.publication import _tables,_database_guid,_recovery_fork,utcnow
    generations,mappings=tables(config);_,receipts,_=_tables(config)
    columns=[_profile(row) for row in observed["columns"]]
    for ordinal,column in enumerate(columns,1):column.ordinal=ordinal
    before=full_snapshot(cur,config,target,columns,keys)
    generation=uuid.uuid4().hex;backup=qualified(plan.schema,truncate_utf16(plan.table,90)+"__previous_"+generation[:16])
    cur.execute(f"SELECT * INTO {backup} FROM {target} WITH (TABLOCKX,HOLDLOCK)")
    copied=full_snapshot(cur,config,backup,columns,keys)
    if before!=copied:raise IngestError("BUSINESS_GENERATION_COPY_MISMATCH","Retained business copy differs in at least one actual value; replacement rolled back",5)
    copied_object=cur.execute("SELECT OBJECT_ID(?)",backup).fetchone()[0]
    # Place a temporary bounded proof before normalized memberships, then seal it.
    proof={"version":1,"generation_id":generation,"target":backup,"object_id":copied_object,"logical_target":target,"original_object_id":object_id,"database_guid":_database_guid(cur),"recovery_fork_guid":_recovery_fork(cur),"replacement_intent":str(intent_id),"created_at_utc":utcnow(),"shape":shape(cur,backup),"columns":[asdict(c) for c in columns],"keys":[asdict(c) for c in keys],"values":before}
    cur.execute(f"INSERT INTO {generations}(generation_id,logical_target,original_object_id,generation_target,generation_object_id,replacement_intent,proof_sha256,proof_json) VALUES(?,?,?,?,?,?,?,?)",generation,target,object_id,backup,copied_object,str(intent_id),"0"*64,"{}")
    cur.execute(f"INSERT INTO {mappings}(intent_id,generation_id) SELECT r.intent_id,? FROM {receipts} r WITH (READCOMMITTEDLOCK) WHERE r.target=? AND JSON_VALUE(r.payload,'$.details.target_strategy')='mapped_business' AND NOT EXISTS(SELECT 1 FROM {mappings} m WITH (READCOMMITTEDLOCK) WHERE m.intent_id=r.intent_id)",generation,target)
    proof["intent_membership"]=_mapping_digest(cur,config,generation)
    encoded=json.dumps(proof,ensure_ascii=False,separators=(",",":"));raw=encoded.encode("utf-8")
    if len(raw)*2>33554432:raise IngestError("BUSINESS_GENERATION_METADATA","Retained generation proof exceeds bounded metadata policy",2)
    proof_hash=hashlib.sha256(raw).hexdigest()
    cur.execute(f"UPDATE {generations} SET proof_sha256=?,proof_json=? WHERE generation_id=?",proof_hash,encoded,generation)
    return {"generation_id":generation,"target":backup,"object_id":copied_object,"proof_sha256":proof_hash,"all_previous_values":before,"intent_membership":proof["intent_membership"],"registry":generations}


def _load(cur,config,generation,pending=None,expected=None,logical=None,original_id=None):
    from db_ingest.publication import _tables,_database_guid,_recovery_fork
    generations,_=tables(config);_,receipts,_=_tables(config)
    size=cur.execute(f"SELECT DATALENGTH(proof_json) FROM {generations} WITH (READCOMMITTEDLOCK) WHERE generation_id=?",generation).fetchone()
    if size is None:raise IngestError("BUSINESS_GENERATION_HISTORY_MISSING","Retained generation proof is missing",5)
    if (size[0] or 0)>33554432:raise IngestError("BUSINESS_GENERATION_METADATA","Retained generation proof exceeds bounded readback policy",5)
    checkpoint((size[0] or 0)*3,force=True)
    row=cur.execute(f"SELECT logical_target,original_object_id,generation_target,generation_object_id,replacement_intent,proof_sha256,proof_json FROM {generations} WITH (READCOMMITTEDLOCK) WHERE generation_id=?",generation).fetchone()
    if logical is not None and (row[0]!=logical or row[1]!=original_id):raise IngestError("BUSINESS_GENERATION_HISTORY_CONFLICT","Retained generation belongs to a different logical target/object",5)
    proof=json.loads(row[6])
    if hashlib.sha256(row[6].encode("utf-8")).hexdigest()!=row[5] or proof.get("generation_id")!=generation:raise IngestError("BUSINESS_GENERATION_HISTORY_CONFLICT","Retained generation proof bytes/identity conflict",5)
    if tuple(row[:5])!=(proof["logical_target"],proof["original_object_id"],proof["target"],proof["object_id"],proof["replacement_intent"]):raise IngestError("BUSINESS_GENERATION_HISTORY_CONFLICT","Retained generation indexed identity conflicts with its proof",5)
    if expected is not None and (expected.get("proof_sha256")!=row[5] or expected.get("target")!=row[2] or expected.get("object_id")!=row[3]):raise IngestError("BUSINESS_GENERATION_HISTORY_CONFLICT","Replacement receipt and generation proof disagree",5)
    if str(proof["database_guid"]).lower()!=_database_guid(cur).lower() or proof["recovery_fork_guid"]!=_recovery_fork(cur):raise IngestError("BUSINESS_GENERATION_IDENTITY","Retained business generation belongs to another database/recovery fork",5)
    if pending is not None and pending["intent_id"]==row[4]:replacement=pending
    else:
        ack=cur.execute(f"SELECT payload FROM {receipts} WITH (READCOMMITTEDLOCK) WHERE intent_id=?",row[4]).fetchone()
        if ack is None:raise IngestError("BUSINESS_GENERATION_ACK_MISSING","Retained generation lacks its acknowledged replacement receipt",5)
        replacement=json.loads(ack[0])
    if qualified(replacement.get("schema",""),replacement.get("table",""))!=row[0] or replacement.get("details",{}).get("target_object_id")!=row[1] or replacement.get("details",{}).get("target_strategy")!="mapped_business" or replacement.get("details",{}).get("operation")!="replace":raise IngestError("BUSINESS_GENERATION_ACK_CONFLICT","Replacement receipt belongs to a different object/operation",5)
    summary=replacement.get("details",{}).get("business_previous_generation")
    if not summary or summary.get("generation_id")!=generation or summary.get("proof_sha256")!=row[5]:raise IngestError("BUSINESS_GENERATION_ACK_CONFLICT","Replacement receipt does not acknowledge this exact retained generation",5)
    target=generation_target(row[2])
    if cur.execute("SELECT OBJECT_ID(?)",target).fetchone()[0]!=row[3] or shape(cur,target)!=proof["shape"]:raise IngestError("BUSINESS_GENERATION_IDENTITY","Retained business object is absent or has changed shape/identity",5)
    if _mapping_digest(cur,config,generation)!=proof["intent_membership"]:raise IngestError("BUSINESS_GENERATION_HISTORY_MISSING","Original intent memberships in retained generation were lost/changed",5)
    columns=[ColumnProfile(**c) for c in proof["columns"]];keys=[ColumnProfile(**c) for c in proof["keys"]]
    if not 1<=len(columns)<=1024:raise IngestError("BUSINESS_GENERATION_METADATA","Retained column count is outside the supported bound",5)
    if full_snapshot(cur,config,target,columns,keys)!=proof["values"]:raise IngestError("BUSINESS_GENERATION_VALUES_CHANGED","Retained business generation differs from its complete acknowledged copy proof",5)
    return target,{"kind":"retained_business_generation","generation_id":generation,"target":target,"object_id":row[3],"proof_sha256":row[5]}


def read_target(cur,config,plan,receipt,pending=None):
    from db_ingest.publication import _tables
    generations,mappings=tables(config);logical=qualified(plan.schema,plan.table)
    exists=[cur.execute("SELECT OBJECT_ID(?)",name).fetchone()[0] is not None for name in (generations,mappings)]
    if not all(exists):
        _,receipts,_=_tables(config)
        replaced=cur.execute(f"SELECT COUNT_BIG(*) FROM {receipts} WITH (READCOMMITTEDLOCK) WHERE target=? AND JSON_VALUE(payload,'$.details.target_strategy')='mapped_business' AND JSON_VALUE(payload,'$.details.operation')='replace'",logical).fetchone()[0]
        if any(exists) or replaced:raise IngestError("BUSINESS_GENERATION_HISTORY_MISSING","Retained business generation metadata is absent or partially missing",5)
        return logical,{"kind":"current","target":logical}
    # Validate every generation's membership seal, including when the current
    # intent's mapping was deleted. Iterate one generation at a time, not a list.
    last=""
    while True:
        row=cur.execute(f"SELECT TOP(1) generation_id,DATALENGTH(proof_json) FROM {generations} WITH (READCOMMITTEDLOCK) WHERE logical_target=? AND generation_id>? ORDER BY generation_id",logical,last).fetchone()
        if row is None:break
        last=row[0]
        if (row[1] or 0)>33554432:raise IngestError("BUSINESS_GENERATION_METADATA","Generation membership proof exceeds bounded metadata policy",5)
        checkpoint((row[1] or 0)*3,force=True)
        metadata,metadata_sha=cur.execute(f"SELECT proof_json,proof_sha256 FROM {generations} WITH (READCOMMITTEDLOCK) WHERE generation_id=?",last).fetchone()
        if hashlib.sha256(metadata.encode("utf-8")).hexdigest()!=metadata_sha:raise IngestError("BUSINESS_GENERATION_HISTORY_CONFLICT","Retained generation proof bytes changed",5)
        proof=json.loads(metadata)
        if _mapping_digest(cur,config,last)!=proof.get("intent_membership"):raise IngestError("BUSINESS_GENERATION_HISTORY_MISSING","Retained business intent membership history changed",5)
    own=receipt.get("details",{}).get("business_previous_generation")
    if own:_load(cur,config,own["generation_id"],pending or receipt,own,logical,receipt["details"]["target_object_id"])
    selected=cur.execute(f"SELECT generation_id FROM {mappings} WITH (READCOMMITTEDLOCK) WHERE intent_id=?",receipt["intent_id"]).fetchone()
    if selected is None:return logical,{"kind":"current","target":logical}
    return _load(cur,config,selected[0],pending,logical=logical,original_id=receipt["details"]["target_object_id"])


def _prepared_path(config,run_id,intent_id):
    import re
    from pathlib import Path
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}",str(run_id or "")) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}",str(intent_id)):raise IngestError("BUSINESS_PREPARED_IDENTITY","Business prepared proof requires bounded path-safe run/intent identities",5)
    return Path(config["preservation"]["work_directory"])/"business"/str(run_id)/str(intent_id)/"prepared.json"


def _read_prepared(path):
    import os
    try:
        with path.open("rb") as stream:raw=stream.read(33554433)
    except OSError as error:raise IngestError("RECOVERY_BUSINESS_BASELINE_MISSING","Business outcome lacks its durable pre-DML baseline; leave intent unknown and obtain explicit review",5,{"artifact":str(path),"cause":type(error).__name__}) from None
    if len(raw)>33554432:raise IngestError("BUSINESS_PREPARED_BUDGET","Business prepared artifact exceeds bounded32MiB policy",5)
    checkpoint(len(raw)*3,force=True)
    try:proof=json.loads(raw.decode("utf-8",errors="strict"))
    except (ValueError,UnicodeError):raise IngestError("RECOVERY_BUSINESS_BASELINE_INVALID","Business prepared artifact is partial or invalid; no absence inferred",5) from None
    return proof,{"path":str(path),"sha256":hashlib.sha256(raw).hexdigest(),"bytes":len(raw),"run_id":proof.get("run_id"),"intent_id":proof.get("intent_id"),"values":proof.get("values")}


def prepare_intent(cur,config,plan,target,object_id,keys,observed,contract,intent_id,run_id):
    """Durably seal the complete existing table BEFORE any business INSERT/DELETE."""
    import os
    from .business import _profile
    from db_ingest.publication import _database_guid,_recovery_fork,utcnow
    from db_ingest.recovery import _history_hash
    from db_ingest.capture import _atomic_replace,_sync_directory
    columns=[_profile(row) for row in observed["columns"]]
    for ordinal,column in enumerate(columns,1):column.ordinal=ordinal
    values=full_snapshot(cur,config,target,columns,keys)
    proof={"version":1,"run_id":str(run_id),"intent_id":str(intent_id),"database":plan.database,"target":target,"target_object_id":object_id,"database_guid":_database_guid(cur),"recovery_fork_guid":_recovery_fork(cur),"business_contract_hash":contract,"source_hash":plan.source_hash,"interpretation_hash":plan.interpretation_hash,"mapping_hash":plan.mapping_hash,"columns":[asdict(c) for c in columns],"keys":[asdict(c) for c in keys],"values":values,"destination_history":_history_hash(cur,config,plan)}
    path=_prepared_path(config,run_id,intent_id)
    if path.exists():
        old,metadata=_read_prepared(path)
        comparable={k:v for k,v in old.items() if k!="prepared_at_utc"}
        if comparable!=proof:raise IngestError("BUSINESS_PREPARED_BASELINE_CHANGED","Existing intent pre-DML baseline differs from current target/history; no silent rebaseline",5,{"artifact":metadata})
    else:
        path.parent.mkdir(parents=True,exist_ok=True)
        proof["prepared_at_utc"]=utcnow();raw=(json.dumps(proof,ensure_ascii=False,separators=(",",":"))+"\n").encode("utf-8")
        if len(raw)>33554432:raise IngestError("BUSINESS_PREPARED_BUDGET","Business prepared proof exceeds bounded32MiB policy",2)
        reserve=int(config.get("preservation",{}).get("minimum_free_disk_bytes",0))
        import shutil
        if shutil.disk_usage(path.parent).free<len(raw)+reserve:raise IngestError("DISK_CAPACITY","Insufficient disk reserve for durable pre-DML business proof; target unchanged",4)
        temporary=path.with_name("prepared."+uuid.uuid4().hex+".partial")
        with temporary.open("xb") as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        _atomic_replace(temporary,path);_sync_directory(path.parent)
        _,metadata=_read_prepared(path)
    callback=config.get("_business_prepared_callback")
    if callback is not None:callback(metadata)
    return metadata,values


def absence_state(cur,config,plan,intent_id):
    from .business import inspect_target,tables as business_tables,verify_history
    from .metadata import schema_hash
    from db_ingest.publication import _database_guid,_recovery_fork
    from db_ingest.recovery import _history_hash
    retained=config.get("_publication_resolution",{}).get("business_prepared") or config.get("_business_prepared") or {}
    run_id=config.get("_publication_run_id") or retained.get("run_id")
    # The file's own checksum is not independent authority. CLI consumers provide
    # the durable journal/event seal; direct API callers must supply an explicit
    # expected seal captured before DML. A pre-callback crash stays unknown.
    import re
    if not isinstance(retained,dict) or not re.fullmatch(r"[0-9a-f]{64}",str(retained.get("sha256",""))) or retained.get("run_id")!=str(run_id) or retained.get("intent_id")!=str(intent_id):raise IngestError("RECOVERY_BUSINESS_BASELINE_UNSEALED","Missing independent pre-DML journal/event seal; a self-checksummed prepared file alone cannot authorize replay",5)
    path=_prepared_path(config,run_id,intent_id);proof,metadata=_read_prepared(path)
    if retained.get("path")!=str(path) or retained.get("sha256")!=metadata["sha256"]:raise IngestError("RECOVERY_BUSINESS_BASELINE_INVALID","Durable journal proof pointer differs from prepared artifact bytes",5)
    target=qualified(plan.schema,plan.table);object_id=cur.execute("SELECT OBJECT_ID(?)",target).fetchone()[0]
    if object_id is None:raise IngestError("RECOVERY_BUSINESS_TARGET_MISSING","Original business target is absent; baseline cannot prove rollback",5)
    cur.execute(f"SELECT COUNT_BIG(*) FROM {target} WITH (TABLOCKX,HOLDLOCK)").fetchone()
    keys,_,contract,observed=inspect_target(cur,config,plan,target,object_id,allow_register=None)
    expected={"version":1,"run_id":str(run_id),"intent_id":str(intent_id),"database":plan.database,"target":target,"target_object_id":object_id,"database_guid":_database_guid(cur),"recovery_fork_guid":_recovery_fork(cur),"business_contract_hash":contract,"source_hash":plan.source_hash,"interpretation_hash":plan.interpretation_hash,"mapping_hash":plan.mapping_hash}
    if any(proof.get(k)!=v for k,v in expected.items()):raise IngestError("RECOVERY_BUSINESS_IDENTITY","Business prepared snapshot differs from current intent/source/database/fork/object/contract",5,{"artifact":metadata})
    _,rows=business_tables(config)
    own=cur.execute(f"SELECT COUNT_BIG(*) FROM {rows} WITH (READCOMMITTEDLOCK) WHERE intent_id=?",str(intent_id)).fetchone()[0]
    if own:raise IngestError("RECOVERY_BUSINESS_ORPHAN_ROWS","Business current-intent sidecars exist without its receipt; no replay",5,{"sidecar_count":own})
    columns=[ColumnProfile(**c) for c in proof["columns"]];saved_keys=[ColumnProfile(**c) for c in proof["keys"]]
    if not 1<=len(columns)<=1024:raise IngestError("RECOVERY_BUSINESS_BASELINE_INVALID","Prepared column count is not supported",5)
    actual=full_snapshot(cur,config,target,columns,saved_keys)
    if actual!=proof.get("values"):raise IngestError("RECOVERY_BUSINESS_BASELINE_CHANGED","Whole business table differs from durable pre-DML baseline; zero sidecars alone cannot authorize replay",5,{"expected":proof.get("values"),"observed":actual,"artifact":metadata})
    if _history_hash(cur,config,plan)!=proof.get("destination_history"):raise IngestError("RECOVERY_BUSINESS_HISTORY_CHANGED","Destination ACK history changed after intent preparation; no automatic replay",5)
    verify_history(cur,config,plan,target)
    return {"database_guid":expected["database_guid"],"recovery_fork_guid":expected["recovery_fork_guid"],"object_id":object_id,"schema_hash":schema_hash(plan),"storage_schema_hash":contract,"target":target,"database":plan.database,"intent_id":str(intent_id),"interpretation_hash":plan.interpretation_hash,"mapping_hash":plan.mapping_hash,"business_prepared":metadata,"business_absence":"entire locked table equals durable before-DML values; current sidecars zero; ACK history unchanged"}
