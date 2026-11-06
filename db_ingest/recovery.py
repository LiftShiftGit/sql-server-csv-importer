"""Transaction-resolved replay proofs using destination, local and central history.

No missing receipt alone authorizes replay. Proofs are rechecked under the publication
lock, and the central history read transaction stays held through the new commit.
"""
from dataclasses import asdict,is_dataclass
import hashlib
import json
from db_ingest.errors import IngestError
from db_ingest.models import SchemaPlan,ColumnProfile
from db_ingest.adapters.mssql.connect import connect
from db_ingest.adapters.mssql.ddl import qualified
from db_ingest.adapters.mssql.metadata import schema_hash,require_supported_target
from db_ingest.adapters.mssql.diagnostics import provider_error
from db_ingest.resources import checkpoint


def _payload(value):return asdict(value) if is_dataclass(value) else value

def _plan(value):
    if isinstance(value,SchemaPlan):return value
    return SchemaPlan(**{**value,"columns":[ColumnProfile(**c) if isinstance(c,dict) else c for c in value["columns"]]})

def identity(payload):
    payload=_payload(payload);details=payload.get("details",{})
    fields=("intent_id","run_id","database","schema","table","source_hash","interpretation_hash","mapping_hash","committed_records","value_digest","assurance","recorded_at_utc")
    result={k:payload.get(k) for k in fields}
    result["destination"]={k:details.get(k) for k in ("database_guid","target_object_id","schema_hash","recovery_fork_guid","operation")}
    return result

def _update(digest,payload):
    encoded=json.dumps(identity(payload),sort_keys=True,separators=(",",":"),ensure_ascii=True).encode()
    digest.update(len(encoded).to_bytes(8,"big"));digest.update(encoded)

def _history_hash(cur,config,plan):
    from .publication import _tables
    _,table,_=_tables(config)
    digest=hashlib.sha256(b"db_ingest:receipt-history:v1\x00");count=0
    cur.execute(f"SELECT payload FROM {table} WITH (READCOMMITTEDLOCK) WHERE target=? ORDER BY intent_id",qualified(plan.schema,plan.table))
    while True:
        row=cur.fetchone()
        if row is None:break
        checkpoint();_update(digest,json.loads(row[0]));count+=1
    return {"count":count,"sha256":digest.hexdigest()}

def _destination_state(cur,config,plan,intent_id):
    if config.get("mapping",{}).get("existing_target_strategy")=="mapped_business":
        from .adapters.mssql.business_generations import absence_state
        return absence_state(cur,config,plan,intent_id)
    from .publication import _tables,_database_guid,_recovery_fork
    target=qualified(plan.schema,plan.table)
    object_id=cur.execute("SELECT OBJECT_ID(?)",target).fetchone()[0]
    _,_,registry=_tables(config)
    owner=cur.execute(f"SELECT target_object_id,schema_hash,mapping_hash,version FROM {registry} WITH (READCOMMITTEDLOCK) WHERE target=?",target).fetchone()
    if owner is not None and owner[3]!=1:raise IngestError("RECOVERY_METADATA_VERSION","Unknown destination ownership metadata version blocks replay proof",5,{"observed_version":owner[3]})
    if object_id is None:
        if owner is not None:raise IngestError("RECOVERY_TARGET_MISSING","Owned target is missing; replay proof cannot rule out restore/replacement",5)
    else:
        if owner is None or owner[0]!=object_id:raise IngestError("RECOVERY_TARGET_IDENTITY","Target is not the original registered landing object",5)
        cur.execute(f"SELECT COUNT_BIG(*) FROM {target} WITH (TABLOCKX,HOLDLOCK)").fetchone()
        storage_plan=plan
        if owner[1]!=schema_hash(plan):
            from .adapters.mssql.evolution import prepare,adapt_inferred
            try:effective,_=adapt_inferred(cur,config,plan,target,object_id,owner)
            except IngestError as exc:raise IngestError("RECOVERY_SCHEMA_DRIFT",str(exc),5,exc.context) from None
            if owner[1]==schema_hash(effective):storage_plan=effective
            else:
                policies=config.get("schema",{})
                if not any(policies.get(key,False) for key in ("allow_add_columns_on_append","allow_widen_on_append","allow_retype_on_append")):raise IngestError("RECOVERY_SCHEMA_DRIFT","Live registered schema differs from the interrupted plan",5)
                # Validate without ALTER or metadata seed: precommit death can leave
                # the acknowledged OLD shape instead of the proposed evolved shape.
                try:diff=prepare(cur,config,effective,target,object_id,owner,dry_run=True)
                except IngestError as exc:raise IngestError("RECOVERY_SCHEMA_DRIFT",str(exc),5,exc.context) from None
                storage_plan=SchemaPlan(plan.database,plan.schema,plan.table,[ColumnProfile(**c) for c in diff["published_columns"]],plan.source_hash,plan.interpretation_hash,plan.mapping_hash)
        try:require_supported_target(cur,storage_plan,target,config=config)
        except IngestError as exc:raise IngestError("RECOVERY_SCHEMA_DRIFT",str(exc),5,exc.context) from None
        own=cur.execute(f"SELECT COUNT_BIG(*) FROM {target} WITH (HOLDLOCK) WHERE [__ingest_intent]=?",intent_id).fetchone()[0]
        if own:raise IngestError("RECOVERY_ORPHAN_ROWS","Current-intent rows exist without the required receipt; replay blocked",5,{"orphan_records":own})
    return {"database_guid":_database_guid(cur),"recovery_fork_guid":_recovery_fork(cur),"object_id":object_id,"schema_hash":schema_hash(plan),"storage_schema_hash":owner[1] if owner is not None else None,"target":target,"database":plan.database,"intent_id":str(intent_id),"interpretation_hash":plan.interpretation_hash,"mapping_hash":plan.mapping_hash}


def _central_history(config,plan,intent_id,username,password):
    """Return a held read transaction and a streaming receipt cursor, never create DBs."""
    database=config.get("destination",{}).get("control_database","IngestControl")
    connection=None
    try:
        try:connection=connect(config,username,password,database)
        except IngestError as original:
            if original.code!="DATABASE_ACCESS":raise
            # Only privileged complete catalog visibility plus the cooperating DB
            # creation lock can establish that the control DB currently is absent.
            from .publication import _lock
            bootstrap=config.get("destination",{}).get("bootstrap_database","master")
            probe=connect(config,username,password,bootstrap)
            try:
                cur=probe.cursor();_lock(cur,"db_ingest:create:"+hashlib.sha256(database.casefold().encode()).hexdigest(),config.get("load",{}).get("target_lock_timeout_seconds",60))
                admin=cur.execute("SELECT IS_SRVROLEMEMBER('sysadmin')").fetchone()[0]
                observed=cur.execute("SELECT name,state_desc FROM sys.databases WHERE name=?",database).fetchone()
                if observed is None and admin==1:return probe,None,{"state":"PRIVILEGED_DATABASE_ABSENT","database":database}
            except Exception:
                probe.rollback();probe.close();raise
            probe.rollback();probe.close()
            if observed is None or observed[1]!="ONLINE":raise original
            connection=connect(config,username,password,database)
        table=qualified(config.get("audit",{}).get("control_schema","audit"),"PublicationReceipts")
        cur=connection.cursor();cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        if cur.execute("SELECT OBJECT_ID(?)",table).fetchone()[0] is None:raise IngestError("RECOVERY_CONTROL_UNAVAILABLE","Central receipt history is absent or hidden; absence proof is unavailable",5)
        cur.execute(f"SELECT receipt_json,intent_id,destination_database,target,source_hash,interpretation_hash,mapping_hash,committed_records,value_digest,assurance FROM {table} WITH (TABLOCK,HOLDLOCK) WHERE (destination_database=? AND target=?) OR intent_id=? ORDER BY intent_id",plan.database,qualified(plan.schema,plan.table),intent_id)
        return connection,cur,{"state":"OBSERVED","database":database}
    except Exception as exc:
        if connection is not None:
            try:connection.rollback()
            finally:connection.close()
        if isinstance(exc,IngestError) and exc.exit_code==5:raise
        error=exc if isinstance(exc,IngestError) else provider_error(exc,"recovery central-history probe",(password,),5)
        raise IngestError("RECOVERY_CONTROL_UNAVAILABLE","Cannot prove the complete central receipt history: "+str(error),5,error.context) from None


def _check_central(cur,control_cursor,config,plan,intent_id,observed):
    from .publication import _get_receipt
    digest=hashlib.sha256(b"db_ingest:central-history:v1\x00");count=0
    if control_cursor is not None:
        while True:
            row=control_cursor.fetchone()
            if row is None:break
            checkpoint();payload=json.loads(row[0])
            columns=("intent_id","database","target","source_hash","interpretation_hash","mapping_hash","committed_records","value_digest","assurance")
            stored=(payload.get("intent_id"),payload.get("database"),qualified(payload.get("schema",""),payload.get("table","")),payload.get("source_hash"),payload.get("interpretation_hash"),payload.get("mapping_hash"),payload.get("committed_records"),payload.get("value_digest"),payload.get("assurance"))
            if tuple(row[1:])!=stored:raise IngestError("RECOVERY_CONTROL_CONFLICT","Central receipt columns conflict with its retained JSON identity",5)
            if payload["intent_id"]==intent_id:raise IngestError("RECOVERY_ACKNOWLEDGED_RECEIPT_MISSING","Central history acknowledges this intent but its destination receipt is missing; no replay",5)
            if payload["database"]!=plan.database or qualified(payload["schema"],payload["table"])!=qualified(plan.schema,plan.table):raise IngestError("RECOVERY_INTENT_CONFLICT","Central intent belongs to a different destination",5)
            historical=SchemaPlan(payload["database"],payload["schema"],payload["table"],plan.columns,payload["source_hash"],payload["interpretation_hash"],payload["mapping_hash"])
            destination=_get_receipt(cur,config,historical,payload["intent_id"])
            if destination is None or identity(destination)!=identity(payload):raise IngestError("RECOVERY_CENTRAL_HISTORY_MISMATCH","Centrally acknowledged receipt is missing or conflicts with destination history",5,{"intent_id":payload["intent_id"]})
            _update(digest,payload);count+=1
    return {**observed,"count":count,"sha256":digest.hexdigest()}


def _close(connection):
    if connection is not None:
        try:connection.rollback()
        finally:connection.close()


def resolve_publication(config,plan,intent_id,known_receipts=(),username=None,password=None,current_receipt=None):
    from .publication import _lock,_resource,_get_receipt,_tables,_verify_current_receipt
    conn=connect(config,username,password,plan.database);control=None
    try:
        cur=conn.cursor();cur.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        _lock(cur,_resource(plan),config.get("load",{}).get("target_lock_timeout_seconds",60))
        _,receipts,registry=_tables(config)
        if cur.execute("SELECT OBJECT_ID(?)",receipts).fetchone()[0] is None or cur.execute("SELECT OBJECT_ID(?)",registry).fetchone()[0] is None:raise IngestError("RECOVERY_METADATA_MISSING","Destination recovery metadata is missing/hidden; no replay",5)
        receipt=_get_receipt(cur,config,plan,intent_id)
        if receipt is not None:
            if current_receipt is not None and identity(receipt)!=identity(current_receipt):raise IngestError("RECEIPT_MISMATCH","Local acknowledged receipt conflicts with destination",5)
            _verify_current_receipt(cur,config,plan,receipt)
            return {"state":"COMMITTED","receipt":receipt}
        if current_receipt is not None:raise IngestError("RECEIPT_MISSING","A locally acknowledged current receipt is missing; no replay",5)
        state=_destination_state(cur,config,plan,intent_id)
        local_count=0;local_hash=hashlib.sha256(b"db_ingest:local-history:v1\x00")
        for item in known_receipts:
            checkpoint();historical=_plan(item["plan"]);payload=_payload(item["receipt"])
            if payload["intent_id"]==intent_id:raise IngestError("RECEIPT_MISSING","Known local history acknowledges this missing current receipt; no replay",5)
            if historical.database.casefold()!=plan.database.casefold() or qualified(historical.schema,historical.table).casefold()!=qualified(plan.schema,plan.table).casefold():raise IngestError("RECOVERY_HISTORY_SCOPE","Caller supplied unrelated destination history",5)
            destination=_get_receipt(cur,config,historical,payload["intent_id"])
            if destination is None or identity(destination)!=identity(payload):raise IngestError("RECOVERY_LOCAL_HISTORY_MISMATCH","A locally acknowledged prior receipt is missing or conflicts with destination",5,{"intent_id":payload["intent_id"]})
            _update(local_hash,payload);local_count+=1
        control,reader,observed=_central_history(config,plan,intent_id,username,password)
        central=_check_central(cur,reader,config,plan,intent_id,observed)
        destination=_history_hash(cur,config,plan)
        if state["object_id"] is None and (local_count or central["count"] or destination["count"]):raise IngestError("RECOVERY_TARGET_HISTORY_MISSING","Target is absent but retained publication history exists; no replay",5)
        proof={"version":1,**state,"local_history":{"count":local_count,"sha256":local_hash.hexdigest()},"central_history":central,"destination_history":destination,"original_transaction":"same publication lock acquired after original transaction resolution","current_intent_records":0,"assurance":"receipt identity/history and current target lineage snapshot; no acknowledged current receipt missing"}
        return {"state":"RESOLVED_ABSENT","proof":proof}
    except IngestError:raise
    except Exception as exc:raise provider_error(exc,"publication outcome resolution",(password,),5) from None
    finally:_close(control);_close(conn)


def validate_resolution(cur,config,plan,intent_id,proof,username,password):
    """Called under the publication lock. Returned control guard stays held to COMMIT."""
    control=None
    try:
        if proof.get("version")!=1:raise IngestError("RECOVERY_PROOF_VERSION","Unknown publication resolution proof",5)
        state=_destination_state(cur,config,plan,intent_id)
        for key,value in state.items():
            if proof.get(key)!=value:raise IngestError("RECOVERY_PROOF_DRIFT","Destination identity/schema changed after resolution; repeat resolution without replay",5,{"field":key,"expected":proof.get(key),"actual":value})
        if _history_hash(cur,config,plan)!=proof.get("destination_history"):raise IngestError("RECOVERY_HISTORY_CHANGED","Destination receipt history changed after resolution; no replay",5)
        control,reader,observed=_central_history(config,plan,intent_id,username,password)
        if _check_central(cur,reader,config,plan,intent_id,observed)!=proof.get("central_history"):raise IngestError("RECOVERY_CONTROL_HISTORY_CHANGED","Central receipt history changed after resolution; no replay",5)
        return control
    except Exception:
        _close(control);raise
