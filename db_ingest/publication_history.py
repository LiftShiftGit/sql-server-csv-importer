"""Fresh-journal duplicate-source gate; current-intent recovery has a stricter proof."""
from .errors import IngestError
from .adapters.mssql.ddl import qualified
from .adapters.mssql.connect import connect
from .adapters.mssql.diagnostics import provider_error


def _preview(cur,table,where,parameters):
    count=cur.execute(f"SELECT COUNT_BIG(*) FROM {table} WITH (READCOMMITTEDLOCK) WHERE "+where,*parameters).fetchone()[0]
    if not count:return 0,[]
    rows=cur.execute(f"SELECT TOP(20) intent_id,run_id,committed_records,recorded_at_utc FROM {table} WITH (READCOMMITTEDLOCK) WHERE "+where+" ORDER BY intent_id",*parameters)
    return count,[{"intent_id":r[0],"run_id":r[1],"committed_records":r[2],"recorded_at_utc":r[3]} for r in rows]


def check_source_history(cur,config,plan,intent_id,username=None,password=None):
    """Called under destination target lock, after handling this intent's receipt.

    Query fixed-size hash/identity fields in SQL, never load an unbounded history
    or historical receipt JSON. A normal new import can retain pending audit while
    control is offline; recovery/replacement use their separate mandatory proofs.
    """
    if config.get("_explicit_new_intent",False):return {"explicit_new_intent":True,"destination":"explicit new-intent choice","central":"explicit new-intent choice"}
    schema=config.get("audit",{}).get("control_schema","audit")
    table=qualified(schema,"__db_ingest_receipts")
    # JSON_VALUE has a bounded nvarchar4000 scalar result; this reads no row payloads.
    where="target=? AND intent_id<>? AND JSON_VALUE(payload,'$.source_hash')=? AND JSON_VALUE(payload,'$.interpretation_hash')=? AND JSON_VALUE(payload,'$.mapping_hash')=?"
    parameters=(qualified(plan.schema,plan.table),str(intent_id),plan.source_hash,plan.interpretation_hash,plan.mapping_hash)
    count=cur.execute(f"SELECT COUNT_BIG(*) FROM {table} WITH (READCOMMITTEDLOCK) WHERE "+where,*parameters).fetchone()[0]
    if count:
        rows=cur.execute(f"SELECT TOP(20) intent_id,JSON_VALUE(payload,'$.run_id'),JSON_VALUE(payload,'$.committed_records'),JSON_VALUE(payload,'$.recorded_at_utc') FROM {table} WITH (READCOMMITTEDLOCK) WHERE "+where+" ORDER BY intent_id",*parameters)
        preview=[{"intent_id":r[0],"run_id":r[1],"committed_records":r[2],"recorded_at_utc":r[3]} for r in rows]
        raise IngestError("NEW_INTENT_REQUIRED","Identical source, interpretation and mapping already have destination receipts; resume the existing run or explicitly choose --new-intent",2,{"history_source":"destination","history_count":count,"history_preview":preview,"history_truncated":count>len(preview)})
    evidence={"explicit_new_intent":False,"destination":"no identical acknowledged source in destination receipts","central":"NOT_CHECKED"}
    central=None
    try:
        central=connect(config,username,password,config["destination"]["control_database"])
        reader=central.cursor();control=qualified(schema,"PublicationReceipts")
        if reader.execute("SELECT OBJECT_ID(?)",control).fetchone()[0] is None:
            evidence["central"]="UNAVAILABLE; receipt metadata absent or hidden"
            return evidence
        where="destination_database=? AND target=? AND intent_id<>? AND source_hash=? AND interpretation_hash=? AND mapping_hash=?"
        parameters=(plan.database,qualified(plan.schema,plan.table),str(intent_id),plan.source_hash,plan.interpretation_hash,plan.mapping_hash)
        count,preview=_preview(reader,control,where,parameters)
        if count:raise IngestError("NEW_INTENT_REQUIRED","Identical source, interpretation and mapping have central acknowledged history; missing destination history may indicate a restore. Review history and explicitly choose a new intent",2,{"history_source":"central","history_count":count,"history_preview":preview,"history_truncated":count>len(preview)})
        evidence["central"]="no identical acknowledged source in visible central receipts"
        return evidence
    except IngestError as error:
        if error.code=="NEW_INTENT_REQUIRED":raise
        evidence.update(central="UNAVAILABLE; normal new-import audit fallback retained",central_diagnostic={"code":error.code,"sqlstate":error.context.get("sqlstate"),"native_code":error.context.get("native_code")})
        return evidence
    except Exception as error:
        diagnostic=provider_error(error,"central duplicate-source history",(password,))
        evidence.update(central="UNAVAILABLE; normal new-import audit fallback retained",central_diagnostic={"code":diagnostic.code,"sqlstate":diagnostic.context.get("sqlstate"),"native_code":diagnostic.context.get("native_code")})
        return evidence
    finally:
        if central is not None:
            try:central.rollback()
            finally:central.close()
