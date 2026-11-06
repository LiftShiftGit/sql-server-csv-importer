"""Verify historical receipts against explicitly retained replacement generations."""
import re
from db_ingest.errors import IngestError
from .metadata import live_columns
from .ddl import qualified
from db_ingest.models import ColumnProfile
from .evolution import projection_compatible


def generation_target(value):
    if not isinstance(value,str) or len(value)>517:raise IngestError("GENERATION_IDENTITY_MISMATCH","Retained generation name is not a bounded qualified identifier",5)
    match=re.fullmatch(r"\[((?:[^\]]|\]\])*)\]\.\[((?:[^\]]|\]\])*)\]",value)
    if match is None:raise IngestError("GENERATION_IDENTITY_MISMATCH","Retained generation must use canonical bracket-quoted identifiers",5)
    try:canonical=qualified(*(part.replace("]]","]") for part in match.groups()))
    except (IngestError,UnicodeError):raise IngestError("GENERATION_IDENTITY_MISMATCH","Retained generation identifier is invalid",5) from None
    if canonical!=value:raise IngestError("GENERATION_IDENTITY_MISMATCH","Retained generation identifier is not canonical",5)
    return canonical


def readback(cur,config,plan,receipt):
    from db_ingest.publication import _readback,_tables,_database_guid,_recovery_fork
    import json
    current=qualified(plan.schema,plan.table)
    count,digest=_readback(cur,plan,current,receipt["intent_id"],config)
    if count==receipt["committed_records"] and digest==receipt["value_digest"]:return count,digest,{"kind":"current","target":current}
    _,receipts,_=_tables(config);last="";started=0
    while True:
        row=cur.execute(f"SELECT TOP(1) intent_id,payload FROM {receipts} WITH (HOLDLOCK) WHERE target=? AND (?=0 OR intent_id>?) AND JSON_VALUE(payload,'$.details.previous_generation') IS NOT NULL ORDER BY intent_id",current,started,last).fetchone()
        if row is None:break
        started=1;last=str(row[0]);replacement=json.loads(row[1]);details=replacement.get("details",{});proof=details.get("previous_generation_proof")
        if not proof:continue
        if str(details.get("database_guid","")).lower()!=_database_guid(cur).lower() or details.get("recovery_fork_guid")!=_recovery_fork(cur):continue
        target=generation_target(details.get("previous_generation"))
        if target!=proof.get("target"):raise IngestError("GENERATION_IDENTITY_MISMATCH","Retained generation name disagrees with its receipt proof",5)
        observed=cur.execute("SELECT OBJECT_ID(?)",target).fetchone()[0]
        if observed is None or observed!=proof.get("object_id"):continue
        shape=live_columns(cur,target)
        if shape!=[tuple(r) for r in proof["shape"]]:raise IngestError("GENERATION_SCHEMA_DRIFT","Retained previous generation shape differs from its acknowledged proof",5)
        retained={c["target_name"]:ColumnProfile(**c) for c in proof["columns"]}
        if any(c.target_name not in retained or not projection_compatible(c,retained[c.target_name]) for c in plan.columns):continue
        n,value=_readback(cur,plan,target,receipt["intent_id"],config)
        if n==receipt["committed_records"] and value==receipt["value_digest"]:
            return n,value,{"kind":"retained_generation","target":target,"object_id":observed,"replacement_intent_id":replacement["intent_id"],"verification":"original receipt digest under original SQL types"}
    return count,digest,{"kind":"current","target":current,"historical_generation":"no exact acknowledged retained generation matched"}
