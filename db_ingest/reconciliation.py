"""Bounded destination readback checked against durable destination-local receipt."""
from dataclasses import asdict,is_dataclass
from db_ingest.errors import IngestError
from db_ingest.adapters.mssql.connect import connect
from db_ingest.adapters.mssql.ddl import qualified
from db_ingest.adapters.mssql.diagnostics import provider_error
from db_ingest.adapters.mssql.metadata import require_supported_target
from db_ingest.publication import _lock,_resource,_get_receipt,_readback

def verify_destination(config,plan,receipt,username=None,password=None):
    payload=asdict(receipt) if is_dataclass(receipt) else receipt
    conn=connect(config,username,password,plan.database)
    try:
        cur=conn.cursor();cur.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        _lock(cur,_resource(plan),config.get("load",{}).get("target_lock_timeout_seconds",60))
        actual=_get_receipt(cur,config,plan,payload["intent_id"])
        if actual is None: raise IngestError("RECEIPT_MISSING","Previously committed receipt is absent; target restore/replacement is possible. Do not replay",5)
        for key in ("committed_records","value_digest","assurance"):
            if actual[key]!=payload[key]:raise IngestError("RECEIPT_MISMATCH",f"Local and destination receipt disagree on {key}",5)
        target=qualified(plan.schema,plan.table)
        cur.execute(f"SELECT COUNT_BIG(*) FROM {target} WITH (TABLOCKX,HOLDLOCK)").fetchone()
        if payload.get("details",{}).get("target_strategy")=="mapped_business":
            from db_ingest.adapters.mssql.business import readback as business_readback,inspect_target,receipt_config
            contract=inspect_target(cur,receipt_config(config,payload),plan,target,payload["details"]["target_object_id"],allow_register=False)[2]
            if contract!=payload["details"]["business_contract_hash"]:raise IngestError("BUSINESS_CONTRACT_MISMATCH","Business receipt/live contract differs",5)
            count,digest,generation=business_readback(cur,config,plan,payload,include_generation=True)
            generation={**generation,"strategy":"mapped_business","sidecar":"exact source/generated-key identity"}
        else:
            require_supported_target(cur,plan,target,config=config)
            from db_ingest.adapters.mssql.generations import readback
            count,digest,generation=readback(cur,config,plan,payload)
        if count!=actual["committed_records"] or digest!=actual["value_digest"]:
            raise IngestError("DESTINATION_MISMATCH","Current destination values/lineage differ from the publication receipt",5,{"receipt_count":actual["committed_records"],"observed_count":count,"receipt_digest":actual["value_digest"],"observed_digest":digest})
        return {"state":"VERIFIED","committed_records":count,"value_digest":digest,"assurance":actual["assurance"],"generation":generation,"snapshot":"current serializable transaction; later external edits require new reconciliation"}
    except IngestError:raise
    except Exception as exc:raise provider_error(exc,"destination reconciliation",(password,),5) from None
    finally:
        try:conn.rollback()
        finally:conn.close()
