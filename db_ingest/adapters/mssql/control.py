"""Idempotent central audit mirrors and real failure-table materialization.

Destination receipts remain authoritative when the control database is unavailable.
All backfill callers must retain local durable events/artifact references until VERIFIED.
"""
from dataclasses import asdict,is_dataclass
from datetime import datetime,timezone
import hashlib
import json
from db_ingest.errors import IngestError
from db_ingest.planning import truncate_utf16
from .ddl import quote_identifier as q,qualified
from .diagnostics import provider_error

def _metadata_retry(function):
    """Retry explicit SQL rollbacks only; each call owns/closes its transaction."""
    from functools import wraps
    @wraps(function)
    def wrapped(config,*args,**kwargs):
        import random,time
        from db_ingest.resources import checkpoint
        retry=config.get("retry",{})
        attempts=int(retry.get("max_attempts",3))
        if function.__name__=="materialize_failures":
            source=args[3] if len(args)>3 else kwargs.get("records")
            if not callable(source):attempts=1 #One-shot iterators cannot safely replay.
            plan=args[0] if args else kwargs["plan"]
            run_id=args[1] if len(args)>1 else kwargs["run_id"]
            if len(args)>6:
                if args[6] is None:args=(*args[:6],failure_table_name(plan.table,run_id),*args[7:])
            elif kwargs.get("table_name") is None:kwargs["table_name"]=failure_table_name(plan.table,run_id)
        if function.__name__=="mirror_file" and kwargs.get("source_profiles") is not None and not isinstance(kwargs["source_profiles"],(list,tuple)):attempts=1
        for attempt in range(1,attempts+1):
            checkpoint()
            try:
                result=function(config,*args,**kwargs)
                if attempt>1 and isinstance(result,dict):result["metadata_attempts"]=attempt
                return result
            except IngestError as error:
                if attempt==attempts or error.context.get("native_code") not in (1205,1222):raise
                delay=min(float(retry.get("maximum_backoff_seconds",15)),float(retry.get("initial_backoff_seconds",1))*2**min(attempt-1,30))
                if retry.get("jitter",True):delay*=random.uniform(0.5,1.0)
                deadline=time.monotonic()+delay
                while time.monotonic()<deadline:
                    checkpoint();time.sleep(min(.1,max(0,deadline-time.monotonic())))
    return wrapped


CONTROL_VERSION=5
TABLES={
 "SchemaMigrations":"version int NOT NULL PRIMARY KEY, applied_at_utc nvarchar(40) NOT NULL",
 "Runs":"run_id nvarchar(64) NOT NULL PRIMARY KEY,state nvarchar(40) NOT NULL,updated_at_utc nvarchar(40) NOT NULL,summary_json nvarchar(max) NULL",
 "Files":"file_id nvarchar(128) NOT NULL,run_id nvarchar(64) NOT NULL,source_hash char(64) NOT NULL,source_path nvarchar(max) NOT NULL,artifact_path nvarchar(max) NOT NULL,summary_json nvarchar(max) NULL,PRIMARY KEY(run_id,file_id)",
 "Interpretations":"interpretation_hash char(64) NOT NULL PRIMARY KEY,policy_json nvarchar(max) NOT NULL",
 "ColumnProfiles":"file_id nvarchar(128) NOT NULL,interpretation_hash char(64) NOT NULL,ordinal int NOT NULL,original_name nvarchar(max) NOT NULL,target_name nvarchar(128) NOT NULL,type_name nvarchar(64) NOT NULL,profile_json nvarchar(max) NOT NULL,PRIMARY KEY(file_id,interpretation_hash,ordinal)",
 "SchemaVersions":"schema_version_id char(64) NOT NULL PRIMARY KEY,schema_hash char(64) NOT NULL,destination_database nvarchar(128) NOT NULL,target nvarchar(260) NOT NULL,schema_json nvarchar(max) NOT NULL",
 "Chunks":"chunk_id nvarchar(128) NOT NULL PRIMARY KEY,intent_id nvarchar(64) NOT NULL,record_count bigint NOT NULL,digest char(64) NOT NULL,state nvarchar(40) NOT NULL",
 "PublicationReceipts":"intent_id nvarchar(64) NOT NULL PRIMARY KEY,run_id nvarchar(64) NOT NULL,destination_database nvarchar(128) NOT NULL,target nvarchar(260) NOT NULL,source_hash char(64) NOT NULL,interpretation_hash char(64) NOT NULL,mapping_hash char(64) NOT NULL,committed_records bigint NOT NULL,value_digest char(64) NOT NULL,assurance nvarchar(16) NOT NULL,recorded_at_utc nvarchar(40) NOT NULL,receipt_json nvarchar(max) NOT NULL",
 "RejectedRecords":"run_id nvarchar(64) NOT NULL,record_id nvarchar(128) NOT NULL,file_id nvarchar(128) NOT NULL,logical_ordinal bigint NULL,byte_start bigint NULL,byte_end bigint NULL,payload_json nvarchar(max) NOT NULL,PRIMARY KEY(run_id,record_id)",
 "RecordErrors":"run_id nvarchar(64) NOT NULL,record_id nvarchar(128) NOT NULL,error_ordinal int NOT NULL,code nvarchar(128) NOT NULL,column_ordinal int NULL,diagnostic_json nvarchar(max) NOT NULL,PRIMARY KEY(run_id,record_id,error_ordinal)",
 "FileIncidents":"incident_id nvarchar(128) NOT NULL PRIMARY KEY,run_id nvarchar(64) NOT NULL,file_id nvarchar(128) NULL,code nvarchar(128) NOT NULL,diagnostic_json nvarchar(max) NOT NULL",
 "Events":"event_id nvarchar(128) NOT NULL PRIMARY KEY,run_id nvarchar(64) NOT NULL,recorded_at_utc nvarchar(40) NOT NULL,category nvarchar(128) NOT NULL,details_json nvarchar(max) NOT NULL",
 "SourceGroups":"intent_id nvarchar(64) NOT NULL PRIMARY KEY,group_key char(64) NOT NULL,run_id nvarchar(64) NOT NULL,destination_database nvarchar(128) NOT NULL,target nvarchar(260) NOT NULL,manifest_sha256 char(64) NOT NULL,contributor_count bigint NOT NULL,source_hash char(64) NOT NULL,details_json nvarchar(max) NOT NULL",
 "SourceGroupContributors":"group_key char(64) NOT NULL,file_id char(64) NOT NULL,relative_path nvarchar(max) NOT NULL,first_intent nvarchar(64) NOT NULL,last_intent nvarchar(64) NOT NULL,payload_json nvarchar(max) NOT NULL,PRIMARY KEY(group_key,file_id)",
 "SourceGroupContributionHistory":"group_key char(64) NOT NULL,intent_id nvarchar(64) NOT NULL,file_id char(64) NOT NULL,signature char(64) NOT NULL,source_sha256 char(64) NOT NULL,interpretation_hash char(64) NOT NULL,mapping_hash varchar(64) NOT NULL,payload_json nvarchar(max) NOT NULL,PRIMARY KEY(group_key,intent_id,file_id),INDEX IX_GroupContributionSignature NONCLUSTERED(group_key,signature),INDEX IX_GroupContributionLegacy NONCLUSTERED(group_key,file_id,source_sha256,interpretation_hash)",
 "Artifacts":"artifact_id nvarchar(128) NOT NULL PRIMARY KEY,file_id nvarchar(128) NOT NULL,sha256 char(64) NOT NULL,byte_length bigint NOT NULL,path nvarchar(max) NOT NULL",
}

def _open(config,username,password):
    from db_ingest.publication import _bootstrap_database,_lock,utcnow
    database=config.get("destination",{}).get("control_database","IngestControl")
    conn=_bootstrap_database(config,database,username,password)
    try:
        schema=config.get("audit",{}).get("control_schema","audit")
        cur=conn.cursor()
        _lock(cur,"db_ingest:control:migrations",config.get("load",{}).get("target_lock_timeout_seconds",60))
        if cur.execute("SELECT SCHEMA_ID(?)",schema).fetchone()[0] is None:
            if not config.get("destination",{}).get("create_schemas",False):raise IngestError("CONTROL_SCHEMA_DISABLED","Control schema creation is disabled")
            cur.execute("CREATE SCHEMA "+q(schema))
        migrations=qualified(schema,"SchemaMigrations")
        if cur.execute("SELECT OBJECT_ID(?)",migrations).fetchone()[0] is not None:
            newest=cur.execute(f"SELECT MAX(version) FROM {migrations}").fetchone()[0]
            if newest is not None and newest>CONTROL_VERSION:raise IngestError("CONTROL_VERSION_UNSUPPORTED","Central control schema is newer than this application")
        for name,columns in TABLES.items():
            target=qualified(schema,name)
            if cur.execute("SELECT OBJECT_ID(?)",target).fetchone()[0] is None:
                if not config.get("destination",{}).get("create_tables",False):raise IngestError("CONTROL_TABLE_DISABLED",f"Control table {name} is absent and creation is disabled")
                cur.execute(f"CREATE TABLE {target} ({columns})")
        runs=qualified(schema,"Runs")
        if cur.execute("SELECT COL_LENGTH(?,?)",runs,"summary_json").fetchone()[0] is None:
            if not config.get("destination",{}).get("create_tables",False):raise IngestError("CONTROL_MIGRATION_REQUIRED","Control schema migration2 requires permission to add Runs.summary_json")
            cur.execute(f"ALTER TABLE {runs} ADD summary_json nvarchar(max) NULL")
        files=qualified(schema,"Files")
        if cur.execute("SELECT COL_LENGTH(?,?)",files,"summary_json").fetchone()[0] is None:
            if not config.get("destination",{}).get("create_tables",False):raise IngestError("CONTROL_MIGRATION_REQUIRED","Control schema migration5 requires permission to add Files.summary_json")
            cur.execute(f"ALTER TABLE {files} ADD summary_json nvarchar(max) NULL")
        migrations=qualified(schema,"SchemaMigrations")
        versions=[r[0] for r in cur.execute(f"SELECT version FROM {migrations}").fetchall()]
        if any(v>CONTROL_VERSION for v in versions):raise IngestError("CONTROL_VERSION_UNSUPPORTED","Central control schema is newer than this application")
        for version in range(1,CONTROL_VERSION+1):
            if version not in versions:cur.execute(f"INSERT INTO {migrations} (version,applied_at_utc) VALUES (?,?)",version,utcnow())
        conn.commit()
        # All metadata writers use a single lock order. Independent run/event IDs
        # otherwise acquire overlapping SERIALIZABLE key ranges in different orders.
        _lock(cur,"db_ingest:control:audit-writes",config.get("load",{}).get("target_lock_timeout_seconds",60))
        return conn,schema
    except Exception:
        try: conn.rollback()
        finally: conn.close()
        raise

@_metadata_retry
def mirror_receipt(config,plan,receipt,username=None,password=None):
    payload=asdict(receipt) if is_dataclass(receipt) else dict(receipt)
    conn=None
    try:
        conn,schema=_open(config,username,password)
        cur=conn.cursor()
        from db_ingest.publication import _lock,utcnow
        _lock(cur,"db_ingest:control:receipt:"+payload["intent_id"],config.get("load",{}).get("target_lock_timeout_seconds",60))
        target=qualified(schema,"PublicationReceipts")
        existing=cur.execute(f"SELECT receipt_json FROM {target} WITH (UPDLOCK,HOLDLOCK) WHERE intent_id=?",payload["intent_id"]).fetchone()
        encoded=json.dumps(payload,ensure_ascii=False,separators=(",",":"))
        if existing is not None:
            old=json.loads(existing[0])
            for key in ("source_hash","mapping_hash","interpretation_hash","value_digest","committed_records","database","schema","table"):
                if old.get(key)!=payload.get(key):raise IngestError("CONTROL_RECEIPT_CONFLICT","Central receipt identity/value evidence conflicts with the destination",5)
        else:
            cur.execute(f"INSERT INTO {target} (intent_id,run_id,destination_database,target,source_hash,interpretation_hash,mapping_hash,committed_records,value_digest,assurance,recorded_at_utc,receipt_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",payload["intent_id"],payload["run_id"],payload["database"],qualified(payload["schema"],payload["table"]),payload["source_hash"],payload["interpretation_hash"],payload["mapping_hash"],payload["committed_records"],payload["value_digest"],payload["assurance"],payload["recorded_at_utc"],encoded)
        runs=qualified(schema,"Runs")
        if cur.execute(f"SELECT run_id FROM {runs} WITH (UPDLOCK,HOLDLOCK) WHERE run_id=?",payload["run_id"]).fetchone() is None:
            cur.execute(f"INSERT INTO {runs} (run_id,state,updated_at_utc) VALUES (?,?,?)",payload["run_id"],"RECEIPT_MIRRORED",utcnow())
        versions=qualified(schema,"SchemaVersions")
        from .metadata import schema_hash
        fingerprint=schema_hash(plan)
        version_id=hashlib.sha256((plan.database+qualified(plan.schema,plan.table)+fingerprint).encode()).hexdigest()
        if cur.execute(f"SELECT schema_version_id FROM {versions} WITH (UPDLOCK,HOLDLOCK) WHERE schema_version_id=?",version_id).fetchone() is None:
            cur.execute(f"INSERT INTO {versions} (schema_version_id,schema_hash,destination_database,target,schema_json) VALUES (?,?,?,?,?)",version_id,fingerprint,plan.database,qualified(plan.schema,plan.table),json.dumps([asdict(c) for c in plan.columns],ensure_ascii=False))
        conn.commit()
        return {"state":"VERIFIED","database":config.get("destination",{}).get("control_database","IngestControl"),"table":target}
    except IngestError:raise
    except Exception as exc:raise provider_error(exc,"central receipt mirror",(password,)) from None
    finally:
        if conn is not None:
            try:conn.rollback()
            finally:conn.close()

def failure_table_name(table,run_id):
    stamp=datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    short=hashlib.sha256(str(run_id).encode()).hexdigest()[:12]
    return truncate_utf16(table,80)+"_fail_"+stamp+"_"+short

@_metadata_retry
def materialize_failures(config,plan,run_id,artifact,records,username=None,password=None,table_name=None):
    if not config.get("audit",{}).get("create_failure_tables",True):return {"state":"DISABLED","reason":"Local durable rejects retained; SQL failure-table materialization disabled"}
    from db_ingest.transports.faults import validate_faults,test_fault
    validate_faults(config)
    conn=None
    a=asdict(artifact) if is_dataclass(artifact) else artifact
    name=table_name or failure_table_name(plan.table,run_id)
    schema=config.get("audit",{}).get("failure_schema","failures")
    target=qualified(schema,name)
    try:
        conn,audit_schema=_open(config,username,password)
        cur=conn.cursor()
        from db_ingest.publication import _lock
        _lock(cur,"db_ingest:failure:"+hashlib.sha256(target.encode()).hexdigest(),config.get("load",{}).get("target_lock_timeout_seconds",60))
        if cur.execute("SELECT SCHEMA_ID(?)",schema).fetchone()[0] is None:
            if not config.get("destination",{}).get("create_schemas",False):raise IngestError("FAILURE_SCHEMA_DISABLED","Failure schema is absent and creation disabled")
            cur.execute("CREATE SCHEMA "+q(schema))
        if cur.execute("SELECT OBJECT_ID(?)",target).fetchone()[0] is None:
            if not config.get("destination",{}).get("create_tables",False):raise IngestError("FAILURE_TABLE_DISABLED","Failure-table creation is disabled")
            cur.execute(f"CREATE TABLE {target} (record_id nvarchar(128) NOT NULL PRIMARY KEY,source_path nvarchar(max) NOT NULL,source_hash char(64) NOT NULL,logical_ordinal bigint NULL,byte_start bigint NULL,byte_end bigint NULL,line_start bigint NULL,line_end bigint NULL,payload_json nvarchar(max) NOT NULL,primary_reason nvarchar(128) NOT NULL,all_errors_json nvarchar(max) NOT NULL,artifact_path nvarchar(max) NOT NULL)")
        test_fault(config,"FAILURE_TABLE_CREATED_BEFORE_COMMIT")
        rejected=qualified(audit_schema,"RejectedRecords");errors=qualified(audit_schema,"RecordErrors")
        count=0
        iterator=records() if callable(records) else records
        for record in iterator:
            r=asdict(record) if is_dataclass(record) else record
            if r.get("kind") not in ("rejected","unresolved") and not r.get("errors"):continue
            payload=json.dumps(r.get("fields",[]),ensure_ascii=False,separators=(",",":"))
            diagnostics=json.dumps(r.get("errors",[]),ensure_ascii=False,separators=(",",":"))
            primary=(r.get("errors") or [{"code":"REJECTED"}])[0].get("code","REJECTED")
            if cur.execute(f"SELECT record_id FROM {target} WITH (UPDLOCK,HOLDLOCK) WHERE record_id=?",r["record_id"]).fetchone() is None:
                cur.execute(f"INSERT INTO {target} (record_id,source_path,source_hash,logical_ordinal,byte_start,byte_end,line_start,line_end,payload_json,primary_reason,all_errors_json,artifact_path) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",r["record_id"],a["original_path"],a["sha256"],r.get("ordinal"),r.get("byte_start"),r.get("byte_end"),r.get("line_start"),r.get("line_end"),payload,primary,diagnostics,a["path"])
            if cur.execute(f"SELECT record_id FROM {rejected} WITH (UPDLOCK,HOLDLOCK) WHERE run_id=? AND record_id=?",run_id,r["record_id"]).fetchone() is None:
                cur.execute(f"INSERT INTO {rejected} (run_id,record_id,file_id,logical_ordinal,byte_start,byte_end,payload_json) VALUES (?,?,?,?,?,?,?)",run_id,r["record_id"],a["capture_id"],r.get("ordinal"),r.get("byte_start"),r.get("byte_end"),payload)
                for ordinal,error in enumerate(r.get("errors",[]),1):
                    cur.execute(f"INSERT INTO {errors} (run_id,record_id,error_ordinal,code,column_ordinal,diagnostic_json) VALUES (?,?,?,?,?,?)",run_id,r["record_id"],ordinal,error.get("code","REJECTED"),error.get("column_ordinal"),json.dumps(error,ensure_ascii=False))
            count+=1
        verified=cur.execute(f"SELECT COUNT_BIG(*) FROM {target}").fetchone()[0]
        if verified!=count:raise IngestError("FAILURE_BACKFILL_MISMATCH","Failure table record IDs/count do not match this backfill",5)
        conn.commit()
        return {"state":"VERIFIED","database":config.get("destination",{}).get("control_database","IngestControl"),"table":target,"rejected_records":count}
    except IngestError:raise
    except Exception as exc:raise provider_error(exc,"failure table/backfill",(password,)) from None
    finally:
        if conn is not None:
            try:conn.rollback()
            finally:conn.close()


@_metadata_retry
def mirror_file(config,plan,receipt,artifact,username=None,password=None,*,source_profiles=None,source_interpretation_hash=None):
    """Populate source/profile/artifact/chunk metadata after a durable local receipt."""
    r=asdict(receipt) if is_dataclass(receipt) else receipt
    a=asdict(artifact) if is_dataclass(artifact) else artifact
    source_identity=source_interpretation_hash or plan.interpretation_hash
    source_columns=source_profiles if source_profiles is not None else plan.columns
    conn=None
    try:
        conn,schema=_open(config,username,password)
        cur=conn.cursor()
        from db_ingest.publication import _lock,utcnow
        _lock(cur,"db_ingest:control:file:"+r["intent_id"],config.get("load",{}).get("target_lock_timeout_seconds",60))
        files=qualified(schema,"Files")
        existing=cur.execute(f"SELECT source_hash,artifact_path FROM {files} WITH (UPDLOCK,HOLDLOCK) WHERE run_id=? AND file_id=?",r["run_id"],a["capture_id"]).fetchone()
        if existing is None:
            cur.execute(f"INSERT INTO {files} (file_id,run_id,source_hash,source_path,artifact_path) VALUES (?,?,?,?,?)",a["capture_id"],r["run_id"],a["sha256"],a["original_path"],a["path"])
        elif existing[0]!=a["sha256"] or existing[1]!=a["path"]:raise IngestError("CONTROL_FILE_CONFLICT","Central file identity conflicts with the durable local artifact",5)
        interpretations=qualified(schema,"Interpretations")
        policy={k:config.get(k,{}) for k in ("parsing","schema","xml","html","columns")}
        policy["source_format"]=config.get("source",{}).get("format","auto")
        policy["application_version"]="0.1.0"
        if cur.execute(f"SELECT interpretation_hash FROM {interpretations} WITH (UPDLOCK,HOLDLOCK) WHERE interpretation_hash=?",source_identity).fetchone() is None:
            cur.execute(f"INSERT INTO {interpretations} (interpretation_hash,policy_json) VALUES (?,?)",source_identity,json.dumps(policy,ensure_ascii=False))
        profiles=qualified(schema,"ColumnProfiles")
        for column in source_columns:
            prior=cur.execute(f"SELECT profile_json FROM {profiles} WITH (UPDLOCK,HOLDLOCK) WHERE file_id=? AND interpretation_hash=? AND ordinal=?",a["capture_id"],source_identity,column.ordinal).fetchone()
            if prior is None:
                cur.execute(f"INSERT INTO {profiles} (file_id,interpretation_hash,ordinal,original_name,target_name,type_name,profile_json) VALUES (?,?,?,?,?,?,?)",a["capture_id"],source_identity,column.ordinal,column.original_name,column.target_name,column.sql_type,json.dumps(column.stats,ensure_ascii=False))
            elif json.loads(prior[0]).get("source_only_profile"):
                cur.execute(f"UPDATE {profiles} SET original_name=?,target_name=?,type_name=?,profile_json=? WHERE file_id=? AND interpretation_hash=? AND ordinal=?",column.original_name,column.target_name,column.sql_type,json.dumps(column.stats,ensure_ascii=False),a["capture_id"],source_identity,column.ordinal)
        artifacts=qualified(schema,"Artifacts")
        if cur.execute(f"SELECT artifact_id FROM {artifacts} WITH (UPDLOCK,HOLDLOCK) WHERE artifact_id=?",a["capture_id"]).fetchone() is None:
            cur.execute(f"INSERT INTO {artifacts} (artifact_id,file_id,sha256,byte_length,path) VALUES (?,?,?,?,?)",a["capture_id"],a["capture_id"],a["sha256"],a["size"],a["path"])
        chunks=qualified(schema,"Chunks");chunk_id=r["intent_id"]+":publication"
        if cur.execute(f"SELECT chunk_id FROM {chunks} WITH (UPDLOCK,HOLDLOCK) WHERE chunk_id=?",chunk_id).fetchone() is None:
            cur.execute(f"INSERT INTO {chunks} (chunk_id,intent_id,record_count,digest,state) VALUES (?,?,?,?,?)",chunk_id,r["intent_id"],r["committed_records"],r["value_digest"],"COMMITTED")
        events=qualified(schema,"Events");event_id=hashlib.sha256((r["intent_id"]+":mirror:"+a["capture_id"]).encode()).hexdigest() if source_profiles is not None else r["intent_id"]+":mirror"
        if cur.execute(f"SELECT event_id FROM {events} WITH (UPDLOCK,HOLDLOCK) WHERE event_id=?",event_id).fetchone() is None:
            cur.execute(f"INSERT INTO {events} (event_id,run_id,recorded_at_utc,category,details_json) VALUES (?,?,?,?,?)",event_id,r["run_id"],utcnow(),"AUDIT_MIRRORED",json.dumps({"intent_id":r["intent_id"],"source_hash":a["sha256"]}))
        conn.commit()
        return {"state":"VERIFIED","file_id":a["capture_id"],"chunk_id":chunk_id,"tables":["Files","Interpretations","ColumnProfiles","Artifacts","Chunks","Events"]}
    except IngestError:raise
    except Exception as exc:raise provider_error(exc,"central source/profile metadata mirror",(password,)) from None
    finally:
        if conn is not None:
            try:conn.rollback()
            finally:conn.close()


@_metadata_retry
def mirror_run_audit(config,journal,run_id,username=None,password=None,final_state=None):
    """Mirror retained incidents/events and the current queryable run summary idempotently.

    Local numeric event IDs are scoped by stable run identity, not the current
    filesystem location, so moving a copied journal cannot create duplicate rows.
    """
    def scrub(value):
        if isinstance(value,str):return value.replace(password,"<redacted>") if password else value
        if isinstance(value,dict):return {k:scrub(v) for k,v in value.items()}
        if isinstance(value,list):return [scrub(v) for v in value]
        return value
    conn=None
    try:
        conn,schema=_open(config,username,password);cur=conn.cursor()
        from db_ingest.publication import _lock,utcnow
        _lock(cur,"db_ingest:run-audit:"+run_id,config["load"]["target_lock_timeout_seconds"])
        run=journal.run(run_id)
        totals={k:0 for k in ("committed","rejected","pending","excluded","unresolved","header")}
        files=0;source_bytes=0;physical_lines=0;logical_records=0;physical_known=True;logical_known=True
        measure_keys=("physical_line_breaks","parsed_data_records","known_parsed_data_records","blank_records","known_blank_records","record_error_events","header_records","conversion_validated_records","sql_staged_records","sql_validated_records")
        measured_totals={key:0 for key in measure_keys};unknown_measures=set()
        for item in journal.files(run_id):
            from .source_audit import mirror_source
            mirror_source(cur,schema,config,item)
            from db_ingest.reporting import file_summary
            source_summary=file_summary(journal,config,item)
            cur.execute(f"UPDATE {qualified(schema,'Files')} SET summary_json=? WHERE run_id=? AND file_id=?",json.dumps(scrub(source_summary),ensure_ascii=False),run_id,item['file_id'])
            files+=1
            source_bytes+=item["artifact"]["size"]
            measured=item.get("counts",{})
            for key in measure_keys:
                if measured.get(key) is None:unknown_measures.add(key)
                else:measured_totals[key]+=measured[key]
            if measured.get("physical_lines") is None:physical_known=False
            else:physical_lines+=measured["physical_lines"]
            if measured.get("logical_records") is None:logical_known=False
            else:logical_records+=measured["logical_records"]
            for key,value in journal.counts(run_id,item["file_id"]).items():
                if key in totals:totals[key]+=value
        summary={"run_id":run_id,"state":final_state or run["state"],"files":files,"counts":totals,"local_journal":str(journal.work/"journal.sqlite3"),"updated_at_utc":utcnow()}
        summary["measures"]={key:None if key in unknown_measures else measured_totals[key] for key in measure_keys}
        summary["known_measures"]={key:measured_totals[key] for key in unknown_measures}
        from db_ingest.config import hashes
        summary.update(configuration_hashes=hashes(config),source_bytes=source_bytes,physical_lines=physical_lines if physical_known else None,logical_records=logical_records if logical_known else None,execution_policy=json.loads(run.get("execution_policy") or "{}"),linked_run=run.get("linked_run"),inventory_state=run.get("inventory_state"),next_commands=["status --run-id "+run_id,"show-errors --run-id "+run_id,"reconcile --run-id "+run_id])
        if journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='projection_events'").fetchone():
            lossy=journal.db.execute("SELECT COUNT(*),COUNT(DISTINCT p.record_id) FROM projection_events p JOIN records r ON r.run_id=p.run_id AND r.file_id=p.file_id AND r.record_id=p.record_id WHERE p.run_id=? AND r.disposition='committed'",(run_id,)).fetchone()
            summary["lossy_projection"]={"published_cells":lossy[0],"published_records":lossy[1],"local_evidence_table":"projection_events"}
        target=qualified(schema,"Runs");payload=json.dumps(scrub(summary),ensure_ascii=False)
        if cur.execute(f"SELECT run_id FROM {target} WITH (UPDLOCK,HOLDLOCK) WHERE run_id=?",run_id).fetchone() is None:
            cur.execute(f"INSERT INTO {target}(run_id,state,updated_at_utc,summary_json) VALUES(?,?,?,?)",run_id,summary["state"],summary["updated_at_utc"],payload)
        else:cur.execute(f"UPDATE {target} SET state=?,updated_at_utc=?,summary_json=? WHERE run_id=?",summary["state"],summary["updated_at_utc"],payload,run_id)
        target=qualified(schema,"FileIncidents");incident_count=0
        for local in journal.db.execute("SELECT * FROM incidents WHERE run_id=? ORDER BY id",(run_id,)):
            item=dict(local);identifier=hashlib.sha256(f"{run_id}:incident:{item['id']}".encode()).hexdigest()
            item["context"]=json.loads(item["context"])
            if cur.execute(f"SELECT incident_id FROM {target} WITH (UPDLOCK,HOLDLOCK) WHERE incident_id=?",identifier).fetchone() is None:
                cur.execute(f"INSERT INTO {target}(incident_id,run_id,file_id,code,diagnostic_json) VALUES(?,?,?,?,?)",identifier,run_id,item["file_id"],item["code"],json.dumps(scrub(item),ensure_ascii=False))
            incident_count+=1
        event_count=0
        if journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'").fetchone():
            target=qualified(schema,"Events")
            for local in journal.db.execute("SELECT * FROM events WHERE run_id=? ORDER BY id",(run_id,)):
                item=dict(local);identifier=hashlib.sha256(f"{run_id}:event:{item['id']}".encode()).hexdigest()
                item["details"]=json.loads(item["details"])
                if cur.execute(f"SELECT event_id FROM {target} WITH (UPDLOCK,HOLDLOCK) WHERE event_id=?",identifier).fetchone() is None:
                    category=item.get("category",item.get("kind","LOCAL_EVENT"))
                    cur.execute(f"INSERT INTO {target}(event_id,run_id,recorded_at_utc,category,details_json) VALUES(?,?,?,?,?)",identifier,run_id,item.get("created",utcnow()),category,json.dumps(scrub(item),ensure_ascii=False))
                event_count+=1
        # Attachment original bytes are separate retained artifacts, never paths read implicitly.
        if journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachment_links'").fetchone():
            target=qualified(schema,"Artifacts")
            rows=journal.db.execute("SELECT DISTINCT l.file_id,a.artifact FROM attachment_links l JOIN attachment_assets a ON l.asset_key=a.asset_key WHERE l.run_id=?",(run_id,))
            for local in rows:
                a=json.loads(local[1])
                if cur.execute(f"SELECT artifact_id FROM {target} WITH (UPDLOCK,HOLDLOCK) WHERE artifact_id=?",a["capture_id"]).fetchone() is None:
                    cur.execute(f"INSERT INTO {target}(artifact_id,file_id,sha256,byte_length,path) VALUES(?,?,?,?,?)",a["capture_id"],local[0],a["sha256"],a["size"],a["path"])
        conn.commit()
        return {"state":"VERIFIED","incidents":incident_count,"events":event_count,"summary":summary}
    except IngestError:raise
    except Exception as exc:raise provider_error(exc,"central run/incident/event backfill",(password,)) from None
    finally:
        if conn is not None:
            try:conn.rollback()
            finally:conn.close()


def group_target_key(plan):
    from db_ingest.config import stable_hash
    return stable_hash({"database":plan.database,"schema":plan.schema,"table":plan.table})


def _group_entries(manifest):
    from db_ingest.contributors import iter_manifest
    try:
        yield from iter_manifest(manifest)
    except IngestError:raise
    except (OSError,ValueError,TypeError,KeyError,UnicodeError) as error:raise IngestError("GROUP_MANIFEST_UNAVAILABLE","Contributor artifact cannot be verified: "+str(error),5) from None


@_metadata_retry
def mirror_group_contributors(config,plan,receipt,manifest,username=None,password=None):
    """Mirror ACKed group contributor history; never remove omitted historical paths."""
    r=asdict(receipt) if is_dataclass(receipt) else receipt
    from db_ingest.contributors import source_manifest_hash
    accepted=0
    def verified():
        nonlocal accepted
        for item in _group_entries(manifest):
            count=item.get("records",0)
            if not isinstance(count,int) or isinstance(count,bool) or count<0:raise IngestError("GROUP_MANIFEST_INVALID","Contributor record counts must be nonnegative integers",5)
            accepted+=count
            yield item
    source_hash=source_manifest_hash(verified())
    if source_hash!=plan.source_hash or r["source_hash"]!=source_hash or accepted!=r["committed_records"]:raise IngestError("GROUP_MANIFEST_RECEIPT_MISMATCH","Contributor source hashes/counts differ from the acknowledged group receipt",5)
    if any(r[key]!=getattr(plan,key) for key in ("database","schema","table")):raise IngestError("GROUP_MANIFEST_RECEIPT_MISMATCH","Group receipt target differs from contributor plan",5)
    key=group_target_key(plan);conn=None
    try:
        conn,schema=_open(config,username,password);cur=conn.cursor()
        from db_ingest.publication import _lock
        _lock(cur,"db_ingest:control:group:"+key,config["load"]["target_lock_timeout_seconds"])
        ack=cur.execute(f"SELECT source_hash FROM {qualified(schema,'PublicationReceipts')} WITH (UPDLOCK,HOLDLOCK) WHERE intent_id=?",r["intent_id"]).fetchone()
        if ack is None or ack[0]!=r["source_hash"]:raise IngestError("GROUP_CENTRAL_ACK_MISSING","Central destination receipt must be mirrored before its contributors",5)
        groups=qualified(schema,"SourceGroups");contributors=qualified(schema,"SourceGroupContributors");history=qualified(schema,"SourceGroupContributionHistory")
        existing=cur.execute(f"SELECT manifest_sha256,contributor_count,source_hash FROM {groups} WITH (UPDLOCK,HOLDLOCK) WHERE intent_id=?",r["intent_id"]).fetchone()
        expected=(manifest["manifest_sha256"],manifest["contributor_count"],r["source_hash"])
        if existing is not None and tuple(existing)!=expected:raise IngestError("GROUP_CENTRAL_CONFLICT","ACKed central contributor manifest conflicts",5)
        if existing is None:cur.execute(f"INSERT INTO {groups}(intent_id,group_key,run_id,destination_database,target,manifest_sha256,contributor_count,source_hash,details_json) VALUES(?,?,?,?,?,?,?,?,?)",r["intent_id"],key,r["run_id"],plan.database,qualified(plan.schema,plan.table),*expected,json.dumps(manifest,ensure_ascii=False))
        count=0
        for item in _group_entries(manifest):
            relative=item["relative_path"];identifier=item["file_id"]
            prior=cur.execute(f"SELECT relative_path FROM {contributors} WITH (UPDLOCK,HOLDLOCK) WHERE group_key=? AND file_id=?",key,identifier).fetchone()
            encoded=json.dumps(item,ensure_ascii=False,separators=(",",":"))
            if prior is None:cur.execute(f"INSERT INTO {contributors}(group_key,file_id,relative_path,first_intent,last_intent,payload_json) VALUES(?,?,?,?,?,?)",key,identifier,relative,r["intent_id"],r["intent_id"],encoded)
            elif prior[0]!=relative:raise IngestError("GROUP_PATH_IDENTITY_CONFLICT","Central contributor path hash conflicts",5)
            else:cur.execute(f"UPDATE {contributors} SET last_intent=?,payload_json=? WHERE group_key=? AND file_id=?",r["intent_id"],encoded,key,identifier)
            from db_ingest.contributors import contribution_signature
            signature=contribution_signature(item)
            old=cur.execute(f"SELECT signature,payload_json FROM {history} WITH (UPDLOCK,HOLDLOCK) WHERE group_key=? AND intent_id=? AND file_id=?",key,r["intent_id"],identifier).fetchone()
            if old is None:
                cur.execute(f"INSERT INTO {history}(group_key,intent_id,file_id,signature,source_sha256,interpretation_hash,mapping_hash,payload_json) VALUES(?,?,?,?,?,?,?,?)",key,r["intent_id"],identifier,signature,item["sha256"],item["interpretation_hash"],item.get("mapping_hash",""),encoded)
            elif old[0]!=signature or json.loads(old[1])!=item:
                raise IngestError("GROUP_CENTRAL_CONFLICT","Acknowledged central contribution version differs from its original manifest",5)
            count+=1
        observed=cur.execute(f"SELECT COUNT_BIG(*) FROM {history} WHERE group_key=? AND intent_id=?",key,r["intent_id"]).fetchone()[0]
        if observed!=count:raise IngestError("GROUP_CENTRAL_HISTORY_INCOMPLETE","Central per-acknowledgment contribution history has an unexpected row set",5)
        conn.commit()
        return {"state":"VERIFIED","contributor_count":count,"group_key":key,"tables":["SourceGroups","SourceGroupContributors","SourceGroupContributionHistory"]}
    except IngestError:raise
    except Exception as error:raise provider_error(error,"central contributor mirror",(password,),5) from None
    finally:
        if conn is not None:
            try:conn.rollback()
            finally:conn.close()


def _existing_group_control(config,username,password):
    """Read-only history connection: absence/hidden metadata cannot manufacture history."""
    from .connect import connect
    try:conn=connect(config,username,password,config["destination"]["control_database"])
    except IngestError as error:raise IngestError("GROUP_CENTRAL_HISTORY_UNAVAILABLE",str(error),5,{"cause":error.code,**error.context}) from None
    schema=config["audit"]["control_schema"]
    try:
        cur=conn.cursor();cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        for table in ("SchemaMigrations","PublicationReceipts","SourceGroups","SourceGroupContributors"):
            if cur.execute("SELECT OBJECT_ID(?)",qualified(schema,table)).fetchone()[0] is None:raise IngestError("GROUP_CENTRAL_HISTORY_UNAVAILABLE","Central group history is absent or hidden; do not create replacement history",5)
        version=cur.execute(f"SELECT MAX(version) FROM {qualified(schema,'SchemaMigrations')}").fetchone()[0]
        if version is None or version<3 or version>CONTROL_VERSION:raise IngestError("CONTROL_VERSION_UNSUPPORTED","Existing central group history needs an explicitly supported schema version",5)
        return conn,schema
    except Exception:
        try:conn.rollback()
        finally:conn.close()
        raise


def export_group_contributors(config,plan,output_path,username=None,password=None):
    """Stream central historical contributors and require all ACK manifests present."""
    from pathlib import Path
    import os,uuid
    conn=None;destination=Path(output_path);temporary=destination.with_name(destination.name+"."+uuid.uuid4().hex+".partial")
    key=group_target_key(plan)
    try:
        conn,schema=_existing_group_control(config,username,password);cur=conn.cursor()
        from db_ingest.publication import _lock
        _lock(cur,"db_ingest:control:group:"+key,config["load"]["target_lock_timeout_seconds"])
        receipts=qualified(schema,"PublicationReceipts");groups=qualified(schema,"SourceGroups")
        missing=cur.execute(f"SELECT COUNT_BIG(*) FROM {receipts} r LEFT JOIN {groups} g ON r.intent_id=g.intent_id WHERE r.destination_database=? AND r.target=? AND g.intent_id IS NULL",plan.database,qualified(plan.schema,plan.table)).fetchone()[0]
        if missing:raise IngestError("GROUP_CENTRAL_HISTORY_INCOMPLETE","Central ACKs lack complete contributor manifests; backfill original group receipts before replacement",5,{"missing_group_manifests":missing})
        destination.parent.mkdir(parents=True,exist_ok=True)
        count=0;digest=hashlib.sha256();cur.execute(f"SELECT relative_path,file_id,payload_json FROM {qualified(schema,'SourceGroupContributors')} WHERE group_key=? ORDER BY CONVERT(varbinary(max),CONVERT(varchar(max),relative_path COLLATE Latin1_General_100_BIN2_UTF8))",key)
        with temporary.open("wb") as stream:
            for relative,identifier,payload in cur:
                from db_ingest.resources import checkpoint
                checkpoint()
                item=json.loads(payload)
                if item["relative_path"]!=relative or item["file_id"]!=identifier:raise IngestError("GROUP_CENTRAL_CONFLICT","Central contributor payload/path differs",5)
                raw=(json.dumps(item,ensure_ascii=False,separators=(",",":"))+"\n").encode("utf-8")
                if len(raw)>8388608:raise IngestError("GROUP_MANIFEST_LIMIT","Central contributor entry exceeds the configured supported line bound",5)
                stream.write(raw);digest.update(raw);count+=1
            stream.flush();os.fsync(stream.fileno())
        from db_ingest.capture import _atomic_replace,_sync_directory
        _atomic_replace(temporary,destination);_sync_directory(destination.parent)
        return {"state":"VERIFIED","manifest_path":str(destination),"manifest_sha256":digest.hexdigest(),"contributor_count":count,"max_line_bytes":8388608,"group_key":key}
    except IngestError:raise
    except OSError as error:raise IngestError("DISK_CAPACITY" if getattr(error,"errno",None)==28 or getattr(error,"winerror",None)==112 else "ARCHIVE_UNAVAILABLE","Cannot durably retain contributor-history evidence: "+str(error),5) from None
    except Exception as error:raise provider_error(error,"central contributor history export",(password,),5) from None
    finally:
        if conn is not None:
            try:conn.rollback()
            finally:conn.close()
