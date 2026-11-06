"""Destination-local atomic publication and receipts.

Only importer-owned landing tables are accepted. The session-local typed stage is
loaded before any target mutation. Same-target publication uses a transaction
application lock and TABLOCKX/HOLDLOCK, also excluding unrelated target writers.
"""
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
import uuid
from db_ingest.errors import IngestError
from db_ingest.models import PublicationReceipt
from db_ingest.planning import truncate_utf16, uses_name_claims
from db_ingest.receipts import new_value_digest,update_value_digest
from db_ingest.staging import converted_record,typed_stage_parameters
from db_ingest.transports.odbc import choose_transport,OdbcBatchWriter,OdbcCanonicalLoader
from db_ingest.resources import checkpoint
from db_ingest.transports.faults import test_fault,validate_faults
from db_ingest.adapters.mssql.connect import connect
from db_ingest.adapters.mssql.ddl import quote_identifier as q,qualified
from db_ingest.adapters.mssql.types import parse_sql_type,read_expression,parameter_value
from db_ingest.adapters.mssql.text import declaration,conversion,verify_plan as verify_text_plan
from db_ingest.adapters.mssql.metadata import schema_hash,validate_plan,require_supported_target
from db_ingest.adapters.mssql.diagnostics import provider_error


def utcnow(): return datetime.now(timezone.utc).isoformat()

def _projection_assurance(receipt,config):
    summary=config.get("_projection_summary") or {}
    if summary.get("applied_cells",0)>0:
        receipt.assurance="lossy"
        receipt.details["lossy_projection"]=summary
    return receipt


def _tables(config):
    schema=config.get("audit",{}).get("control_schema","audit")
    return schema,qualified(schema,"__db_ingest_receipts"),qualified(schema,"__db_ingest_targets")

def _lock(cur,resource,timeout,owner="Transaction"):
    if owner == "Transaction": cur.execute("IF @@TRANCOUNT=0 BEGIN TRANSACTION")
    row=cur.execute("DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=?,@LockMode='Exclusive',@LockOwner=?,@LockTimeout=?; SELECT @r",resource,owner,int(timeout*1000)).fetchone()
    if row is None or row[0]<0: raise IngestError("TARGET_LOCK","Could not acquire publication lock; no replay attempted",4,{"lock_result":None if row is None else row[0]})

def _ownership_mapping_hash(plan):
    value={"database":plan.database,"schema":plan.schema,"table":plan.table,"columns":[(c.ordinal,c.original_name,c.target_name) for c in plan.columns]}
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,separators=(",",":")).encode("utf-8")).hexdigest()

def _resource(plan):
    return "db_ingest:target:"+hashlib.sha256((plan.schema+"."+plan.table).casefold().encode("utf-8")).hexdigest()

def _bootstrap_database(config,database,username,password):
    try: return connect(config,username,password,database)
    except IngestError as original:
        if original.code!="DATABASE_ACCESS" or not config.get("destination",{}).get("create_databases",False): raise
    bootstrap=config.get("destination",{}).get("bootstrap_database","master")
    conn=connect(config,username,password,bootstrap,autocommit=True)
    try:
        cur=conn.cursor()
        _lock(cur,"db_ingest:create:"+hashlib.sha256(database.casefold().encode()).hexdigest(),config.get("load",{}).get("target_lock_timeout_seconds",60),"Session")
        observed=cur.execute("SELECT name,state_desc FROM sys.databases WHERE name=?",database).fetchone()
        if observed is not None:
            if observed[1]!="ONLINE":raise IngestError("DATABASE_ACCESS",f"Database {database!r} is visible in state {observed[1]}; no creation was attempted",4)
            # A cooperating creator may have finished while this session waited for
            # the creation lock. Release the lock, then re-probe actual connectivity.
        else:
            # A hidden database is possible; CREATE can fail harmlessly.
            try:cur.execute("CREATE DATABASE "+q(database))
            except Exception as exc:raise provider_error(exc,"authorized CREATE DATABASE (database may be hidden or inaccessible)",(password,)) from None
    finally: conn.close()
    return connect(config,username,password,database)

def _bootstrap_metadata(conn,config):
    schema,receipts,targets=_tables(config)
    cur=conn.cursor()
    _lock(cur,"db_ingest:metadata:v1",config.get("load",{}).get("target_lock_timeout_seconds",60))
    if cur.execute("SELECT SCHEMA_ID(?)",schema).fetchone()[0] is None:
        if not config.get("destination",{}).get("create_schemas",False): raise IngestError("SCHEMA_CREATION_DISABLED",f"Required audit schema {schema!r} is inaccessible or absent")
        cur.execute("CREATE SCHEMA "+q(schema))
    if cur.execute("SELECT OBJECT_ID(?)",receipts).fetchone()[0] is None:
        if not config.get("destination",{}).get("create_tables",False): raise IngestError("RECEIPT_CREATION_DISABLED","Destination-local receipt table is required")
        cur.execute(f"CREATE TABLE {receipts} ([intent_id] nvarchar(64) NOT NULL PRIMARY KEY,[target] nvarchar(260) NOT NULL,[target_object_id] int NOT NULL,[database_guid] uniqueidentifier NOT NULL,[payload] nvarchar(max) NOT NULL)")
    if cur.execute("SELECT OBJECT_ID(?)",targets).fetchone()[0] is None:
        if not config.get("destination",{}).get("create_tables",False): raise IngestError("REGISTRY_CREATION_DISABLED","Destination ownership registry is required")
        cur.execute(f"CREATE TABLE {targets} ([target] nvarchar(260) NOT NULL PRIMARY KEY,[target_object_id] int NOT NULL,[schema_hash] char(64) NOT NULL,[mapping_hash] char(64) NOT NULL,[version] int NOT NULL CHECK ([version]=1))")
    if uses_name_claims(config):
        from .adapters.mssql.name_claims import bootstrap
        bootstrap(cur,config)
    if config.get("_publication_group"):
        from .adapters.mssql.contributors import bootstrap
        bootstrap(cur,config)
    if config.get("mapping",{}).get("existing_target_strategy")=="mapped_business":
        from .adapters.mssql.business import bootstrap as bootstrap_business
        bootstrap_business(cur,config)
    from .adapters.mssql.evolution import bootstrap as bootstrap_evolution
    bootstrap_evolution(cur,config)
    conn.commit()

def _database_guid(cur):
    row=cur.execute("SELECT CONVERT(nvarchar(36),database_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID()").fetchone()
    if row is None or not row[0]: raise IngestError("DATABASE_IDENTITY_UNAVAILABLE","Full receipt recovery requires observable database GUID; request metadata visibility from the DBA")
    return str(row[0])

def _recovery_fork(cur):
    row=cur.execute("SELECT CONVERT(nvarchar(36),recovery_fork_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID()").fetchone()
    return str(row[0]) if row and row[0] else None


def _verify_current_receipt(cur,config,plan,receipt):
    target=qualified(plan.schema,plan.table)
    cur.execute(f"SELECT COUNT_BIG(*) FROM {target} WITH (TABLOCKX,HOLDLOCK)").fetchone()
    if receipt.get("details",{}).get("target_strategy")=="mapped_business":
        from .adapters.mssql.business import readback as business_readback,inspect_target,receipt_config
        contract=inspect_target(cur,receipt_config(config,receipt),plan,target,receipt["details"]["target_object_id"],allow_register=False)[2]
        if contract!=receipt["details"]["business_contract_hash"]:raise IngestError("BUSINESS_CONTRACT_MISMATCH","Business receipt contract differs from live acknowledged mapping",5)
        count,digest=business_readback(cur,config,plan,receipt)
        if count!=receipt["committed_records"] or digest!=receipt["value_digest"]:raise IngestError("DESTINATION_MISMATCH","Mapped business values differ from their acknowledged receipt",5)
        return
    require_supported_target(cur,plan,target,config=config)
    from .adapters.mssql.generations import readback
    count,digest,_=readback(cur,config,plan,receipt)
    if count!=receipt["committed_records"] or digest!=receipt["value_digest"]:raise IngestError("DESTINATION_MISMATCH","Current target differs from its committed receipt; replay blocked",5)


def resolve_publication(config,plan,intent_id,known_receipts=(),username=None,password=None,current_receipt=None):
    from .recovery import resolve_publication as resolve
    return resolve(config,plan,intent_id,known_receipts,username,password,current_receipt)


def _get_receipt(cur,config,plan,intent_id):
    _,table,_=_tables(config)
    row=cur.execute(f"SELECT target,target_object_id,CONVERT(nvarchar(36),database_guid),payload FROM {table} WITH (READCOMMITTEDLOCK) WHERE intent_id=?",intent_id).fetchone()
    if row is None:return None
    payload=json.loads(row[3])
    expected={"database":plan.database,"schema":plan.schema,"table":plan.table,"source_hash":plan.source_hash,"interpretation_hash":plan.interpretation_hash,"mapping_hash":plan.mapping_hash}
    differences={k:{"expected":v,"receipt":payload.get(k)} for k,v in expected.items() if payload.get(k)!=v}
    if row[0]!=qualified(plan.schema,plan.table):differences["receipt_target_column"]="Receipt target column disagrees with its payload"
    current_object=cur.execute("SELECT OBJECT_ID(?)",qualified(plan.schema,plan.table)).fetchone()[0]
    if current_object!=row[1] or str(row[2]).lower()!=_database_guid(cur).lower(): differences["destination_identity"]="Destination object or database GUID no longer matches the receipt"
    if payload.get("details",{}).get("recovery_fork_guid") is not None and payload["details"]["recovery_fork_guid"]!=_recovery_fork(cur):differences["recovery_fork_guid"]="Recovery fork changed since the acknowledged publication"
    if differences: raise IngestError("RECEIPT_IDENTITY_MISMATCH","Destination receipt does not match this intent or the live destination",5,differences)
    return payload

def find_receipt(config,plan,intent_id,username=None,password=None):
    conn=connect(config,username,password,plan.database)
    try:
        cur=conn.cursor();cur.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        _lock(cur,_resource(plan),config.get("load",{}).get("target_lock_timeout_seconds",60))
        _,table,_=_tables(config)
        if cur.execute("SELECT OBJECT_ID(?)",table).fetchone()[0] is None: return None
        return _get_receipt(cur,config,plan,intent_id)
    except IngestError: raise
    except Exception as exc: raise provider_error(exc,"resolve destination receipt",(password,),5) from None
    finally:
        conn.rollback();conn.close()

def _readback(cur,plan,target,intent_id,config=None):
    limits=(config or {}).get("limits",{})
    record_limit=min(limits.get("max_in_memory_record_bytes",8388608),limits.get("fetch_bytes",8388608))
    field_limit=limits.get("max_in_memory_field_bytes",8388608)
    lengths=["COALESCE(CONVERT(bigint,DATALENGTH("+(read_expression(c) if parse_sql_type(c.sql_type).base=="varchar" else q(c.target_name))+")),0)" for c in plan.columns]
    # Test storage sizes under the same held publication/reconciliation snapshot before
    # fetching LOB values. External edits cannot force an unbounded driver readback.
    metrics=",".join("MAX("+x+")" for x in lengths)
    sizes=cur.execute(f"SELECT MAX({' + '.join(lengths)}),{metrics} FROM {target} WITH (HOLDLOCK) WHERE [__ingest_intent]=?",intent_id).fetchone()
    if sizes and ((sizes[0] or 0)>record_limit or any((x or 0)>field_limit for x in sizes[1:])):
        raise IngestError("READBACK_BUDGET","Destination storage exceeds the bounded readback policy; values were not fetched",5,{"record_limit":record_limit,"field_limit":field_limit})
    expressions=",".join(read_expression(c) for c in plan.columns)
    cur.execute(f"SELECT [__ingest_record],[__ingest_ordinal],{expressions} FROM {target} WITH (HOLDLOCK) WHERE [__ingest_intent]=? ORDER BY [__ingest_ordinal]",intent_id)
    digest=new_value_digest();count=0
    while True:
        checkpoint()
        row=cur.fetchone()
        if row is None: break
        try:update_value_digest(digest,row[0],row[1],list(row[2:]),plan.columns)
        except IngestError as error:raise IngestError("DESTINATION_VALUE_INVALID","Destination values no longer fit their original exact typed projection",5,{"cause":error.code}) from None
        count+=1
    return count,digest.hexdigest()

def _stage_records(config, plan, records, intent_id, loader, writer):
    """Stream bounded, ordered records into the stage; never commit or mutate targets."""
    expected = new_value_digest()
    count = 0
    last_ordinal = 0
    iterator = records() if callable(records) else records
    for record in iterator:
        checkpoint()
        if record.kind in ("header", "blank"):
            continue
        if record.kind != "data" or record.errors:
            raise IngestError(
                "UNRESOLVED_RECORD",
                "Publication iterator contains rejected/unresolved records; resolve or filter known quarantines explicitly",
            )
        if len(record.record_id) > 128 or record.ordinal <= last_ordinal:
            raise IngestError(
                "RECORD_IDENTITY_INVALID",
                "Record IDs must fit 128 characters and ordinals must increase strictly",
            )
        values = converted_record(plan, record)
        sizes = [
            max(len(value.encode("utf-8")), len(value.encode("utf-16-le")))
            if isinstance(value, str) else len(value) if isinstance(value, bytes) else 64
            for value in values
        ]
        limits = config.get("limits", {})
        if any(size > limits.get("max_in_memory_field_bytes", 8388608) for size in sizes):
            raise IngestError("FIELD_BUDGET", "Typed field exceeds the configured native parameter/readback bound")
        record_limit = min(
            limits.get("max_in_memory_record_bytes", 8388608),
            config.get("load", {}).get("batch_bytes", 8388608),
        )
        if sum(sizes) > record_limit:
            raise IngestError("RECORD_BUDGET", "Typed staging record exceeds the configured byte bound")
        if loader is not None:
            loader.add(record, values)
        else:
            writer.add([
                *typed_stage_parameters(plan.columns, values),
                str(intent_id), record.record_id, record.ordinal,
            ])
        update_value_digest(expected, record.record_id, record.ordinal, values, plan.columns)
        count += 1
        last_ordinal = record.ordinal
    return count, expected


def _prepare_landing_target(cur, config, plan, target, object_id, registered, declarations, metadata):
    """Validate/adapt or create a landing table inside the caller's transaction."""
    exists = object_id is not None
    mode = config.get('load', {}).get('mode', 'append')
    _, _, registry = _tables(config)
    schema_diff = None
    if registered is not None and registered[3] != 1:
        raise IngestError('DESTINATION_METADATA_VERSION', 'Destination ownership metadata version is not supported; no mutation attempted', 2, {'observed_version': registered[3], 'supported_version': 1})
    fingerprint = schema_hash(plan)
    ownership_mapping = _ownership_mapping_hash(plan)
    checkpoint(force=True)
    if exists:
        if registered is None or registered[0] != object_id:
            raise IngestError('TARGET_LINEAGE_UNSUPPORTED', 'Existing target is not owned by this importer registry; full publication blocked before mutation')
        cur.execute(f'SELECT COUNT_BIG(*) FROM {target} WITH (TABLOCKX,HOLDLOCK)').fetchone()
        from .adapters.mssql.evolution import adapt_inferred
        storage_plan, inferred_adaptations = adapt_inferred(cur, config, plan, target, object_id, registered)
        storage_fingerprint = schema_hash(storage_plan)
        if registered[1] != storage_fingerprint or registered[2] != ownership_mapping:
            policies = config.get('schema', {})
            if not any((policies.get(key, False) for key in ('allow_add_columns_on_append', 'allow_widen_on_append', 'allow_retype_on_append'))):
                from .adapters.mssql.evolution import inspection
                raise IngestError('SCHEMA_DRIFT', 'Registered schema/source mapping differs; explicit schema policy is required', 2, {'schema_diff': {'published_hash': registered[1], 'proposed_hash': fingerprint, 'proposed_columns': [asdict(c) for c in plan.columns], 'inspection': inspection(cur, target), 'actions': [], 'blocked': ['schema policy disabled']}})
            from .adapters.mssql.evolution import prepare
            schema_diff = prepare(cur, config, storage_plan, target, object_id, registered)
            cur.execute(f'UPDATE {registry} SET schema_hash=?,mapping_hash=? WHERE target=?', storage_fingerprint, ownership_mapping, target)
        require_supported_target(cur, storage_plan, target, replace=mode == 'replace', config=config)
        if inferred_adaptations:
            schema_diff = {**(schema_diff or {}), 'inferred_source_projection': inferred_adaptations, 'source_schema_hash': fingerprint, 'storage_schema_hash': storage_fingerprint}
    else:
        storage_plan = plan
        storage_fingerprint = fingerprint
        if registered is not None:
            raise IngestError('TARGET_REPLACED', 'Registry references a target that is absent; possible restore/replacement', 5)
        d = config.get('destination', {})
        if not d.get('create_tables', False):
            raise IngestError('TABLE_CREATION_DISABLED', 'Target table is absent and creation is disabled')
        if cur.execute('SELECT SCHEMA_ID(?)', plan.schema).fetchone()[0] is None:
            if not d.get('create_schemas', False):
                raise IngestError('SCHEMA_CREATION_DISABLED', 'Target schema is absent and creation is disabled')
            cur.execute('CREATE SCHEMA ' + q(plan.schema))
        cur.execute(f'CREATE TABLE {target} ({declarations},{metadata},PRIMARY KEY ([__ingest_intent],[__ingest_record]))')
        object_id = cur.execute('SELECT OBJECT_ID(?)', target).fetchone()[0]
        cur.execute(f'INSERT INTO {registry} (target,target_object_id,schema_hash,mapping_hash,version) VALUES (?,?,?,?,1)', target, object_id, fingerprint, ownership_mapping)
        test_fault(config, 'AFTER_TARGET_SCHEMA_CREATION')
    return (storage_plan, storage_fingerprint, schema_diff, object_id)


def _store_receipt(cur, receipt_table, intent_id, target, object_id, receipt):
    """Write acknowledgement in the target transaction; the caller owns its commit."""
    payload = json.dumps(asdict(receipt), ensure_ascii=False, separators=(",", ":"))
    cur.execute(
        f"INSERT INTO {receipt_table} (intent_id,target,target_object_id,database_guid,payload) VALUES (?,?,?,?,?)",
        str(intent_id), target, object_id, receipt.details["database_guid"], payload,
    )


def publish(config,plan,records,intent_id,run_id,username=None,password=None):
    config={**config,"_publication_run_id":str(run_id)}
    validate_faults(config)
    if os.environ.get("DB_INGEST_TEST_MODE")=="1" and os.environ.get("DB_INGEST_TEST_FAULT") and not plan.database.casefold().startswith("dbingesttest_"):raise IngestError("TEST_FAULT_DESTINATION","Fault hooks require the actual planned database to be explicitly disposable",2)
    validate_plan(plan)
    transport=choose_transport(config)
    if config.get("mapping",{}).get("table_strategy","per_file")!="per_file" and not config.get("_publication_group"):
        raise IngestError("MERGED_PUBLICATION_UNSUPPORTED","Merged table publication/contributor manifests are not yet implemented; use per_file tables")
    mode=config.get("load",{}).get("mode","append")
    business_strategy=config.get("mapping",{}).get("existing_target_strategy")=="mapped_business"
    if business_strategy and mode=="replace" and not config.get("load",{}).get("retain_previous_generation",True):raise IngestError("BUSINESS_REPLACE_RETENTION","Mapped business replacement requires retaining the previous complete generation",2)
    if business_strategy and config.get("_publication_group"):raise IngestError("BUSINESS_GROUP_UNSUPPORTED","Mapped business groups require a separately verified key/provenance strategy",2)
    if mode not in ("append","replace","create_only"):raise IngestError("MODE_INVALID","Mode must be append, replace or create_only")
    if len(str(intent_id))>64:raise IngestError("INTENT_INVALID","Intent identifiers are limited to 64 characters")
    for column in plan.columns:q(column.target_name)
    target=qualified(plan.schema,plan.table)
    conn = None
    committing = False
    backup = backup_proof = None
    bcp_loader = staging_batches = None
    transport_verified = False
    recovery_control = group_control = None
    group_details = schema_diff = source_history = None
    try:
        conn=connect(config,username,password,plan.database) if business_strategy else _bootstrap_database(config,plan.database,username,password)
        if business_strategy:
            from .adapters.mssql.business import inspect_target
            preflight=conn.cursor();existing=preflight.execute("SELECT OBJECT_ID(?)",target).fetchone()[0]
            if config.get("_target_review"):
                from .target_review import verify_review
                verify_review(preflight,config,plan,intent_id)
            inspect_target(preflight,config,plan,target,existing,allow_register=None)
            conn.rollback() # Read-only capability probe; no registry/stage was made.
        # Staging is session-local and verified before acquiring the target lock.
        _bootstrap_metadata(conn,config)
        cur=conn.cursor();cur.execute("SET XACT_ABORT ON")
        lock_ms=int(config.get("load",{}).get("target_lock_timeout_seconds",60))*1000
        if not 1<=lock_ms<=2147483647:raise IngestError("LOCK_TIMEOUT_RANGE","Target lock timeout must fit SQL Server signed millisecond range")
        cur.execute("SET LOCK_TIMEOUT "+str(lock_ms))
        major=cur.execute("SELECT CONVERT(int,SERVERPROPERTY('ProductMajorVersion'))").fetchone()[0]
        if major<15:raise IngestError("ENGINE_TOO_OLD","SQL Server 2019 (15.x) or newer is required")
        names=",".join(q(c.target_name) for c in plan.columns)
        text_evidence=verify_text_plan(cur,plan)
        declarations=",".join(declaration(c) for c in plan.columns)
        metadata="[__ingest_intent] nvarchar(64) NOT NULL,[__ingest_record] nvarchar(128) NOT NULL,[__ingest_ordinal] bigint NOT NULL"
        cur.execute("CREATE TABLE #db_ingest_stage ("+declarations+","+metadata+",PRIMARY KEY ([__ingest_record]))")
        # Commit only empty session-local DDL. No business target exists or changes.
        conn.commit()
        markers=",".join("CASE WHEN ?=1 THEN CAST(NULL AS "+str(parse_sql_type(c.sql_type))+") ELSE "+conversion(c,"?")+" END" for c in plan.columns)
        insert_stage=f"INSERT INTO #db_ingest_stage ({names},[__ingest_intent],[__ingest_record],[__ingest_ordinal]) VALUES ({markers},?,?,?)"
        if transport["engine"]=="bcp":
            from db_ingest.transports.bcp import BcpStageLoader
            bcp_loader=BcpStageLoader(config,plan,conn,transport,intent_id,run_id,username,password)
        if transport["engine"]=="odbc" and config.get("load",{}).get("fast_executemany",False):
            bcp_loader=OdbcCanonicalLoader(config,plan,conn,transport,intent_id,run_id,username,password)
            transport["fast_array_wire"]="D3 canonical raw rows; complete exact typed decode and verification"
        writer=OdbcBatchWriter(cur,insert_stage,config) if bcp_loader is None else None
        count, expected = _stage_records(config, plan, records, intent_id, bcp_loader, writer)
        staging_batches=bcp_loader.finish() if bcp_loader is not None else writer.finish()
        staged_count,staged_digest=_readback(cur,plan,"#db_ingest_stage",intent_id,config)
        if staged_count!=count or staged_digest!=expected.hexdigest():raise IngestError("STAGING_VERIFICATION_FAILED","Driver staging IDs/values differ from source expectations; target untouched",4,{"expected_count":count,"staged_count":staged_count})
        conn.commit() # Only a session-local stage exists; no business target mutation.
        transport_verified=True
        test_fault(config,"STAGED_BEFORE_PUBLICATION")
        # Receipt resolution precedes every target mutation, including create-only checks.
        cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        _lock(cur,_resource(plan),config.get("load",{}).get("target_lock_timeout_seconds",60))
        prior=_get_receipt(cur,config,plan,intent_id)
        if prior is not None:
            _verify_current_receipt(cur,config,plan,prior)
            return PublicationReceipt(**prior)
        from .publication_history import check_source_history
        source_history=check_source_history(cur,config,plan,str(intent_id),username,password)
        if config.get("_publication_resolution"):
            from .recovery import validate_resolution
            recovery_control=validate_resolution(cur,config,plan,intent_id,config["_publication_resolution"],username,password)
        _,receipt_table,registry=_tables(config)
        object_id=cur.execute("SELECT OBJECT_ID(?)",target).fetchone()[0]
        exists=object_id is not None
        if mode=="create_only" and exists:
            raise IngestError("TARGET_EXISTS","create_only refuses an existing target; resume the original run if recovering an import. No rows were added or replaced.",2,{"target":target})
        if uses_name_claims(config):
            from .adapters.mssql.name_claims import claim
            claim(cur,config,target,exists)
        if config.get("_target_review"):
            if exists:cur.execute(f"SELECT COUNT_BIG(*) FROM {target} WITH (TABLOCKX,HOLDLOCK)").fetchone()
            from .target_review import verify_review
            verify_review(cur,config,plan,intent_id)
        if config.get("mapping",{}).get("existing_target_strategy")=="mapped_business":
            from .adapters.mssql.business import publish_from_stage
            receipt=publish_from_stage(cur,config,plan,target,object_id,intent_id,run_id,count,expected.hexdigest(),transport,staging_batches,text_evidence)
            _projection_assurance(receipt,config)
            receipt.details["source_history"]=source_history
            _store_receipt(cur, receipt_table, intent_id, target, object_id, receipt)
            committing=True;conn.commit()
            test_fault(config,"AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT")
            committing=False;receipt.details["commit_observed_at_utc"]=utcnow()
            return receipt
        registered=cur.execute(f"SELECT target_object_id,schema_hash,mapping_hash,version FROM {registry} WITH (READCOMMITTEDLOCK) WHERE target=?",target).fetchone()
        if config.get("_publication_group"):
            from .adapters.mssql import contributors
            group=config["_publication_group"]
            group_details=contributors.prepare(cur,config,plan,group,mode,exists)
            if mode in ("append","create_only") and not config.get("_explicit_new_intent",False):
                group_control,contribution_history=contributors.check_duplicate_contributions(cur,config,plan,group,username,password)
                group_details["contribution_history"]=contribution_history
            if mode=="replace" and exists:
                group_control,history=contributors.history_guard(cur,config,plan,group,username,password)
                group_details["central_history"]=history
            contributors.check_omissions(cur,config,group,mode,group_details)
        storage_plan, storage_fingerprint, schema_diff, object_id = _prepare_landing_target(
            cur, config, plan, target, object_id, registered, declarations, metadata
        )
        fingerprint = schema_hash(plan)
        if mode=="replace":
            if not count and not config.get("load",{}).get("allow_empty_replace",False):raise IngestError("EMPTY_REPLACE_BLOCKED","An empty source cannot replace the usable target")
            if exists:
                if not config.get("load",{}).get("retain_previous_generation",True):raise IngestError("REPLACE_RETENTION_REQUIRED","This replacement strategy requires keeping the previous generation")
                backup=qualified(plan.schema,truncate_utf16(plan.table,90)+"__previous_"+uuid.uuid4().hex[:16])
                from .adapters.mssql.evolution import _old_values
                from .adapters.mssql.metadata import live_columns
                previous_values=_old_values(cur,config,storage_plan,target)
                cur.execute(f"SELECT * INTO {backup} FROM {target} WITH (TABLOCKX,HOLDLOCK)")
                backup_values=_old_values(cur,config,storage_plan,backup)
                if previous_values!=backup_values:raise IngestError("GENERATION_COPY_MISMATCH","Retained previous generation differs from original typed values; replace rolled back",5)
                backup_proof={"target":backup,"object_id":cur.execute("SELECT OBJECT_ID(?)",backup).fetchone()[0],"shape":live_columns(cur,backup),"columns":[asdict(c) for c in storage_plan.columns],"all_previous_values":previous_values}
                cur.execute(f"DELETE FROM {target}")
                test_fault(config,"REPLACE_AFTER_DELETE_BEFORE_INSERT")
        cur.execute(f"INSERT INTO {target} ({names},[__ingest_intent],[__ingest_record],[__ingest_ordinal]) SELECT {names},[__ingest_intent],[__ingest_record],[__ingest_ordinal] FROM #db_ingest_stage")
        verified,digest=_readback(cur,plan,target,intent_id,config)
        if verified!=count or digest!=expected.hexdigest():raise IngestError("VALUE_VERIFICATION_FAILED","Target post-insert values or identities differ; publication transaction will roll back",5,{"expected_count":count,"observed_count":verified})
        from .adapters.mssql.evolution import snapshot
        snapshot(cur,config,storage_plan,target,object_id,{"evolution":schema_diff} if schema_diff else None)
        if schema_hash(storage_plan)!=fingerprint:snapshot(cur,config,plan,target,object_id,{"source_projection_only":True,"storage_schema_hash":storage_fingerprint})
        if group_details is not None:
            contributors.commit(cur,config,plan,config["_publication_group"],str(intent_id),mode)
        receipt=PublicationReceipt(str(intent_id),str(run_id),plan.database,plan.schema,plan.table,plan.source_hash,plan.interpretation_hash,plan.mapping_hash,count,digest,config.get("verification",{}).get("level","full"),utcnow(),{"transport":transport,"schema_hash":fingerprint,"target_object_id":object_id,"database_guid":_database_guid(cur),"recovery_fork_guid":_recovery_fork(cur),"operation":mode,"previous_generation":backup,"value_verification":"all values inside publication transaction","control_mirror_state":"pending","staging_batches":staging_batches,"publication_attempt":config.get("_publication_attempt",{"number":1}),"varchar_representation":text_evidence})
        _projection_assurance(receipt,config)
        if backup_proof is not None:receipt.details["previous_generation_proof"]=backup_proof
        receipt.details["source_history"]=source_history
        receipt.details["storage_schema_hash"]=storage_fingerprint
        if schema_diff is not None:receipt.details["schema_diff"]=schema_diff
        if group_details is not None:receipt.details["source_group"]=group_details
        _store_receipt(cur, receipt_table, intent_id, target, object_id, receipt)
        committing=True
        conn.commit()
        test_fault(config,"AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT")
        committing=False
        receipt.details["commit_observed_at_utc"]=utcnow()
        return receipt
    except IngestError:raise
    except Exception as exc:
        if committing:raise provider_error(exc,"COMMIT_OUTCOME_UNKNOWN; reconnect and resolve destination receipt before replay",(password,),5) from None
        raise provider_error(exc,"stage/validate/publish (transaction not acknowledged committed)",(password,)) from None
    finally:
        try:
            if bcp_loader is not None:bcp_loader.close(success=transport_verified)
        finally:
            try:
                if group_control is not None:
                    try:group_control.rollback()
                    finally:group_control.close()
            finally:
                try:
                    if recovery_control is not None:
                        try:recovery_control.rollback()
                        finally:recovery_control.close()
                finally:
                    if conn is not None:
                        try:conn.rollback()
                        except Exception:pass
                        conn.close()
