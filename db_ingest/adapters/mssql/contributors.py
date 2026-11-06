"""Bounded source-manifest evidence and transactional merged-table contributors."""
import hashlib
import json
from pathlib import Path,PurePosixPath
import re
from db_ingest.errors import IngestError
from db_ingest.resources import checkpoint
from .ddl import qualified


from db_ingest.contributors import normalized,source_manifest_hash,iter_manifest,contribution_signature,contribution_descriptor


def tables(config):
    schema=config.get("audit",{}).get("control_schema","audit")
    return qualified(schema,"__db_ingest_contributors"),qualified(schema,"__db_ingest_groups")


def history_table(config):return qualified(config.get("audit",{}).get("control_schema","audit"),"__db_ingest_contribution_history")


def bootstrap(cur,config):
    contributors,groups=tables(config)
    definitions={contributors:"[target] nvarchar(260) NOT NULL,[file_id] char(64) NOT NULL,[relative_path] nvarchar(max) NOT NULL,[source_sha256] char(64) NOT NULL,[source_size] bigint NOT NULL,[metadata_json] nvarchar(max) NOT NULL,[active_generation] bit NOT NULL,[last_intent] nvarchar(64) NOT NULL,PRIMARY KEY([target],[file_id])",groups:"[target] nvarchar(260) NOT NULL PRIMARY KEY,[group_id] nvarchar(128) NOT NULL,[manifest_sha256] char(64) NOT NULL,[source_hash] char(64) NOT NULL,[contributor_count] bigint NOT NULL,[last_intent] nvarchar(64) NOT NULL"}
    definitions[history_table(config)]="[target] nvarchar(260) NOT NULL,[intent_id] nvarchar(64) NOT NULL,[file_id] char(64) NOT NULL,[signature] char(64) NOT NULL,[source_sha256] char(64) NOT NULL,[source_size] bigint NOT NULL,[interpretation_hash] char(64) NOT NULL,[mapping_hash] varchar(64) NOT NULL,[metadata_json] nvarchar(max) NOT NULL,PRIMARY KEY([target],[intent_id],[file_id])"
    for name,definition in definitions.items():
        if cur.execute("SELECT OBJECT_ID(?)",name).fetchone()[0] is None:
            if not config.get("destination",{}).get("create_tables",False):raise IngestError("GROUP_METADATA_UNAVAILABLE","Merged publication requires its contributor registry; creation disabled")
            cur.execute(f"CREATE TABLE {name} ({definition})")
            if name==history_table(config):
                cur.execute(f"CREATE INDEX IX_ContributionSignature ON {name} (target,signature)")
                cur.execute(f"CREATE INDEX IX_ContributionLegacy ON {name} (target,file_id,source_sha256,interpretation_hash)")


def history_guard(cur,config,plan,group,username,password):
    """Hold central contributor history stable through destination commit."""
    from .control import _existing_group_control,group_target_key
    from db_ingest.publication import _lock
    conn=None
    try:
        conn,schema=_existing_group_control(config,username,password)
        central=conn.cursor();key=group_target_key(plan)
        _lock(central,"db_ingest:control:group:"+key,config["load"]["target_lock_timeout_seconds"])
        receipts=qualified(schema,"PublicationReceipts");groups=qualified(schema,"SourceGroups")
        target=qualified(plan.schema,plan.table)
        missing=central.execute(f"SELECT COUNT_BIG(*) FROM {receipts} r LEFT JOIN {groups} g ON r.intent_id=g.intent_id WHERE r.destination_database=? AND r.target=? AND g.intent_id IS NULL",plan.database,target).fetchone()[0]
        if missing:raise IngestError("GROUP_CENTRAL_HISTORY_INCOMPLETE","Central ACKs lack complete contributor manifests; replacement blocked",5,{"missing_group_manifests":missing})
        central.execute(f"SELECT file_id,relative_path FROM {qualified(schema,'SourceGroupContributors')} WITH (HOLDLOCK) WHERE group_key=?",key)
        count=0
        for identifier,relative in central:
            checkpoint();_require_path(cur,identifier,relative);count+=1
        return conn,{"state":"LOCKED_THROUGH_PUBLICATION","group_key":key,"contributor_count":count}
    except Exception:
        if conn is not None:
            try:conn.rollback()
            finally:conn.close()
        raise


def _require_path(cur,identifier,relative):
    try:verified=normalized({"relative_path":relative,"sha256":"0"*64,"size":0})
    except IngestError as error:raise IngestError("GROUP_PATH_IDENTITY_CONFLICT","Historical contributor has an invalid relative path",5,{"cause":error.code}) from None
    if verified["file_id"]!=identifier:raise IngestError("GROUP_PATH_IDENTITY_CONFLICT","Historical contributor identifier does not match its exact UTF8 path",5)
    prior=cur.execute("SELECT relative_path FROM #db_ingest_required_contributors WHERE file_id=?",identifier).fetchone()
    if prior is not None:
        if prior[0]!=relative:raise IngestError("GROUP_PATH_IDENTITY_CONFLICT","Contributor hash conflicts with its full relative path",5)
    else:cur.execute("INSERT INTO #db_ingest_required_contributors(file_id,relative_path) VALUES(?,?)",identifier,relative)


def prepare(cur,config,plan,group,mode,target_exists):
    """Populate only session-temp metadata before any business mutation."""
    if not isinstance(group,dict) or not isinstance(group.get("group_id"),str) or not 1<=len(group["group_id"].encode("utf-16-le"))//2<=128 or not isinstance(group.get("contributor_count"),int) or isinstance(group.get("contributor_count"),bool) or group["contributor_count"]<0:raise IngestError("GROUP_MANIFEST_REQUIRED","Merged target requires a validated bounded contributor manifest")
    if group.get("has_unresolved",False):raise IngestError("GROUP_UNRESOLVED","Unresolved source framing blocks merged publication",5)
    if mode=="replace" and not group.get("complete_input",False):raise IngestError("GROUP_REPLACE_INCOMPLETE","Merged replacement requires complete selected input with no rejects/exclusions")
    from db_ingest.transports.odbc import OdbcBatchWriter
    contributors,groups=tables(config);target=qualified(plan.schema,plan.table)
    existing=cur.execute(f"SELECT group_id FROM {groups} WITH (READCOMMITTEDLOCK) WHERE target=?",target).fetchone()
    if existing is not None and existing[0]!=group["group_id"]:raise IngestError("GROUP_IDENTITY_CHANGED","Existing target belongs to a different source group")
    if target_exists and existing is None:raise IngestError("GROUP_HISTORY_UNAVAILABLE","Existing group target lacks a complete contributor registry; no replacement/append guessed",5)
    cur.execute("CREATE TABLE #db_ingest_selected_contributors (file_id char(64) NOT NULL PRIMARY KEY,relative_path nvarchar(max) NOT NULL,source_sha256 char(64) NOT NULL,source_size bigint NOT NULL,metadata_json nvarchar(max) NOT NULL,signature char(64) NOT NULL,interpretation_hash char(64) NOT NULL,mapping_hash varchar(64) NOT NULL)")
    direct={**config,"load":{**config.get("load",{}),"fast_executemany":False}}
    writer=OdbcBatchWriter(cur,"INSERT INTO #db_ingest_selected_contributors (file_id,relative_path,source_sha256,source_size,metadata_json,signature,interpretation_hash,mapping_hash) VALUES (?,?,?,?,?,?,?,?)",direct)
    def selected():
        for item in iter_manifest(group):
            descriptor=contribution_descriptor(item)
            writer.add([item["file_id"],item["relative_path"],item["sha256"],item["size"],json.dumps(item,ensure_ascii=True,separators=(",",":")),contribution_signature(item),descriptor["interpretation_hash"],descriptor["mapping_hash"]])
            yield item
    source_hash=source_manifest_hash(selected());writer.finish()
    if source_hash!=plan.source_hash or group.get("source_hash")!=plan.source_hash:raise IngestError("GROUP_SOURCE_HASH_MISMATCH","Composite source hash does not match exact contributor source bytes",5)
    # Required history is the union of all destination, local ACK and centrally
    # ACKed paths. The caller populates central history under a held group lock.
    cur.execute("CREATE TABLE #db_ingest_required_contributors(file_id char(64) NOT NULL PRIMARY KEY,relative_path nvarchar(max) NOT NULL)")
    bad=cur.execute(f"SELECT COUNT_BIG(*) FROM {contributors} WITH (READCOMMITTEDLOCK) WHERE target=? AND file_id<>LOWER(CONVERT(char(64),HASHBYTES('SHA2_256',CONVERT(varbinary(max),CONVERT(varchar(max),relative_path COLLATE Latin1_General_100_BIN2_UTF8))),2))",target).fetchone()[0]
    if bad:raise IngestError("GROUP_PATH_IDENTITY_CONFLICT","Destination contributor path identifiers differ from their exact UTF8 paths",5,{"conflict_count":bad})
    cur.execute(f"INSERT INTO #db_ingest_required_contributors(file_id,relative_path) SELECT file_id,relative_path FROM {contributors} WITH (READCOMMITTEDLOCK) WHERE target=?",target)
    if group.get("required_manifest_path"):
        required={"manifest_path":group["required_manifest_path"],"manifest_sha256":group["required_manifest_sha256"],"contributor_count":group["required_contributor_count"],"max_line_bytes":group.get("max_line_bytes",8388608)}
        for item in iter_manifest(required):_require_path(cur,item["file_id"],item["relative_path"])
    return {"group_id":group["group_id"],"manifest_path":str(group["manifest_path"]),"manifest_sha256":group["manifest_sha256"],"contributor_count":group["contributor_count"],"source_hash":source_hash,"source_kind":"length-framed contributor path/raw-SHA256/byte-size manifest","metadata_batches":writer.summary}


def check_omissions(cur,config,group,mode,summary):
    collision=cur.execute("SELECT TOP(1) old.relative_path,selected.relative_path FROM #db_ingest_required_contributors old JOIN #db_ingest_selected_contributors selected ON old.file_id=selected.file_id WHERE CONVERT(varbinary(max),old.relative_path)<>CONVERT(varbinary(max),selected.relative_path)").fetchone()
    if collision is not None:raise IngestError("GROUP_PATH_IDENTITY_CONFLICT","Contributor identifier maps to conflicting full paths",5,{"known_path":collision[0],"selected_path":collision[1]})
    omitted=cur.execute("SELECT COUNT_BIG(*) FROM #db_ingest_required_contributors AS old WHERE NOT EXISTS (SELECT 1 FROM #db_ingest_selected_contributors AS selected WHERE selected.file_id=old.file_id)").fetchone()[0]
    preview=[]
    if omitted:
        rows=cur.execute("SELECT TOP(20) relative_path FROM #db_ingest_required_contributors AS old WHERE NOT EXISTS (SELECT 1 FROM #db_ingest_selected_contributors AS selected WHERE selected.file_id=old.file_id) ORDER BY file_id")
        preview=[row[0] for row in rows.fetchall()]
    scope=config.get("load",{}).get("replace_scope","complete_group")
    if mode=="replace" and scope=="complete_group" and omitted:raise IngestError("GROUP_REPLACE_OMITTED_CONTRIBUTORS","Complete-group replacement omitted known contributing files; target unchanged",2,{"omitted_count":omitted,"omitted_preview":preview,"registry":"destination + local ACK + central ACK contributor union","selection_manifest":group["manifest_path"]})
    summary.update(omitted_count=omitted,omitted_preview=preview,replace_scope=scope)
    return summary


def commit(cur,config,plan,group,intent_id,mode):
    contributors,groups=tables(config);target=qualified(plan.schema,plan.table)
    history=history_table(config)
    cur.execute(f"INSERT INTO {history}(target,intent_id,file_id,signature,source_sha256,source_size,interpretation_hash,mapping_hash,metadata_json) SELECT ?,?,file_id,signature,source_sha256,source_size,interpretation_hash,mapping_hash,metadata_json FROM #db_ingest_selected_contributors",target,intent_id)
    if mode=="replace":cur.execute(f"UPDATE {contributors} SET active_generation=0 WHERE target=?",target)
    cur.execute(f"UPDATE old SET source_sha256=selected.source_sha256,source_size=selected.source_size,metadata_json=selected.metadata_json,active_generation=1,last_intent=? FROM {contributors} AS old JOIN #db_ingest_selected_contributors AS selected ON selected.file_id=old.file_id WHERE old.target=?",intent_id,target)
    cur.execute(f"INSERT INTO {contributors} (target,file_id,relative_path,source_sha256,source_size,metadata_json,active_generation,last_intent) SELECT ?,selected.file_id,selected.relative_path,selected.source_sha256,selected.source_size,selected.metadata_json,1,? FROM #db_ingest_selected_contributors AS selected WHERE NOT EXISTS (SELECT 1 FROM {contributors} AS old WHERE old.target=? AND old.file_id=selected.file_id)",target,intent_id,target)
    if cur.execute(f"SELECT target FROM {groups} WITH (READCOMMITTEDLOCK) WHERE target=?",target).fetchone() is None:
        cur.execute(f"INSERT INTO {groups} (target,group_id,manifest_sha256,source_hash,contributor_count,last_intent) VALUES (?,?,?,?,?,?)",target,group["group_id"],group["manifest_sha256"],plan.source_hash,group["contributor_count"],intent_id)
    else:cur.execute(f"UPDATE {groups} SET manifest_sha256=?,source_hash=?,contributor_count=?,last_intent=? WHERE target=?",group["manifest_sha256"],plan.source_hash,group["contributor_count"],intent_id,target)


def _destination_coverage(cur,config,plan):
    from db_ingest.publication import _tables
    _,receipts,_=_tables(config);history=history_table(config);target=qualified(plan.schema,plan.table)
    missing=cur.execute(f"SELECT COUNT_BIG(*) FROM {receipts} r WITH (READCOMMITTEDLOCK) WHERE r.target=? AND (TRY_CONVERT(bigint,JSON_VALUE(r.payload,'$.details.source_group.contributor_count')) IS NULL OR TRY_CONVERT(bigint,JSON_VALUE(r.payload,'$.details.source_group.contributor_count'))<>(SELECT COUNT_BIG(*) FROM {history} h WITH (READCOMMITTEDLOCK) WHERE h.target=r.target AND h.intent_id=r.intent_id))",target).fetchone()[0]
    if missing:raise IngestError("GROUP_CONTRIBUTION_HISTORY_INCOMPLETE","Prior group ACKs lack their complete per-contribution history; resume/backfill original manifests before another default append",5,{"incomplete_receipts":missing,"target":target})


def check_duplicate_contributions(cur,config,plan,group,username,password):
    """Same-target locked duplicate policy across ALL acknowledged contributor versions."""
    from db_ingest.publication import _lock
    from .control import _existing_group_control,group_target_key
    target=qualified(plan.schema,plan.table);history=history_table(config)
    _destination_coverage(cur,config,plan)
    match="h.file_id=s.file_id AND (h.signature=s.signature OR (h.mapping_hash='' AND h.source_sha256=s.source_sha256 AND h.interpretation_hash=s.interpretation_hash))"
    count=cur.execute(f"SELECT COUNT_BIG(*) FROM {history} h WITH (READCOMMITTEDLOCK) JOIN #db_ingest_selected_contributors s ON {match} WHERE h.target=?",target).fetchone()[0]
    if count:
        preview=[{"relative_path":r[0],"prior_intent_id":r[1]} for r in cur.execute(f"SELECT TOP(20) LEFT(s.relative_path COLLATE Latin1_General_100_CI_AS_SC,512),h.intent_id FROM {history} h WITH (READCOMMITTEDLOCK) JOIN #db_ingest_selected_contributors s ON {match} WHERE h.target=? ORDER BY h.intent_id,h.file_id",target)]
        raise IngestError("NEW_INTENT_REQUIRED","Selected contributors already have acknowledged source/interpretation/mapping history; explicitly choose --new-intent to append them again",2,{"history_source":"destination_contributions","history_count":count,"contributors":preview,"preview_truncated":count>len(preview)})
    central=None
    try:
        central,schema=_existing_group_control(config,username,password);reader=central.cursor();key=group_target_key(plan)
        _lock(reader,"db_ingest:control:group:"+key,config["load"]["target_lock_timeout_seconds"])
        version=reader.execute(f"SELECT MAX(version) FROM {qualified(schema,'SchemaMigrations')}").fetchone()[0]
        central_history=qualified(schema,"SourceGroupContributionHistory")
        if version is None or version<4 or reader.execute("SELECT OBJECT_ID(?)",central_history).fetchone()[0] is None:raise IngestError("GROUP_CONTRIBUTION_HISTORY_INCOMPLETE","Central group history predates per-contribution signatures; backfill original acknowledged manifests",5)
        groups=qualified(schema,"SourceGroups");receipts=qualified(schema,"PublicationReceipts")
        missing=reader.execute(f"SELECT COUNT_BIG(*) FROM {receipts} r WITH (READCOMMITTEDLOCK) LEFT JOIN {groups} g WITH (READCOMMITTEDLOCK) ON r.intent_id=g.intent_id WHERE r.destination_database=? AND r.target=? AND (g.intent_id IS NULL OR g.contributor_count<>(SELECT COUNT_BIG(*) FROM {central_history} h WITH (READCOMMITTEDLOCK) WHERE h.group_key=g.group_key AND h.intent_id=g.intent_id))",plan.database,target).fetchone()[0]
        if missing:raise IngestError("GROUP_CONTRIBUTION_HISTORY_INCOMPLETE","Central group ACKs have incomplete per-contribution history; no absence inferred",5,{"incomplete_receipts":missing})
        # Selected metadata is already in a destination temp table. Query one bounded
        # signature at a time on the held central group transaction, never a big IN list.
        last="";checked=0
        while True:
            selected=cur.execute("SELECT TOP(1) file_id,signature,source_sha256,interpretation_hash,LEFT(relative_path COLLATE Latin1_General_100_CI_AS_SC,512) FROM #db_ingest_selected_contributors WHERE file_id>? ORDER BY file_id",last).fetchone()
            if selected is None:break
            last=selected[0];checkpoint();checked+=1
            found=reader.execute(f"SELECT TOP(1) intent_id FROM {central_history} WITH (READCOMMITTEDLOCK) WHERE group_key=? AND file_id=? AND (signature=? OR (mapping_hash='' AND source_sha256=? AND interpretation_hash=?)) ORDER BY intent_id",key,selected[0],selected[1],selected[2],selected[3]).fetchone()
            if found:raise IngestError("NEW_INTENT_REQUIRED","Selected contributor has central acknowledged history; explicitly choose --new-intent to append it again",2,{"history_source":"central_contributions","relative_path":selected[4],"prior_intent_id":found[0]})
        return central,{"state":"CHECKED_UNDER_HELD_GROUP_LOCK","selected_contributors":checked,"central_version":version,"destination_history":"complete per-ACK contributors"}
    except Exception:
        if central is not None:
            try:central.rollback()
            finally:central.close()
        raise


def mirror_destination_contributions(config,plan,receipt,manifest,username=None,password=None):
    """Hydrate versioned history ONLY from a verified original ACK/manifest."""
    from dataclasses import asdict,is_dataclass
    from .connect import connect
    from .diagnostics import provider_error
    from db_ingest.publication import _lock,_resource,_get_receipt,_verify_current_receipt
    from db_ingest.recovery import identity
    expected=asdict(receipt) if is_dataclass(receipt) else receipt;conn=None
    declared=expected.get("details",{}).get("source_group",{})
    if manifest.get("manifest_sha256")!=declared.get("manifest_sha256") or manifest.get("contributor_count")!=declared.get("contributor_count"):raise IngestError("GROUP_CONTRIBUTION_MANIFEST_CONFLICT","Original ACK manifest seal/count differs; history cannot be synthesized",5)
    try:
        conn=connect(config,username,password,plan.database);cur=conn.cursor();cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        _lock(cur,_resource(plan),config["load"]["target_lock_timeout_seconds"])
        actual=_get_receipt(cur,config,plan,expected["intent_id"])
        if actual is None or identity(actual)!=identity(expected):raise IngestError("GROUP_CONTRIBUTION_ACK_MISSING","Original acknowledged destination receipt is missing/conflicting; no history backfill",5)
        _verify_current_receipt(cur,config,plan,actual)
        _lock(cur,"db_ingest:metadata:v1",config["load"]["target_lock_timeout_seconds"])
        bootstrap(cur,config);history=history_table(config);target=qualified(plan.schema,plan.table);accepted=0;count=0
        def verified():
            nonlocal accepted,count
            for item in iter_manifest(manifest):
                checkpoint();descriptor=contribution_descriptor(item);signature=contribution_signature(item);encoded=json.dumps(item,ensure_ascii=True,separators=(",",":"))
                records=item.get("records",0)
                if not isinstance(records,int) or isinstance(records,bool) or records<0:raise IngestError("GROUP_CONTRIBUTION_COUNT","Original contributor record count is invalid",5)
                accepted+=records;count+=1
                prior=cur.execute(f"SELECT signature,DATALENGTH(metadata_json) FROM {history} WITH (READCOMMITTEDLOCK) WHERE target=? AND intent_id=? AND file_id=?",target,expected["intent_id"],item["file_id"]).fetchone()
                if prior is None:cur.execute(f"INSERT INTO {history}(target,intent_id,file_id,signature,source_sha256,source_size,interpretation_hash,mapping_hash,metadata_json) VALUES(?,?,?,?,?,?,?,?,?)",target,expected["intent_id"],item["file_id"],signature,item["sha256"],item["size"],descriptor["interpretation_hash"],descriptor["mapping_hash"],encoded)
                else:
                    if prior[0]!=signature or (prior[1] or 0)>33554432:raise IngestError("GROUP_CONTRIBUTION_HISTORY_CONFLICT","Immutable contributor signature/budget conflicts with original ACK",5)
                    checkpoint((prior[1] or 0)*2)
                    stored=cur.execute(f"SELECT metadata_json FROM {history} WITH (READCOMMITTEDLOCK) WHERE target=? AND intent_id=? AND file_id=?",target,expected["intent_id"],item["file_id"]).fetchone()[0]
                    if stored!=encoded:raise IngestError("GROUP_CONTRIBUTION_HISTORY_CONFLICT","Immutable contributor payload differs from original sealed manifest",5)
                yield item
        source_hash=source_manifest_hash(verified())
        total=cur.execute(f"SELECT COUNT_BIG(*) FROM {history} WITH (READCOMMITTEDLOCK) WHERE target=? AND intent_id=?",target,expected["intent_id"]).fetchone()[0]
        if source_hash!=plan.source_hash or accepted!=expected["committed_records"] or total!=count:raise IngestError("GROUP_CONTRIBUTION_HISTORY_CONFLICT","Original source/accepted count/history coverage differs from ACK",5)
        conn.commit();return {"state":"VERIFIED","history_rows":count,"signature_version":1,"manifest_sha256":manifest["manifest_sha256"]}
    except IngestError:raise
    except Exception as error:raise provider_error(error,"destination contributor history backfill",(password,),5) from None
    finally:
        if conn is not None:
            try:conn.rollback()
            finally:conn.close()
