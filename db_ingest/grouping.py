"""Disk-backed merged-table projections over authoritative captured source records.

Groups have one atomic publication intent/receipt. Files remain real sources, with
source ordinals and states retained independently from derived group ordinals.
"""
from dataclasses import asdict
import base64
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid
from .config import effective_for_file,hashes,stable_hash
from .models import schema_plan_from_dict as _plan
from .errors import IngestError
from .journal import Journal
from .models import ColumnProfile,FieldValue,RecordEnvelope,SchemaPlan
from .planning import map_destination,identifier
from .profile import profile_records,column_name
from .resources import checkpoint
from .adapters.mssql.types import convert_field,parse_sql_type
from .adapters.mssql.contributors import iter_manifest,source_manifest_hash


def initialize(journal):
    with journal.lease("journal-group-bootstrap",timeout=30):
        marker=journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='group_schema'").fetchone()
        if marker:
            version=journal.db.execute("SELECT COALESCE(MAX(version),0) FROM group_schema").fetchone()[0]
            if version>2:raise IngestError("GROUP_SCHEMA_VERSION","Merged journal schema is newer than this application")
            if version==2:return
        journal.db.executescript("""CREATE TABLE IF NOT EXISTS group_schema(version INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS groups(run_id TEXT NOT NULL,group_id TEXT NOT NULL,intent_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,target TEXT NOT NULL,plan TEXT,manifest TEXT,counts TEXT NOT NULL DEFAULT '{}',receipt TEXT,error TEXT,PRIMARY KEY(run_id,group_id));
        CREATE TABLE IF NOT EXISTS group_members(run_id TEXT NOT NULL,group_id TEXT NOT NULL,selection_id TEXT NOT NULL,file_id TEXT,relative_path TEXT NOT NULL,mapping TEXT,ordinal_offset INTEGER NOT NULL DEFAULT 0,source_max_ordinal INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(run_id,group_id,selection_id),UNIQUE(run_id,selection_id));
        CREATE INDEX IF NOT EXISTS group_member_order ON group_members(run_id,group_id,relative_path);
        CREATE TABLE IF NOT EXISTS group_records(run_id TEXT NOT NULL,group_id TEXT NOT NULL,record_id TEXT NOT NULL,source_file_id TEXT NOT NULL,source_ordinal INTEGER NOT NULL,ordinal INTEGER NOT NULL,disposition TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(run_id,group_id,record_id),UNIQUE(run_id,group_id,ordinal));
        CREATE INDEX IF NOT EXISTS group_record_order ON group_records(run_id,group_id,ordinal);
        CREATE TABLE IF NOT EXISTS group_record_events(run_id TEXT NOT NULL,group_id TEXT NOT NULL,record_id TEXT NOT NULL,source_column_ordinal INTEGER NOT NULL,group_column_ordinal INTEGER NOT NULL,policy TEXT NOT NULL,source_value_sha256 TEXT NOT NULL,result_value_sha256 TEXT NOT NULL,PRIMARY KEY(run_id,group_id,record_id,source_column_ordinal,policy));
        CREATE TABLE IF NOT EXISTS group_contributors(group_id TEXT NOT NULL,path_hash TEXT NOT NULL,relative_path TEXT NOT NULL,source_sha256 TEXT NOT NULL,source_size INTEGER NOT NULL,metadata TEXT NOT NULL,last_run_id TEXT NOT NULL,last_intent TEXT NOT NULL,PRIMARY KEY(group_id,path_hash)); CREATE INDEX IF NOT EXISTS group_source_counts ON group_records(run_id,group_id,source_file_id,disposition); CREATE INDEX IF NOT EXISTS group_ack_history ON groups(group_id) WHERE receipt IS NOT NULL; INSERT OR IGNORE INTO group_schema VALUES(2);""")


def available(journal):
    return bool(journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='groups'").fetchone())


def group(journal,run_id,group_id):
    row=journal.db.execute("SELECT * FROM groups WHERE run_id=? AND group_id=?",(run_id,group_id)).fetchone()
    if row is None:raise IngestError("GROUP_UNKNOWN","Merged source group is not in the durable journal")
    item=dict(row)
    for key in ("target","plan","manifest","counts","receipt","error"):
        item[key]=json.loads(item[key]) if item[key] else None
    return item


def groups(journal,run_id):
    if not available(journal):return
    last=0
    while True:
        row=journal.db.execute("SELECT rowid,group_id FROM groups WHERE run_id=? AND rowid>? ORDER BY rowid LIMIT 1",(run_id,last)).fetchone()
        if row is None:break
        last=row[0];yield group(journal,run_id,row[1])


def update(journal,run_id,group_id,**values):
    if set(values)-{"state","plan","manifest","counts","receipt","error"}:raise ValueError("Unknown group journal field")
    fields=",".join(k+"=?" for k in values)
    encoded=[v if k=="state" else json.dumps(v,ensure_ascii=True) for k,v in values.items()]
    with journal.db:journal.db.execute("UPDATE groups SET "+fields+" WHERE run_id=? AND group_id=?",(*encoded,run_id,group_id))


def members(journal,run_id,group_id):
    reader=sqlite3.connect(journal.work/"journal.sqlite3",timeout=30);reader.row_factory=sqlite3.Row
    try:
        for row in reader.execute("SELECT * FROM group_members WHERE run_id=? AND group_id=? ORDER BY relative_path COLLATE BINARY",(run_id,group_id)):
            item=dict(row);item["mapping"]=json.loads(item["mapping"]) if item["mapping"] else None
            yield item
    finally:reader.close()


def records(journal,run_id,group_id,disposition=None):
    journal.flush();reader=sqlite3.connect(journal.work/"journal.sqlite3",timeout=30)
    try:
        sql="SELECT payload,disposition,record_id,ordinal FROM group_records WHERE run_id=? AND group_id=?";parameters=[run_id,group_id]
        if disposition:sql+=" AND disposition=?";parameters.append(disposition)
        for row in reader.execute(sql+" ORDER BY ordinal",parameters):
            checkpoint()
            try:
                item=json.loads(row[0])
                if not isinstance(item,dict) or not isinstance(item.get("fields"),list) or not isinstance(item.get("errors"),list) or not all(isinstance(e,dict) for e in item["errors"]):raise ValueError("Invalid envelope structure")
                if not all(k in item for k in ("record_id","ordinal","kind")):raise ValueError("Missing identity")
            except (ValueError,TypeError) as error:raise IngestError("GROUP_LEDGER_PAYLOAD_INVALID","Retained merged envelope is malformed",5,{"record_id":row[2]}) from error
            if row[1] not in ("pending","committed","rejected"):raise IngestError("GROUP_DISPOSITION_INVALID","Merged record has an unknown disposition",5,{"record_id":row[2]})
            if (item["record_id"],item["ordinal"])!=(row[2],row[3]):raise IngestError("GROUP_LEDGER_IDENTITY_MISMATCH","Merged record payload differs from its indexed identity/order",5)
            if (row[1] in ("pending","committed") and (item["kind"]!="data" or item["errors"])) or (row[1]=="rejected" and (item["kind"]!="rejected" or not item["errors"])):raise IngestError("GROUP_DISPOSITION_INVALID","Merged disposition conflicts with its projection envelope/errors",5,{"record_id":row[2]})
            try:item["fields"]=[FieldValue(**f) for f in item["fields"]];record=RecordEnvelope(**item)
            except (TypeError,ValueError) as error:raise IngestError("GROUP_LEDGER_PAYLOAD_INVALID","Retained merged fields/envelope do not match the supported model",5) from error
            yield record
    finally:reader.close()


def group_counts(journal,run_id,group_id):
    values={k:0 for k in ("pending","committed","rejected")}
    for disposition,count in journal.db.execute("SELECT disposition,COUNT(*) FROM group_records WHERE run_id=? AND group_id=? GROUP BY disposition",(run_id,group_id)):
        if disposition not in values:raise IngestError("GROUP_DISPOSITION_INVALID","Merged record has an unknown disposition",5)
        values[disposition]=count
    return values




def _group_id(config,target):
    value={"server":str(config["destination"]["server"]).casefold(),"root":str(Path(config["source"]["root"]).absolute()).casefold(),"target":{k:target[k] for k in ("database","schema","table")}}
    return stable_hash(value)


def _label(value):
    return {"preview":value[:128],"characters":len(value),"sha256":hashlib.sha256(value.encode("utf-8")).hexdigest()}


def _keys(columns,policy):
    labels=[];seen=set();answer=[]
    for column in columns:
        override=column.stats.get("override",{})
        label=override.get("target_name",column.original_name)
        explicit="target_name" in override
        if policy=="union_by_name" and (not label or label in seen):
            raise IngestError("GROUP_COLUMN_AMBIGUOUS","Union-by-name needs unique nonblank names; declare ordinal-disambiguated target_name overrides",2,{"column":_label(label),"source_ordinal":column.ordinal})
        seen.add(label);labels.append(label)
        key=label if policy=="union_by_name" else json.dumps([column.ordinal,label],ensure_ascii=True,separators=(",",":"))
        answer.append((key,label,explicit))
    return answer


def _canonical_columns(journal,config,run_id,group_id):
    policy=config["mapping"]["directory_merge_schema"]
    canonical={};reference=None;label_bytes=0
    for member in members(journal,run_id,group_id):
        checkpoint()
        if not member["file_id"]:raise IngestError("GROUP_MEMBER_UNCAPTURED","Merged input includes an uncaptured source",4,{"relative_path":member["relative_path"]})
        item=journal.file(run_id,member["file_id"])
        selected=journal.selection(run_id,member["selection_id"])
        if selected["state"]!="PROFILED" or item["plan"] is None:
            error=selected.get("error") or {"code":"GROUP_MEMBER_UNPROFILED","message":"A contributing file is not fully profiled","exit_code":2}
            raise IngestError("GROUP_MEMBER_BLOCKED",error["message"],error.get("exit_code",2),{"relative_path":member["relative_path"],"source_error":error["code"]})
        columns=_plan(item["plan"]).columns
        keys=_keys(columns,policy)
        labels=[value[1] for value in keys]
        if policy=="strict":
            if reference is None:reference=labels
            elif labels!=reference:
                raise IngestError("GROUP_SCHEMA_MISMATCH","Strict merged schemas require equivalent ordered source/declared target labels",2,{"relative_path":member["relative_path"],"expected_preview":[_label(v) for v in reference[:20]],"observed_preview":[_label(v) for v in labels[:20]]})
        for column,(key,label,explicit_name) in zip(columns,keys,strict=True):
            override=column.stats.get("override",{})
            explicit_type=bool(set(override)&{"target_type","length","precision","scale"})
            if key not in canonical:
                label_bytes+=len(label.encode("utf-8"))
                if len(canonical)>=1021 or label_bytes>config["limits"]["max_in_memory_record_bytes"]:
                    raise IngestError("GROUP_SCHEMA_LIMIT","Merged schema exceeds its bounded column/name representation",2)
                checkpoint(label_bytes*4)
                canonical[key]={"key":key,"label":label,"explicit_name":False,"type":None,"nullable_override":None,"binary":None,"varchar_policy":None,"policy_original":None,"force_text":False}
            entry=canonical[key]
            if explicit_name:entry["explicit_name"]=True
            spec=parse_sql_type(column.sql_type)
            declared=parse_sql_type(column.stats.get("source_declared_type",column.sql_type))
            policy_original=str(declared)
            lexical_fallback=column.stats.get("length_policy")=="retype_text" and declared.base!=spec.base
            if entry.get("policy_original") is None:entry["policy_original"]=policy_original
            same_length_origin=entry["policy_original"]==policy_original and config["schema"]["on_length_overflow"] in ("widen","retype_text")
            if lexical_fallback:entry["force_text"]=True
            if entry["force_text"] and not same_length_origin:
                raise IngestError("GROUP_EXPLICIT_TYPE_CONFLICT","Lexical length fallback requires matching original declared contributor types",2,{"column":_label(label),"first":entry["policy_original"],"other":policy_original})
            binary=declared.base=="varbinary"
            if entry["binary"] is not None and entry["binary"]!=binary:
                raise IngestError("GROUP_EXPLICIT_TYPE_CONFLICT","A declared binary field cannot merge implicitly with ordinary text/numeric cells",2,{"column":_label(label)})
            entry["binary"]=binary
            if declared.base=="varchar":
                pair=(column.stats.get("collation"),column.stats.get("text_encoding"))
                if not all(pair):raise IngestError("GROUP_VARCHAR_POLICY","Explicit VARCHAR group fields require verified collation/text_encoding")
                if entry["varchar_policy"] is not None and entry["varchar_policy"]!=pair:
                    raise IngestError("GROUP_VARCHAR_POLICY_CONFLICT","Contributors declare incompatible VARCHAR representation/collation",2,{"column":_label(label)})
                entry["varchar_policy"]=pair
            if explicit_type:
                if entry["type"] is not None and entry["type"]!=str(spec):
                    prior=parse_sql_type(entry["type"])
                    if same_length_origin and entry["force_text"]:
                        entry["type"]=None
                    elif same_length_origin and prior.base==spec.base and spec.base in ("nvarchar","varchar","varbinary"):
                        length="max" if "max" in (prior.length,spec.length) else max(prior.length,spec.length)
                        entry["type"]=f"{spec.base}({length})"
                    else:raise IngestError("GROUP_EXPLICIT_TYPE_CONFLICT","Contributors declare conflicting explicit destination types; unify their overrides",2,{"column":_label(label),"first":entry["type"],"other":str(spec)})
                elif not entry["force_text"]:entry["type"]=str(spec)
            if "nullable" in override:
                if entry["nullable_override"] is not None and entry["nullable_override"]!=bool(override["nullable"]):
                    raise IngestError("GROUP_NULLABILITY_CONFLICT","Contributors declare conflicting destination nullability",2,{"column":_label(label)})
                entry["nullable_override"]=bool(override["nullable"])
        with journal.db:journal.db.execute("UPDATE group_members SET mapping=? WHERE run_id=? AND group_id=? AND selection_id=?",(json.dumps([x[0] for x in keys],ensure_ascii=True),run_id,group_id,member["selection_id"]))
    if policy=="union_by_name":
        historical=journal.db.execute("SELECT plan FROM groups WHERE group_id=? AND run_id<>? AND receipt IS NOT NULL ORDER BY rowid DESC LIMIT 1",(group_id,run_id)).fetchone()
        if historical:
            for old in _plan(json.loads(historical[0])).columns:
                key=old.stats.get("group_key",old.original_name)
                if key not in canonical:
                    if not old.nullable:raise IngestError("GROUP_REQUIRED_HISTORICAL_COLUMN","Selected union omits an acknowledged nonnullable column",2,{"column":old.target_name})
                    canonical[key]={"key":key,"label":old.original_name,"explicit_name":False,"type":old.sql_type,"nullable_override":True,"binary":parse_sql_type(old.sql_type).base=="varbinary","varchar_policy":(old.stats.get("collation"),old.stats.get("text_encoding")) if parse_sql_type(old.sql_type).base=="varchar" else None,"historical":asdict(old)}
                else:canonical[key]["historical"]=asdict(old)
    if policy=="strict":
        historical=journal.db.execute("SELECT plan FROM groups WHERE group_id=? AND run_id<>? AND receipt IS NOT NULL ORDER BY rowid DESC LIMIT 1",(group_id,run_id)).fetchone()
        if historical:
            for old in _plan(json.loads(historical[0])).columns:
                key=old.stats.get("group_key")
                if key in canonical:canonical[key]["historical"]=asdict(old)
    for entry in canonical.values():
        old=entry.get("historical",{})
        old_stats=old.get("stats",{})
        if config["schema"]["on_length_overflow"]=="retype_text" and old_stats.get("length_policy")=="retype_text" and old_stats.get("source_declared_type")==entry.get("policy_original"):
            entry["force_text"]=True
        if entry.get("force_text"):entry["type"]=None
    ordered=[canonical[k] for k in sorted(canonical)] if policy=="union_by_name" else list(canonical.values())
    indices={entry["key"]:i for i,entry in enumerate(ordered)}
    for member in members(journal,run_id,group_id):
        mapped=[indices[key] for key in member["mapping"]]
        with journal.db:journal.db.execute("UPDATE group_members SET mapping=? WHERE run_id=? AND group_id=? AND selection_id=?",(json.dumps(mapped),run_id,group_id,member["selection_id"]))
    return ordered


def _project(journal,run_id,group_id,source,columns,mapping,canonical,offset,events):
    fields=[FieldValue("MISSING",None) for _ in canonical]
    for column,field,index in zip(columns,source.fields,mapping,strict=True):
        derived=copy.copy(field)
        if field.state in ("VALUE","EMPTY"):
            spec=parse_sql_type(column.sql_type)
            policy=None
            if spec.base=="varbinary" and not canonical[index].get("force_text"):
                derived.text=base64.b64encode(convert_field(field,column)).decode("ascii")
                policy="declared_binary_to_canonical_base64"
            elif spec.base in ("date","time","datetime2","datetimeoffset") and (column.stats.get("date_format",column.stats.get("temporal_format","iso8601"))!="iso8601"):
                derived.text=str(convert_field(field,column));policy="declared_temporal_format_to_exact_iso"
            if policy and derived.text!=field.text:
                events.append((run_id,group_id,source.record_id,column.ordinal,index+1,policy,hashlib.sha256((field.text or "").encode()).hexdigest(),hashlib.sha256((derived.text or "").encode()).hexdigest()))
        fields[index]=derived
    return RecordEnvelope(source.record_id,source.ordinal+offset,source.byte_start,source.byte_end,source.line_start,source.line_end,fields,"data",[])


def _spool_records(journal,config,run_id,group_id,canonical):
    offset=0
    with journal.db:
        journal.db.execute("DELETE FROM group_records WHERE run_id=? AND group_id=?",(run_id,group_id))
        journal.db.execute("DELETE FROM group_record_events WHERE run_id=? AND group_id=?",(run_id,group_id))
    for member in members(journal,run_id,group_id):
        item=journal.file(run_id,member["file_id"]);source_columns=_plan(item["plan"]).columns
        maximum=journal.db.execute("SELECT COALESCE(MAX(ordinal),0) FROM records WHERE run_id=? AND file_id=?",(run_id,member["file_id"])).fetchone()[0]
        with journal.db:journal.db.execute("UPDATE group_members SET ordinal_offset=?,source_max_ordinal=? WHERE run_id=? AND group_id=? AND selection_id=?",(offset,maximum,run_id,group_id,member["selection_id"]))
        prepared=[];projection_events=[];prepared_bytes=0
        def flush_prepared():
            with journal.db:
                journal.db.executemany("INSERT INTO group_records VALUES(?,?,?,?,?,?,?,?)",prepared)
                journal.db.executemany("INSERT OR REPLACE INTO group_record_events VALUES(?,?,?,?,?,?,?,?)",projection_events)
        for source in journal.records(run_id,member["file_id"],"pending"):
            checkpoint();events=[]
            projected=_project(journal,run_id,group_id,source,source_columns,member["mapping"],canonical,offset,events)
            row=(run_id,group_id,projected.record_id,member["file_id"],source.ordinal,projected.ordinal,"pending",json.dumps(asdict(projected),ensure_ascii=True))
            size=len(row[-1])+512*len(events)
            if prepared and prepared_bytes+size>1048576:
                flush_prepared();prepared=[];projection_events=[];prepared_bytes=0
            prepared.append(row);projection_events.extend(events);prepared_bytes+=size;checkpoint(prepared_bytes*2)
            if len(prepared)>=100 or prepared_bytes>=1048576:
                flush_prepared();prepared=[];projection_events=[];prepared_bytes=0
        if prepared:flush_prepared()
        offset+=maximum


def _manifest(journal,config,run_id,group_id):
    from .capture import _atomic_replace,_sync_directory
    import os
    folder=journal.work/"groups"/hashlib.sha256(run_id.encode()).hexdigest()[:32]/group_id
    folder.mkdir(parents=True,exist_ok=True)
    path=folder/"contributors.jsonl";temporary=folder/"contributors.partial.jsonl"
    digest=hashlib.sha256();count=0;complete=True;unresolved=False;source_bytes=0
    with temporary.open("wb") as stream:
        for member in members(journal,run_id,group_id):
            checkpoint();item=journal.file(run_id,member["file_id"]);counts=journal.counts(run_id,member["file_id"])
            accepted=journal.db.execute("SELECT COUNT(*) FROM group_records WHERE run_id=? AND group_id=? AND source_file_id=? AND disposition IN ('pending','committed')",(run_id,group_id,member["file_id"])).fetchone()[0]
            projected_rejects=journal.db.execute("SELECT COUNT(*) FROM group_records WHERE run_id=? AND group_id=? AND source_file_id=? AND disposition='rejected'",(run_id,group_id,member["file_id"])).fetchone()[0]
            complete=complete and not(counts["rejected"] or counts["excluded"] or counts["unresolved"] or projected_rejects)
            unresolved=unresolved or bool(counts["unresolved"])
            entry={"relative_path":member["relative_path"],"sha256":item["artifact"]["sha256"],"size":item["artifact"]["size"],"capture_id":member["file_id"],"records":accepted,"interpretation_hash":item["plan"]["interpretation_hash"],"mapping_hash":item["plan"]["mapping_hash"],"column_mapping":member["mapping"],"ordinal_offset":member["ordinal_offset"],"source_logical_max_ordinal":member["source_max_ordinal"],"artifact_path":item["artifact"]["path"]}
            encoded=json.dumps(entry,ensure_ascii=True,separators=(",",":")).encode()+b"\n"
            if len(encoded)>config["limits"]["max_in_memory_record_bytes"]:raise IngestError("GROUP_MANIFEST_LIMIT","One contributor mapping exceeds the bounded manifest-row limit",2)
            stream.write(encoded);digest.update(encoded);count+=1;source_bytes+=entry["size"]
        stream.flush();os.fsync(stream.fileno())
    _atomic_replace(temporary,path);_sync_directory(folder)
    metadata={"manifest_version":2,"group_id":group_id,"manifest_path":str(path),"manifest_sha256":digest.hexdigest(),"contributor_count":count,"max_line_bytes":min(8388608,config["limits"]["max_in_memory_record_bytes"]),"complete_input":bool(complete),"has_unresolved":unresolved,"source_bytes":source_bytes}
    metadata["source_hash"]=source_manifest_hash(iter_manifest(metadata))
    return metadata


def ledger_hash(journal,run_id,group_id):
    digest=hashlib.sha256(b"db_ingest:group-records:v1\x00")
    for record in records(journal,run_id,group_id):
        value=json.dumps(asdict(record),sort_keys=True,ensure_ascii=True,separators=(",",":")).encode()
        digest.update(len(value).to_bytes(8,"big"));digest.update(value)
    return digest.hexdigest()


def _apply_group_rejections(journal,config,run_id,group_id):
    """Finish sealed projection outcomes idempotently after a local crash."""
    from .pipeline import local_evidence
    for projected in records(journal,run_id,group_id,"rejected"):
        linked=journal.db.execute("SELECT source_file_id FROM group_records WHERE run_id=? AND group_id=? AND record_id=?",(run_id,group_id,projected.record_id)).fetchone()
        stored=journal.db.execute("SELECT payload FROM records WHERE run_id=? AND file_id=? AND record_id=?",(run_id,linked[0],projected.record_id)).fetchone()
        if stored is None:raise IngestError("GROUP_SOURCE_LEDGER_MISSING","A sealed reject lost its original source envelope",5)
        raw=json.loads(stored[0]);raw["fields"]=[FieldValue(**f) for f in raw["fields"]];source=RecordEnvelope(**raw)
        source.errors=[e for e in source.errors if not(e.get("stage")=="merged_projection_validation" and e.get("group_id",group_id)==group_id)]
        source.errors.extend(projected.errors)
        journal.set_disposition(run_id,linked[0],source.record_id,"rejected",source)
    for member in members(journal,run_id,group_id):
        item=journal.file(run_id,member["file_id"])
        counts=local_evidence(journal,run_id,member["file_id"],item["artifact"],item["decision"])
        counts.update({k:v for k,v in item["counts"].items() if k.startswith("plan_artifact") or k in ("publication_resolution","business_prepared","target_review")})
        journal.update_file(run_id,member["file_id"],counts=counts)
    item=group(journal,run_id,group_id);counts={**item["counts"],"validation_applied":True}
    update(journal,run_id,group_id,state="PROFILED",counts=counts,error=None)


def verify_group(journal,run_id,group_id):
    item=group(journal,run_id,group_id)
    if item["plan"] is None or item["manifest"] is None:raise IngestError("GROUP_UNPROFILED","Group has no approved full projection",2)
    if source_manifest_hash(iter_manifest(item["manifest"]))!=item["plan"]["source_hash"]:
        raise IngestError("GROUP_SOURCE_HASH_MISMATCH","Group source manifest differs from its approved schema intent",5)
    if ledger_hash(journal,run_id,group_id)!=item["counts"].get("ledger_sha256"):
        raise IngestError("GROUP_LEDGER_MISMATCH","Derived group records differ from their approved full profile",5)
    from .projections import summary as projection_summary,verify_seal
    actual_projection=projection_summary(journal,run_id,group_id=group_id)
    verify_seal(actual_projection,item["counts"].get("projection_summary"))
    verify_seal(actual_projection,(item.get("receipt") or {}).get("details",{}).get("lossy_projection"))
    from itertools import zip_longest
    for source,member in zip_longest(iter_manifest(item["manifest"]),members(journal,run_id,group_id)):
        if source is None or member is None:raise IngestError("GROUP_MEMBER_MANIFEST_MISMATCH","Contributor selection differs from its approved full manifest",5)
        original=journal.file(run_id,member["file_id"])
        accepted=journal.db.execute("SELECT COUNT(*) FROM group_records WHERE run_id=? AND group_id=? AND source_file_id=? AND disposition IN ('pending','committed')",(run_id,group_id,member["file_id"])).fetchone()[0]
        expected={"relative_path":member["relative_path"],"capture_id":member["file_id"],"sha256":original["artifact"]["sha256"],"size":original["artifact"]["size"],"interpretation_hash":original["plan"]["interpretation_hash"],"column_mapping":member["mapping"],"ordinal_offset":member["ordinal_offset"],"source_logical_max_ordinal":member["source_max_ordinal"],"records":accepted}
        if item["manifest"].get("manifest_version",1)>=2:expected["mapping_hash"]=original["plan"]["mapping_hash"]
        if any(source.get(key)!=value for key,value in expected.items()):raise IngestError("GROUP_MEMBER_MANIFEST_MISMATCH","Contributor source bytes/mapping/counts differ from the approved manifest",5,{"relative_path":member["relative_path"]})
        bad=journal.db.execute("SELECT 1 FROM group_records WHERE run_id=? AND group_id=? AND source_file_id=? AND ordinal<>source_ordinal+? LIMIT 1",(run_id,group_id,member["file_id"],member["ordinal_offset"])).fetchone()
        if bad:raise IngestError("GROUP_SOURCE_ORDINAL_MISMATCH","Merged destination ordinal no longer matches its original source ordinal and offset",5)
    broken=journal.db.execute("SELECT 1 FROM group_records g LEFT JOIN records r ON r.run_id=g.run_id AND r.file_id=g.source_file_id AND r.record_id=g.record_id WHERE g.run_id=? AND g.group_id=? AND (r.record_id IS NULL OR r.ordinal<>g.source_ordinal) LIMIT 1",(run_id,group_id)).fetchone()
    if broken:raise IngestError("GROUP_SOURCE_LINK_MISMATCH","Derived row identity/source ordinal no longer matches its retained source",5)
    if item["counts"].get("validation_applied"):
        mismatch=journal.db.execute("SELECT 1 FROM group_records g JOIN records r ON r.run_id=g.run_id AND r.file_id=g.source_file_id AND r.record_id=g.record_id WHERE g.run_id=? AND g.group_id=? AND g.disposition<>r.disposition LIMIT 1",(run_id,group_id)).fetchone()
        if mismatch:raise IngestError("GROUP_SOURCE_DISPOSITION_MISMATCH","Merged/source identity disposition sets disagree",5)
    path=item["counts"].get("plan_artifact")
    if not path or not item["counts"].get("plan_artifact_sha256"):raise IngestError("GROUP_PLAN_MISSING","Approved group lacks its immutable full plan artifact seal",5)
    if path:
        digest=hashlib.sha256()
        try:
            with Path(path).open("rb") as stream:
                while block:=stream.read(1048576):checkpoint();digest.update(block)
            if digest.hexdigest()!=item["counts"].get("plan_artifact_sha256"):raise IngestError("GROUP_PLAN_MISMATCH","Group plan artifact hash differs from durable metadata",5)
            checkpoint(Path(path).stat().st_size*4)
            saved=json.loads(Path(path).read_text(encoding="utf-8"))
            if not isinstance(saved,dict):raise IngestError("GROUP_PLAN_MISMATCH","Full merged plan document is malformed",5)
            if saved.get("format_version")!=1 or saved.get("run_id")!=run_id or saved.get("file_id")!="group:"+group_id:raise IngestError("GROUP_PLAN_IDENTITY_MISMATCH","Full merged plan belongs to another group/run identity",5)
            expected_source={"kind":"contributor_manifest","path":item["manifest"]["manifest_path"],"sha256":item["manifest"]["manifest_sha256"],"size":Path(item["manifest"]["manifest_path"]).stat().st_size}
            if stable_hash(saved.get("source_artifact"))!=stable_hash(expected_source):raise IngestError("GROUP_PLAN_SOURCE_MISMATCH","Merged plan is bound to another contributor manifest",5)
            if stable_hash(saved["plan"])!=stable_hash(item["plan"]):raise IngestError("GROUP_PLAN_MISMATCH","Journal group schema differs from its approved plan artifact",5)
        except (OSError,ValueError,TypeError,KeyError) as error:raise IngestError("GROUP_PLAN_MISMATCH","Full merged plan artifact is unavailable or malformed: "+str(error),5) from error
    return item


def _profile_one(payload):
    from .pipeline import local_evidence,_write_plan_artifact
    config,run_id,gid=payload["config"],payload["run_id"],payload["group_id"]
    journal=Journal(config["preservation"]["work_directory"]);initialize(journal)
    lease=journal.lease("group:"+run_id+":"+gid);lease.__enter__()
    try:
        journal.event(run_id,None,"GROUP_WORKER_STARTED",{"group_id":gid,"phase":"profile"})
        item=group(journal,run_id,gid)
        if item["receipt"] is not None or item["state"] in ("PUBLISHING","COMMIT_OUTCOME_UNKNOWN","COMMITTED","RECONCILED"):
            return {"group_id":gid,"exit_code":0,"state":item["state"]}
        if item["plan"] is not None:
            verify_group(journal,run_id,gid)
            if not item["counts"].get("validation_applied"):_apply_group_rejections(journal,config,run_id,gid)
            if item["state"]=="NEEDS_DECISION" and item.get("error"):
                return {"group_id":gid,"exit_code":item["error"]["exit_code"],"state":item["state"]}
            update(journal,run_id,gid,state="PROFILED",error=None)
            return {"group_id":gid,"exit_code":0,"state":"PROFILED"}
        canonical=_canonical_columns(journal,config,run_id,gid)
        if not canonical:raise IngestError("GROUP_NO_COLUMNS","Merged input has no declared relational columns")
        _spool_records(journal,config,run_id,gid,canonical)
        profile_config=copy.deepcopy(config);profile_config["columns"]=[]
        policy=config["mapping"]["directory_merge_schema"]
        for index,entry in enumerate(canonical,1):
            target=entry["label"] if entry["explicit_name"] else (identifier(entry["label"],"group-column:"+entry["key"]) if policy=="union_by_name" else column_name(entry["label"],index))
            override={"source_ordinal":index,"target_name":target}
            if entry["type"] is not None:override["target_type"]=entry["type"]
            if entry["nullable_override"] is not None:override["nullable"]=entry["nullable_override"]
            profile_config["columns"].append(override)
        columns=profile_records(records(journal,run_id,gid),[entry["label"] for entry in canonical],profile_config)
        for column,entry in zip(columns,canonical,strict=True):
            if entry.get("force_text"):
                import math
                length=max(1,math.ceil(column.stats["max_utf16_units"]*(1+config["schema"].get("text_length_headroom_percent",20)/100)))
                if length>4000 and not config["schema"].get("allow_max_types",True):
                    if column.stats["max_utf16_units"]<=4000:length=4000
                    else:raise IngestError("LENGTH_POLICY_LIMIT","Merged lexical fallback requires NVARCHAR(MAX), but MAX is disabled")
                column.sql_type=f"nvarchar({length})" if length<=4000 else "nvarchar(max)"
                column.stats.update(source_declared_type=entry["policy_original"],length_policy="retype_text",length_policy_representation="contributor_lexical_with_captured_attachment_base64",length_policy_scope="merged contributor projection")
            elif entry["binary"]:column.stats["binary_encoding"]="base64"
            if config["schema"]["on_length_overflow"]=="widen" and entry.get("policy_original") and column.sql_type!=entry["policy_original"]:
                column.stats.update(source_declared_type=entry["policy_original"],length_policy="widen",length_policy_representation="declared_exact_value",length_policy_scope="merged contributor projection")
            if entry["varchar_policy"]:
                column.stats["collation"],column.stats["text_encoding"]=entry["varchar_policy"]
            if policy=="union_by_name":column.stats["missing_policy"]="null_with_provenance"
            column.stats["group_key"]=entry["key"]
            if entry.get("historical"):
                from .adapters.mssql.evolution import compatible
                old=ColumnProfile(**entry["historical"])
                column.target_name=old.target_name
                # A selected subset must not infer a narrower replacement schema.
                old_spec=parse_sql_type(old.sql_type);new_spec=parse_sql_type(column.sql_type)
                preserve=compatible(column,old)
                # A smaller selected input may infer a different narrower family:
                # lexical values still fit acknowledged text, or integers fit an
                # acknowledged exact decimal. This is a storage-preserving choice.
                if entry["type"] is None:
                    if old_spec.base=="nvarchar" and (old_spec.length=="max" or column.stats["max_utf16_units"]<=int(old_spec.length)):preserve=True
                    if old_spec.base=="decimal" and new_spec.base in ("tinyint","smallint","int","bigint","decimal") and column.stats["max_scale"]<=old_spec.scale and column.stats["max_integer_digits"]<=old_spec.precision-old_spec.scale:preserve=True
                    if not column.stats["value_count"] and not column.stats["empty_count"]:preserve=True
                if preserve:
                    column.sql_type=old.sql_type;column.nullable=old.nullable or column.nullable
                    for key in ("collation","text_encoding"):
                        if key in old.stats:column.stats[key]=old.stats[key]
        target=item["target"]
        policy_hash=hashlib.sha256(b"db_ingest:group-interpretations:v1\x00")
        for member in members(journal,run_id,gid):
            source=journal.file(run_id,member["file_id"])
            for value in (member["relative_path"],source["plan"]["interpretation_hash"]):
                raw=value.encode();policy_hash.update(len(raw).to_bytes(8,"big"));policy_hash.update(raw)
        interpretation=stable_hash({"members":policy_hash.hexdigest(),"merge_policy":policy,"version":1})
        mapping=stable_hash({"configuration":hashes(config)["mapping"],"target":target,"columns":[(c.original_name,c.target_name) for c in columns]})
        plan=SchemaPlan(target["database"],target["schema"],target["table"],columns,"0"*64,interpretation,mapping)
        from .staging import validate_record
        from .adapters.mssql.metadata import validate_plan
        validate_plan(plan)
        for record in records(journal,run_id,gid,"pending"):
            errors=validate_record(plan,record)
            if not errors:continue
            linked=journal.db.execute("SELECT source_file_id,source_ordinal FROM group_records WHERE run_id=? AND group_id=? AND record_id=?",(run_id,gid,record.record_id)).fetchone()
            member=journal.db.execute("SELECT mapping FROM group_members WHERE run_id=? AND group_id=? AND file_id=?",(run_id,gid,linked[0])).fetchone()
            source_mapping=json.loads(member[0]);inverse={group_index+1:source_index+1 for source_index,group_index in enumerate(source_mapping)}
            original_columns=_plan(journal.file(run_id,linked[0])["plan"]).columns
            for error in errors:
                error["group_column_ordinal"]=error.get("column_ordinal")
                error["group_original_name"]=error.get("original_name",error.get("column_name"))
                error["column_ordinal"]=inverse.get(error.get("column_ordinal"))
                source_ordinal=error["column_ordinal"]
                original=original_columns[source_ordinal-1].original_name if source_ordinal is not None else None
                error["original_name"]=original;error["column_name"]=original
                error["stage"]="merged_projection_validation";error["group_id"]=gid
            record.errors.extend(errors);record.kind="rejected"
            with journal.db:journal.db.execute("UPDATE group_records SET disposition='rejected',payload=? WHERE run_id=? AND group_id=? AND record_id=?",(json.dumps(asdict(record),ensure_ascii=True),run_id,gid,record.record_id))
        manifest=_manifest(journal,config,run_id,gid);plan.source_hash=manifest["source_hash"]
        counts=group_counts(journal,run_id,gid);counts["ledger_sha256"]=ledger_hash(journal,run_id,gid)
        from .projections import summary as projection_summary
        counts["projection_summary"]=projection_summary(journal,run_id,group_id=gid)
        counts.update(_write_plan_artifact(journal,run_id,"group:"+gid,plan,{"kind":"contributor_manifest","path":manifest["manifest_path"],"sha256":manifest["manifest_sha256"],"size":Path(manifest["manifest_path"]).stat().st_size}))
        counts["validation_applied"]=False
        update(journal,run_id,gid,state="APPLYING_VALIDATION",plan=asdict(plan),manifest=manifest,counts=counts,error=None)
        _apply_group_rejections(journal,config,run_id,gid)
        return {"group_id":gid,"exit_code":0,"state":"PROFILED","source_bytes":manifest["source_bytes"],"processed_records":counts["pending"]+counts["rejected"]}
    except IngestError as error:
        journal.flush();journal.incident(run_id,None,error)
        update(journal,run_id,gid,state="NEEDS_DECISION" if error.exit_code==2 else "FAILED",error={"code":error.code,"message":str(error),"exit_code":error.exit_code,"context":error.context})
        return {"group_id":gid,"exit_code":error.exit_code,"state":"FAILED"}
    finally:
        journal.event(run_id,None,"GROUP_WORKER_FINISHED",{"group_id":gid,"phase":"profile"})
        lease.__exit__(None,None,None);journal.close()


def prepare_groups(journal,config,run_id):
    from .scheduling import bounded_map
    from .pipeline import SeverityAccumulator
    initialize(journal);codes=SeverityAccumulator();metrics={}
    for selected in journal.selections(run_id,selected_only=False):
        if selected["status"] not in ("selected","unreadable"):continue
        try:target=map_destination(config,selected["relative_path"])
        except IngestError as error:journal.incident(run_id,selected.get("file_id"),error);codes.append(error.exit_code);continue
        target={k:target[k] for k in ("database","schema","table")};gid=_group_id(config,target)
        with journal.db:
            journal.db.execute("INSERT OR IGNORE INTO groups(run_id,group_id,intent_id,state,target) VALUES(?,?,?,'DISCOVERED',?)",(run_id,gid,uuid.uuid4().hex,json.dumps(target)))
            journal.db.execute("INSERT INTO group_members(run_id,group_id,selection_id,file_id,relative_path) VALUES(?,?,?,?,?) ON CONFLICT(run_id,group_id,selection_id) DO UPDATE SET file_id=excluded.file_id",(run_id,gid,selected["selection_id"],selected.get("file_id"),selected["relative_path"]))
    def arguments():
        for item in groups(journal,run_id):yield {"config":config,"run_id":run_id,"group_id":item["group_id"]}
    for argument,outcome in bounded_map(config,_profile_one,arguments(),metrics):
        if outcome["ok"]:codes.append(outcome["value"]["exit_code"])
        else:
            error=outcome["error"];journal.incident(run_id,None,IngestError(error["code"],error["message"],error["exit_code"],error["context"]))
            update(journal,run_id,argument["group_id"],state="FAILED",error=error);codes.append(error["exit_code"])
    return codes,metrics


def report_groups(journal,run_id,limit=100,max_bytes=8388608):
    result=[];used=0
    for item in groups(journal,run_id):
        size=sum(len(v) for v in json.JSONEncoder(ensure_ascii=True).iterencode(item))
        if len(result)>=limit or used+size>max_bytes:break
        result.append(item);used+=size
    return result
