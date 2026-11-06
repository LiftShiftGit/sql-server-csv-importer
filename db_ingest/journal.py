"""Local durable journal. Artifacts and row envelopes remain replayable offline."""
from contextlib import contextmanager
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
from datetime import datetime, timezone
from .models import RecordEnvelope, FieldValue
from .errors import IngestError

import threading
_LEASES={}
_LEASE_GUARD=threading.RLock()

def utcnow():return datetime.now(timezone.utc).isoformat()

class Journal:
    SCHEMA_VERSION=5
    def __init__(self,work):
        from .windows_paths import validate_local_work
        validate_local_work(work)
        self.work=Path(work).resolve();self.work.mkdir(parents=True,exist_ok=True)
        self.db=None
        # WAL negotiation and schema migration require a process-wide OS lease,
        # not just SQLite's connection busy timeout. Workers skip DDL once known.
        try:
            with self.lease("journal-bootstrap",timeout=30):
                self.db=sqlite3.connect(self.work/"journal.sqlite3",timeout=30)
                self.db.row_factory=sqlite3.Row
                self.db.execute("PRAGMA busy_timeout=30000")
                exists=self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='versions'").fetchone()
                version=(self.db.execute("SELECT COALESCE(MAX(version),0) FROM versions").fetchone()[0] if exists else 0)
                if version>self.SCHEMA_VERSION:
                    raise IngestError("JOURNAL_VERSION_UNSUPPORTED","Local journal schema is newer than this application",2,{"observed_version":version,"supported_version":self.SCHEMA_VERSION})
                mode=self.db.execute("PRAGMA journal_mode").fetchone()[0]
                if mode.lower()!="wal":
                    mode=self.db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                    if mode.lower()!="wal":raise IngestError("JOURNAL_MODE_UNAVAILABLE","A durable local WAL journal could not be established",4)
                self.db.execute("PRAGMA synchronous=FULL")
                self.db.execute("PRAGMA foreign_keys=ON")
                self.db.execute("PRAGMA temp_store=FILE")
                if version<self.SCHEMA_VERSION:
                    self.db.executescript("""
                    CREATE TABLE IF NOT EXISTS versions(version INTEGER PRIMARY KEY); INSERT OR IGNORE INTO versions VALUES(1);
                    CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY,state TEXT NOT NULL,created TEXT NOT NULL,updated TEXT NOT NULL,config TEXT NOT NULL,linked_run TEXT,exit_code INTEGER NOT NULL DEFAULT 4);
                    CREATE TABLE IF NOT EXISTS files(run_id TEXT NOT NULL,file_id TEXT NOT NULL,intent_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,artifact TEXT NOT NULL,decision TEXT,plan TEXT,counts TEXT NOT NULL DEFAULT '{}',receipt TEXT, PRIMARY KEY(run_id,file_id));
                    CREATE TABLE IF NOT EXISTS records(run_id TEXT NOT NULL,file_id TEXT NOT NULL,record_id TEXT NOT NULL,ordinal INTEGER NOT NULL,kind TEXT NOT NULL,disposition TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(run_id,file_id,record_id));
                    CREATE TABLE IF NOT EXISTS incidents(id INTEGER PRIMARY KEY,run_id TEXT,file_id TEXT,code TEXT NOT NULL,message TEXT NOT NULL,context TEXT NOT NULL,created TEXT NOT NULL);
                    CREATE INDEX IF NOT EXISTS source_hashes ON files(state);
                    CREATE INDEX IF NOT EXISTS record_order ON records(run_id,file_id,ordinal);
                    CREATE TABLE IF NOT EXISTS selections(run_id TEXT NOT NULL,selection_id TEXT NOT NULL,path TEXT NOT NULL,relative_path TEXT NOT NULL,status TEXT NOT NULL,reason TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'DISCOVERED',file_id TEXT,intent_id TEXT NOT NULL,artifact TEXT,error TEXT,inventory_seen INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(run_id,selection_id),UNIQUE(run_id,path));
                    CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,run_id TEXT NOT NULL,file_id TEXT,category TEXT NOT NULL,pid INTEGER NOT NULL,created TEXT NOT NULL,details TEXT NOT NULL);
                    """)
                    # Additive local metadata migration, preserving existing artifacts and receipts.
                    if "failures" not in {row[1] for row in self.db.execute("PRAGMA table_info(files)")}:
                        with self.db:self.db.execute("ALTER TABLE files ADD COLUMN failures TEXT NOT NULL DEFAULT '{}'")
                    if "inventory_seen" not in {row[1] for row in self.db.execute("PRAGMA table_info(selections)")}:
                        with self.db:self.db.execute("ALTER TABLE selections ADD COLUMN inventory_seen INTEGER NOT NULL DEFAULT 1")
                    columns={row[1] for row in self.db.execute("PRAGMA table_info(runs)")}
                    for name,definition in (("inventory_state", "TEXT NOT NULL DEFAULT 'LEGACY'"), ("planning_new_intent", "INTEGER NOT NULL DEFAULT 0"), ("execution_policy", "TEXT NOT NULL DEFAULT '{}'")):
                        if name not in columns:
                            with self.db:self.db.execute("ALTER TABLE runs ADD COLUMN " + name + " " + definition)
                    with self.db:self.db.execute("INSERT OR IGNORE INTO versions VALUES(?)",(self.SCHEMA_VERSION,))
                else:
                    required={"runs":{"inventory_state","planning_new_intent","execution_policy"},"files":{"failures"},"selections":{"inventory_seen"}}
                    for table,names in required.items():
                        actual={row[1] for row in self.db.execute("PRAGMA table_info("+table+")")}
                        if not names<=actual:raise IngestError("JOURNAL_SCHEMA_MISMATCH","Journal version marker disagrees with its required columns",5,{"table":table,"missing_columns":sorted(names-actual)})
        except (sqlite3.Error,OSError) as error:
            if self.db is not None:self.db.close()
            raise IngestError("JOURNAL_INITIALIZATION","Local journal initialization failed: "+str(error),4,{"sqlite_errorcode":getattr(error,"sqlite_errorcode",None),"sqlite_errorname":getattr(error,"sqlite_errorname",None),"work_directory":str(self.work)}) from error
        except BaseException:
            if self.db is not None:self.db.close()
            raise
    def close(self):self.db.close()
    def run(self,run_id):
        row=self.db.execute("SELECT * FROM runs WHERE run_id=?",(run_id,)).fetchone()
        if row is None:raise IngestError("RUN_UNKNOWN",f"Unknown run {run_id}")
        return dict(row)
    def create_run(self,run_id,config,linked_run=None):
        now=utcnow()
        with self.db:self.db.execute("INSERT INTO runs(run_id,state,created,updated,config,linked_run,exit_code,inventory_state) VALUES(?,?,?,?,?,?,4,'OPEN')",(run_id,"DISCOVERED",now,now,json.dumps(config),linked_run))
    def state(self,run_id,state,exit_code=4):
        with self.db:self.db.execute("UPDATE runs SET state=?,updated=?,exit_code=? WHERE run_id=?",(state,utcnow(),exit_code,run_id))
    def add_file(self,run_id,artifact,intent_id):
        with self.db:self.db.execute("INSERT INTO files(run_id,file_id,intent_id,state,artifact) VALUES(?,?,?,'CAPTURED',?)",(run_id,artifact.capture_id,intent_id,json.dumps(asdict(artifact))))
    def update_file(self,run_id,file_id,**values):
        allowed={"state","decision","plan","counts","receipt","failures"}
        if set(values)-allowed:raise ValueError("Unknown file journal field")
        fields=",".join(k+"=?" for k in values)
        encoded=[v if k=="state" else json.dumps(v) for k,v in values.items()]
        with self.db:self.db.execute(f"UPDATE files SET {fields} WHERE run_id=? AND file_id=?",(*encoded,run_id,file_id))
    def files(self,run_id):
        last=0
        while True:
            row=self.db.execute("SELECT rowid AS _rowid,* FROM files WHERE run_id=? AND rowid>? ORDER BY rowid LIMIT 1",(run_id,last)).fetchone()
            if row is None:break
            d=dict(row);last=d.pop("_rowid")
            for k in ("artifact","decision","plan","counts","receipt","failures"):
                d[k]=json.loads(d[k]) if d[k] else None
            yield d
    def file(self,run_id,file_id):
        row=self.db.execute("SELECT * FROM files WHERE run_id=? AND file_id=?",(run_id,file_id)).fetchone()
        if row is None:raise IngestError("FILE_UNKNOWN","Unknown captured file in journal")
        d=dict(row)
        for k in ("artifact","decision","plan","counts","receipt","failures"):
            d[k]=json.loads(d[k]) if d[k] else None
        return d
    def report_files(self,run_id,limit=1000,max_bytes=16777216):
        """Bound both entry count and encoded metadata size before loading plans."""
        result=[];last=0;used=0
        while len(result)<limit:
            from .resources import checkpoint
            checkpoint()
            remaining=max_bytes-used
            row=self.db.execute("SELECT rowid AS _rowid,run_id,file_id,intent_id,state,artifact,decision,counts,receipt,failures,CASE WHEN COALESCE(length(plan),0)<=? THEN plan ELSE NULL END AS plan,COALESCE(length(plan),0) AS _plan_size FROM files WHERE run_id=? AND rowid>? ORDER BY rowid LIMIT 1",(remaining,run_id,last)).fetchone()
            if row is None:break
            item=dict(row);last=item.pop("_rowid");plan_size=item.pop("_plan_size")
            for key in ("artifact","decision","plan","counts","receipt","failures"):
                item[key]=json.loads(item[key]) if item[key] else None
            if plan_size and item["plan"] is None:
                item["plan_omitted_from_report"]=True
                item["plan_artifact"]=(item.get("counts") or {}).get("plan_artifact")
            size=sum(len(chunk) for chunk in json.JSONEncoder(ensure_ascii=True).iterencode(item))
            if size>remaining:
                if result:break
                # Preserve stable identities and disposition summary. Full evidence
                # remains in SQLite and the full plan artifact, never discarded.
                item={k:item[k] for k in ("run_id","file_id","intent_id","state","counts")}
                item["details_omitted_from_report"]=True
                size=sum(len(chunk) for chunk in json.JSONEncoder(ensure_ascii=True).iterencode(item))
                if size>remaining:raise IngestError("REPORT_BUDGET","One minimal file summary exceeds the report byte budget",4)
            result.append(item);used+=size
        return result
    @staticmethod
    def prepare_record(run_id,file_id,record):
        from .errors import complete_record_errors
        disposition={"data":"pending","header":"header","blank":"rejected","rejected":"rejected","unresolved":"unresolved","excluded":"excluded"}.get(record.kind,"pending")
        complete_record_errors(record,stage="source_parse",disposition=disposition)
        return (run_id,file_id,record.record_id,record.ordinal,record.kind,disposition,json.dumps(asdict(record),ensure_ascii=True))
    def save_record(self,run_id,file_id,record):
        self.db.execute("INSERT OR REPLACE INTO records VALUES(?,?,?,?,?,?,?)",self.prepare_record(run_id,file_id,record))
    def save_prepared_records(self,rows):
        # Callers prepare a bounded concrete list BEFORE opening a write TX.
        # Never accept a lazy parser iterator that would frame while holding WAL.
        if not isinstance(rows,list):raise TypeError("Prepared record batch must be a bounded concrete list")
        with self.db:self.db.executemany("INSERT OR REPLACE INTO records VALUES(?,?,?,?,?,?,?)",rows)
    def flush(self):self.db.commit()
    def records(self,run_id,file_id,disposition=None):
        # Separate read snapshot avoids upgrading a long-lived SELECT to a writer
        # while another file worker commits unrelated records to this WAL journal.
        self.flush()
        reader=sqlite3.connect(self.work/"journal.sqlite3",timeout=30)
        try:
            sql="SELECT payload,disposition,record_id,ordinal,kind FROM records WHERE run_id=? AND file_id=?";params=[run_id,file_id]
            if disposition:sql+=" AND disposition=?";params.append(disposition)
            sql+=" ORDER BY ordinal"
            for row in reader.execute(sql,params):
                from .resources import checkpoint
                checkpoint()
                try:
                    r=json.loads(row[0])
                    if not isinstance(r,dict) or not isinstance(r.get("fields"),list) or not isinstance(r.get("errors"),list) or not all(isinstance(e,dict) for e in r["errors"]):raise ValueError("Invalid record envelope structure")
                    if not all(k in r for k in ("record_id","ordinal","kind")):raise ValueError("Missing record identity")
                except (ValueError,TypeError) as error:raise IngestError("LEDGER_PAYLOAD_INVALID","Retained record envelope is malformed",5,{"record_id":row[2]}) from error
                disposition=row[1]
                if disposition not in {"pending","committed","rejected","excluded","header","unresolved"}:
                    raise IngestError("DISPOSITION_INVALID","Retained record has an unknown disposition",5,{"record_id":row[2],"disposition_preview":str(disposition)[:128]})
                if (r["record_id"],r["ordinal"],r["kind"])!=(row[2],row[3],row[4]):raise IngestError("LEDGER_IDENTITY_MISMATCH","Record payload identity/order/kind differs from its journal index",5,{"record_id":row[2]})
                kind,errors=r["kind"],r["errors"]
                valid=(disposition=="pending" and kind=="data" or disposition=="committed" and kind=="data" and not errors or disposition=="header" and kind=="header" or disposition=="unresolved" and kind=="unresolved" or disposition=="excluded" and kind=="excluded" or disposition=="rejected" and kind in ("data","rejected","blank") and bool(errors))
                if not valid:raise IngestError("DISPOSITION_INVALID","Retained disposition conflicts with its source envelope/rejection evidence",5,{"record_id":row[2],"disposition":disposition,"kind":kind})
                try:
                    r["fields"]=[FieldValue(**f) for f in r["fields"]];record=RecordEnvelope(**r)
                except (TypeError,ValueError) as error:raise IngestError("LEDGER_PAYLOAD_INVALID","Retained fields/envelope do not match the supported model",5,{"record_id":row[2]}) from error
                yield record
        finally:reader.close()
    def set_disposition(self,run_id,file_id,record_id,disposition,payload=None):
        if disposition not in {"pending","committed","rejected","excluded","header","unresolved"}:raise IngestError("DISPOSITION_INVALID","Unknown requested record disposition",5,{"disposition_preview":str(disposition)[:128]})
        encoded=None
        if payload is not None:
            from .errors import complete_record_errors
            complete_record_errors(payload,disposition=disposition)
            encoded=json.dumps(asdict(payload))
        with self.db:
            self.db.execute("UPDATE records SET disposition=? WHERE run_id=? AND file_id=? AND record_id=?",(disposition,run_id,file_id,record_id))
            if encoded is not None:self.db.execute("UPDATE records SET payload=? WHERE run_id=? AND file_id=? AND record_id=?",(encoded,run_id,file_id,record_id))
    def committed(self,run_id,file_id):
        with self.db:
            self.db.execute("UPDATE records SET disposition='committed' WHERE run_id=? AND file_id=? AND disposition='pending'",(run_id,file_id))
            row=self.db.execute("SELECT counts,receipt FROM files WHERE run_id=? AND file_id=?",(run_id,file_id)).fetchone()
            if row and row[1]:
                receipt=json.loads(row[1]);counts=json.loads(row[0])
                counts.update(pending=0,committed=receipt["committed_records"],staged_records=receipt["committed_records"],validated_records=receipt["committed_records"],sql_staged_records=receipt["committed_records"],sql_validated_records=receipt["committed_records"],sql_stage_metric_basis="acknowledged publication receipt",accounting="all known records disposed")
                self.db.execute("UPDATE files SET counts=? WHERE run_id=? AND file_id=?",(json.dumps(counts),run_id,file_id))
    def counts(self,run_id,file_id):
        d={k:0 for k in ("pending","committed","rejected","excluded","header","unresolved")}
        for disposition,count in self.db.execute("SELECT disposition,COUNT(*) FROM records WHERE run_id=? AND file_id=? GROUP BY disposition",(run_id,file_id)):
            if disposition not in d:raise IngestError("DISPOSITION_INVALID","Retained record has an unknown disposition",5,{"disposition_preview":str(disposition)[:128]})
            d[disposition]=count
        return d
    def incident(self,run_id,file_id,error):
        with self.db:self.db.execute("INSERT INTO incidents(run_id,file_id,code,message,context,created) VALUES(?,?,?,?,?,?)",(run_id,file_id,error.code,str(error),json.dumps(error.context),utcnow()))
    def errors(self,run_id=None,limit=100,max_bytes=1048576):
        """Bounded console previews; complete incident JSON remains in SQLite."""
        limit=max(1,min(int(limit),1000));budget=max(0,min(1048576,int(max_bytes)))
        row=self.db.execute("SELECT CASE WHEN json_valid(config) THEN json_extract(config,'$.limits.max_queue_bytes') ELSE NULL END FROM runs WHERE run_id=?",(run_id,)).fetchone() if run_id is not None else None
        if row and isinstance(row[0],int) and not isinstance(row[0],bool) and row[0]>0:budget=min(budget,row[0])
        result=[];used=2;last=None;truncated=False
        while len(result)<limit:
            remaining=max(0,budget-used-64)
            if remaining<256:truncated=True;break
            context_budget=max(0,remaining-1536)
            message_cap=min(4096,max(32,remaining//24))
            sql="SELECT id,substr(file_id,1,128),substr(code,1,128),substr(message,1,?),length(message),CASE WHEN length(CAST(context AS BLOB))<=? THEN context ELSE NULL END,length(CAST(context AS BLOB)),substr(created,1,96),run_id FROM incidents WHERE 1=1"
            parameters=[message_cap,context_budget]
            if run_id is not None:sql+=" AND run_id=?";parameters.append(run_id)
            if last is not None:sql+=" AND id<?";parameters.append(last)
            row=self.db.execute(sql+" ORDER BY id DESC LIMIT 1",parameters).fetchone()
            if row is None:break
            last=row[0];item={"incident_id":row[0],"run_id":row[8],"file_id":row[1],"code":row[2],"message":row[3],"created":row[7]}
            if row[4]>len(row[3]):item["message_truncated"]=True;item["message_characters"]=row[4]
            if row[5] is not None:
                try:item["context"]=json.loads(row[5])
                except (ValueError,TypeError,RecursionError):item["context_invalid"]=True;item["context"]={"retained_in_journal":True,"incident_id":row[0]};item["context_truncated"]=True
            else:
                item["context"]={"retained_in_journal":True,"incident_id":row[0],"field":"incidents.context"}
                item["context_truncated"]=True;item["context_bytes"]=row[6]
            size=len(json.dumps(item,ensure_ascii=True).encode("ascii"))
            if size>remaining:
                item["context"]={"retained_in_journal":True,"incident_id":row[0],"field":"incidents.context"};item["context_truncated"]=True;item["context_bytes"]=row[6]
                size=len(json.dumps(item,ensure_ascii=True).encode("ascii"))
            while size>remaining and item["message"]:
                item["message"]=item["message"][:len(item["message"])//2];item["message_truncated"]=True;item["message_characters"]=row[4]
                size=len(json.dumps(item,ensure_ascii=True).encode("ascii"))
            if size>remaining:truncated=True;break
            result.append(item);used+=size+2
        more=self.db.execute("SELECT 1 FROM incidents WHERE "+("run_id=? AND " if run_id is not None else "")+"id<? LIMIT 1",(run_id,last) if run_id is not None else (last,)).fetchone() if result else None
        if result and (truncated or more):result[-1]["additional_incidents_retained"]=True
        return result
    def history(self,source_hash,interpretation_hash,mapping_hash):
        for row in self.db.execute("SELECT intent_id,run_id,artifact,plan,state FROM files WHERE receipt IS NOT NULL"):
            artifact=json.loads(row[2]);plan=json.loads(row[3])
            if artifact["sha256"]==source_hash and plan["interpretation_hash"]==interpretation_hash and plan["mapping_hash"]==mapping_hash:yield {"intent_id":row[0],"run_id":row[1],"state":row[4]}

    def event(self,run_id,file_id,category,details=None):
        import os
        with self.db:self.db.execute("INSERT INTO events(run_id,file_id,category,pid,created,details) VALUES(?,?,?,?,?,?)",(run_id,file_id,category,os.getpid(),utcnow(),json.dumps(details or {})))
    def events(self,run_id,limit=1000):
        return [dict(r) for r in self.db.execute("SELECT * FROM events WHERE run_id=? ORDER BY id DESC LIMIT ?",(run_id,min(limit,1000)))]
    def begin_inventory(self,run_id,new_intent=False):
        with self.db:
            self.db.execute("UPDATE selections SET inventory_seen=0 WHERE run_id=?",(run_id,))
            self.db.execute("UPDATE runs SET inventory_state='OPEN',planning_new_intent=?,updated=? WHERE run_id=?",(int(new_intent),utcnow(),run_id))
    def add_selection(self,run_id,path,relative,status,reason,artifact=None):
        import uuid
        retained=self.db.execute("SELECT selection_id,intent_id,status FROM selections WHERE run_id=? AND path=?",(run_id,str(path))).fetchone()
        if retained:
            if retained[2]=="selected" and status!="selected":
                reason="Previously selected input now "+status+": "+reason
                status="selected"
            self.db.execute("UPDATE selections SET inventory_seen=1,relative_path=?,status=?,reason=? WHERE run_id=? AND selection_id=?",(relative,status,reason,run_id,retained[0]))
            return retained[0]
        existing=self.db.execute("SELECT file_id,intent_id FROM files WHERE run_id=? AND json_extract(artifact,'$.original_path')=?",(run_id,str(path))).fetchone()
        selection_id=uuid.uuid4().hex
        intent_id=existing[1] if existing else uuid.uuid4().hex
        self.db.execute("INSERT INTO selections(run_id,selection_id,path,relative_path,status,reason,state,file_id,intent_id,artifact,inventory_seen) VALUES(?,?,?,?,?,?,?,?,?,?,1)",(run_id,selection_id,str(path),relative,status,reason,"CAPTURED" if artifact or existing else "DISCOVERED",existing[0] if existing else (artifact or {}).get("capture_id"),intent_id,json.dumps(artifact) if artifact else None))
        return selection_id
    def complete_inventory(self,run_id):
        with self.db:
            self.db.execute("UPDATE selections SET reason='Previously selected source was not observed during resumed inventory; retained as a required selection' WHERE run_id=? AND inventory_seen=0 AND status='selected'",(run_id,))
            self.db.execute("UPDATE runs SET inventory_state='COMPLETE',updated=? WHERE run_id=?",(utcnow(),run_id))
    def selections(self,run_id,selected_only=True):
        last=0
        while True:
            sql="SELECT rowid AS _rowid,* FROM selections WHERE run_id=? AND rowid>?"
            if selected_only:sql+=" AND status='selected'"
            row=self.db.execute(sql+" ORDER BY rowid LIMIT 1",(run_id,last)).fetchone()
            if row is None:break
            item=dict(row);last=item.pop("_rowid")
            item["artifact"]=json.loads(item["artifact"]) if item["artifact"] else None
            item["error"]=json.loads(item["error"]) if item["error"] else None
            yield item
    def selection(self,run_id,selection_id):
        row=self.db.execute("SELECT * FROM selections WHERE run_id=? AND selection_id=?",(run_id,selection_id)).fetchone()
        if row is None:raise IngestError("SELECTION_UNKNOWN","Unknown inventory selection")
        item=dict(row)
        for key in ("artifact","error"):item[key]=json.loads(item[key]) if item[key] else None
        return item
    def update_selection(self,run_id,selection_id,**values):
        if set(values)-{"state","file_id","artifact","error"}:raise ValueError("Unknown selection field")
        fields=",".join(k+"=?" for k in values)
        encoded=[json.dumps(v) if k in ("artifact","error") else v for k,v in values.items()]
        with self.db:self.db.execute("UPDATE selections SET "+fields+" WHERE run_id=? AND selection_id=?",(*encoded,run_id,selection_id))
    def reset_unpublished_profile(self,run_id,file_id):
        item=self.file(run_id,file_id)
        if item["receipt"] is not None or item["state"] in ("PUBLISHING","COMMIT_OUTCOME_UNKNOWN","COMMITTED","RECONCILED") or self.counts(run_id,file_id)["committed"]:
            raise IngestError("PROFILE_RECOVERY_UNSAFE","Publication evidence exists; resolve receipts instead of rebuilding the source ledger",5)
        with self.db:
            self.db.execute("DELETE FROM records WHERE run_id=? AND file_id=?",(run_id,file_id))
            self.db.execute("UPDATE files SET state='CAPTURED',decision=NULL,plan=NULL,counts='{}' WHERE run_id=? AND file_id=?",(run_id,file_id))
    def policy(self,run_id,policy):
        with self.db:self.db.execute("UPDATE runs SET execution_policy=? WHERE run_id=?",(json.dumps(policy),run_id))

    @contextmanager
    def lease(self,key,timeout=0):
        """OS-released bounded lease; process death cannot leave a stale lock."""
        import hashlib
        import os
        import threading
        import time
        folder=self.work/"locks";folder.mkdir(exist_ok=True)
        identity=str(folder/hashlib.sha256(key.encode()).hexdigest())
        owner=threading.get_ident();deadline=time.monotonic()+timeout
        while True:
            acquired=False
            with _LEASE_GUARD:
                held=_LEASES.get(identity)
                if held and held[0]==owner:
                    held[1]+=1;acquired=True
                elif not held:
                    handle=open(identity,"a+b",buffering=0)
                    if handle.seek(0,2)==0:handle.write(b"L")
                    handle.seek(0)
                    try:
                        if os.name=="nt":
                            import msvcrt
                            msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
                        else:
                            import fcntl
                            fcntl.lockf(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB,1)
                    except OSError:
                        handle.close()
                    else:
                        _LEASES[identity]=[owner,1,handle];acquired=True
            if acquired:break
            if time.monotonic()>=deadline:
                raise IngestError("RUN_BUSY","Another live process/thread owns this bounded journal/import lease",4,{"lease":key,"timeout_seconds":timeout})
            time.sleep(min(0.025,max(0,deadline-time.monotonic())))
        try:yield
        finally:
            with _LEASE_GUARD:
                held=_LEASES[identity];held[1]-=1
                if held[1]==0:
                    handle=held[2];handle.seek(0)
                    if os.name=="nt":
                        import msvcrt
                        msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)
                    else:
                        import fcntl
                        fcntl.lockf(handle.fileno(),fcntl.LOCK_UN,1)
                    handle.close();del _LEASES[identity]
