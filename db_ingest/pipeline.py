"""Captured-source planning, journaled publication and commit-safe recovery."""
from dataclasses import asdict
import copy
import hashlib
import json
import os
from pathlib import Path
import uuid
from .config import load_config, hashes, redact, resolve_columns, stable_hash, effective_for_file
from .models import schema_plan_from_dict as _plan_from_dict
from .errors import IngestError
from .journal import Journal
from .models import SourceArtifact, DialectDecision, ColumnProfile, SchemaPlan
from .planning import map_destination, uses_name_claims
from .transports.odbc import choose_transport

SEVERITY={0:0,3:1,2:2,4:3,5:4}
def worst(codes):return max(codes,key=lambda n:SEVERITY[n],default=0)

class SeverityAccumulator(list):
    """Retain one worst exit code rather than one integer per discovered file."""
    def append(self,code):
        if not self or SEVERITY[code]>SEVERITY[self[0]]:self[:]=[code]


def _modules():
    from .discovery import discover
    from .capture import capture_source
    from .parsers import decide,parse
    from .profile import profile_records
    return discover,capture_source,decide,parse,profile_records


def _check_capture(artifact):
    h=hashlib.sha256();size=0
    try:
        with Path(artifact["path"]).open("rb") as f:
            for block in iter(lambda:f.read(1024*1024),b""):
                from .resources import checkpoint
                checkpoint()
                h.update(block);size+=len(block)
    except OSError as exc:raise IngestError("ARCHIVE_UNAVAILABLE",str(exc),5) from exc
    if size!=artifact["size"] or h.hexdigest()!=artifact["sha256"]:raise IngestError("ARCHIVE_MISMATCH","Preserved bytes differ from capture metadata",5)
    return {"size":size,"sha256":h.hexdigest(),"verified":True}


def local_evidence(journal,run_id,file_id,artifact,decision):
    import codecs
    from .attachments import verify_references
    verified_attachments=verify_references(journal,run_id,file_id)
    from .projections import verify_references as verify_projection_references
    projection_summary=verify_projection_references(journal,run_id,file_id)
    counts=journal.counts(run_id,file_id)
    known=counts["committed"]+counts["pending"]+counts["rejected"]+counts["excluded"]
    ledger=hashlib.sha256();logical=0;blank_records=0;parsed_data_records=0;record_error_events=0;conversion_validated_records=0
    cursor=decision.get("evidence",{}).get("bom_bytes",0)
    structured=decision.get("evidence",{}).get("adapter") in ("xml","html")
    for record in journal.records(run_id,file_id):
        from .resources import checkpoint
        checkpoint()
        encoded=json.dumps(asdict(record),sort_keys=True,ensure_ascii=True,separators=(",",":")).encode()
        ledger.update(len(encoded).to_bytes(8,"big"));ledger.update(encoded)
        if record.ordinal!=0:logical+=1
        is_blank=any(str(error.get("code","")).startswith("BLANK") for error in record.errors)
        blank_records+=int(is_blank);record_error_events+=len(record.errors)
        if record.kind!="header" and not is_blank and record.fields:parsed_data_records+=1
        if record.kind=="data" and record.fields and not record.errors:conversion_validated_records+=1
        if structured:continue
        if record.byte_start!=cursor or record.byte_end is None or record.byte_end<cursor or record.byte_end>artifact["size"]:
            raise IngestError("BYTE_COVERAGE_MISMATCH","Record byte spans overlap, leave gaps, or leave the retained source",5)
        cursor=record.byte_end
    if not structured and cursor!=artifact["size"]:raise IngestError("BYTE_COVERAGE_MISMATCH","Record spans do not cover the captured original",5)
    physical_breaks=0;physical_lines=0;prev_cr=False;last="";decoded=False
    nel_is_newline=decision.get("evidence",{}).get("nel_is_newline",False)
    try:
        decoder=codecs.getincrementaldecoder(decision["encoding"])(errors="strict")
        with Path(artifact["path"]).open("rb") as source:
            source.seek(decision.get("evidence",{}).get("bom_bytes",0))
            while True:
                from .resources import checkpoint
                checkpoint()
                block=source.read(65536)
                text=decoder.decode(block,final=not block)
                if text:
                    physical_breaks+=(text.count("\x85") if nel_is_newline else 0)
                    physical_breaks+=text.count("\r")+text.count("\n")-text.count("\r\n")-(1 if prev_cr and text.startswith("\n") else 0)
                    prev_cr=text.endswith("\r");last=text[-1];decoded=True
                if not block:break
        physical_lines=physical_breaks+(1 if decoded and last not in ("\r\n\x85" if nel_is_newline else "\r\n") else 0)
    except UnicodeDecodeError:physical_lines=None;physical_breaks=None
    source_receipt=journal.file(run_id,file_id).get("receipt")
    staged=source_receipt.get("committed_records") if source_receipt else None
    stage_scope="acknowledged shared group contribution" if source_receipt and source_receipt.get("kind")=="group_member" else "acknowledged publication receipt" if source_receipt else "not acknowledged; partial raw staging is separate"
    return {**counts,"blank_records":None if counts["unresolved"] else blank_records,"known_blank_records":blank_records,"parsed_data_records":None if counts["unresolved"] else parsed_data_records,"known_parsed_data_records":parsed_data_records,"record_error_events":record_error_events,"header_records":counts["header"],"conversion_validated_records":conversion_validated_records,"staged_records":staged,"validated_records":staged,"sql_staged_records":staged,"sql_validated_records":staged,"sql_stage_metric_basis":stage_scope,"projection_summary":projection_summary,"verified_attachment_artifacts":verified_attachments,"ledger_sha256":ledger.hexdigest(),"data_records":None if counts["unresolved"] else known,"known_data_records":known,"logical_records":None if counts["unresolved"] else logical,"physical_lines":physical_lines,"physical_line_breaks":physical_breaks,"source_bytes":artifact["size"],"byte_coverage":"archive_exact; structured row offsets unavailable" if structured else "complete contiguous spans plus BOM","accounting":"pending/unresolved" if counts["pending"] or counts["unresolved"] else "all known records disposed"}

def _preflight_features(config):
    from .transports.faults import validate_faults
    validate_faults(config)
    # Until these policies have an end-to-end implementation they must fail closed.
    unsupported=[]
    if config["source"]["follow_symlinks"]:unsupported.append("following symlinks/junctions")
    if config["parsing"]["repair_policy"]!="none":unsupported.append("quote/row repairs")
    if config["parsing"]["repeated_header_policy"]!="preserve_as_data":unsupported.append("repeated-header removal")
    if config["parsing"]["ragged_row_policy"]!="quarantine":unsupported.append("ragged-row repair")
    if config["load"]["partial_publish"] not in ("new_or_append","none"):unsupported.append("partial_publish="+config["load"]["partial_publish"])
    if config["schema"]["inference"]!="full":unsupported.append("sampled execution schema")
    for rule in config.get("file_rules",[]):
        for column in rule.get("columns",[]):
            from .column_policy import DATE_FORMATS
            if column.get("temporal_format","iso8601") not in ("iso8601",*DATE_FORMATS):unsupported.append("unsupported temporal_format")
            if column.get("fractional_second_policy","preserve_exact")!="preserve_exact":unsupported.append("lossy fractional_second_policy")
            if column.get("timezone_policy","preserve")!="preserve":unsupported.append("timezone conversion")
    for key,required in (("numeric_policy","exact"),("date_policy","iso_unambiguous"),("timezone_policy","preserve"),("fractional_second_policy","preserve_exact"),("text_fallback","nvarchar")):
        if config["schema"][key]!=required:unsupported.append("schema."+key+"="+str(config["schema"][key]))
    if unsupported:raise IngestError("UNSUPPORTED_CAPABILITY","Unsupported policies: "+", ".join(unsupported))

def _guard_resume_config(config, original):
    before, after = hashes(original), hashes(config)
    if before["interpretation"] != after["interpretation"] or before["mapping"] != after["mapping"]:
        raise IngestError("RESUME_SEMANTIC_CHANGE", "Parsing/schema/mapping changed; use reimport for a new linked interpretation")
    selectors = ("root", "path", "recursive", "include", "exclude", "follow_symlinks")
    if any(original["source"].get(k) != config["source"].get(k) for k in selectors):
        raise IngestError("RESUME_SELECTION_CHANGE", "Resume must retain the durable source selection; use a new run for another source subset")


def _inventory(journal, config, run_id, new_intent=False, artifacts=None):
    from .discovery import inventory
    from .resources import checkpoint
    journal.begin_inventory(run_id,new_intent)
    count=0
    if uses_name_claims(config):
        journal.db.execute("CREATE TEMP TABLE IF NOT EXISTS filename_selection_names (name TEXT PRIMARY KEY, path TEXT NOT NULL)")
        journal.db.execute("DELETE FROM filename_selection_names")
        # TEMP DML opens a deferred SQLite transaction. End it before any main
        # journal read so another inventory writer cannot stale our snapshot.
        journal.flush()
    entries=None
    try:
        if artifacts is None:
            entries=inventory(config)
        else:
            entries=({"path":a["original_path"],"relative_path":a["relative_path"],"status":"selected","reason":"linked preserved artifact","artifact":a} for a in artifacts)
        for entry in entries:
            checkpoint()
            journal.add_selection(run_id,entry["path"],entry["relative_path"],entry["status"],entry["reason"],entry.get("artifact"))
            if uses_name_claims(config) and entry["status"]=="selected":
                from .planning import name_claim_source
                claimed_source=name_claim_source(config,entry["relative_path"])
                name=map_destination(config,entry["relative_path"])["table"].casefold()
                previous=journal.db.execute("SELECT path FROM filename_selection_names WHERE name=?",(name,)).fetchone()
                if previous is not None and previous[0]!=claimed_source:
                    raise IngestError("TABLE_NAME_COLLISION","Distinct source files/folders map to the same readable table; rename a source or use stable_hash_suffix",context={"table":name,"first_source":previous[0],"second_source":entry["relative_path"]})
                journal.db.execute("INSERT OR IGNORE INTO filename_selection_names VALUES (?,?)",(name,claimed_source))
            count+=1
            if count%100==0:journal.flush()
        journal.flush()
        # Unreadable entries are retained in the manifest; the inventory itself
        # is complete but those entries remain explicit capture blockers.
        journal.complete_inventory(run_id)
        journal.event(run_id,None,"INVENTORY_COMPLETED",{"entries":count})
    except BaseException:
        journal.flush()
        raise
    finally:
        close=getattr(entries,"close",None)
        if close is not None:close()


from contextlib import contextmanager


@contextmanager
def _phase_event(journal,run_id,file_id,phase):
    import time
    started=time.perf_counter();succeeded=False
    try:
        yield
        succeeded=True
    finally:
        journal.event(run_id,file_id,"PHASE_TIMING",{"phase":phase,"elapsed_seconds":time.perf_counter()-started,"succeeded":succeeded})


def _prepare_one(payload):
    from .resources import checkpoint
    config,run_id,selection_id=payload["config"],payload["run_id"],payload["selection_id"]
    journal=Journal(config["preservation"]["work_directory"])
    file_id=None;artifact=None
    outcome_code=4
    lease=journal.lease("selection:"+run_id+":"+selection_id);lease.__enter__()
    try:
        selection=journal.selection(run_id,selection_id)
        journal.event(run_id,selection.get("file_id"),"WORKER_STARTED",{"phase":"profile","selection_id":selection_id})
        checkpoint(force=True)
        if selection["status"]=="unreadable":
            raise IngestError("SOURCE_ACCESS",selection["reason"],4,{"source":selection["path"]})
        artifact=selection.get("artifact")
        file_id=selection.get("file_id")
        existing=None
        if file_id:
            try:existing=journal.file(run_id,file_id)
            except IngestError:pass
        if existing is not None:
            artifact=existing["artifact"]
        if artifact is None and file_id:
            # Recover an atomic capture finalized just before process death but
            # not yet linked into the journal. Partial captures are never reused.
            metadata=journal.work/"captures"/file_id/"artifact.json"
            if metadata.is_file():
                recovered=json.loads(metadata.read_text(encoding="utf-8"))
                if recovered["original_path"]!=selection["path"]:
                    raise IngestError("CAPTURE_IDENTITY_MISMATCH","Orphan capture belongs to another source",5)
                artifact=recovered
                journal.event(run_id,file_id,"CAPTURE_ORPHAN_RECOVERED",{"selection_id":selection_id,"capture_id":file_id,"intent_id":selection["intent_id"],"artifact_path":recovered["path"],"sha256":recovered["sha256"],"size":recovered["size"]})
        if artifact is None:
            # No complete snapshot means unread bytes have no trusted identity.
            # Checking mtime/size or the retained prefix cannot establish that a
            # reconnecting source is the same export. Also inspect older attempts:
            # a prior offline retry may have replaced file_id with an empty attempt.
            from .capture import require_safe_capture_retry
            require_safe_capture_retry(journal,run_id,selection_id,file_id)
            from .capture import capture_source
            old_capture_id=file_id;file_id=uuid.uuid4().hex
            if old_capture_id:
                old_folder=journal.work/"captures"/old_capture_id
                old_path=old_folder/"source.partial"
                if not old_path.exists():old_path=old_folder/"source.bin"
                journal.event(run_id,old_capture_id,"CAPTURE_RETRY_ALLOCATED",{"selection_id":selection_id,"intent_id":selection["intent_id"],"previous_capture_id":old_capture_id,"replacement_capture_id":file_id,"retained_previous_artifact":str(old_path),"retained_previous_bytes":old_path.stat().st_size if old_path.exists() else None,"reuse":"forbidden; incomplete capture retained"})
            journal.update_selection(run_id,selection_id,state="CAPTURING",file_id=file_id,error=None)
            journal.event(run_id,file_id,"CAPTURE_ATTEMPT_STARTED",{"selection_id":selection_id,"capture_id":file_id,"intent_id":selection["intent_id"],"source_path":selection["path"],"partial_artifact":str(journal.work/"captures"/file_id/"source.partial")})
            with _phase_event(journal,run_id,file_id,"capture"):
                artifact=asdict(capture_source(selection["path"],config["source"]["root"],str(journal.work),config["preservation"]["minimum_free_disk_bytes"],capture_id=file_id,_fault_config=config))
        with _phase_event(journal,run_id,file_id,"archive_verify"):
            _check_capture(artifact)
        file_id=artifact["capture_id"]
        journal.update_selection(run_id,selection_id,state="CAPTURED",artifact=artifact,file_id=file_id,error=None)
        if existing is None:
            journal.add_file(run_id,SourceArtifact(**artifact),selection["intent_id"])
        elif existing["plan"] is not None:
            if journal.counts(run_id,file_id)["unresolved"]:
                raise IngestError("UNRESOLVED_FRAMING","Captured source still has an unresolved tail; the affected file remains blocked",5)
            if existing["state"]=="NEEDS_DECISION":
                raise IngestError("FILE_DECISION_REQUIRED","A corrected interpretation requires a linked reimport")
            # A failed length-abort profile is not an approved partial load.
            if existing["state"]=="FAILED" and config["schema"]["on_length_overflow"]=="abort":
                errors=journal.errors(run_id)
                if any(e["file_id"]==file_id and e["code"]=="LENGTH_OVERFLOW_ABORT" for e in errors):
                    raise IngestError("LENGTH_OVERFLOW_ABORT","Configured abort policy still blocks publication",4)
            journal.update_selection(run_id,selection_id,state="PROFILED",error=None)
            outcome_code=0
            return {"file_id":file_id,"exit_code":0,"state":existing["state"],"source_bytes":artifact["size"],"processed_records":0}
        else:
            journal.reset_unpublished_profile(run_id,file_id)
        journal.update_file(run_id,file_id,state="PROFILING")
        _profile_capture(journal,config,run_id,SourceArtifact(**artifact),bool(journal.run(run_id)["planning_new_intent"]),DialectDecision(**existing["decision"]) if existing and existing.get("decision") else None)
        journal.update_selection(run_id,selection_id,state="PROFILED",error=None)
        outcome_code=0
        return {"file_id":file_id,"exit_code":0,"state":"PROFILED","source_bytes":artifact["size"],"processed_records":sum(v for k,v in journal.counts(run_id,file_id).items() if k in ("pending","committed","rejected","excluded"))}
    except IngestError as error:
        journal.flush()
        journal.incident(run_id,file_id,error)
        journal.update_selection(run_id,selection_id,state="PROFILE_FAILED" if file_id else "CAPTURE_FAILED",error={"code":error.code,"message":str(error),"exit_code":error.exit_code,"context":error.context})
        if file_id and journal.db.execute("SELECT 1 FROM files WHERE run_id=? AND file_id=?",(run_id,file_id)).fetchone():
            journal.update_file(run_id,file_id,state="NEEDS_DECISION" if error.exit_code==2 else "FAILED")
        outcome_code=error.exit_code
        return {"file_id":file_id,"exit_code":error.exit_code,"state":"FAILED","code":error.code,"source_bytes":artifact["size"] if artifact else 0,"processed_records":sum(v for k,v in journal.counts(run_id,file_id).items() if k in ("pending","committed","rejected","excluded")) if file_id else 0}
    finally:
        journal.event(run_id,file_id,"WORKER_FINISHED",{"phase":"profile","selection_id":selection_id,"exit_code":outcome_code})
        lease.__exit__(None,None,None)
        journal.close()


def _report_files(journal,run_id,config=None):
    from .attachments import references
    budget=min(16777216,(config or json.loads(journal.run(run_id)["config"]))["limits"]["max_queue_bytes"])
    attachment_budget=min(1048576,budget//4)
    files=journal.report_files(run_id,max_bytes=budget-attachment_budget)
    remaining=1000;remaining_bytes=max(0,attachment_budget-128*len(files))
    for item in files:
        # Only live disposition counters are overlaid. Approved ledger/projection
        # seals and historical evidence remain exactly as stored.
        item["counts"]={**(item.get("counts") or {}),**journal.counts(run_id,item["file_id"])}
        linked=references(journal,run_id,item["file_id"],limit=remaining,max_bytes=remaining_bytes)
        if linked["count"]:item["attachments"]=linked
        remaining-=len(linked["artifacts"])
        remaining_bytes=max(0,remaining_bytes-linked.get("serialized_bytes",0))
    return files


_GLOBAL_FAILURE_CODES={"CAPTURE_RETRY_UNVERIFIED","CAPTURE_RETRY_EVIDENCE_INVALID","CAPTURE_IO","SOURCE_ACCESS","SOURCE_CHANGED","DISK_CAPACITY","CAPTURE_IDENTITY_MISMATCH","ARCHIVE_MISMATCH","ARCHIVE_UNAVAILABLE","BYTE_COVERAGE_MISMATCH","LEDGER_MISMATCH","CANCELLED","WORKER_FAILED","WORKER_LOST","RESOURCE_BUDGET","RESOURCE_ADMISSION","RESOURCE_MEASUREMENT_UNAVAILABLE","PROCESS_JOB_UNAVAILABLE","QUEUE_BUDGET","REPORT_BUDGET","PLAN_ARTIFACT_MISMATCH","PLAN_MISMATCH","RUN_BUSY"}


def _global_failure(code):
    return code in _GLOBAL_FAILURE_CODES or code.startswith(("CONFIG_","RESOURCE_","JOURNAL_"))


def _publication_policy(journal,run_id,metrics=None):
    run=journal.run(run_id)
    retained=json.loads(run.get("execution_policy") or "{}")
    current=metrics if metrics is not None else retained
    blocked=run["inventory_state"]!="COMPLETE" or current.get("loader",{}).get("state")=="BLOCKED"
    publishable=0;codes=SeverityAccumulator();failures=0
    for selected in journal.selections(run_id,selected_only=False):
        if selected["status"] not in ("selected","unreadable"):continue
        if selected["state"]=="PROFILED" and selected.get("file_id"):
            publishable+=1
        else:
            failures+=1
            error=selected.get("error")
            if error:
                codes.append(int(error.get("exit_code",4)))
                if _global_failure(error["code"]):blocked=True
            else:
                blocked=True;codes.append(4)
    floor=current.get("planning_incident_floor",0)
    for row in journal.db.execute("SELECT code FROM incidents WHERE run_id=? AND file_id IS NULL AND id>?",(run_id,floor)):
        if _global_failure(row[0]):blocked=True
    publication_groups=None
    if json.loads(run["config"])["mapping"]["table_strategy"]=="per_directory":
        from .grouping import available,groups
        publishable=0;publication_groups=0
        if available(journal):
            for merged in groups(journal,run_id):
                if merged["state"] in ("PROFILED","PUBLISHING","COMMIT_OUTCOME_UNKNOWN","COMMITTED","RECONCILED","PARTIAL") and merged["plan"] is not None:
                    publication_groups+=1
                    publishable+=journal.db.execute("SELECT COUNT(*) FROM group_members WHERE run_id=? AND group_id=?",(run_id,merged["group_id"])).fetchone()[0]
                elif merged.get("error"):
                    codes.append(merged["error"].get("exit_code",4))
                    if _global_failure(merged["error"]["code"]):blocked=True
    result={"publishable_files":publishable,"failed_selections":failures,"global_publication_blocked":blocked,"selection_exit_code":worst(codes),"inventory_state":run["inventory_state"]}
    if publication_groups is not None:result["publishable_groups"]=publication_groups
    return result


def _planning_report(journal,config,run_id,code,metrics):
    try:metrics["loader"]=choose_transport(config)
    except IngestError as error:
        journal.incident(run_id,None,error)
        metrics["loader"]={"state":"BLOCKED","code":error.code,"reason":str(error)}
        code=worst([code,error.exit_code])
    state={0:"PROFILED",2:"NEEDS_DECISION",3:"PARTIAL",4:"FAILED",5:"RECONCILIATION_FAILED"}[code]
    journal.state(run_id,state,code)
    publication_policy=_publication_policy(journal,run_id,metrics)
    metrics.update(publication_policy)
    journal.policy(run_id,metrics)
    total=journal.db.execute("SELECT COUNT(*) FROM files WHERE run_id=?",(run_id,)).fetchone()[0]
    files=_report_files(journal,run_id)
    merged_report={}
    if config["mapping"]["table_strategy"]=="per_directory":
        from .grouping import report_groups
        merged_report["groups"]=report_groups(journal,run_id)
    return {"run_id":run_id,"state":state,**publication_policy,**merged_report,"files":files,"file_count":total,"files_report_truncated":total>len(files),"exit_code":code,"errors":journal.errors(run_id),"effective_config":redact(config),"hashes":hashes(config),"provenance":config.get("_provenance",{}),"execution_policy":metrics}


def _schedule_profiles(journal,config,run_id):
    from .scheduling import bounded_map
    floor=journal.db.execute("SELECT COALESCE(MAX(id),0) FROM incidents WHERE run_id=?",(run_id,)).fetchone()[0]
    metrics={"planning_incident_floor":floor};codes=SeverityAccumulator();count=0
    journal.event(run_id,None,"PLANNING_ATTEMPT_STARTED",{"incident_floor":floor})
    def arguments():
        nonlocal count
        for selected in journal.selections(run_id,selected_only=False):
            if selected["status"] not in ("selected","unreadable"):continue
            count+=1
            yield {"config":config,"run_id":run_id,"selection_id":selected["selection_id"]}
    try:
        for argument,outcome in bounded_map(config,_prepare_one,arguments(),metrics):
            if outcome["ok"]:
                codes.append(outcome["value"]["exit_code"])
            else:
                detail=outcome["error"];error=IngestError(detail["code"],detail["message"],detail["exit_code"],detail["context"])
                selection=journal.selection(run_id,argument["selection_id"])
                journal.incident(run_id,selection.get("file_id"),error)
                journal.update_selection(run_id,argument["selection_id"],state="WORKER_FAILED",error=detail)
                codes.append(error.exit_code)
    except IngestError as error:
        journal.incident(run_id,None,error);codes.append(error.exit_code)
    except KeyboardInterrupt:
        journal.incident(run_id,None,IngestError("CANCELLED","Scheduling cancelled; completed captures and intent IDs retained",4));codes.append(4)
    selected_total=journal.db.execute("SELECT COUNT(*) FROM selections WHERE run_id=? AND status IN ('selected','unreadable')",(run_id,)).fetchone()[0]
    if selected_total==0:
        error=IngestError("NO_SELECTED_FILES","No selected files; inspect the durable source inventory")
        journal.incident(run_id,None,error);codes.append(error.exit_code)
    # Tasks not admitted after cancellation/budget failure remain durable pending
    # selections and prevent an empty/small prefix from becoming a clean plan.
    pending=journal.db.execute("SELECT COUNT(*) FROM selections WHERE run_id=? AND status IN ('selected','unreadable') AND state!='PROFILED'",(run_id,)).fetchone()[0]
    if pending and not any(codes):codes.append(4)
    metrics["pending_selections"]=pending
    metrics["selected_files"]=selected_total
    metrics["task_arguments_consumed"]=count
    if config["mapping"]["table_strategy"]=="per_directory":
        source_policy=_publication_policy(journal,run_id,metrics)
        if not source_policy["global_publication_blocked"]:
            from .grouping import prepare_groups
            try:
                group_codes,group_metrics=prepare_groups(journal,config,run_id)
                for value in group_codes:codes.append(value)
                metrics["group_planning"]=group_metrics
                metrics["source_planning_seconds"]=metrics.get("elapsed_seconds",0)
                metrics["elapsed_seconds"]=metrics.get("elapsed_seconds",0)+group_metrics.get("elapsed_seconds",0)
                metrics["sampled_peak_process_tree_rss_bytes"]=max(metrics.get("sampled_peak_process_tree_rss_bytes",0),group_metrics.get("sampled_peak_process_tree_rss_bytes",0))
            except IngestError as error:
                journal.incident(run_id,None,error);codes.append(error.exit_code)
    return _planning_report(journal,config,run_id,worst(codes),metrics)


def _with_resources(function):
    from functools import wraps
    @wraps(function)
    def wrapped(config,*args,**kwargs):
        from .resources import resource_scope
        with resource_scope(config):
            return function(config,*args,**kwargs)
    return wrapped


@_with_resources
def prepare(config,config_path=None,overrides=None,run_id=None,linked_run=None,new_intent=False,artifacts=None):
    _preflight_features(config)
    run_id=run_id or uuid.uuid4().hex
    journal=Journal(config["preservation"]["work_directory"])
    journal.create_run(run_id,redact(config),linked_run)
    lease=journal.lease("run:"+run_id);lease.__enter__()
    try:
        _inventory(journal,config,run_id,new_intent,artifacts)
        return _schedule_profiles(journal,config,run_id)
    except IngestError as error:
        error.context.setdefault("run_id",run_id)
        error.context.setdefault("next_command",f"resume --run-id {run_id} --plan-only")
        journal.incident(run_id,None,error)
        return _planning_report(journal,config,run_id,error.exit_code,{"inventory_state":"OPEN"})
    finally:
        lease.__exit__(None,None,None)
        journal.close()


@_with_resources
def recover_plan(config,run_id):
    """Complete inventory/profile checkpoints under the original import intents."""
    _preflight_features(config)
    journal=Journal(config["preservation"]["work_directory"])
    lease=journal.lease("run:"+run_id);lease.__enter__()
    try:
        run=journal.run(run_id)
        _guard_resume_config(config,json.loads(run["config"]))
        if run["inventory_state"]!="COMPLETE":
            try:_inventory(journal,config,run_id,bool(run["planning_new_intent"]))
            except IngestError as error:
                journal.incident(run_id,None,error)
                report=_planning_report(journal,config,run_id,error.exit_code,{"inventory_state":"OPEN"})
                report.update(code="PLANNING_INCOMPLETE",message="The complete selection could not be inventoried; no publication attempted")
                return report
        report=_schedule_profiles(journal,config,run_id)
        if report["exit_code"]:
            report.update(code="PLANNING_INCOMPLETE",message="Some durable selections could not be captured/profiled; no incomplete selection was published")
        return report
    finally:
        lease.__exit__(None,None,None)
        journal.close()



def _write_plan_artifact(journal,run_id,file_id,plan,artifact):
    from .capture import _atomic_replace,_sync_directory
    from .resources import checkpoint
    import os
    folder=journal.work/"plans"/hashlib.sha256(run_id.encode()).hexdigest()[:32]
    folder.mkdir(parents=True,exist_ok=True)
    target=folder/(hashlib.sha256(file_id.encode()).hexdigest()+".json")
    temporary=target.with_suffix(".partial.json")
    with temporary.open("w",encoding="utf-8",newline="\n") as output:
        for chunk in json.JSONEncoder(ensure_ascii=True,indent=2).iterencode({"format_version":1,"run_id":run_id,"file_id":file_id,"source_artifact":artifact,"plan":asdict(plan)}):
            checkpoint()
            output.write(chunk)
        output.flush();os.fsync(output.fileno())
    _atomic_replace(temporary,target);_sync_directory(folder)
    digest=hashlib.sha256()
    with target.open("rb") as source:
        while block:=source.read(1048576):digest.update(block)
    return {"plan_artifact":str(target),"plan_artifact_sha256":digest.hexdigest()}


def _profile_capture(journal,config,run_id,artifact,new_intent=False,decision_override=None):
    from .resources import checkpoint
    _,_,decide,parse,profile_records=_modules()
    effective=effective_for_file(config,artifact.relative_path)
    file_id=artifact.capture_id
    with _phase_event(journal,run_id,file_id,"dialect_decision"):
        decision=decision_override or decide(artifact,effective)
    journal.update_file(run_id,file_id,decision=asdict(decision))
    headers=None;count=0;invalid_header=False
    with _phase_event(journal,run_id,file_id,"parse"):
        # Frame/serialize outside SQLite write transactions. Small prepared rows
        # share a FULL-fsync commit; a large row is committed on its own.
        prepared=[];prepared_bytes=0
        for record in parse(artifact,decision,effective):
            if count==0 and decision.header=="present" and record.kind!="header":invalid_header=True
            if record.kind=="header":headers=[f.text or "" for f in record.fields]
            elif headers is None and record.kind=="data":headers=[f"column_{i}" for i in range(1,len(record.fields)+1)]
            checkpoint()
            encoded=journal.prepare_record(run_id,file_id,record);size=len(encoded[-1])
            if prepared and prepared_bytes+size>1048576:
                journal.save_prepared_records(prepared);prepared=[];prepared_bytes=0
            prepared.append(encoded);prepared_bytes+=size;count+=1
            checkpoint(prepared_bytes*2)
            if len(prepared)>=100 or prepared_bytes>=1048576:
                journal.save_prepared_records(prepared);prepared=[];prepared_bytes=0
        if prepared:journal.save_prepared_records(prepared)
        journal.flush()
    if invalid_header:raise IngestError("INVALID_HEADER","The declared first header record is unusable; retained bytes require a corrected interpretation")
    headers=effective["parsing"].get("column_names",[]) or headers or []
    with _phase_event(journal,run_id,file_id,"column_normalization"):
        effective["columns"]=resolve_columns(effective,headers)
        from .column_policy import normalize_record
        for record in journal.records(run_id,file_id,"pending"):
            checkpoint()
            if normalize_record(record,effective["columns"],effective["parsing"]):
                journal.set_disposition(run_id,file_id,record.record_id,"pending",record)
    with _phase_event(journal,run_id,file_id,"profile"):
        columns=profile_records(journal.records(run_id,file_id),headers,effective)
    for column in columns:
        for spec in effective.get("columns",[]):
            if spec["source_ordinal"]==column.ordinal:column.stats.update(spec)
    mapping=map_destination(effective,artifact.relative_path);identity=hashes(effective)
    mapping_target={k:mapping[k] for k in ("database","schema","table")}
    if uses_name_claims(effective):
        mapping_target.update(source_relative_path=artifact.relative_path,source_root=os.path.normcase(str(Path(effective["source"]["root"]).resolve())))
    plan=SchemaPlan(mapping["database"],mapping["schema"],mapping["table"],columns,artifact.sha256,identity["interpretation"],stable_hash({"configuration":identity["mapping"],"target":mapping_target}))
    from itertools import islice
    previous=list(islice(journal.history(artifact.sha256,plan.interpretation_hash,plan.mapping_hash),100))
    if previous and not new_intent:raise IngestError("NEW_INTENT_REQUIRED","Identical source and mapping already published; use --new-intent or resume the prior run",context={"history":previous})
    from .staging import validate_record
    from .attachments import AttachmentContext
    attachment_context=AttachmentContext(journal,run_id,artifact,columns,effective)
    from .projections import ProjectionContext
    projection_context=ProjectionContext(journal,run_id,artifact,columns,effective)
    from .length_policy import LengthPolicyContext
    length_context=LengthPolicyContext(journal,run_id,artifact,columns,effective)
    with _phase_event(journal,run_id,file_id,"conversion_validation"):
        length_decision=False;length_abort=False;length_evidence=[]
        for record in journal.records(run_id,file_id,"pending"):
            checkpoint()
            attachment_errors=attachment_context.process(record)
            projection_errors=projection_context.process(record) if not attachment_errors else []
            length_policy_errors=length_context.process(record,{e.get("column_ordinal") for e in attachment_errors+projection_errors})
            failed_columns={e.get("column_ordinal") for e in attachment_errors+projection_errors+length_policy_errors}
            errors=attachment_errors+projection_errors+length_policy_errors+[e for e in validate_record(plan,record) if e.get("column_ordinal") not in failed_columns]
            if (attachment_context.columns or projection_context.changed) and not errors:
                journal.set_disposition(run_id,file_id,record.record_id,"pending",record)
            if errors:
                length_errors=[e for e in errors if "UTF-16 units exceed" in e["message"] or "encoded bytes exceed" in e["message"] or "binary exceeds destination length" in e["message"]]
                record.errors.extend(errors)
                from .errors import complete_record_errors
                complete_record_errors(record,stage="conversion_validation",columns=plan.columns)
                policy=effective["schema"]["on_length_overflow"]
                if length_errors and policy=="ask":
                    length_decision=True
                    if len(length_evidence)<10:length_evidence.extend(length_errors)
                    journal.set_disposition(run_id,file_id,record.record_id,"pending",record)
                else:
                    if length_errors and policy=="abort":length_abort=True
                    journal.set_disposition(run_id,file_id,record.record_id,"rejected",record)
    counts=local_evidence(journal,run_id,file_id,asdict(artifact),asdict(decision))
    with _phase_event(journal,run_id,file_id,"plan_artifact"):
        counts.update(_write_plan_artifact(journal,run_id,file_id,plan,asdict(artifact)))
    journal.update_file(run_id,file_id,state="PROFILED",plan=asdict(plan),counts=counts)
    if counts["unresolved"]:raise IngestError("UNRESOLVED_FRAMING","Unknown record boundaries block affected publication",5)
    if length_decision:raise IngestError("LENGTH_DECISION_REQUIRED","An explicit target length cannot hold observed values. Correct target_type and reimport, select on_length_overflow=quarantine, or abort",2,{"columns":length_evidence,"destination":mapping,"options":["widen target_type","retype to nvarchar(max)","quarantine","stop"]})
    if length_abort:raise IngestError("LENGTH_OVERFLOW_ABORT","Explicit target length exceeded and configured policy is abort",4)



def _backfill_rejections(journal,config,plan,item,username,password):
    from .adapters.mssql.control import materialize_failures,failure_table_name
    metadata=copy.deepcopy(item.get("failures") or {})
    metadata.setdefault("name",failure_table_name(plan.table,item["run_id"]))
    metadata["state"]="pending"
    journal.update_file(item["run_id"],item["file_id"],failures=metadata)
    try:
        result=materialize_failures(config,plan,item["run_id"],item["artifact"],lambda:journal.records(item["run_id"],item["file_id"],"rejected"),username,password,table_name=metadata["name"])
        metadata.update(result)
    except IngestError as exc:
        if exc.exit_code==5:raise
        journal.incident(item["run_id"],item["file_id"],exc)
    journal.update_file(item["run_id"],item["file_id"],failures=metadata)
    item["failures"]=metadata
    return metadata

def _audit_backfill(journal,config,plan,receipt,item,username,password):
    from .adapters.mssql.control import mirror_receipt,mirror_file,materialize_failures,failure_table_name
    payload=copy.deepcopy(receipt)
    details=payload.setdefault("details",{})
    file_id=item["file_id"];run_id=item["run_id"]
    if config["audit"]["mirror_receipts_after_commit"]:
        try:
            mirror=mirror_receipt(config,plan,payload,username,password)
            mirror_file(config,plan,payload,item["artifact"],username,password)
            details["control_mirror_state"]=mirror["state"]
        except IngestError as exc:
            if exc.exit_code==5:raise
            journal.incident(run_id,file_id,exc);details["control_mirror_state"]="pending"
    else:details["control_mirror_state"]="disabled_by_policy"
    if journal.counts(run_id,file_id)["rejected"]:
        result=_backfill_rejections(journal,config,plan,item,username,password)
        details["failure_table_state"]=result["state"]
        details["failure_table_name"]=result["name"]
        details["failure_table"]=result
    journal.update_file(run_id,file_id,receipt=payload)
    return payload

def _known_receipts(journal,config,plan):
    """Stream every acknowledged local receipt for this configured destination."""
    server=str(config["destination"]["server"]).casefold()
    target=(plan.database.casefold(),plan.schema.casefold(),plan.table.casefold())
    sql="SELECT f.plan,f.receipt,r.config FROM files f JOIN runs r ON f.run_id=r.run_id WHERE f.receipt IS NOT NULL AND f.plan IS NOT NULL ORDER BY f.rowid"
    for row in journal.db.execute(sql):
        original=json.loads(row[2])
        if str(original["destination"]["server"]).casefold()!=server:continue
        old_plan=json.loads(row[0])
        if tuple(str(old_plan[k]).casefold() for k in ("database","schema","table"))!=target:continue
        receipt=json.loads(row[1])
        if receipt.get("kind")=="group_member":continue
        yield {"plan":_plan_from_dict(old_plan),"receipt":receipt}
    from .grouping import available
    if available(journal):
        for row in journal.db.execute("SELECT g.plan,g.receipt,r.config FROM groups g JOIN runs r ON g.run_id=r.run_id WHERE g.receipt IS NOT NULL AND g.plan IS NOT NULL ORDER BY g.rowid"):
            original=json.loads(row[2]);old_plan=json.loads(row[0])
            if str(original["destination"]["server"]).casefold()!=server:continue
            if tuple(str(old_plan[k]).casefold() for k in ("database","schema","table"))!=target:continue
            yield {"plan":_plan_from_dict(old_plan),"receipt":json.loads(row[1])}


def _repair_business_prepared_pointer(journal,item):
    """Restore a missing pointer only from its original durable pre-DML event.

    Never read/hash the prepared file to mint authority. The SQL resolver later
    verifies these independent expected bytes against the file and live target.
    """
    import re,os
    from .journal import utcnow
    latest=journal.file(item["run_id"],item["file_id"]);counts=latest.get("counts") or {}
    if "business_prepared" in counts and counts["business_prepared"] is not None:return counts["business_prepared"]
    row=journal.db.execute("SELECT id,details FROM events WHERE run_id=? AND file_id=? AND category='BUSINESS_PREPARED' ORDER BY id DESC LIMIT 1",(item["run_id"],item["file_id"])).fetchone()
    if row is None:return None
    try:
        metadata=json.loads(row[1]);path=metadata.get("path")
        valid=isinstance(path,str) and Path(path).is_absolute() and Path(path).resolve().is_relative_to(journal.work.resolve()) and metadata.get("run_id")==item["run_id"] and metadata.get("intent_id")==item["intent_id"] and bool(re.fullmatch(r"[0-9a-f]{64}",str(metadata.get("sha256",""))))
        if not valid:raise ValueError("Original prepared event identity/seal/path is invalid")
    except (ValueError,TypeError,AttributeError,OSError) as error:raise IngestError("BUSINESS_PREPARED_EVENT_INVALID","Original pre-DML event cannot authorize pointer recovery",5,{"event_id":row[0]}) from error
    updated={**counts,"business_prepared":metadata}
    details={"original_event_id":row[0],"prepared":metadata,"basis":"original durable BUSINESS_PREPARED event; no new file-derived seal"}
    with journal.db:
        journal.db.execute("UPDATE files SET counts=? WHERE run_id=? AND file_id=?",(json.dumps(updated),item["run_id"],item["file_id"]))
        journal.db.execute("INSERT INTO events(run_id,file_id,category,pid,created,details) VALUES(?,?,?,?,?,?)",(item["run_id"],item["file_id"],"BUSINESS_PREPARED_POINTER_REPAIRED",os.getpid(),utcnow(),json.dumps(details)))
    return metadata


def _resolve_interrupted(journal,config,plan,item,username=None,password=None):
    from .publication import resolve_publication
    counts=journal.counts(item["run_id"],item["file_id"])
    if item.get("receipt") is None and (counts["committed"] or item["state"] in ("COMMITTED","RECONCILED")):
        raise IngestError("RECEIPT_MISSING","Local acknowledgment evidence exists without its receipt; automatic replay is blocked",5)
    scoped=copy.deepcopy(config);scoped["_publication_run_id"]=item["run_id"]
    prepared=_repair_business_prepared_pointer(journal,item) if config["mapping"]["existing_target_strategy"]=="mapped_business" else (item.get("counts") or {}).get("business_prepared")
    if prepared:scoped["_business_prepared"]=prepared
    return resolve_publication(scoped,plan,item["intent_id"],known_receipts=_known_receipts(journal,config,plan),username=username,password=password,current_receipt=item.get("receipt"))


def _record_resolution(journal,run_id,file_id,proof):
    item=journal.file(run_id,file_id)
    counts={**(item.get("counts") or {}),"publication_resolution":proof}
    journal.event(run_id,file_id,"COMMIT_OUTCOME_RESOLVED",{"outcome":"RESOLVED_ABSENT","proof":proof})
    journal.update_file(run_id,file_id,state="PROFILED",counts=counts)


def _publish_with_retry(journal,config,plan,records,intent_id,run_id,username,password,file_id):
    """Replay only explicit transient rollback failures after receipt resolution."""
    import random
    import time
    from .adapters.base import get_adapter
    adapter=get_adapter(config)
    publish,find_receipt=adapter.stage_validate_publish,adapter.read_receipt
    from .resources import checkpoint
    retry=config["retry"]
    attempts=int(retry.get("max_attempts",1))
    for attempt in range(1,attempts+1):
        checkpoint()
        journal.update_file(run_id,file_id,state="PUBLISHING")
        journal.event(run_id,file_id,"LOAD_ATTEMPT",{"attempt":attempt,"maximum_attempts":attempts})
        try:
            attempt_config=copy.deepcopy(config)
            if uses_name_claims(config):
                if not config.get("_publication_group"):
                    from .planning import name_claim_source
                    attempt_config["_filename_source_path"]=name_claim_source(config,journal.file(run_id,file_id)["artifact"]["relative_path"])
            attempt_config["_publication_attempt"]={"number":attempt,"max_attempts":attempts}
            attempt_config["_publication_run_id"]=run_id
            if config["mapping"]["existing_target_strategy"]=="mapped_business":
                def prepared_callback(metadata):
                    import os
                    from .journal import utcnow
                    if metadata.get("run_id")!=run_id or metadata.get("intent_id")!=intent_id:raise IngestError("BUSINESS_PREPARED_IDENTITY","Prepared business baseline belongs to another run/intent",5)
                    latest=journal.file(run_id,file_id);saved=latest.get("counts") or {}
                    if saved.get("business_prepared")==metadata:return
                    saved={**saved,"business_prepared":copy.deepcopy(metadata)}
                    with journal.db:
                        journal.db.execute("UPDATE files SET counts=? WHERE run_id=? AND file_id=?",(json.dumps(saved),run_id,file_id))
                        journal.db.execute("INSERT INTO events(run_id,file_id,category,pid,created,details) VALUES(?,?,?,?,?,?)",(run_id,file_id,"BUSINESS_PREPARED",os.getpid(),utcnow(),json.dumps(metadata)))
                attempt_config["_business_prepared_callback"]=prepared_callback
            attempt_config["_explicit_new_intent"]=bool(journal.run(run_id)["planning_new_intent"])
            from .projections import summary as projection_summary
            if config.get("_publication_group"):
                attempt_config["_projection_summary"]=projection_summary(journal,run_id,group_id=config["_publication_group"]["group_id"])
            else:attempt_config["_projection_summary"]=projection_summary(journal,run_id,file_id=file_id)
            retained=journal.file(run_id,file_id).get("counts") or {}
            from .target_review import reference as target_review_reference
            reviewed=target_review_reference(journal,run_id,file_id,retained)
            if reviewed:attempt_config["_target_review"]=reviewed
            if retained.get("publication_resolution"):
                attempt_config["_publication_resolution"]=retained["publication_resolution"]
            with _phase_event(journal,run_id,file_id,"sql_stage_validate_publish"):
                return publish(attempt_config,plan,records,intent_id,run_id,username,password)
        except IngestError as error:
            uncertain=error.exit_code==5
            transient=bool(error.context.get("retryable")) and error.context.get("native_code") in (1205,1222)
            if not uncertain and (not transient or attempt==attempts):raise
            journal.incident(run_id,file_id,error)
            # find_receipt serializes against the original publisher's DB lock.
            # Its absence never authorizes replay after a lost commit acknowledgment.
            try:receipt=find_receipt(config,plan,intent_id,username,password)
            except IngestError as resolution:
                error.context["receipt_resolution"]={"code":resolution.code,"message":str(resolution)}
                raise error from None
            if receipt is not None:
                from .reconciliation import verify_destination
                verify_destination(config,plan,receipt,username=username,password=password)
                journal.event(run_id,file_id,"RETRY_RECEIPT_RESOLVED",{"attempt":attempt,"outcome":"committed; verified without replay"})
                return receipt
            if uncertain:
                error.context["receipt_resolution"]="absent; commit/restore uncertainty retained; no replay"
                raise
            # The provider explicitly reported a deadlock/lock timeout before
            # COMMIT, and the original connection has closed. The serialization
            # lock has now been acquired/released, resolving the previous unit.
            journal.update_file(run_id,file_id,state="PROFILED")
            journal.event(run_id,file_id,"TRANSIENT_TRANSACTION_RESOLVED",{"attempt":attempt,"native_code":error.context.get("native_code"),"outcome":"no receipt; explicit pre-COMMIT rollback failure resolved"})
            delay=min(float(retry.get("maximum_backoff_seconds",15)),float(retry.get("initial_backoff_seconds",1))*(2**(attempt-1)))
            if retry.get("jitter",True):delay=random.uniform(0,delay)
            journal.event(run_id,file_id,"RETRY_BACKOFF",{"after_attempt":attempt,"seconds":delay})
            until=time.monotonic()+delay
            while time.monotonic()<until:
                checkpoint()
                time.sleep(min(0.05,max(0,until-time.monotonic())))


def _execute_one(payload):
    from .adapters.base import get_adapter
    from .resources import checkpoint
    config,run_id,file_id=payload["config"],payload["run_id"],payload["file_id"]
    adapter=get_adapter(config)
    publish,find_receipt=adapter.stage_validate_publish,adapter.read_receipt
    username,password=payload.get("username"),payload.get("password")
    journal=Journal(config["preservation"]["work_directory"]);codes=SeverityAccumulator()
    selection=journal.db.execute("SELECT selection_id FROM selections WHERE run_id=? AND file_id=?",(run_id,file_id)).fetchone()
    key=selection[0] if selection else file_id
    lease=journal.lease("selection:"+run_id+":"+key);lease.__enter__()
    try:
        item=journal.file(run_id,file_id);run=journal.run(run_id)
        journal.event(run_id,file_id,"WORKER_STARTED",{"phase":"publish"})
        checkpoint(force=True)
        if item["plan"] is None:
            codes.append(2)
            return {"file_id":file_id,"exit_code":2}
        try:
            _check_capture(item["artifact"])
            plan=_plan_from_dict(item["plan"])
            _verify_plan_artifact(item)
            if item["state"]=="NEEDS_DECISION":raise IngestError("FILE_DECISION_REQUIRED","This file needs a corrected linked reimport before publication")
            # Parse/schema meaning must remain unchanged on resume. Operational changes are allowed.
            original=json.loads(run["config"])
            baseline=hashes(original)
            current=hashes(config)
            if baseline["interpretation"]!=current["interpretation"] or baseline["mapping"]!=current["mapping"]:raise IngestError("RESUME_SEMANTIC_CHANGE","Parsing/schema/mapping changed; use reimport for a new linked interpretation")
            counts=local_evidence(journal,run_id,file_id,item["artifact"],item["decision"])
            counts.update({k:v for k,v in journal.file(run_id,file_id)["counts"].items() if k.startswith("plan_artifact") or k in ("publication_resolution","business_prepared","target_review")})
            if counts["ledger_sha256"]!=item["counts"].get("ledger_sha256"):raise IngestError("LEDGER_MISMATCH","Retained parsed record evidence differs from the approved full profile",5)
            if counts["committed"] and item.get("receipt") is None:
                raise IngestError("RECEIPT_MISSING","Local committed dispositions exist without their acknowledged receipt; no replay is permitted",5)
            if item["state"]=="RECONCILED":
                if counts["pending"] or counts["unresolved"] or not item.get("receipt") or counts["committed"]!=item["receipt"]["committed_records"]:raise IngestError("SOURCE_RECEIPT_COUNT_MISMATCH","Completed source dispositions differ from the acknowledged publication count",5)
                from .reconciliation import verify_destination
                resolved=find_receipt(config,plan,item["intent_id"],username,password)
                if resolved is None:raise IngestError("RECEIPT_MISSING","Completed run no longer has its destination receipt",5)
                verify_destination(config,plan,resolved,username=username,password=password)
                # Resume explicitly retries pending audit/failure materialization.
                resolved=_audit_backfill(journal,config,plan,resolved,item,username,password)
                item["receipt"]=resolved
                partial=counts["rejected"] or counts["excluded"] or config["verification"]["level"]!="full" or item["receipt"].get("assurance")=="lossy" or item["receipt"].get("details",{}).get("control_mirror_state")=="pending"
                codes.append(3 if partial else 0)
                return {"file_id":file_id,"exit_code":codes[-1],"source_bytes":item["artifact"]["size"]}
            if counts["rejected"]:_backfill_rejections(journal,config,plan,item,username,password)
            if counts["rejected"] and config["load"]["partial_publish"]=="none":raise IngestError("PARTIAL_PUBLICATION_DISABLED","Rejected source records block this file under partial_publish=none")
            if counts["unresolved"]:raise IngestError("UNRESOLVED_FRAMING","Resolve the captured unknown span before publishing",5)
            if config["load"]["mode"]=="replace" and (counts["rejected"] or counts["excluded"]):raise IngestError("REPLACE_INCOMPLETE","Replacement blocked by rejected or excluded records")
            if not counts["pending"] and not item["receipt"] and counts["rejected"]:
                journal.update_file(run_id,file_id,state="PARTIAL",counts=counts)
                codes.append(3)
                return {"file_id":file_id,"exit_code":codes[-1],"source_bytes":item["artifact"]["size"]}
            if not counts["pending"] and not item["receipt"] and not config["load"]["allow_empty_replace"]:raise IngestError("NO_DATA","No publishable records; destination untouched")
            if item.get("receipt") is not None or item["state"] in ("PUBLISHING","COMMIT_OUTCOME_UNKNOWN","COMMITTED"):
                resolution=_resolve_interrupted(journal,config,plan,item,username,password)
                if resolution["state"]=="COMMITTED":receipt=resolution["receipt"]
                elif resolution["state"]=="RESOLVED_ABSENT":
                    _record_resolution(journal,run_id,file_id,resolution["proof"])
                    receipt=_publish_with_retry(journal,config,plan,lambda:journal.records(run_id,file_id,"pending"),item["intent_id"],run_id,username,password,file_id)
                else:raise IngestError("COMMIT_OUTCOME_UNKNOWN","Resolver did not establish a committed or proven-absent publication",5)
            else:
                journal.update_file(run_id,file_id,state="PUBLISHING")
                receipt=_publish_with_retry(journal,config,plan,lambda:journal.records(run_id,file_id,"pending"),item["intent_id"],run_id,username,password,file_id)
            receipt_dict=asdict(receipt) if not isinstance(receipt,dict) else receipt
            if receipt_dict["committed_records"]!=counts["pending"]+counts["committed"]:raise IngestError("RECEIPT_COUNT_MISMATCH","Destination receipt differs from local source dispositions",5)
            journal.update_file(run_id,file_id,state="COMMITTED",receipt=receipt_dict)
            journal.committed(run_id,file_id)
            from .transports.faults import test_fault
            test_fault(config,"AFTER_LOCAL_RECEIPT_BEFORE_AUDIT")
            receipt_dict=_audit_backfill(journal,config,plan,receipt_dict,item,username,password)
            counts=local_evidence(journal,run_id,file_id,item["artifact"],item["decision"])
            counts.update({k:v for k,v in journal.file(run_id,file_id)["counts"].items() if k.startswith("plan_artifact") or k in ("publication_resolution","business_prepared","target_review")})
            partial=counts["rejected"] or counts["excluded"] or config["verification"]["level"]!="full" or receipt_dict.get("assurance")=="lossy"
            details=receipt_dict.get("details",{})
            if details.get("control_mirror_state") in ("pending","FAILED") or details.get("audit_mirror_state") in ("pending","FAILED") or details.get("failure_table_state")=="pending":partial=True
            journal.update_file(run_id,file_id,state="RECONCILED",counts=counts)
            codes.append(3 if partial else 0)
        except IngestError as exc:
            journal.incident(run_id,file_id,exc);codes.append(exc.exit_code)
            state="COMMIT_OUTCOME_UNKNOWN" if exc.exit_code==5 else "FAILED"
            journal.update_file(run_id,file_id,state=state,counts={**(journal.file(run_id,file_id).get("counts") or {}),**journal.counts(run_id,file_id)})
        code=worst(codes)
        return {"file_id":file_id,"exit_code":code,"source_bytes":item["artifact"]["size"],"processed_records":journal.counts(run_id,file_id)["committed"]}
    finally:
        journal.event(run_id,file_id,"WORKER_FINISHED",{"phase":"publish","exit_code":worst(codes) if codes else 4})
        lease.__exit__(None,None,None)
        journal.close()


def _verify_plan_artifact(item):
    from .resources import checkpoint
    path=item["counts"].get("plan_artifact");expected=item["counts"].get("plan_artifact_sha256")
    if not path or not expected:raise IngestError("PLAN_ARTIFACT_MISSING","Approved profile has no full plan artifact seal; automatic publication/reconciliation is blocked",5)
    digest=hashlib.sha256()
    try:
        with Path(path).open("rb") as source:
            while block:=source.read(1048576):checkpoint();digest.update(block)
        if digest.hexdigest()!=expected:raise IngestError("PLAN_ARTIFACT_MISMATCH","Full plan artifact hash differs from its durable profile",5)
        checkpoint(Path(path).stat().st_size*4)
        with Path(path).open("r",encoding="utf-8") as source:saved=json.load(source)
    except (OSError,ValueError) as error:raise IngestError("PLAN_ARTIFACT_MISMATCH","Full plan artifact is unavailable or invalid: "+str(error),5) from error
    if not isinstance(saved,dict):raise IngestError("PLAN_ARTIFACT_MISMATCH","Full plan artifact has an invalid document structure",5)
    if saved.get("format_version")!=1 or saved.get("run_id")!=item["run_id"] or saved.get("file_id")!=item["file_id"]:raise IngestError("PLAN_ARTIFACT_IDENTITY_MISMATCH","Full plan belongs to another source/run identity",5)
    if stable_hash(saved.get("plan"))!=stable_hash(item["plan"]):raise IngestError("PLAN_MISMATCH","Journal schema/mapping differs from the approved full plan artifact",5)
    if stable_hash(saved.get("source_artifact"))!=stable_hash(item["artifact"]):raise IngestError("PLAN_SOURCE_MISMATCH","Approved plan source artifact differs from the retained capture identity",5)


def _verify_local_one(payload):
    from .resources import checkpoint
    from .attachments import verify_references
    config,run_id,file_id=payload["config"],payload["run_id"],payload["file_id"]
    journal=Journal(config["preservation"]["work_directory"])
    lease=journal.lease("selection:"+run_id+":"+payload["selection_id"]);lease.__enter__()
    code=4
    try:
        journal.event(run_id,file_id,"WORKER_STARTED",{"phase":"local_preflight"})
        item=journal.file(run_id,file_id)
        if item.get("receipt") is None:
            from .target_review import reference as target_review_reference
            target_review_reference(journal,run_id,file_id,item.get("counts"))
        _check_capture(item["artifact"])
        verify_references(journal,run_id,file_id)
        if item["plan"] is not None and item["decision"] is not None:
            counts=local_evidence(journal,run_id,file_id,item["artifact"],item["decision"])
            if counts["ledger_sha256"]!=item["counts"].get("ledger_sha256"):
                raise IngestError("LEDGER_MISMATCH","Local parsed evidence differs from the approved profile",5)
            if item["counts"].get("projection_summary",{}).get("evidence_sha256")!=counts.get("projection_summary",{}).get("evidence_sha256"):
                raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Projection evidence differs from the approved profile",5)
            _verify_plan_artifact(item)
        code=0
        return {"file_id":file_id,"exit_code":0,"source_bytes":item["artifact"]["size"],"processed_records":sum(v for k,v in journal.counts(run_id,file_id).items() if k in ("pending","committed","rejected","excluded"))}
    except IngestError as error:
        code=error.exit_code
        journal.incident(run_id,file_id,error)
        journal.update_selection(run_id,payload["selection_id"],state="INTEGRITY_FAILED",error={"code":error.code,"message":str(error),"exit_code":error.exit_code,"context":error.context})
        return {"file_id":file_id,"exit_code":code,"error":error.code}
    finally:
        journal.event(run_id,file_id,"WORKER_FINISHED",{"phase":"local_preflight","exit_code":code})
        lease.__exit__(None,None,None);journal.close()


def _preflight_local_all(journal,config,run_id):
    from .scheduling import bounded_map
    metrics={};codes=SeverityAccumulator()
    def arguments():
        for selected in journal.selections(run_id):
            if selected.get("file_id") and journal.db.execute("SELECT 1 FROM files WHERE run_id=? AND file_id=?",(run_id,selected["file_id"])).fetchone():
                yield {"config":config,"run_id":run_id,"file_id":selected["file_id"],"selection_id":selected["selection_id"]}
    try:
        for argument,outcome in bounded_map(config,_verify_local_one,arguments(),metrics):
            if outcome["ok"]:codes.append(outcome["value"]["exit_code"])
            else:
                detail=outcome["error"]
                journal.incident(run_id,argument["file_id"],IngestError(detail["code"],detail["message"],detail["exit_code"],detail["context"]))
                journal.update_selection(run_id,argument["selection_id"],state="INTEGRITY_FAILED",error=detail)
                codes.append(detail["exit_code"])
    except IngestError as error:
        journal.incident(run_id,None,error);codes.append(error.exit_code)
    except KeyboardInterrupt:
        journal.incident(run_id,None,IngestError("CANCELLED","Local evidence preflight cancelled before publication",4));codes.append(4)
    return worst(codes),metrics


def _finish_run_audit(journal,config,run_id,code,state,username,password):
    if not config["audit"]["mirror_receipts_after_commit"]:
        return code,state,{"state":"disabled_by_policy"}
    receipts=journal.db.execute("SELECT COUNT(*) FROM files WHERE run_id=? AND receipt IS NOT NULL",(run_id,)).fetchone()[0]
    incidents=journal.db.execute("SELECT COUNT(*) FROM incidents WHERE run_id=?",(run_id,)).fetchone()[0]
    captures=journal.db.execute("SELECT COUNT(*) FROM files WHERE run_id=?",(run_id,)).fetchone()[0]
    if not receipts and not incidents and not captures:
        return code,state,{"state":"not_needed"}
    from .adapters.mssql.control import mirror_run_audit
    try:
        from .resources import checkpoint
        checkpoint(force=True)
        audit=mirror_run_audit(config,journal,run_id,username,password,final_state=state)
        checkpoint(force=True)
        return code,state,audit
    except IngestError as error:
        journal.event(run_id,None,"AUDIT_PENDING",{"code":error.code,"message":str(error),"context":error.context})
        previous_code=code
        code=worst([code,5 if error.exit_code==5 else 3])
        if code!=previous_code:
            state={0:"CLEAN_VERIFIED",2:"NEEDS_DECISION",3:"PARTIAL",4:"FAILED",5:"RECONCILIATION_FAILED"}[code]
        journal.state(run_id,state,code)
        return code,state,{"state":"pending","code":error.code,"reason":str(error)}


@_with_resources
def execute(config,run_id,username=None,password=None):
    from .scheduling import bounded_map
    journal=Journal(config["preservation"]["work_directory"]);codes=SeverityAccumulator();metrics={}
    lease=journal.lease("run:"+run_id);lease.__enter__()
    try:
        run=journal.run(run_id)
        try:_guard_resume_config(config,json.loads(run["config"]))
        except IngestError as error:
            journal.incident(run_id,None,error)
            journal.state(run_id,"NEEDS_DECISION",error.exit_code)
            return {"run_id":run_id,"state":"NEEDS_DECISION","files":_report_files(journal,run_id),"exit_code":error.exit_code,"errors":journal.errors(run_id)}
        policy=_publication_policy(journal,run_id)
        if run["inventory_state"]!="COMPLETE" or policy["failed_selections"] or policy["global_publication_blocked"]:
            recovered=recover_plan(config,run_id)
            policy=_publication_policy(journal,run_id)
            if recovered["exit_code"] and (policy["global_publication_blocked"] or not policy["publishable_files"]):
                code,state,audit=_finish_run_audit(journal,config,run_id,recovered["exit_code"],recovered["state"],username,password)
                recovered.update(exit_code=code,state=state,audit_state=audit["state"],audit=audit)
                return recovered
        if policy["global_publication_blocked"]:
            code=worst([policy["selection_exit_code"],4]);state="RECONCILIATION_FAILED" if code==5 else "FAILED"
            journal.state(run_id,state,code)
            code,state,audit=_finish_run_audit(journal,config,run_id,code,state,username,password)
            return {"run_id":run_id,"state":state,"exit_code":code,"code":"PLANNING_INCOMPLETE",**policy,"audit_state":audit["state"],"audit":audit,"files":_report_files(journal,run_id),"errors":journal.errors(run_id),"message":"A global inventory/durability/resource failure blocks publication"}
        codes.append(policy["selection_exit_code"])
        metrics["planning_incident_floor"]=json.loads(journal.run(run_id).get("execution_policy") or "{}").get("planning_incident_floor",0)
        file_count=journal.db.execute("SELECT COUNT(*) FROM files WHERE run_id=?",(run_id,)).fetchone()[0]
        if not policy["publishable_files"]:
            code=worst([policy["selection_exit_code"],2]);state={2:"NEEDS_DECISION",4:"FAILED",5:"RECONCILIATION_FAILED"}.get(code,"NEEDS_DECISION")
            journal.state(run_id,state,code)
            code,state,audit=_finish_run_audit(journal,config,run_id,code,state,username,password)
            return {"run_id":run_id,"state":state,"exit_code":code,"code":"PLANNING_INCOMPLETE",**policy,"audit_state":audit["state"],"audit":audit,"files":_report_files(journal,run_id),"errors":journal.errors(run_id),"message":"No fully profiled file is eligible for publication"}
        verification_code,verification_metrics=_preflight_local_all(journal,config,run_id)
        grouped=config["mapping"]["table_strategy"]=="per_directory"
        if grouped and not verification_code:
            from .grouping import groups,verify_group
            try:
                from .target_review import reference as target_review_reference
                for merged in groups(journal,run_id):
                    if merged.get("receipt") is None:target_review_reference(journal,run_id,merged["group_id"],merged.get("counts"))
                    if merged["plan"] is not None:verify_group(journal,run_id,merged["group_id"])
            except IngestError as error:
                journal.incident(run_id,None,error);verification_code=error.exit_code
        if verification_code:
            code=worst([policy["selection_exit_code"],verification_code])
            state="RECONCILIATION_FAILED" if code==5 else "FAILED"
            journal.state(run_id,state,code)
            code,state,audit=_finish_run_audit(journal,config,run_id,code,state,username,password)
            return {"run_id":run_id,"state":state,"exit_code":code,"code":"LOCAL_EVIDENCE_BLOCKED","global_publication_blocked":True,"publishable_files":0,"files":_report_files(journal,run_id),"errors":journal.errors(run_id),"execution_policy":verification_metrics,"audit_state":audit["state"]}
        previous_policy=json.loads(journal.run(run_id).get("execution_policy") or "{}")
        metrics["phase_peaks"]={"planning":previous_policy.get("sampled_peak_process_tree_rss_bytes",0),"local_preflight":verification_metrics.get("sampled_peak_process_tree_rss_bytes",0)}
        metrics["phase_seconds"]={"planning":previous_policy.get("elapsed_seconds",0),"local_preflight":verification_metrics.get("elapsed_seconds",0)}
        committed_before=journal.db.execute("SELECT COUNT(*) FROM records WHERE run_id=? AND disposition='committed'",(run_id,)).fetchone()[0]
        worker=_execute_one
        if grouped:
            from .group_execution import execute_one as worker
        def arguments():
            if grouped:
                from .grouping import groups
                for merged in groups(journal,run_id):
                    if merged["plan"] is not None and merged["state"] in ("PROFILED","PUBLISHING","COMMIT_OUTCOME_UNKNOWN","COMMITTED","RECONCILED","PARTIAL"):
                        yield {"config":config,"run_id":run_id,"group_id":merged["group_id"],"username":username,"password":password}
            else:
                for selected in journal.selections(run_id):
                    if selected["state"]!="PROFILED" or not selected.get("file_id"):continue
                    yield {"config":config,"run_id":run_id,"file_id":selected["file_id"],"username":username,"password":password}
        try:
            for argument,outcome in bounded_map(config,worker,arguments(),metrics):
                if outcome["ok"]:codes.append(outcome["value"]["exit_code"])
                else:
                    detail=outcome["error"]
                    error=IngestError(detail["code"],detail["message"],detail["exit_code"],detail["context"])
                    journal.incident(run_id,argument.get("file_id"),error)
                    # A vanished publisher requires receipt resolution before replay.
                    if grouped:
                        from .grouping import group,update
                        item=group(journal,run_id,argument["group_id"])
                        state="COMMIT_OUTCOME_UNKNOWN" if item["state"] in ("PUBLISHING","COMMITTED") else "FAILED"
                        update(journal,run_id,argument["group_id"],state=state)
                    else:
                        item=journal.file(run_id,argument["file_id"])
                        state="COMMIT_OUTCOME_UNKNOWN" if item["state"] in ("PUBLISHING","COMMITTED") else "FAILED"
                        journal.update_file(run_id,argument["file_id"],state=state)
                    codes.append(5 if state=="COMMIT_OUTCOME_UNKNOWN" else error.exit_code)
        except IngestError as error:
            journal.incident(run_id,None,error);codes.append(error.exit_code)
        except KeyboardInterrupt:
            journal.incident(run_id,None,IngestError("CANCELLED","Scheduling cancelled; resolve any in-flight destination receipts before retry",4));codes.append(4)
        code=worst(codes);state={0:"CLEAN_VERIFIED",2:"NEEDS_DECISION",3:"PARTIAL",4:"FAILED",5:"RECONCILIATION_FAILED"}[code]
        if code==5 and journal.db.execute("SELECT 1 FROM files WHERE run_id=? AND state='COMMIT_OUTCOME_UNKNOWN' LIMIT 1",(run_id,)).fetchone():state="COMMIT_OUTCOME_UNKNOWN"
        if grouped and code==5 and journal.db.execute("SELECT 1 FROM groups WHERE run_id=? AND state='COMMIT_OUTCOME_UNKNOWN' LIMIT 1",(run_id,)).fetchone():state="COMMIT_OUTCOME_UNKNOWN"
        if "phase_peaks" in metrics:
            metrics["phase_peaks"]["publication"]=metrics.get("sampled_peak_process_tree_rss_bytes",0)
            metrics["sampled_peak_process_tree_rss_bytes"]=max(metrics["phase_peaks"].values())
        metrics["phase_seconds"]["publication"]=metrics.get("elapsed_seconds",0)
        committed_after=journal.db.execute("SELECT COUNT(*) FROM records WHERE run_id=? AND disposition='committed'",(run_id,)).fetchone()[0]
        metrics["newly_committed_records"]=max(0,committed_after-committed_before)
        metrics["new_rows_per_second"]=metrics["newly_committed_records"]/metrics["elapsed_seconds"] if metrics.get("elapsed_seconds") else 0
        journal.event(run_id,None,"EXECUTION_PHASE_SUMMARY",{"phase_seconds":metrics["phase_seconds"],"phase_peaks":metrics["phase_peaks"],"newly_committed_records":metrics["newly_committed_records"]})
        journal.state(run_id,state,code)
        journal.policy(run_id,metrics)
        code,state,audit=_finish_run_audit(journal,config,run_id,code,state,username,password)
        metrics["audit_state"]=audit["state"]
        journal.policy(run_id,metrics)
        files=_report_files(journal,run_id)
        merged_report={}
        if grouped:
            from .grouping import report_groups
            merged_report["groups"]=report_groups(journal,run_id)
        return {"run_id":run_id,"state":state,**policy,**merged_report,"audit_state":audit["state"],"audit":audit,"files":files,"file_count":file_count,"files_report_truncated":file_count>len(files),"exit_code":code,"errors":journal.errors(run_id),"execution_policy":metrics,"next_commands":[f"status --run-id {run_id}",f"show-errors --run-id {run_id}",f"resume --run-id {run_id}"] if code else []}
    finally:
        lease.__exit__(None,None,None)
        journal.close()


def status(config,run_id=None):
    journal=Journal(config["preservation"]["work_directory"])
    try:
        if not run_id:
            from .preview import BoundedPreview
            preview=BoundedPreview(max(0,min(1048576,config["limits"]["max_queue_bytes"])-256))
            total=journal.db.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
            for row in journal.db.execute("SELECT run_id,state,created,updated,exit_code FROM runs ORDER BY created DESC"):
                if not preview.append(dict(row)) or preview.full:break
            return {"runs":preview.items,"run_count":total,"runs_report_truncated":total>len(preview.items),"details_retained_in_journal":total>len(preview.items),"exit_code":0}
        run=journal.run(run_id);run.pop("config",None)
        run["execution_policy"]=json.loads(run.get("execution_policy") or "{}")
        files=_report_files(journal,run_id)
        count=journal.db.execute("SELECT COUNT(*) FROM files WHERE run_id=?",(run_id,)).fetchone()[0]
        from .grouping import report_groups
        return {**run,"files":files,"groups":report_groups(journal,run_id),"file_count":count,"files_report_truncated":count>len(files)}
    finally:journal.close()

def show_errors(config,run_id=None):
    from .preview import BoundedPreview
    journal=Journal(config["preservation"]["work_directory"])
    try:
        budget=min(1048576,config["limits"]["max_queue_bytes"])
        incidents=journal.errors(run_id,limit=1000,max_bytes=max(0,budget//2))
        incident_bytes=sum(len(chunk) for chunk in json.JSONEncoder(ensure_ascii=True).iterencode(incidents))
        preview=BoundedPreview(max(0,budget-incident_bytes-512));last=None
        where="disposition IN ('rejected','unresolved')"+(" AND run_id=?" if run_id is not None else "")
        parameters=[run_id] if run_id is not None else []
        total=journal.db.execute("SELECT COUNT(*) FROM records WHERE "+where,parameters).fetchone()[0]
        while not preview.full:
            remaining=max(0,preview.max_bytes-preview.used-512)
            errors_expr="CASE WHEN json_valid(payload) THEN json_extract(payload,'$.errors') ELSE NULL END"
            sql="SELECT rowid,run_id,file_id,record_id,ordinal,CASE WHEN json_valid(payload) THEN json_extract(payload,'$.byte_start') ELSE NULL END,CASE WHEN json_valid(payload) THEN json_extract(payload,'$.byte_end') ELSE NULL END,CASE WHEN length(CAST(("+errors_expr+") AS BLOB))<=? THEN "+errors_expr+" ELSE NULL END,length(CAST(("+errors_expr+") AS BLOB)) FROM records WHERE "+where
            args=[remaining,*parameters]
            if last is not None:sql+=" AND rowid<?";args.append(last)
            row=journal.db.execute(sql+" ORDER BY rowid DESC LIMIT 1",args).fetchone()
            if row is None:break
            last=row[0];entry={"run_id":row[1],"file_id":row[2],"record_id":row[3],"ordinal":row[4],"byte_start":row[5],"byte_end":row[6]}
            if row[7] is not None:
                try:entry["errors"]=json.loads(row[7])
                except (ValueError,TypeError,RecursionError):entry["errors"]=[];entry["errors_truncated"]=True
            else:entry["errors"]=[];entry["errors_truncated"]=True
            if entry.get("errors_truncated"):entry.update(errors_bytes=row[8],retained_in_journal=True,evidence_table="records.payload")
            if not preview.append(entry):
                entry.update(errors=[],errors_truncated=True,errors_bytes=row[8],retained_in_journal=True,evidence_table="records.payload")
                # A large diagnostic is replaced by its compact identity/reference.
                preview.total-=1
                if not preview.append(entry):break
        return {"incidents":incidents,"record_errors":preview.items,"record_error_count":total,"record_errors_truncated":total>len(preview.items),"limit":1000,"preview_byte_budget":budget,"details_retained_in_journal":True,"exit_code":0}
    finally:journal.close()


@_with_resources
def reconcile(config,run_id,username=None,password=None):
    from .publication import find_receipt
    from .reconciliation import verify_destination
    from .preview import BoundedPreview
    journal=Journal(config["preservation"]["work_directory"]);results=BoundedPreview(max(0,min(1048576,config["limits"]["max_queue_bytes"])-512));partial=False
    try:
        run=journal.run(run_id)
        expected=journal.db.execute("SELECT COUNT(*),COUNT(s.file_id),COUNT(DISTINCT s.file_id),SUM(CASE WHEN f.file_id IS NULL THEN 1 ELSE 0 END) FROM selections s LEFT JOIN files f ON f.run_id=s.run_id AND f.file_id=s.file_id WHERE s.run_id=? AND s.status IN ('selected','unreadable')",(run_id,)).fetchone()
        captured=journal.db.execute("SELECT COUNT(*) FROM files WHERE run_id=?",(run_id,)).fetchone()[0]
        if run["inventory_state"]!="COMPLETE" or not captured or expected[0]!=captured or expected[1]!=captured or expected[2]!=captured or expected[3]:raise IngestError("RECONCILE_INVENTORY_INCOMPLETE","Reconciliation requires a complete nonempty one-to-one selection/capture inventory",5)
        pending_audit=config["audit"]["mirror_receipts_after_commit"] and json.loads(run.get("execution_policy") or "{}").get("audit_state") in ("pending","FAILED")
        if config["mapping"]["table_strategy"]=="per_directory":
            from .group_execution import reconcile as reconcile_groups
            result=reconcile_groups(journal,config,run_id,username,password)
            if pending_audit:result["exit_code"]=worst([result["exit_code"],3]);result["audit_state"]="pending"
            return result
        for item in journal.files(run_id):
            if item["plan"] is None or item["decision"] is None:raise IngestError("ACCOUNTING_PENDING","Source has no approved complete interpretation/profile",5)
            source=_check_capture(item["artifact"]);counts=local_evidence(journal,run_id,item["file_id"],item["artifact"],item["decision"])
            if item["counts"].get("ledger_sha256")!=counts["ledger_sha256"]:raise IngestError("LEDGER_MISMATCH","Retained parsed source evidence differs from its approved immutable ledger",5)
            _verify_plan_artifact(item)
            if not item["receipt"] and counts["rejected"] and not counts["pending"] and not counts["unresolved"]:
                partial=True
                results.append({"file_id":item["file_id"],"source":source,"counts":counts,"destination":{"state":"NO_PUBLICATION_ALL_RECORDS_REJECTED"},"failures":item.get("failures",{})})
                continue
            if not item["receipt"]:raise IngestError("RECEIPT_MISSING","No committed destination receipt exists for this file",5)
            plan=_plan_from_dict(item["plan"])
            receipt=find_receipt(config,plan,item["intent_id"],username,password)
            if receipt is None:raise IngestError("RECEIPT_MISSING","Destination no longer has the receipt; possible restore or replacement",5)
            if counts["pending"] or counts["unresolved"]:raise IngestError("ACCOUNTING_PENDING","Records or byte spans remain unresolved",5)
            if counts["committed"]!=receipt["committed_records"]:raise IngestError("SOURCE_RECEIPT_COUNT_MISMATCH","Committed source dispositions differ from the destination receipt",5)
            verified=verify_destination(config,plan,item["receipt"],username=username,password=password)
            local_details=item["receipt"].get("details",{})
            audit_pending=any(local_details.get(key) in ("pending","FAILED") for key in ("control_mirror_state","audit_mirror_state","failure_table_state"))
            partial=partial or bool(counts["rejected"] or counts["excluded"] or receipt.get("assurance","full")!="full" or audit_pending)
            results.append({"file_id":item["file_id"],"source":source,"counts":counts,"destination":verified,"assurance":receipt.get("assurance","full"),"audit_pending":audit_pending})
        return {"run_id":run_id,"files":results.items,"file_count":results.total,"files_report_truncated":results.truncated,"all_files_verified":True,"exit_code":3 if pending_audit or partial or config["verification"]["level"]!="full" else 0}
    except IngestError as error:
        journal.incident(run_id,None,error)
        journal.state(run_id,"RECONCILIATION_FAILED" if error.exit_code==5 else "NEEDS_DECISION" if error.exit_code==2 else "FAILED",error.exit_code)
        raise
    finally:journal.close()
