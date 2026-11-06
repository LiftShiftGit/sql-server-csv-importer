"""Atomic group publication over streamed source and contributor evidence."""
from dataclasses import asdict
import copy
import hashlib
import json
import os
from pathlib import Path
from .errors import IngestError
from .journal import Journal
from .models import ColumnProfile
from .config import effective_for_file,stable_hash
from . import grouping as g


class GroupJournal:
    """Adapt existing same-intent retry machinery without synthetic source files."""
    def __init__(self,journal):self.journal=journal
    def __getattr__(self,name):return getattr(self.journal,name)
    def file(self,run_id,gid):return {**g.group(self.journal,run_id,gid),'file_id':gid}
    def update_file(self,run_id,gid,**values):g.update(self.journal,run_id,gid,**values)
    def counts(self,run_id,gid):return g.group_counts(self.journal,run_id,gid)
    def event(self,run_id,gid,kind,details):self.journal.event(run_id,None,kind,{**details,'group_id':gid})
    def incident(self,run_id,gid,error):
        error.context.setdefault('group_id',gid);self.journal.incident(run_id,None,error)


def _required_history(journal,item):
    """Write all locally acknowledged contributors, preserving omitted paths."""
    from .capture import _atomic_replace,_sync_directory
    # The normalized history is a cache. Every original local ACK manifest is
    # authoritative, so a lost/edited cache cannot erase an omitted contributor.
    for row in journal.db.execute('SELECT run_id,intent_id,manifest,receipt FROM groups WHERE group_id=? AND receipt IS NOT NULL ORDER BY rowid',(item['group_id'],)):
        old_run,old_intent=row[0],row[1];manifest=json.loads(row[2]);receipt=json.loads(row[3])
        acknowledged=receipt.get('details',{}).get('source_group',{})
        if any(acknowledged.get(key)!=manifest.get(key) for key in ('group_id','manifest_sha256','contributor_count','source_hash')):
            raise IngestError('GROUP_LOCAL_HISTORY_CONFLICT','Acknowledged local contributor manifest differs from its immutable receipt',5,{'intent_id':old_intent})
        try:
            if g.source_manifest_hash(g.iter_manifest(manifest))!=receipt['source_hash']:
                raise IngestError('GROUP_LOCAL_HISTORY_CONFLICT','Acknowledged composite source identity no longer matches its manifest',5,{'intent_id':old_intent})
            with journal.db:
                accepted=0
                for source in g.iter_manifest(manifest):
                    accepted+=int(source['records'])
                    journal.db.execute('INSERT INTO group_contributors VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(group_id,path_hash) DO UPDATE SET source_sha256=excluded.source_sha256,source_size=excluded.source_size,metadata=excluded.metadata,last_run_id=excluded.last_run_id,last_intent=excluded.last_intent',(item['group_id'],source['file_id'],source['relative_path'],source['sha256'],source['size'],json.dumps(source,ensure_ascii=True,separators=(',',':')),old_run,old_intent))
                if accepted!=receipt['committed_records']:raise IngestError('GROUP_LOCAL_HISTORY_CONFLICT','Acknowledged manifest accepted counts differ from its receipt',5,{'intent_id':old_intent})
        except OSError as error:raise IngestError('GROUP_LOCAL_HISTORY_UNAVAILABLE','An acknowledged local contributor manifest is unavailable: '+str(error),5,{'intent_id':old_intent}) from error
    folder=Path(item['manifest']['manifest_path']).parent
    path=folder/'required-contributors.jsonl';partial=folder/'required-contributors.partial.jsonl'
    digest=hashlib.sha256();count=0
    with partial.open('wb') as stream:
        for row in journal.db.execute('SELECT metadata FROM group_contributors WHERE group_id=? ORDER BY relative_path COLLATE BINARY',(item['group_id'],)):
            raw=row[0].encode('utf-8')+b'\n';stream.write(raw);digest.update(raw);count+=1
        stream.flush();os.fsync(stream.fileno())
    _atomic_replace(partial,path);_sync_directory(folder)
    return {'required_manifest_path':str(path),'required_manifest_sha256':digest.hexdigest(),'required_contributor_count':count}


def _acknowledge(journal,run_id,gid,receipt):
    from .pipeline import local_evidence
    # The authoritative group ACK precedes any member disposition change.
    g.update(journal,run_id,gid,state='COMMITTED',receipt=receipt)
    for source in g.iter_manifest(g.group(journal,run_id,gid)['manifest']):
        with journal.db:journal.db.execute('INSERT INTO group_contributors VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(group_id,path_hash) DO UPDATE SET source_sha256=excluded.source_sha256,source_size=excluded.source_size,metadata=excluded.metadata,last_run_id=excluded.last_run_id,last_intent=excluded.last_intent',(gid,source['file_id'],source['relative_path'],source['sha256'],source['size'],json.dumps(source,ensure_ascii=True,separators=(',',':')),run_id,receipt['intent_id']))
    for member in g.members(journal,run_id,gid):
        fid=member['file_id'];item=journal.file(run_id,fid)
        accepted=journal.db.execute("SELECT COUNT(*) FROM group_records WHERE run_id=? AND group_id=? AND source_file_id=? AND disposition IN ('pending','committed')",(run_id,gid,fid)).fetchone()[0]
        pointer={'kind':'group_member','group_id':gid,'publication_intent_id':receipt['intent_id'],'group_receipt_sha256':stable_hash(receipt),'committed_records':accepted,'assurance':receipt['assurance']}
        # One SQLite transaction makes each source's pointer and accepted rows agree.
        with journal.db:
            journal.db.execute("UPDATE records SET disposition='committed' WHERE run_id=? AND file_id=? AND disposition='pending' AND record_id IN (SELECT record_id FROM group_records WHERE run_id=? AND group_id=? AND disposition IN ('pending','committed'))",(run_id,fid,run_id,gid))
            journal.db.execute("UPDATE group_records SET disposition='committed' WHERE run_id=? AND group_id=? AND source_file_id=? AND disposition='pending'",(run_id,gid,fid))
            journal.db.execute("UPDATE files SET state='COMMITTED',receipt=? WHERE run_id=? AND file_id=?",(json.dumps(pointer),run_id,fid))
        counts=local_evidence(journal,run_id,fid,item['artifact'],item['decision'])
        counts.update({k:v for k,v in item['counts'].items() if k.startswith('plan_artifact') or k in ('publication_resolution','business_prepared','target_review')})
        journal.update_file(run_id,fid,state='RECONCILED',counts=counts)
    item=g.group(journal,run_id,gid)
    g.update(journal,run_id,gid,counts={**item['counts'],**g.group_counts(journal,run_id,gid)})


def _backfill_source_failures(journal,config,item,plan,username,password):
    from .pipeline import _backfill_rejections
    from .adapters.mssql.control import failure_table_name
    totals={"rejected":0,"excluded":0,"pending":0,"committed":0,"failure_pending":False}
    for member in g.members(journal,item["run_id"],item["group_id"]):
        source=journal.file(item["run_id"],member["file_id"]);counts=journal.counts(item["run_id"],member["file_id"])
        for key in ("rejected","excluded","pending","committed"):totals[key]+=counts[key]
        if counts["rejected"]:
            source["failures"]=source.get("failures") or {}
            source["failures"].setdefault("name",failure_table_name(plan.table,item["run_id"]+":"+member["file_id"]))
            failure=_backfill_rejections(journal,config,plan,source,username,password)
            totals["failure_pending"]=totals["failure_pending"] or failure.get("state")=="pending"
    return totals


def _audit(journal,config,item,plan,receipt,username,password):
    from .adapters.mssql.control import mirror_receipt,mirror_file,mirror_group_contributors,failure_table_name
    from .pipeline import _backfill_rejections
    result=copy.deepcopy(receipt);details=result.setdefault('details',{})
    partial=False
    from .adapters.mssql.contributors import mirror_destination_contributions
    try:details["destination_contribution_history"]=mirror_destination_contributions(config,plan,result,item['manifest'],username,password)
    except IngestError as error:
        if error.exit_code==5:raise
        journal.incident(item['run_id'],None,error);details["destination_contribution_history"]={"state":"pending","code":error.code};partial=True
    if config['audit']['mirror_receipts_after_commit']:
        try:
            mirror_receipt(config,plan,result,username,password)
            # Source profiles retain source ordinals/names and canonical target types.
            for member in g.members(journal,item['run_id'],item['group_id']):
                source=journal.file(item['run_id'],member['file_id']);profiles=[]
                for column,index in zip(g._plan(source['plan']).columns,member['mapping'],strict=True):
                    mapped=copy.deepcopy(plan.columns[index]);mapped.ordinal=column.ordinal;mapped.original_name=column.original_name
                    mapped.stats={**mapped.stats,'source_candidate':asdict(column)};profiles.append(mapped)
                effective=effective_for_file(config,source['artifact']['relative_path'])
                mirror_file(effective,plan,result,source['artifact'],username,password,source_profiles=profiles,source_interpretation_hash=source['plan']['interpretation_hash'])
            mirror_group_contributors(config,plan,result,item['manifest'],username,password)
            details['control_mirror_state']='VERIFIED'
        except IngestError as error:
            if error.exit_code==5:raise
            journal.incident(item['run_id'],None,error);details['control_mirror_state']='pending';partial=True
    else:details['control_mirror_state']='disabled_by_policy'
    failures=_backfill_source_failures(journal,config,item,plan,username,password)
    partial=partial or bool(failures["rejected"] or failures["excluded"] or failures["failure_pending"])
    g.update(journal,item['run_id'],item['group_id'],receipt=result)
    # Refresh pointers to the final locally enriched group receipt, idempotently.
    _acknowledge(journal,item['run_id'],item['group_id'],result)
    return result,partial


def execute_one(payload):
    from .pipeline import _publish_with_retry,_resolve_interrupted,_record_resolution
    from .publication import find_receipt
    from .reconciliation import verify_destination
    config,run_id,gid=payload['config'],payload['run_id'],payload['group_id']
    username,password=payload.get('username'),payload.get('password')
    journal=Journal(config['preservation']['work_directory']);g.initialize(journal);lease=journal.lease('group:'+run_id+':'+gid);lease.__enter__()
    code=4
    try:
        journal.event(run_id,None,'GROUP_WORKER_STARTED',{'group_id':gid,'phase':'publish'})
        item=g.verify_group(journal,run_id,gid);plan=g._plan(item['plan']);counts=g.group_counts(journal,run_id,gid)
        scoped=copy.deepcopy(config);scoped['_publication_group']={**item['manifest'],**_required_history(journal,item)}
        if config['mapping']['name_collision']=='path_error':
            from .planning import name_claim_source
            first=next(g.members(journal,run_id,gid))
            scoped['_filename_source_path']=name_claim_source(config,first['relative_path'])
        proxy=GroupJournal(journal)
        if item['receipt'] is not None or item['state'] in ('PUBLISHING','COMMIT_OUTCOME_UNKNOWN','COMMITTED','RECONCILED'):
            resolution=_resolve_interrupted(proxy,scoped,plan,{**item,'file_id':gid},username,password)
            if resolution['state']=='COMMITTED':receipt=resolution['receipt']
            elif resolution['state']=='RESOLVED_ABSENT':
                _record_resolution(proxy,run_id,gid,resolution['proof'])
                receipt=_publish_with_retry(proxy,scoped,plan,lambda:g.records(journal,run_id,gid,'pending'),item['intent_id'],run_id,username,password,gid)
            else:raise IngestError('COMMIT_OUTCOME_UNKNOWN','Group publication outcome remains unresolved',5)
        else:
            failures=_backfill_source_failures(journal,scoped,item,plan,username,password)
            if item['manifest']['has_unresolved']:raise IngestError('GROUP_UNRESOLVED','A source contributor has unresolved framing',5)
            if not item['manifest']['complete_input'] and (config['load']['mode']=='replace' or config['load']['partial_publish']=='none'):raise IngestError('GROUP_INCOMPLETE','Selected merged input contains rejected or excluded records')
            if not counts['pending'] and failures['rejected']:
                g.update(journal,run_id,gid,state='PARTIAL',error=None)
                for member in g.members(journal,run_id,gid):journal.update_file(run_id,member['file_id'],state='PARTIAL')
                code=3
                return {'group_id':gid,'exit_code':3,'source_bytes':item['manifest']['source_bytes'],'processed_records':0}
            if not counts['pending'] and not(config['load']['mode']=='replace' and config['load']['allow_empty_replace']):raise IngestError('NO_DATA','No accepted merged rows; destination untouched')
            receipt=_publish_with_retry(proxy,scoped,plan,lambda:g.records(journal,run_id,gid,'pending'),item['intent_id'],run_id,username,password,gid)
        receipt=receipt if isinstance(receipt,dict) else asdict(receipt)
        if receipt['committed_records']!=counts['pending']+counts['committed']:raise IngestError('RECEIPT_COUNT_MISMATCH','Group receipt differs from accepted source rows',5)
        _acknowledge(journal,run_id,gid,receipt)
        from .transports.faults import test_fault
        test_fault(scoped,"AFTER_LOCAL_RECEIPT_BEFORE_AUDIT")
        receipt,partial=_audit(journal,scoped,item,plan,receipt,username,password)
        code=3 if partial or receipt['assurance']!='full' or config['verification']['level']!='full' else 0
        g.update(journal,run_id,gid,state='RECONCILED',error=None)
        return {'group_id':gid,'exit_code':code,'source_bytes':item['manifest']['source_bytes'],'processed_records':receipt['committed_records']}
    except IngestError as error:
        code=error.exit_code;journal.incident(run_id,None,error)
        prior=g.group(journal,run_id,gid)
        state='COMMIT_OUTCOME_UNKNOWN' if error.exit_code==5 and prior['state'] in ('PUBLISHING','COMMITTED','RECONCILED','COMMIT_OUTCOME_UNKNOWN') else ('NEEDS_DECISION' if code==2 else 'FAILED')
        g.update(journal,run_id,gid,state=state,error={'code':error.code,'message':str(error),'exit_code':code,'context':error.context})
        return {'group_id':gid,'exit_code':code}
    finally:
        journal.event(run_id,None,'GROUP_WORKER_FINISHED',{'group_id':gid,'phase':'publish','exit_code':code})
        lease.__exit__(None,None,None);journal.close()


def reconcile(journal,config,run_id,username,password):
    from .pipeline import _check_capture,local_evidence,_verify_plan_artifact
    from .publication import find_receipt
    from .reconciliation import verify_destination
    from .preview import BoundedPreview
    budget=max(0,min(1048576,config['limits']['max_queue_bytes'])-512)
    files=BoundedPreview(budget//2);results=BoundedPreview(budget-budget//2);partial=False
    if journal.run(run_id)['inventory_state']!='COMPLETE' or not g.available(journal) or not journal.db.execute('SELECT 1 FROM groups WHERE run_id=? LIMIT 1',(run_id,)).fetchone():
        raise IngestError('GROUP_INVENTORY_INCOMPLETE','A complete nonempty source-group inventory is required for reconciliation',5)
    for source in journal.files(run_id):
        if source['plan'] is None or source['decision'] is None:raise IngestError('ACCOUNTING_PENDING','A contributor has no complete interpretation/profile',5)
        checked=_check_capture(source['artifact']);counts=local_evidence(journal,run_id,source['file_id'],source['artifact'],source['decision'])
        if source['counts'].get('ledger_sha256')!=counts['ledger_sha256']:raise IngestError('LEDGER_MISMATCH','Source contributor differs from its approved immutable ledger',5)
        _verify_plan_artifact(source)
        if source.get('receipt') and counts['committed']!=source['receipt']['committed_records']:raise IngestError('SOURCE_RECEIPT_COUNT_MISMATCH','Source contributor committed marks differ from its group acknowledgment',5)
        if counts['pending'] or counts['unresolved']:raise IngestError('ACCOUNTING_PENDING','Source group contributor remains unresolved',5)
        files.append({'file_id':source['file_id'],'source':checked,'counts':counts});partial=partial or bool(counts['rejected'] or counts['excluded'])
    for item in g.groups(journal,run_id):
        g.verify_group(journal,run_id,item['group_id'])
        counts=g.group_counts(journal,run_id,item['group_id'])
        if not item['receipt']:
            rejected=journal.db.execute("SELECT COUNT(*) FROM records r JOIN group_members m ON m.run_id=r.run_id AND m.file_id=r.file_id WHERE m.run_id=? AND m.group_id=? AND r.disposition='rejected'",(run_id,item['group_id'])).fetchone()[0]
            if not counts['pending'] and not counts['committed'] and rejected:
                partial=True;results.append({'group_id':item['group_id'],'destination':{'state':'NO_PUBLICATION_ALL_RECORDS_REJECTED'}});continue
            raise IngestError('RECEIPT_MISSING','Merged source group has no acknowledged receipt',5)
        if counts['pending'] or counts['committed']!=item['receipt']['committed_records']:raise IngestError('GROUP_RECEIPT_COUNT_MISMATCH','Merged committed dispositions differ from the acknowledged group receipt',5)
        plan=g._plan(item['plan']);receipt=find_receipt(config,plan,item['intent_id'],username,password)
        if receipt is None:raise IngestError('RECEIPT_MISSING','Acknowledged group receipt is missing at the destination',5)
        for member in g.members(journal,run_id,item['group_id']):
            source=journal.file(run_id,member['file_id']);pointer=source.get('receipt') or {}
            accepted=journal.db.execute("SELECT COUNT(*) FROM group_records WHERE run_id=? AND group_id=? AND source_file_id=? AND disposition='committed'",(run_id,item['group_id'],member['file_id'])).fetchone()[0]
            if pointer.get('kind')!='group_member' or pointer.get('group_id')!=item['group_id'] or pointer.get('publication_intent_id')!=item['intent_id'] or pointer.get('group_receipt_sha256')!=stable_hash(item['receipt']) or pointer.get('committed_records')!=accepted:
                raise IngestError('GROUP_MEMBER_ACK_MISMATCH','Source contributor acknowledgment is incomplete or differs from its authoritative group receipt; resume to repair derived metadata',5,{'relative_path':member['relative_path']})
        result=verify_destination(config,plan,item['receipt'],username=username,password=password)
        from .adapters.mssql.contributors import mirror_destination_contributions
        result['contribution_history']=mirror_destination_contributions(config,plan,item['receipt'],item['manifest'],username,password)
        if config['audit']['mirror_receipts_after_commit']:
            from .adapters.mssql.control import mirror_receipt,mirror_group_contributors
            mirror_receipt(config,plan,item['receipt'],username,password)
            mirror_group_contributors(config,plan,item['receipt'],item['manifest'],username,password)
        details=item['receipt'].get('details',{})
        partial=partial or receipt['assurance']!='full' or any(details.get(key) in ('pending','FAILED') for key in ('control_mirror_state','audit_mirror_state','failure_table_state'))
        results.append({'group_id':item['group_id'],'destination':result})
    return {'run_id':run_id,'files':files.items,'file_count':files.total,'files_report_truncated':files.truncated,'groups':results.items,'group_count':results.total,'groups_report_truncated':results.truncated,'all_files_verified':True,'all_groups_verified':True,'exit_code':3 if partial or config['verification']['level']!='full' else 0}
