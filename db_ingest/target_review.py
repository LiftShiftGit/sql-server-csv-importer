"""Optional read-only destination review, sealed separately from source planning."""
import hashlib
import json
import os
import uuid
from pathlib import Path
from .errors import IngestError
from .adapters.mssql.ddl import qualified

MAX_REVIEW_BYTES=33554432


def _snapshot_hash(value):
    from .resources import checkpoint
    digest=hashlib.sha256()
    for chunk in json.JSONEncoder(sort_keys=True,ensure_ascii=True,separators=(',',':'),default=str).iterencode(value):
        if len(chunk)>1048576:checkpoint(len(chunk)*2)
        digest.update(chunk.encode('utf-8'))
    return digest.hexdigest()


def _read(pointer):
    from .resources import checkpoint
    path=Path(pointer['path']);size=path.stat().st_size
    if size>MAX_REVIEW_BYTES:raise ValueError('review exceeds32MiB')
    checkpoint(size*4)
    with path.open('rb') as stream:raw=stream.read(size+1)
    if len(raw)!=size or hashlib.sha256(raw).hexdigest()!=pointer['sha256']:raise ValueError('review bytes differ from original seal')
    return json.loads(raw)


def snapshot(cur, plan):
    from .publication import _database_guid, _recovery_fork
    from .adapters.mssql.evolution import inspection
    target=qualified(plan.schema,plan.table)
    oid=cur.execute('SELECT OBJECT_ID(?)',target).fetchone()[0]
    if oid is None:
        visible=cur.execute("SELECT HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION')").fetchone()[0]
        if visible!=1:raise IngestError('TARGET_REVIEW_VISIBILITY','Target is absent or hidden; database VIEW DEFINITION is required to establish absence',2)
    else:
        visible=cur.execute("SELECT HAS_PERMS_BY_NAME(?,'OBJECT','VIEW DEFINITION')",target).fetchone()[0]
        if visible!=1:raise IngestError('TARGET_REVIEW_VISIBILITY','Target metadata visibility is incomplete; no schema absence inferred',2)
    metadata=inspection(cur,target) if oid is not None else None
    return {'database_guid':_database_guid(cur),'recovery_fork_guid':_recovery_fork(cur),'target':target,'object_id':oid,'metadata':metadata}


def difference(plan, observed):
    proposed=[{'name':c.target_name,'type':c.sql_type,'nullable':c.nullable} for c in plan.columns]
    if observed['metadata'] is None:return {'create_target':True,'proposed_columns':proposed,'changes':[{'action':'create target'}]}
    actual={row[0]:row for row in observed['metadata']['columns'] if not row[0].startswith('__ingest_')}
    changes=[]
    for column in proposed:
        row=actual.pop(column['name'],None)
        if row is None:changes.append({'action':'add mapped column','proposed':column});continue
        base,length,precision,scale=row[2:6]
        if base in ('nvarchar','varchar','varbinary'):kind=base+'('+str('max' if length==-1 else length//2 if base=='nvarchar' else length)+')'
        elif base in ('decimal','numeric'):kind='decimal('+str(precision)+','+str(scale)+')'
        elif base in ('time','datetime2','datetimeoffset'):kind=base+'('+str(scale)+')'
        else:kind=base
        if (kind,bool(row[6]))!=(column['type'],column['nullable']):changes.append({'action':'compare type/nullability','current':{'name':row[0],'type':kind,'nullable':bool(row[6])},'proposed':column})
    changes.extend({'action':'existing unmapped column','name':name} for name in actual)
    return {'create_target':False,'proposed_columns':proposed,'changes':changes,'basis':'candidate named comparison; final compatibility/approved ALTER is revalidated under publication locks'}


def review(config, report, username=None, password=None):
    from .journal import Journal,utcnow
    from .pipeline import _plan_from_dict
    from .adapters.mssql.connect import connect
    from .resources import resource_scope,checkpoint
    from .capture import _atomic_replace,_sync_directory
    from .transports.odbc import choose_transport
    journal=Journal(config['preservation']['work_directory']);run_id=report['run_id'];reviewed=0
    try:
        with journal.lease('run:'+run_id),resource_scope(config):
            if config['mapping']['table_strategy']=='per_directory':
                from . import grouping
                items=grouping.groups(journal,run_id)
            else:items=journal.files(run_id)
            for item in items:
                if not item.get('plan') or item.get('receipt'):continue
                if (item.get('counts') or {}).get('target_review'):
                    reference(journal,run_id,item.get('file_id',item.get('group_id')),item['counts'])
                    continue # Existing authority is immutable; never rebase it onto current SQL.
                key=item.get('file_id',item.get('group_id'))
                journal.event(run_id,None,'TARGET_REVIEW_REQUESTED',{'subject_id':key,'intent_id':item['intent_id']})
                plan=_plan_from_dict(item['plan']);conn=connect(config,username,password,plan.database)
                try:
                    cur=conn.cursor();cur.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
                    observed=snapshot(cur,plan)
                    permissions={}
                    for permission in ('SELECT','INSERT','DELETE','ALTER','VIEW DEFINITION'):
                        permissions[permission]=cur.execute("SELECT HAS_PERMS_BY_NAME(?,'OBJECT',?)",qualified(plan.schema,plan.table),permission).fetchone()[0] if observed['object_id'] is not None else None
                    permissions['CREATE TABLE']=cur.execute("SELECT HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','CREATE TABLE')").fetchone()[0]
                    permissions['CREATE SCHEMA']=cur.execute("SELECT HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','CREATE SCHEMA')").fetchone()[0]
                    permissions['basis']='read-only HAS_PERMS_BY_NAME observations; NULL is unknown/not applicable; no write or ALTER probe'
                finally:
                    conn.rollback();conn.close()
                data={'version':1,'run_id':run_id,'intent_id':item['intent_id'],'server':config['destination']['server'],'database':plan.database,'source_hash':plan.source_hash,'interpretation_hash':plan.interpretation_hash,'mapping_hash':plan.mapping_hash,'reviewed_at_utc':utcnow(),'snapshot':observed,'snapshot_hash':_snapshot_hash(observed),'schema_diff':difference(plan,observed),'permissions':permissions,'transport':choose_transport(config),'resource_policy':{key:config[key] for key in ('limits','parallel','load')},'scope':'read-only destination snapshot; publication rechecks the sealed snapshot and actual supported semantics'}
                folder=journal.work/'target-reviews'/run_id;folder.mkdir(parents=True,exist_ok=True)
                path=folder/(key+'-'+uuid.uuid4().hex+'.json');partial=path.with_suffix('.partial')
                size=0;digest=hashlib.sha256()
                with partial.open('xb') as stream:
                    for chunk in json.JSONEncoder(ensure_ascii=True,sort_keys=True,separators=(',',':')).iterencode(data):
                        if len(chunk)>1048576:checkpoint(len(chunk)*2)
                        raw=chunk.encode('utf-8');size+=len(raw)
                        if size>MAX_REVIEW_BYTES:raise IngestError('TARGET_REVIEW_BUDGET','Destination review exceeds32MiB; partial retained, no review sealed',2)
                        stream.write(raw);digest.update(raw)
                    stream.flush();os.fsync(stream.fileno())
                _atomic_replace(partial,path);_sync_directory(folder)
                pointer={'intent_id':item['intent_id'],'run_id':run_id,'path':str(path),'sha256':digest.hexdigest(),'bytes':size,'snapshot_hash':data['snapshot_hash'],'reviewed_at_utc':data['reviewed_at_utc'],'object_id':observed['object_id'],'changes_preview':data['schema_diff']['changes'][:50],'changes_truncated':len(data['schema_diff']['changes'])>50,'permissions':permissions,'chosen_transport':data['transport']['engine']}
                table='groups' if config['mapping']['table_strategy']=='per_directory' else 'files'
                key_column='group_id' if table=='groups' else 'file_id'
                with journal.db:
                    journal.db.execute(f'UPDATE {table} SET counts=? WHERE run_id=? AND {key_column}=?',(json.dumps({**item['counts'],'target_review':pointer}),run_id,key))
                    journal.db.execute('INSERT INTO events(run_id,file_id,category,pid,created,details) VALUES(?,?,?,?,?,?)',(run_id,None,'TARGET_REVIEW',os.getpid(),utcnow(),json.dumps({'subject_id':key,**pointer})))
                reviewed+=1
            from .pipeline import _report_files
            report['files']=_report_files(journal,run_id)
            if config['mapping']['table_strategy']=='per_directory':report['groups']=grouping.report_groups(journal,run_id)
        return {**report,'destination_review':{'reviewed_targets':reviewed,'read_only':True}}
    finally:journal.close()


def verify_review(cur,config,plan,intent_id=None):
    pointer=config.get('_target_review')
    if not pointer:return
    from .resources import checkpoint
    try:
        data=_read(pointer)
        expected={'version':1,'run_id':config.get('_publication_run_id'),'server':config['destination']['server'],'database':plan.database,'source_hash':plan.source_hash,'interpretation_hash':plan.interpretation_hash,'mapping_hash':plan.mapping_hash,'intent_id':str(intent_id) if intent_id is not None else pointer['intent_id']}
        if pointer.get('run_id')!=expected['run_id'] or pointer.get('intent_id')!=expected['intent_id'] or any(data.get(k)!=v for k,v in expected.items()) or _snapshot_hash(data['snapshot'])!=data['snapshot_hash'] or data['snapshot_hash']!=pointer['snapshot_hash']:raise ValueError('review identity/hash differs')
    except (OSError,KeyError,ValueError,UnicodeError) as error:raise IngestError('TARGET_REVIEW_INTEGRITY','Reviewed target evidence is missing or altered: '+str(error),5) from None
    observed=snapshot(cur,plan)
    if _snapshot_hash(observed)!=data['snapshot_hash']:raise IngestError('TARGET_REVIEW_DRIFT','Live destination identity/schema differs from the reviewed plan; review a new linked operation before publication',2,{'reviewed_snapshot_hash':data['snapshot_hash'],'actual_snapshot_hash':_snapshot_hash(observed),'review_artifact':pointer['path']})


def reference(journal,run_id,subject_id,counts):
    """Require the independently recorded original review pointer before publication."""
    pointer=(counts or {}).get('target_review')
    event=journal.db.execute("SELECT details FROM events WHERE run_id=? AND category='TARGET_REVIEW' AND json_extract(details,'$.subject_id')=? ORDER BY id DESC LIMIT 1",(run_id,subject_id)).fetchone()
    if pointer is None and event is None:
        requested=journal.db.execute("SELECT 1 FROM events WHERE run_id=? AND category='TARGET_REVIEW_REQUESTED' AND json_extract(details,'$.subject_id')=? LIMIT 1",(run_id,subject_id)).fetchone()
        if requested:raise IngestError('TARGET_REVIEW_INCOMPLETE','Requested destination review did not finish; retry resume --plan-only --check-target before publication',2,{'run_id':run_id})
        return None
    if pointer is None or event is None:raise IngestError('TARGET_REVIEW_INTEGRITY','Sealed target review pointer/event is missing; no automatic review reset',5)
    recorded=json.loads(event[0]);recorded.pop('subject_id',None)
    if recorded!=pointer:raise IngestError('TARGET_REVIEW_INTEGRITY','Target review pointer differs from its original durable event',5)
    try:
        data=_read(pointer)
        if data['run_id']!=run_id or data['intent_id']!=pointer['intent_id'] or data['snapshot_hash']!=pointer['snapshot_hash'] or _snapshot_hash(data['snapshot'])!=data['snapshot_hash']:raise ValueError('review identity/hash differs')
    except (OSError,KeyError,ValueError,UnicodeError) as error:raise IngestError('TARGET_REVIEW_INTEGRITY','Reviewed target evidence is missing or altered: '+str(error),5) from None
    return pointer
