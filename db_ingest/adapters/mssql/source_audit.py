"""Central captured-source metadata independent of publication acknowledgment.

Runs with rejected/uninterpretable sources still have exact capture references.
Only actual receipt mirroring creates publication/chunk/schema ACK metadata.
"""
import copy
import hashlib
import json
from db_ingest.config import effective_for_file
from db_ingest.errors import IngestError
from db_ingest.resources import checkpoint
from .ddl import qualified


def _conflict(code,message,file_id):raise IngestError(code,message,5,{'file_id':file_id})


def mirror_source(cur,schema,config,item):
    artifact=item['artifact'];run_id=item['run_id'];file_id=item['file_id']
    if artifact['capture_id']!=file_id:_conflict('CONTROL_FILE_CONFLICT','Captured source identity differs from its local file identity',file_id)
    checkpoint()
    files=qualified(schema,'Files')
    old=cur.execute(f'SELECT source_hash,source_path,artifact_path FROM {files} WITH (UPDLOCK,HOLDLOCK) WHERE run_id=? AND file_id=?',run_id,file_id).fetchone()
    expected=(artifact['sha256'],artifact['original_path'],artifact['path'])
    if old is None:cur.execute(f'INSERT INTO {files}(file_id,run_id,source_hash,source_path,artifact_path) VALUES(?,?,?,?,?)',file_id,run_id,*expected)
    elif tuple(old)!=expected:_conflict('CONTROL_FILE_CONFLICT','Central source hash/path differs from its durable capture reference',file_id)
    artifacts=qualified(schema,'Artifacts')
    old=cur.execute(f'SELECT file_id,sha256,byte_length,path FROM {artifacts} WITH (UPDLOCK,HOLDLOCK) WHERE artifact_id=?',file_id).fetchone()
    expected=(file_id,artifact['sha256'],artifact['size'],artifact['path'])
    if old is None:cur.execute(f'INSERT INTO {artifacts}(artifact_id,file_id,sha256,byte_length,path) VALUES(?,?,?,?,?)',file_id,*expected)
    elif tuple(old)!=expected:_conflict('CONTROL_ARTIFACT_CONFLICT','Central artifact hash/byte count/path differs from the retained capture identity',file_id)
    plan=item.get('plan');profiles=0;identity=None
    if plan is not None:
        identity=plan['interpretation_hash']
        effective=effective_for_file(config,artifact['relative_path'])
        policy={key:effective.get(key,{}) for key in ('parsing','schema','xml','html','columns')}
        policy.update(source_format=effective.get('source',{}).get('format','auto'),application_version='0.1.0')
        interpretations=qualified(schema,'Interpretations')
        if cur.execute(f'SELECT interpretation_hash FROM {interpretations} WITH (UPDLOCK,HOLDLOCK) WHERE interpretation_hash=?',identity).fetchone() is None:
            cur.execute(f'INSERT INTO {interpretations}(interpretation_hash,policy_json) VALUES(?,?)',identity,json.dumps(policy,ensure_ascii=False))
        target=qualified(schema,'ColumnProfiles')
        for column in plan['columns']:
            checkpoint()
            ordinal=column['ordinal'];candidate=copy.deepcopy(column)
            stats={**column['stats'],'source_only_profile':True,'source_candidate':candidate}
            expected=(column['original_name'],column['target_name'],column['sql_type'])
            old=cur.execute(f'SELECT original_name,target_name,type_name,profile_json FROM {target} WITH (UPDLOCK,HOLDLOCK) WHERE file_id=? AND interpretation_hash=? AND ordinal=?',file_id,identity,ordinal).fetchone()
            if old is None:
                cur.execute(f'INSERT INTO {target}(file_id,interpretation_hash,ordinal,original_name,target_name,type_name,profile_json) VALUES(?,?,?,?,?,?,?)',file_id,identity,ordinal,*expected,json.dumps(stats,ensure_ascii=False))
            else:
                try:prior=json.loads(old[3])
                except (ValueError,TypeError) as error:raise IngestError('CONTROL_PROFILE_CONFLICT','Central source profile JSON is malformed',5,{'file_id':file_id,'ordinal':ordinal}) from error
                if prior.get('source_only_profile'):
                    if tuple(old[:3])!=expected or prior!=stats:_conflict('CONTROL_PROFILE_CONFLICT','An immutable source-only profile conflicts with the retained candidate',file_id)
                elif 'source_candidate' in prior:
                    # An acknowledged merged projection may use a wider canonical
                    # target type/name; its stored original candidate remains exact.
                    if prior['source_candidate']!=candidate:_conflict('CONTROL_PROFILE_CONFLICT','Acknowledged projection refers to a different source candidate',file_id)
                elif tuple(old[:3])!=expected or prior!=column['stats']:
                    _conflict('CONTROL_PROFILE_CONFLICT','Acknowledged source profile differs from its retained candidate',file_id)
            profiles+=1
    event_id=hashlib.sha256((run_id+':source-capture:'+file_id).encode()).hexdigest()
    events=qualified(schema,'Events')
    if cur.execute(f'SELECT event_id FROM {events} WITH (UPDLOCK,HOLDLOCK) WHERE event_id=?',event_id).fetchone() is None:
        from db_ingest.journal import utcnow
        details={'file_id':file_id,'source_hash':artifact['sha256'],'source_bytes':artifact['size'],'artifact_path':artifact['path'],'interpretation_hash':identity,'source_state_at_first_mirror':item['state'],'scope':'capture/profile references only; no publication acknowledgment implied'}
        cur.execute(f'INSERT INTO {events}(event_id,run_id,recorded_at_utc,category,details_json) VALUES(?,?,?,?,?)',event_id,run_id,utcnow(),'SOURCE_CAPTURE_MIRRORED',json.dumps(details,ensure_ascii=False))
    return {'state':'VERIFIED','file_id':file_id,'source_profiles':profiles,'publication_acknowledged':False}
