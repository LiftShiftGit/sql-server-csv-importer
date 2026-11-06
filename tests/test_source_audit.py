"""Central source metadata writer simulations, never publication/SQL value claims."""
import copy,json,re
from unittest.mock import patch
import pytest
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.adapters.mssql.source_audit import mirror_source

class MemoryCursor:
    def __init__(self):self.tables={key:{} for key in ('Files','Artifacts','Interpretations','ColumnProfiles','Events')};self.writes=[];self.result=None
    def execute(self,sql,*p):
        found=re.search(r'\[audit\]\.\[(\w+)\]',sql);assert found,sql
        table=found[1];assert table in self.tables,'Source-only metadata must never create publication ACKs'
        if sql.startswith('SELECT'):
            key=(p[0],p[1]) if table=='Files' else (p[0],p[1],p[2]) if table=='ColumnProfiles' else p[0]
            self.result=self.tables[table].get(key)
        elif sql.startswith('INSERT'):
            if table=='Files':key=(p[1],p[0]);value=p[2:]
            elif table=='Artifacts':key=p[0];value=p[1:]
            elif table=='ColumnProfiles':key=p[:3];value=p[3:]
            else:key=p[0];value=p
            assert key not in self.tables[table]
            self.tables[table][key]=value;self.writes.append(table)
        else:raise AssertionError(sql)
        return self
    def fetchone(self):return self.result

@pytest.fixture
def source():
    config=load_config(environ={})
    item={'run_id':'run','file_id':'capture','state':'FAILED','receipt':None,'artifact':{'capture_id':'capture','sha256':'a'*64,'size':42,'path':'D:/unit/source.bin','original_path':'D:/unit/input.csv','relative_path':'input.csv'},'plan':None}
    return config,item,MemoryCursor()

def test_uninterpretable_source_records_files_artifacts_without_fake_ack(source):
    config,item,cur=source
    first=mirror_source(cur,'audit',config,item);mirror_source(cur,'audit',config,item)
    assert first['state']=='VERIFIED' and first['publication_acknowledged'] is False
    assert first['source_profiles']==0
    assert cur.writes==['Files','Artifacts','Events']
    assert not cur.tables['ColumnProfiles'] and not cur.tables['Interpretations']
    event=next(iter(cur.tables['Events'].values()));assert event[3]=='SOURCE_CAPTURE_MIRRORED'
    assert 'no publication acknowledgment implied' in json.loads(event[4])['scope']

def profile(item):
    col={'ordinal':1,'original_name':'raw','target_name':'raw_1_hash','sql_type':'int','nullable':True,'stats':{'records':1,'candidate':'integer'}}
    item['plan']={'interpretation_hash':'c'*64,'columns':[col]}
    return col

def test_all_rejected_source_profiles_are_candidate_metadata_and_idempotent(source):
    config,item,cur=source;col=profile(item)
    mirror_source(cur,'audit',config,item);mirror_source(cur,'audit',config,item)
    stored=cur.tables['ColumnProfiles'][('capture','c'*64,1)];metadata=json.loads(stored[3])
    assert metadata['source_only_profile'] is True and metadata['source_candidate']==col
    assert cur.writes.count('ColumnProfiles')==1
    assert set(cur.tables)=={'Files','Artifacts','Interpretations','ColumnProfiles','Events'}

def test_artifact_hash_bytes_and_source_profiles_cannot_be_silently_rewritten(source):
    config,item,cur=source;profile(item);mirror_source(cur,'audit',config,item)
    old=cur.tables['Artifacts']['capture'];cur.tables['Artifacts']['capture']=(old[0],old[1],43,old[3])
    with pytest.raises(IngestError,match='artifact') as raised:mirror_source(cur,'audit',config,item)
    assert raised.value.exit_code==5
    cur.tables['Artifacts']['capture']=old
    item['plan']['columns'][0]['sql_type']='bigint'
    with pytest.raises(IngestError,match='profile') as raised:mirror_source(cur,'audit',config,item)
    assert raised.value.exit_code==5

def test_acknowledged_group_projection_keeps_its_immutable_source_candidate(source):
    config,item,cur=source;col=profile(item);mirror_source(cur,'audit',config,item)
    key=('capture','c'*64,1)
    acknowledged=('raw','canonical_target','bigint',json.dumps({'source_candidate':copy.deepcopy(col),'group_key':'raw'}))
    cur.tables['ColumnProfiles'][key]=acknowledged
    mirror_source(cur,'audit',config,item)
    assert cur.tables['ColumnProfiles'][key]==acknowledged
