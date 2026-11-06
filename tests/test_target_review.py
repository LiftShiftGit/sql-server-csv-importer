"""Sealed destination-review authority; local tests do not claim live SQL coverage."""
import hashlib,json
from dataclasses import asdict
import pytest
from db_ingest.config import load_config,stable_hash
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.models import ColumnProfile,SchemaPlan
from db_ingest.target_review import reference,verify_review,difference

@pytest.fixture
def sealed(tmp_path):
    config=load_config(environ={});config['_publication_run_id']='run'
    plan=SchemaPlan('DbIngestTest_Landing','dbo','input',[ColumnProfile(1,'a','a','nvarchar(8)')],'a'*64,'b'*64,'c'*64)
    snapshot={'database_guid':'guid','recovery_fork_guid':'fork','target':'[dbo].[input]','object_id':7,'metadata':{'columns':[]}}
    data={'version':1,'run_id':'run','intent_id':'intent','server':config['destination']['server'],'database':plan.database,'source_hash':plan.source_hash,'interpretation_hash':plan.interpretation_hash,'mapping_hash':plan.mapping_hash,'snapshot':snapshot,'snapshot_hash':stable_hash(snapshot)}
    path=tmp_path/'review.json';raw=json.dumps(data).encode();path.write_bytes(raw)
    pointer={'run_id':'run','intent_id':'intent','path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'snapshot_hash':data['snapshot_hash']}
    journal=Journal(tmp_path/'work');journal.create_run('run',config)
    journal.event('run',None,'TARGET_REVIEW',{'subject_id':'file',**pointer})
    yield config,plan,snapshot,path,pointer,journal
    journal.close()

def test_original_pointer_and_snapshot_are_required(sealed,monkeypatch):
    config,plan,snapshot,path,pointer,journal=sealed
    assert reference(journal,'run','file',{'target_review':pointer})==pointer
    config['_target_review']=pointer
    monkeypatch.setattr('db_ingest.target_review.snapshot',lambda cur,plan:snapshot)
    verify_review(None,config,plan,'intent')
    monkeypatch.setattr('db_ingest.target_review.snapshot',lambda cur,plan:{**snapshot,'object_id':8})
    with pytest.raises(IngestError) as error:verify_review(None,config,plan,'intent')
    assert error.value.code=='TARGET_REVIEW_DRIFT' and error.value.exit_code==2

@pytest.mark.parametrize('change',['file','pointer','event','run','intent'])
def test_tamper_or_rebinding_cannot_authorize_publication(sealed,monkeypatch,change):
    config,plan,snapshot,path,pointer,journal=sealed
    monkeypatch.setattr('db_ingest.target_review.snapshot',lambda cur,plan:pytest.fail('SQL must not be read after invalid local proof'))
    if change=='file':path.write_text('{}')
    elif change=='pointer':pointer={**pointer,'sha256':'0'*64}
    elif change=='event':
        with journal.db:journal.db.execute("DELETE FROM events WHERE category='TARGET_REVIEW'")
    elif change=='run':config['_publication_run_id']='other'
    else:pointer={**pointer,'intent_id':'other'}
    config['_target_review']=pointer
    with pytest.raises(IngestError) as error:
        if change in ('file','pointer','event'):reference(journal,'run','file',{'target_review':pointer})
        else:verify_review(None,config,plan,'intent')
    assert error.value.code=='TARGET_REVIEW_INTEGRITY' and error.value.exit_code==5

def test_requested_but_incomplete_review_blocks_until_retried(tmp_path):
    journal=Journal(tmp_path);journal.create_run('run',{})
    try:
        assert reference(journal,'run','file',{}) is None
        journal.event('run',None,'TARGET_REVIEW_REQUESTED',{'subject_id':'file'})
        with pytest.raises(IngestError) as error:reference(journal,'run','file',{})
        assert error.value.code=='TARGET_REVIEW_INCOMPLETE' and error.value.exit_code==2
    finally:journal.close()

def test_named_candidate_diff_preserves_decimal_temporal_length_and_nullability():
    plan=SchemaPlan('db','dbo','x',[ColumnProfile(1,'amount','amount','decimal(38,7)',False),ColumnProfile(2,'new','new','datetime2(7)')],'a','b','c')
    result=difference(plan,{'metadata':{'columns':[['amount',1,'decimal',17,38,4,True],['old',2,'nvarchar',8,0,0,True]]}})
    assert result['changes']==[{'action':'compare type/nullability','current':{'name':'amount','type':'decimal(38,4)','nullable':True},'proposed':{'name':'amount','type':'decimal(38,7)','nullable':False}},{'action':'add mapped column','proposed':{'name':'new','type':'datetime2(7)','nullable':True}},{'action':'existing unmapped column','name':'old'}]
    assert difference(plan,{'metadata':None})['create_target'] is True
