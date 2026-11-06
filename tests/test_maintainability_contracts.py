"""Behavior contracts for shared plan decoding and streaming publication guards."""
from dataclasses import asdict
import pytest
from db_ingest.models import ColumnProfile,SchemaPlan,FieldValue,RecordEnvelope,schema_plan_from_dict
from db_ingest.publication import _stage_records
from db_ingest.planning import uses_name_claims
from db_ingest.errors import IngestError


def plan():
    return SchemaPlan('db','dbo','records',[ColumnProfile(1,'label','label','nvarchar(max)',stats={'nested':{'values':[1]}})],'source','interpretation','mapping')


def test_decoding_preserves_saved_shape_and_isolates_mutable_statistics():
    saved=asdict(plan())
    decoded=schema_plan_from_dict(saved)
    assert asdict(decoded)==saved
    decoded.columns[0].stats['nested']['values'].append(2)
    assert saved['columns'][0]['stats']['nested']['values']==[1]
    saved['columns'][0]['stats']['nested']['values'].append(3)
    assert decoded.columns[0].stats['nested']['values']==[1,2]


@pytest.mark.parametrize('kind',['extra_plan_field','missing_column_field','extra_column_field'])
def test_invalid_saved_plan_is_not_silently_normalized(kind):
    saved=asdict(plan())
    if kind=='extra_plan_field':saved['unexpected']=True
    elif kind=='missing_column_field':del saved['columns'][0]['sql_type']
    else:saved['columns'][0]['unexpected']=True
    with pytest.raises(TypeError):schema_plan_from_dict(saved)


@pytest.mark.parametrize('policy,expected',[(None,False),('stable_hash_suffix',False),('filename_error',True),('path_error',True)])
def test_claim_policy_is_explicit(policy,expected):
    assert uses_name_claims({'mapping':{'name_collision':policy}}) is expected


class Loader:
    def __init__(self):self.rows=[]
    def add(self,record,values):self.rows.append((record.ordinal,values))


def record(ordinal=1,text='😀',kind='data',errors=None):
    return RecordEnvelope(str(ordinal),ordinal,None,None,None,None,[FieldValue('VALUE',text)],kind,errors or [])


def test_stage_skips_headers_but_retains_unicode_and_order():
    loader=Loader()
    count,digest=_stage_records({},plan(),lambda:iter([record(0,kind='header'),record(1),record(0,kind='blank'),record(2,'next')]),'intent',loader,None)
    assert count==2 and loader.rows==[(1,['😀']),(2,['next'])]
    assert len(digest.hexdigest())==64


@pytest.mark.parametrize('settings,expected',[
    ({'limits':{'max_in_memory_field_bytes':3}},'FIELD_BUDGET'),
    ({'limits':{'max_in_memory_record_bytes':3}},'RECORD_BUDGET'),
    ({'load':{'batch_bytes':3}},'RECORD_BUDGET'),
])
def test_utf8_and_utf16_budgets_are_enforced_before_driver_write(settings,expected):
    loader=Loader()
    with pytest.raises(IngestError) as caught:_stage_records(settings,plan(),[record()],'intent',loader,None)
    assert caught.value.code==expected and loader.rows==[]


@pytest.mark.parametrize('bad,expected',[
    (record(1),'RECORD_IDENTITY_INVALID'),
    (record(2,kind='unresolved'),'UNRESOLVED_RECORD'),
    (record(2,errors=[{'code':'invalid'}]),'UNRESOLVED_RECORD'),
])
def test_staging_does_not_send_invalid_following_rows(bad,expected):
    loader=Loader()
    with pytest.raises(IngestError) as caught:_stage_records({},plan(),[record(),bad],'intent',loader,None)
    assert caught.value.code==expected and loader.rows==[(1,['😀'])]
