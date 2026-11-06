"""Authored negative outcome expectations and complete schema declarations."""
import json
from pathlib import Path
from db_ingest.fixturegen import create_test_files,verify_fixture_outcome

def test_every_category_has_complete_ordered_schema_and_reason_metadata(tmp_path):
 manifest=create_test_files(tmp_path,'all',rows=3,size_bytes=4096)
 for fixture in manifest['fixtures']:
  schema=fixture['sql_schema'];assert schema
  assert [c['source_ordinal'] for c in schema]==list(range(1,len(schema)+1))
  assert len({c['target_name'] for c in schema})==len(schema)
  assert fixture['expected_record_outcomes']
  if fixture['expected_outcome']!='parsed':assert fixture['expected_reason_codes']
  expected=tmp_path/fixture['expected_path']
  for line in expected.read_text(encoding='utf-8').splitlines():assert len(json.loads(line)['values'])==len(schema)
 large=next(f for f in manifest['fixtures'] if f['category']=='csv_large_record')
 assert large['expected_outcome']=='parsed' and large['expected_rows']==1

def test_ragged_oracle_requires_exact_rejected_ordinals_and_width_reasons(tmp_path):
 fixture=next(f for f in create_test_files(tmp_path,'csv_bad_format')['fixtures'] if f['path']=='ragged.csv')
 rows=[(1,'header',{'ordinal':1,'errors':[]}),(2,'committed',{'ordinal':2,'errors':[]}),(3,'rejected',{'ordinal':3,'errors':[{'code':'ROW_WIDTH'}]}),(4,'rejected',{'ordinal':4,'errors':[{'code':'ROW_WIDTH'}]})]
 assert not verify_fixture_outcome(fixture,{},iter(rows))['errors']
 wrong=list(rows);wrong[2]=(3,'committed',{'ordinal':3,'errors':[]})
 assert 'FIXTURE_REJECT_OUTCOME' in {e['code'] for e in verify_fixture_outcome(fixture,{},iter(wrong))['errors']}

def test_unresolved_tail_can_never_be_certified_as_committed(tmp_path):
 fixture=next(f for f in create_test_files(tmp_path,'csv_bad_format')['fixtures'] if f['path']=='unclosed.csv')
 rows=[(1,'header',{'ordinal':1,'errors':[]}),(2,'pending',{'ordinal':2,'errors':[]}),(3,'unresolved',{'ordinal':3,'errors':[{'code':'FRAMING_UNRESOLVED'}]})]
 assert not verify_fixture_outcome(fixture,{},iter(rows))['errors']
 rows[1]=(2,'committed',{'ordinal':2,'errors':[]})
 codes={e['code'] for e in verify_fixture_outcome(fixture,{'receipt':{'committed_records':1}},iter(rows))['errors']}
 assert {'FIXTURE_UNSAFE_PUBLICATION','FIXTURE_UNSAFE_RECEIPT'}<=codes

def test_encoding_failure_requires_observed_exact_reason_not_generic_failure(tmp_path):
 fixture=next(f for f in create_test_files(tmp_path,'csv_bad_format')['fixtures'] if f['path']=='invalid_utf8.csv')
 assert not verify_fixture_outcome(fixture,{},[],['ENCODING_DECISION'])['errors']
 assert verify_fixture_outcome(fixture,{},[],['TRANSPORT_FAILED'])['errors']


def test_unknown_or_incomplete_outcome_is_never_certified(tmp_path):
 fixture=create_test_files(tmp_path,'csv')['fixtures'][0]
 for edited in [{**fixture,'expected_outcome':'imagined_success'},{**fixture,'sql_schema':[]}]:
  result=verify_fixture_outcome(edited,{},[])
  assert result['errors'][0]['code']=='FIXTURE_EXPECTATION_UNSUPPORTED'
