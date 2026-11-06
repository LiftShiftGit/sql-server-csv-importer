"""Actual queryable mutable per-file summaries separate from immutable source/ACK evidence."""
import hashlib,json,os,runpy
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.journal import Journal
from db_ingest.adapters.mssql.control import mirror_run_audit
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL required')

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('scenario',['source_only','mixed','unresolved','all_rejected'])
def test_queryable_file_summary_has_actual_dispositions_unknowns_and_no_fake_ack(engine,scenario):
 case=API['TypeCase'](engine=engine,batch_rows=7)
 raw={'source_only':b'id,text\r\n1,A\r\n2,B\r\n','mixed':b'id,text\r\n1,"one\r\ntwo  "\r\nbad,invalid\r\n','unresolved':b'id,text\n1,known\n2,"unknown\nremainder','all_rejected':b'id,text\nnot-an-integer,rejected\n'}[scenario]
 case.source.write_bytes(raw);text=case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
 text+='[[file_rules]]\nglob='+API['lit'](case.source.name)+'\n'
 for ordinal,name,kind in [(1,'id','bigint'),(2,'text','nvarchar(20)')]:text+='[[file_rules.columns]]\nsource_ordinal='+str(ordinal)+'\ntarget_name='+API['lit'](name)+'\ntarget_type='+API['lit'](kind)+'\n'
 case.config.write_text(text,encoding='utf-8')
 report=case.cli('plan' if scenario in ['source_only','unresolved'] else 'run','--new-intent','--no-parallel','--non-interactive',exit_code=5 if scenario=='unresolved' else 3 if scenario in ['mixed','all_rejected'] else 0)
 item=report['files'][0];run_id=report['run_id'];config=load_config(case.config,environ={});journal=Journal(config['preservation']['work_directory'])
 try:
  # Explicit audit API call, not an implicit SQL side effect of local plan.
  mirror_run_audit(config,journal,run_id,os.environ['DB_USERNAME'],os.environ['DB_PASSWORD'],final_state=report['state'])
  mirror_run_audit(config,journal,run_id,os.environ['DB_USERNAME'],os.environ['DB_PASSWORD'],final_state=report['state'])
 finally:journal.close()
 control=API['q'](case.d['control_database'])+'.[audit].';conn=case.connection()
 try:
  cur=conn.cursor();rows=cur.execute('SELECT summary_json,source_hash,artifact_path FROM '+control+'Files WHERE run_id=? AND file_id=?',run_id,item['file_id']).fetchall();assert len(rows)==1
  summary=json.loads(rows[0][0]);measures=summary['measures'];assert rows[0][1]==hashlib.sha256(raw).hexdigest()
  assert Path(rows[0][2]).read_bytes()==raw and measures['source_bytes']==len(raw)
  assert summary['source']['sha256']==hashlib.sha256(raw).hexdigest() and summary['intent_id']==item['intent_id']
  assert summary['format']['encoding']=='utf-8' and summary['format']['header']=='present'
  assert set(summary['configuration_hashes'])=={'interpretation','mapping','operational'}
  assert summary['target_after_records'] is None and 'source publication count' in summary['target_totals_basis']
  assert summary['next_commands'] and all(run_id in command for command in summary['next_commands'])
  assert summary['phase_timings'] and all(p['elapsed_seconds_all_attempts']>=0 for p in summary['phase_timings'])
  assert 'run-level sampled' in summary['resources']['basis']
  assert measures['header']==measures['header_records']==1
  expected={'source_only':(2,0,0,0,3,3),'mixed':(0,1,1,0,3,4),'unresolved':(1,0,0,1,None,4),'all_rejected':(0,0,1,0,2,2)}[scenario]
  assert tuple(measures[k] for k in ['pending','committed','rejected','unresolved','logical_records','physical_lines'])==expected
  receipt_count=cur.execute('SELECT COUNT(*) FROM '+control+'PublicationReceipts WHERE intent_id=?',item['intent_id']).fetchval()
  chunks=cur.execute('SELECT COUNT(*) FROM '+control+'Chunks WHERE intent_id=?',item['intent_id']).fetchval()
  if scenario!='mixed':
   assert receipt_count==chunks==0 and summary['source_only_profile'] is True
   assert measures['sql_staged_records'] is None and measures['sql_validated_records'] is None
  else:
   assert receipt_count==chunks==1 and summary['source_only_profile'] is False
   assert measures['sql_staged_records']==measures['sql_validated_records']==1
   target=API['q'](item['plan']['schema'])+'.'+API['q'](item['plan']['table'])
   assert [tuple(r) for r in cur.execute('SELECT id,text FROM '+target+' WHERE __ingest_intent=?',item['intent_id'])]==[(1,'one\r\ntwo  ')]
  if scenario=='unresolved':assert measures['parsed_data_records'] is None and measures['known_parsed_data_records']==1
  if scenario in ['mixed','all_rejected']:
   assert summary['failure_table'] and measures['record_error_events']>=1
   errors=[json.loads(r[0]) for r in cur.execute('SELECT diagnostic_json FROM '+control+'RecordErrors WHERE run_id=?',run_id)]
   required={'code','category','stage','record_id','column_ordinal','original_name','expected','observed','remediation','retryable','recorded_at_utc','provider','sqlstate','native_code','raw_diagnostic','provider_diagnostic_completeness','policy','policy_reference','exit_code','exit_code_basis','validator','tool'}
   assert errors and all(required<=set(e) and e['retryable'] is False and e['code']=='VALUE_INVALID' and e['exit_code']==3 for e in errors)
 finally:conn.rollback();conn.close()
