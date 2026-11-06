"""Lossless explicit length widening/retyping, independent SQL values and raw bytes."""
import hashlib,json,os,runpy,sqlite3
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
def configured(engine,policy):
 case=API['TypeCase'](engine=engine,fast=False,batch_rows=2)
 raw='n,u,b\r\nA🙂B,é🙂 ,AAECAw==\r\nok,ok,%%%\r\n,,\r\n\\N,\\N,\\N\r\n'.encode('utf-8');case.source.write_bytes(raw)
 prefix=case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
 text=prefix+'\n[schema]\non_length_overflow='+API['lit'](policy)+'\ntext_length_headroom_percent=0\n[[file_rules]]\nglob='+API['lit'](case.source.name)+'\n'
 for ordinal,name,kind,extra in [(1,'n','nvarchar(2)',{}),(2,'u','varchar(2)',{'collation':'Latin1_General_100_BIN2_UTF8','text_encoding':'utf-8'}),(3,'b','varbinary(1)',{'binary_encoding':'base64'})]:
  text+='\n[[file_rules.columns]]\n'+'\n'.join(k+'='+API['lit'](v) for k,v in dict(source_ordinal=ordinal,target_name=name,target_type=kind,**extra).items())+'\n'
 case.config.write_text(text,encoding='utf-8');return case,raw

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('policy',['widen','retype_text'])
def test_lossless_length_policies_keep_values_lexical_bytes_and_original_encoding_checks(engine,policy):
 case,raw=configured(engine,policy);report=case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=3)
 item=report['files'][0];assert item['counts']['committed']==3 and item['counts']['rejected']==1
 assert item['receipt']['assurance']=='full' and Path(item['artifact']['path']).read_bytes()==raw
 assert item['receipt']['source_hash']==hashlib.sha256(raw).hexdigest()
 assert [c['sql_type'] for c in item['plan']['columns']]==(['nvarchar(4)','varchar(7)','varbinary(4)'] if policy=='widen' else ['nvarchar(4)','nvarchar(4)','nvarchar(8)'])
 target=API['q'](item['plan']['schema'])+'.'+API['q'](item['plan']['table']);conn=case.connection()
 try:
  actual=[tuple(r) for r in conn.cursor().execute('SELECT n,u,b FROM '+target+' ORDER BY __ingest_ordinal')]
  assert actual==[('A🙂B','é🙂 ',b'\x00\x01\x02\x03' if policy=='widen' else 'AAECAw=='),('','',b'' if policy=='widen' else ''),(None,None,None)]
 finally:conn.rollback();conn.close()
 errors=case.cli('show-errors','--run-id',report['run_id'])
 assert len(errors['record_errors'])==1 and 'VALUE_INVALID' in str(errors)
 db=sqlite3.connect(case.folder/'work/journal.sqlite3')
 try:
  events=list(db.execute('SELECT details,sha256 FROM length_policy_events WHERE run_id=? ORDER BY column_ordinal',(report['run_id'],)))
  assert len(events)==3
  for details,sha in events:
   assert hashlib.sha256(details.encode()).hexdigest()==sha
   value=json.loads(details);assert value['policy']==policy and value['record_id']
   assert value['source_artifact']['sha256']==hashlib.sha256(raw).hexdigest()
 finally:db.close()
 result=case.cli('reconcile','--run-id',report['run_id'],'--non-interactive',exit_code=3)
 assert result['files'][0]['destination']['state']=='VERIFIED'

@pytest.mark.parametrize('allow_max',[False,True])
def test_length_max_policy_explicitly_blocks_or_preserves_complete_value(allow_max):
 case,raw=configured('odbc','widen');raw=('n\r\n'+'x'*4001+'\r\n').encode();case.source.write_bytes(raw)
 text=case.config.read_text(encoding='utf-8');text=text[:text.index('[[file_rules.columns]]')]+ '\n[[file_rules.columns]]\nsource_ordinal=1\ntarget_name="n"\ntarget_type="nvarchar(1)"\n'
 text=text.replace('text_length_headroom_percent=0','text_length_headroom_percent=0\nallow_max_types='+API['lit'](allow_max));case.config.write_text(text,encoding='utf-8')
 report=case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=0 if allow_max else 2)
 assert Path(report['files'][0]['artifact']['path']).read_bytes()==raw
 if not allow_max:
  assert any(error['code'] in ('LENGTH_POLICY_LIMIT','TEXT_REPRESENTATION_LIMIT') for error in report['errors'])
  assert not report['files'][0].get('receipt')
  return
 item=report['files'][0];assert item['plan']['columns'][0]['sql_type']=='nvarchar(max)'
 target=API['q'](item['plan']['schema'])+'.'+API['q'](item['plan']['table']);conn=case.connection()
 try:assert conn.cursor().execute('SELECT n FROM '+target).fetchval()=='x'*4001
 finally:conn.rollback();conn.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_length_widening_existing_target_still_requires_explicit_alter_authorization(engine):
 case,_=configured(engine,'widen');old_raw=b'n,u,b\r\na,a,AQ==\r\n';case.source.write_bytes(old_raw)
 original_config=case.config.read_text(encoding='utf-8');first=case.cli('run','--new-intent','--no-parallel','--non-interactive');old_item=first['files'][0]
 p=old_item['plan'];target=API['q'](p['schema'])+'.'+API['q'](p['table']);new_raw='n,u,b\r\nA🙂B,é🙂 ,AAECAw==\r\n'.encode('utf-8');case.source.write_bytes(new_raw)
 refused=case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 assert 'SCHEMA_DRIFT' in str(refused)
 conn=case.connection()
 try:assert [tuple(r) for r in conn.cursor().execute('SELECT n,u,b FROM '+target)]==[('a','a',b'\x01')]
 finally:conn.rollback();conn.close()
 approved=original_config.replace('on_length_overflow="widen"','on_length_overflow="widen"\nallow_widen_on_append=true');assert approved!=original_config;case.config.write_text(approved,encoding='utf-8')
 later=case.cli('run','--new-intent','--no-parallel','--non-interactive')
 assert later['files'][0]['receipt']['details']['target_object_id']==old_item['receipt']['details']['target_object_id']
 conn=case.connection()
 try:
  actual=[tuple(r) for r in conn.cursor().execute('SELECT n,u,b FROM '+target+' ORDER BY n')]
  assert set(actual)=={('a','a',b'\x01'),('A🙂B','é🙂 ',b'\x00\x01\x02\x03')}
 finally:conn.rollback();conn.close()
 assert Path(old_item['artifact']['path']).read_bytes()==old_raw
 oldconfig=case.folder/'old-length-config.toml';oldconfig.write_text(original_config,encoding='utf-8');case.config=oldconfig
 result=case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
 assert result['files'][0]['destination']['state']=='VERIFIED' and result['files'][0]['destination']['value_digest']==old_item['receipt']['value_digest']
