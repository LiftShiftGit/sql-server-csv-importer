"""Authored folder naming/create-only SQL oracles, both transports and crash recovery."""
import os,runpy,json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest
API=runpy.run_path(str(Path(__file__).with_name('test_live_filename_naming.py')))
Case=API['Case'];q=API['API']['q']
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Disposable SQL required')

def setup(engine,strategy='per_file'):
 c=Case(engine);c.settings.setdefault('mapping',{}).update(name_collision='path_error',table_strategy=strategy)
 c.settings['load']['mode']='create_only';c.select(c.first.parent)
 if strategy=='per_directory':
  (c.first.parent/'part2.csv').write_bytes(b'code,label\r\n003,charlie\r\n')
  c.select(c.first.parent)
 return c

def verify(c,report,strategy='per_file'):
 item=report['groups'][0] if strategy=='per_directory' else report['files'][0]
 p=item['plan'];target=q(p['schema'])+'.'+q(p['table']);assert p['table']==('a' if strategy=='per_directory' else 'a_orders')
 rows=c.sql('SELECT '+','.join(q(x['target_name']) for x in p['columns'])+' FROM '+target+' ORDER BY '+q(p['columns'][0]['target_name']))
 assert rows==[('001','alpha'),('002','bravo')]+([('003','charlie')] if strategy=='per_directory' else [])
 assert c.sql('SELECT COUNT(*) FROM audit.__db_ingest_receipts WHERE intent_id=?',item['intent_id'])==[(1,)]
 return item,target

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('strategy',['per_file','per_directory'])
def test_create_only_load_repeat_and_changed_source(engine,strategy):
 c=setup(engine,strategy);report=c.base.cli('run','--non-interactive');item,target=verify(c,report,strategy)
 for _ in range(2):verify(c,c.base.cli('resume','--run-id',report['run_id'],'--non-interactive'),strategy)
 c.base.cli('reconcile','--run-id',report['run_id'],'--non-interactive')
 c.first.write_bytes(b'code,label\r\n004,DIFFERENT\r\n')
 blocked=c.base.cli('run','--non-interactive','--new-intent',exit_code=2)
 assert any(e['code']=='TARGET_EXISTS' for e in blocked['errors']),blocked
 verify(c,report,strategy)

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('strategy',['per_file','per_directory'])
@pytest.mark.parametrize('fault',['AFTER_TARGET_SCHEMA_CREATION','AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT'])
def test_create_only_recovery(engine,strategy,fault):
 c=setup(engine,strategy)
 c.base.cli('run','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':fault})
 run=c.base.cli('status')['runs'][0]['run_id']
 report=c.base.cli('resume','--run-id',run,'--non-interactive');verify(c,report,strategy)
 verify(c,c.base.cli('resume','--run-id',run,'--non-interactive'),strategy)

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_existing_unowned_table_preserved(engine):
 c=setup(engine);target=q(c.settings['destination']['schema'])+'.[a_orders]';conn=c.base.connection()
 try:
  conn.execute('CREATE SCHEMA '+q(c.settings['destination']['schema']));conn.execute('CREATE TABLE '+target+' (sentinel int)');conn.execute('INSERT INTO '+target+' VALUES (123)');conn.commit()
 finally:conn.close()
 report=c.base.cli('run','--non-interactive',exit_code=2);assert any(e['code']=='TARGET_EXISTS' for e in report['errors']);assert c.sql('SELECT sentinel FROM '+target)==[(123,)]

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_path_claim_collision_across_runs(engine):
 c=setup(engine);c.settings['load']['mode']='append';c.select(c.first);report=c.base.cli('run','--non-interactive');verify(c,report)
 other=c.base.folder/'a_orders.csv';other.write_bytes(b'code,label\r\n009,collision\r\n');c.select(other)
 blocked=c.base.cli('run','--non-interactive',exit_code=2);assert any(e['code']=='TABLE_NAME_COLLISION' for e in blocked['errors']);verify(c,report)


@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_create_only_concurrent_imports(engine):
 import copy
 c=setup(engine);other=setup(engine)
 work=other.settings['preservation']['work_directory'];other.settings=copy.deepcopy(c.settings);other.settings['preservation']['work_directory']=work;other.select(c.first,root=c.base.folder)
 def run(case):
  # Each runner retains its own output; either may win the publication lock.
  import subprocess,sys
  env={k:v for k,v in os.environ.items() if not k.startswith('DB_') or k in ('DB_USERNAME','DB_PASSWORD')}
  result=subprocess.run([sys.executable,'-m','db_ingest','--config',str(case.base.config),'run','--new-intent','--json','--non-interactive'],env=env,cwd=API['ROOT'],capture_output=True,text=True,timeout=240)
  (case.base.folder/'concurrent.json').write_text(result.stdout+result.stderr,encoding='utf-8')
  return result.returncode,json.loads(result.stdout)
 with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(run,[c,other]))
 assert sorted(code for code,_ in results)==[0,2]
 winner=next(report for code,report in results if code==0);verify(c,winner)
 loser=next(report for code,report in results if code==2);assert any(e['code']=='TARGET_EXISTS' for e in loser['errors'])

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_directory_claim_collision_across_runs(engine):
 c=setup(engine,'per_directory');c.settings['load']['mode']='append'
 first=c.base.folder/'a_b/one.csv';second=c.base.folder/'a/b/one.csv'
 for p in (first,second):p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'code,label\r\n001,keep\r\n')
 c.select(first);report=c.base.cli('run','--non-interactive');p=report['groups'][0]['plan'];target=q(p['schema'])+'.'+q(p['table']);assert p['table']=='a_b'
 c.select(second);blocked=c.base.cli('run','--non-interactive',exit_code=2);assert any(e['code']=='TABLE_NAME_COLLISION' for e in blocked['errors'])
 assert c.sql('SELECT COUNT(*) FROM '+target)==[(1,)]
