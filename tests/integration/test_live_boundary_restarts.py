"""Independent actual CLI restart at capture/audit/reject/cancellation boundaries."""
import json,os,runpy,sqlite3,subprocess,sys,time
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')

def fault(case,point):
 case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':point})
 listing=case.cli('status');run_id=listing['runs'][0]['run_id'];return run_id,case.cli('status','--run-id',run_id,exit_code=4 if point.startswith('CAPTURE_') else 0)

def selections(case,run_id):
 db=sqlite3.connect(case.folder/'work/journal.sqlite3')
 try:return list(db.execute('SELECT selection_id,file_id,intent_id,state,artifact FROM selections WHERE run_id=? ORDER BY path',(run_id,)))
 finally:db.close()

def target(item):return API['q'](item['plan']['schema'])+'.'+API['q'](item['plan']['table'])
def control(case,name):return API['q'](case.d['control_database'])+'.[audit].'+API['q'](name)
def sql_count(case,sql,*args):
 conn=case.connection()
 try:return conn.cursor().execute(sql,*args).fetchval()
 finally:conn.rollback();conn.close()

@pytest.mark.parametrize('point',['CAPTURE_FIRST_BLOCK_DURABLE','CAPTURE_FINALIZED_BEFORE_JOURNAL'])
def test_capture_hard_exit_retains_partial_or_reuses_finalized_capture(point):
 case=API['TypeCase'](engine='odbc',batch_rows=5000,batch_bytes=8388608)
 if point=='CAPTURE_FIRST_BLOCK_DURABLE':
  case.manifest=API['ORACLE']['write_fixture'](case.source,1001,'middle');case.expected=list(API['ORACLE']['authored_rows'](1001,'middle'))
  assert case.source.stat().st_size>1048576
 raw=case.source.read_bytes();run_id,before=fault(case,point);old=selections(case,run_id);assert len(old)==1
 capture=case.folder/'work/captures'/old[0][1]
 if point=='CAPTURE_FIRST_BLOCK_DURABLE':
  partial=capture/'source.partial';assert partial.read_bytes()==raw[:1048576]
  assert not (capture/'source.bin').exists() and not (capture/'artifact.json').exists()
 else:
  assert (capture/'source.bin').read_bytes()==raw and (capture/'artifact.json').exists()
  assert old[0][4] is None #Final artifact exists but selection has not acknowledged it.
 planned=case.cli('resume','--run-id',run_id,'--plan-only','--no-parallel','--non-interactive')
 assert planned['run_id']==run_id;new=selections(case,run_id);assert new[0][0]==old[0][0] and new[0][2]==old[0][2]
 item=planned['files'][0];assert sql_count(case,'SELECT OBJECT_ID(?)',target(item)) is None
 if point=='CAPTURE_FIRST_BLOCK_DURABLE':
  assert item['file_id']!=old[0][1] and partial.read_bytes()==raw[:1048576]
 else:assert item['file_id']==old[0][1]
 after=case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive');assert after['run_id']==run_id
 assert after['files'][0]['intent_id']==old[0][2];case.verify(after)
 assert sql_count(case,'SELECT COUNT(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',old[0][2])==1

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_after_local_ack_before_audit_resumes_same_receipt_without_republication(engine):
 case=API['TypeCase'](engine=engine,batch_rows=5000,batch_bytes=8388608);run_id,before=fault(case,'AFTER_LOCAL_RECEIPT_BEFORE_AUDIT');item=before['files'][0]
 assert item['receipt'] and item['counts']['committed']==17 and item['counts']['pending']==0
 assert sql_count(case,'SELECT COUNT(*) FROM '+control(case,'PublicationReceipts')+' WHERE intent_id=?',item['intent_id'])==0
 case.verify(before)
 after=case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive');case.verify(after)
 assert after['files'][0]['file_id']==item['file_id'] and after['files'][0]['intent_id']==item['intent_id']
 assert sql_count(case,'SELECT COUNT_BIG(*) FROM '+target(item))==17
 assert sql_count(case,'SELECT COUNT(*) FROM '+control(case,'PublicationReceipts')+' WHERE intent_id=?',item['intent_id'])==1

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_reject_table_create_before_commit_rolls_back_and_backfills_same_name(engine):
 case=API['TypeCase'](engine=engine,batch_rows=5000,batch_bytes=8388608,negative=True);run_id,before=fault(case,'FAILURE_TABLE_CREATED_BEFORE_COMMIT');item=before['files'][0]
 assert item['receipt'] is None and item['counts']['pending']==17 and item['counts']['rejected']==2
 assert sql_count(case,'SELECT OBJECT_ID(?)',target(item)) is None
 name=item['failures']['name'];table=API['q'](case.d['control_database'])+'.[failures].'+API['q'](name)
 assert sql_count(case,'SELECT OBJECT_ID(?)',table) is None
 after=case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive',exit_code=3);case.verify(after)
 assert after['files'][0]['file_id']==item['file_id'] and after['files'][0]['intent_id']==item['intent_id']
 assert after['files'][0]['failures']['name']==name and after['files'][0]['failures']['state']=='VERIFIED'
 assert sql_count(case,'SELECT COUNT_BIG(*) FROM '+table)==2
 db=sqlite3.connect(case.folder/'work/journal.sqlite3')
 try:ids={r[0] for r in db.execute("SELECT record_id FROM records WHERE run_id=? AND disposition='rejected'",(run_id,))}
 finally:db.close()
 conn=case.connection()
 try:
  actual={r[0] for r in conn.cursor().execute('SELECT record_id FROM '+table)};assert actual==ids
  assert conn.cursor().execute('SELECT COUNT(*) FROM '+control(case,'RejectedRecords')+' WHERE run_id=?',run_id).fetchval()==2
  assert conn.cursor().execute('SELECT COUNT(*) FROM '+control(case,'RecordErrors')+' WHERE run_id=?',run_id).fetchval()>=4
 finally:conn.rollback();conn.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_injected_multiworker_cancellation_closes_native_children_then_same_intent_resume(engine):
 import psutil
 parallel=runpy.run_path(str(ROOT/'tests/integration/test_live_parallel_oracle.py'))
 case=parallel['configured'](engine,4)
 text=case.config.read_text(encoding='utf-8').replace('batch_rows = 7','batch_rows = 1');case.config.write_text(text,encoding='utf-8')
 env={k:v for k,v in os.environ.items() if not k.startswith('DB_') or k in ('DB_USERNAME','DB_PASSWORD')};env.update(DB_INGEST_TEST_MODE='1',DB_INGEST_TEST_FAULT='CANCEL_MULTIPLE_WORKERS')
 stdout=case.folder/'cancel.stdout';stderr=case.folder/'cancel.stderr';observed={};names={}
 with stdout.open('w',encoding='utf-8') as out,stderr.open('w',encoding='utf-8') as err:
  process=subprocess.Popen([sys.executable,'-m','db_ingest','--config',str(case.config),'run','--parallel','--new-intent','--non-interactive','--json'],cwd=ROOT,env=env,stdout=out,stderr=err,shell=False)
  parent=psutil.Process(process.pid);started=time.monotonic()
  while process.poll() is None:
   if time.monotonic()-started>=240:
    process.kill();process.wait(timeout=30)
    for pid,created in observed.items():
     try:
      owned=psutil.Process(pid)
      if owned.create_time()==created:owned.kill()
     except (psutil.NoSuchProcess,psutil.AccessDenied):pass
    pytest.fail('Owned cancellation CLI timed out; only original owned process identities were cleaned up')
   try:
    for child in parent.children(recursive=True):
     try:observed[child.pid]=child.create_time();names[child.pid]=child.name()
     except (psutil.NoSuchProcess,psutil.AccessDenied):pass
   except psutil.NoSuchProcess:pass
   time.sleep(.01)
 secret=os.environ.get('DB_PASSWORD');combined=stdout.read_text(encoding='utf-8')+stderr.read_text(encoding='utf-8')
 for path in [stdout,stderr]:path.write_text(path.read_text(encoding='utf-8').replace(secret,'<redacted>') if secret else path.read_text(encoding='utf-8'),encoding='utf-8')
 assert not secret or secret not in combined,'Credential leaked by owned CLI';assert process.returncode==4,{'exit':process.returncode,'output':str(stdout)}
 status=case.cli('status');run_id=status['runs'][0]['run_id'];before=case.cli('status','--run-id',run_id,exit_code=4);initial={i['file_id']:i['intent_id'] for i in before['files']};assert len(initial)==4
 db=sqlite3.connect(case.folder/'work/journal.sqlite3')
 try:events=[json.loads(r[0]) for r in db.execute("SELECT details FROM events WHERE run_id=? AND category='TEST_CANCELLATION_REQUESTED'",(run_id,))]
 finally:db.close()
 assert len(events)==1 and len(events[0]['worker_pids'])>=2 and 'injected' in events[0]['signal']
 pids=events[0]['worker_pids']+events[0]['native_pids'];assert all(pid in observed for pid in events[0]['worker_pids'])
 if engine=='bcp':assert events[0]['native_pids'] and any(name.lower()=='bcp.exe' for name in names.values())
 # A native child may finish between10ms external samples. Its OS FILETIME creation
 # identity retained at the cancellation point permits independent PID-reuse-safe verification.
 import ctypes
 from ctypes import wintypes
 kernel=ctypes.WinDLL('kernel32',use_last_error=True)
 kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD];kernel.OpenProcess.restype=wintypes.HANDLE
 kernel.GetProcessTimes.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.FILETIME),ctypes.POINTER(wintypes.FILETIME),ctypes.POINTER(wintypes.FILETIME),ctypes.POINTER(wintypes.FILETIME)];kernel.CloseHandle.argtypes=[wintypes.HANDLE]
 def current_identity(pid):
  handle=kernel.OpenProcess(0x1000,False,pid)
  if not handle:
   assert not psutil.pid_exists(pid),'Cannot verify an existing owned process identity'
   return None
  try:
   created,exited,kernel_time,user_time=[wintypes.FILETIME() for _ in range(4)]
   assert kernel.GetProcessTimes(handle,created,exited,kernel_time,user_time)
   return (created.dwHighDateTime<<32)|created.dwLowDateTime
  finally:kernel.CloseHandle(handle)
 for pid in pids:
  expected=events[0]['creation_identities'][str(pid)];assert expected
  assert current_identity(pid)!=expected,'An original owned worker/native child survived CLI cancellation'
 after=case.cli('resume','--run-id',run_id,'--parallel','--non-interactive');assert {i['file_id']:i['intent_id'] for i in after['files']}==initial
 for item in after['files']:
  case.verify({'files':[item]});assert sql_count(case,'SELECT COUNT_BIG(*) FROM '+target(item))==17
  assert sql_count(case,'SELECT COUNT(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',item['intent_id'])==1
 (case.folder/'cancel-evidence.json').write_text(json.dumps({'hook':events[0],'observed_process_names':names,'original_children_gone':True,'same_run_id':run_id,'same_capture_intents':True,'all_four_targets_exactly17rows':True,'result':'PASSED'},indent=2),encoding='utf-8')
