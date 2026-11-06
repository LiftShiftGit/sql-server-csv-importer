"""Read-only SQL review and immutable reviewed-target authority; actual disposable SQL."""
import hashlib,json,os,runpy
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
BUSINESS=runpy.run_path(str(ROOT/'tests/integration/test_live_business_oracle.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL required')

def state(case,target):
 conn=case.connection()
 try:
  cur=conn.cursor()
  objects=[tuple(r) for r in cur.execute("SELECT object_id,name,type,CONVERT(nvarchar(40),modify_date,126) FROM sys.objects WHERE is_ms_shipped=0 ORDER BY object_id")]
  rows=[tuple(r) for r in cur.execute('SELECT * FROM '+target+' ORDER BY id')]
  return objects,rows
 finally:conn.rollback();conn.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('action',['success','alter','tamper'])
def test_review_existing_target_readonly_then_same_intent_or_precise_refusal(engine,action):
 business=BUSINESS['BusinessCase'](engine,'generate',named=True);case=business.case
 before=state(case,business.target)
 reviewed=case.cli('plan','--new-intent','--check-target','--no-parallel','--non-interactive')
 assert reviewed['destination_review']=={'reviewed_targets':1,'read_only':True}
 assert state(case,business.target)==before,'Review performed DDL or target DML'
 item=reviewed['files'][0];pointer=item['counts']['target_review'];artifact=Path(pointer['path'])
 assert hashlib.sha256(artifact.read_bytes()).hexdigest()==pointer['sha256']
 data=json.loads(artifact.read_bytes());assert data['snapshot']['object_id'] is not None
 assert data['schema_diff']['create_target'] is False and data['permissions']['INSERT']==1
 assert data['intent_id']==item['intent_id'] and data['run_id']==reviewed['run_id']
 if action=='alter':
  conn=case.connection()
  try:conn.cursor().execute('ALTER TABLE '+business.target+' ADD external_change int NULL');conn.commit()
  finally:conn.close()
 elif action=='tamper':artifact.write_bytes(artifact.read_bytes()+b' ')
 after=case.cli('resume','--run-id',reviewed['run_id'],'--no-parallel','--non-interactive',exit_code=0 if action=='success' else 2 if action=='alter' else 5)
 assert after['files'][0]['intent_id']==item['intent_id']
 assert after['files'][0]['artifact']['capture_id']==item['artifact']['capture_id']
 if action=='success':business.verify(after)
 else:
  assert ('TARGET_REVIEW_DRIFT' if action=='alter' else 'TARGET_REVIEW_INTEGRITY') in str(after)
  assert state(case,business.target)[1]==before[1]

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_review_absent_target_concurrent_creation_blocks_without_touching_created_values(engine):
 case=API['TypeCase'](engine=engine)
 reviewed=case.cli('plan','--new-intent','--check-target','--no-parallel','--non-interactive');item=reviewed['files'][0]
 data=json.loads(Path(item['counts']['target_review']['path']).read_bytes());assert data['snapshot']['object_id'] is None
 target=API['q'](item['plan']['schema'])+'.'+API['q'](item['plan']['table'])
 conn=case.connection()
 try:conn.cursor().execute('CREATE TABLE '+target+'(id int NOT NULL,note nvarchar(20)); INSERT INTO '+target+" VALUES(91,N'untouched  ')");conn.commit()
 finally:conn.close()
 after=case.cli('resume','--run-id',reviewed['run_id'],'--no-parallel','--non-interactive',exit_code=2)
 assert 'TARGET_REVIEW_DRIFT' in str(after)
 conn=case.connection()
 try:assert [tuple(r) for r in conn.cursor().execute('SELECT id,note FROM '+target)]==[(91,'untouched  ')]
 finally:conn.rollback();conn.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_review_pointer_loss_cannot_remove_review_authority(engine):
 import sqlite3
 case=API['TypeCase'](engine=engine);reviewed=case.cli('plan','--new-intent','--check-target','--no-parallel');item=reviewed['files'][0]
 db=sqlite3.connect(case.folder/'work/journal.sqlite3')
 try:
  counts=json.loads(db.execute('SELECT counts FROM files WHERE run_id=? AND file_id=?',(reviewed['run_id'],item['file_id'])).fetchone()[0]);del counts['target_review']
  db.execute('UPDATE files SET counts=? WHERE run_id=? AND file_id=?',(json.dumps(counts),reviewed['run_id'],item['file_id']));db.commit()
 finally:db.close()
 refused=case.cli('resume','--run-id',reviewed['run_id'],'--no-parallel',exit_code=5)
 assert 'TARGET_REVIEW_INTEGRITY' in str(refused)
 conn=case.connection()
 try:assert conn.cursor().execute('SELECT OBJECT_ID(?)',API['q'](item['plan']['schema'])+'.'+API['q'](item['plan']['table'])).fetchval() is None
 finally:conn.rollback();conn.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_failed_requested_review_blocks_publication_until_same_intent_review_completes(engine):
 import sqlite3,uuid
 case=API['TypeCase'](engine=engine)
 failure=case.cli('plan','--new-intent','--check-target','--no-parallel',exit_code=4,extra_env={'DB_USERNAME':'NoSuchFixtureLogin_'+uuid.uuid4().hex,'DB_PASSWORD':'deliberately-invalid-fixture-password'})
 assert failure['context']['native_code']==18456
 db=sqlite3.connect(case.folder/'work/journal.sqlite3')
 try:
  run_id=db.execute('SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1').fetchone()[0]
  intent=db.execute('SELECT intent_id FROM files WHERE run_id=?',(run_id,)).fetchone()[0]
  assert db.execute("SELECT COUNT(*) FROM events WHERE run_id=? AND category='TARGET_REVIEW_REQUESTED'",(run_id,)).fetchone()[0]==1
 finally:db.close()
 refused=case.cli('resume','--run-id',run_id,'--no-parallel',exit_code=2);assert 'TARGET_REVIEW_INCOMPLETE' in str(refused)
 completed=case.cli('resume','--run-id',run_id,'--plan-only','--check-target','--no-parallel')
 assert completed['files'][0]['intent_id']==intent and completed['destination_review']['reviewed_targets']==1
 after=case.cli('resume','--run-id',run_id,'--no-parallel');case.verify(after);assert after['files'][0]['intent_id']==intent
