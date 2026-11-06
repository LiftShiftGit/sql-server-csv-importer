"""Independent full business replacement including unimported baseline/generation rows."""
import base64,csv,hashlib,json,os,runpy,sqlite3
from decimal import Decimal
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_business_oracle.py'))
TYPE=API['API']
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
class ReplaceCase:
 def __init__(self,engine,audit=False):
  self.business=API['BusinessCase'](engine,'audit_trigger' if audit else 'generate',named=True);self.case=self.business.case;self.target=self.business.target
  conn=self.case.connection()
  try:conn.cursor().execute('INSERT INTO '+self.target+"(note,amount,optional) VALUES(N'baseline A',1.50,N'manual  '),(N'baseline B',2.50,NULL)");conn.commit()
  finally:conn.close()
  self.first=self.case.cli('run','--new-intent','--no-parallel','--non-interactive');self.old_config=self.case.config.read_text(encoding='utf-8');self.old_raw=self.case.source.read_bytes();self.before=self.rows(self.target)
  assert [row[:6] for row in self.before]==[(1,'baseline A',Decimal('1.50'),'manual  ','factory  ',Decimal('3.00')),(2,'baseline B',Decimal('2.50'),None,'factory  ',Decimal('5.00')),(3,'é raw  ',Decimal('12.34'),'opt  ','factory  ',Decimal('24.68')),(4,'é raw  ',Decimal('12.34'),'opt  ','factory  ',Decimal('24.68'))]
  assert len({row[6] for row in self.before})==4 and all(len(row[6])==8 for row in self.before)
  # Independent forensic authority is durable BEFORE any replacement attempt.
  def encoded(value):
   if isinstance(value,Decimal):return {'decimal':format(value,'f')}
   if isinstance(value,bytes):return {'base64':base64.b64encode(value).decode('ascii')}
   return value
  conn=self.case.connection()
  try:
   object_id=conn.cursor().execute('SELECT OBJECT_ID(?)',self.target).fetchval()
   assert object_id==self.first['files'][0]['receipt']['details']['target_object_id']
  finally:conn.rollback();conn.close()
  forensic={'table':self.target,'database':self.case.d['database'],'server':self.case.d['server'],'target_object_id':object_id,'columns':['id','note','amount','optional','factory','twice','version'],'rows':[[encoded(value) for value in row] for row in self.before],'unimported_baseline_ids':[1,2],'first_import_intent':self.first['files'][0]['intent_id'],'source_sha256':hashlib.sha256(self.old_raw).hexdigest(),'authority':'Independent direct SQL snapshot durably written before replacement child CLI starts'}
  raw=(json.dumps(forensic,ensure_ascii=True,sort_keys=True,indent=2)+'\n').encode('utf-8');self.before_sha=hashlib.sha256(raw).hexdigest();self.before_path=self.case.folder/'independent-before-state.json'
  with self.before_path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
  with (self.case.folder/'independent-before-state.sha256').open('x',encoding='ascii') as stream:stream.write(self.before_sha+'\n');stream.flush();os.fsync(stream.fileno())
 def rows(self,target):
  conn=self.case.connection()
  try:return [tuple(row) for row in conn.cursor().execute('SELECT id,note,amount,optional,factory,twice,CONVERT(varbinary(8),version) FROM '+target+' ORDER BY id')]
  finally:conn.rollback();conn.close()
 def prepare(self,scenario):
  self.preserve=scenario=='preserve';self.empty=scenario in ['empty','empty_approved'];source=self.case.folder/'replacement.csv'
  names=(['id'] if self.preserve else [])+['note','amount','optional'];rows=[] if self.empty else [(['7'] if self.preserve else [])+['new  ','3.50',r'\N']]
  if scenario in ['constraint','incomplete']:rows.append(['bad','-1.00' if scenario=='constraint' else 'not-number',r'\N'])
  with source.open('w',encoding='utf-8',newline='') as stream:writer=csv.writer(stream);writer.writerow(names);writer.writerows(rows)
  self.new_raw=source.read_bytes();prefix=self.old_config.split('[[file_rules]]')[0].replace('path = '+TYPE['lit'](str(self.case.source)),'path = '+TYPE['lit'](str(source))).replace('include = '+TYPE['lit']([self.case.source.name]),'include = '+TYPE['lit']([source.name])).replace('mode = "append"','mode = "replace"')
  prefix=prefix.replace('identity_policy="generate"','identity_policy='+TYPE['lit']('preserve' if self.preserve else 'generate'))
  omissions=([] if self.preserve else ['id'])+['factory','twice','version']
  prefix='\n'.join('omit_target_columns='+TYPE['lit'](omissions) if line.startswith('omit_target_columns=') else line for line in prefix.splitlines())+'\n'
  if scenario=='empty_approved':prefix=prefix.replace('mode = "replace"','mode = "replace"\nallow_empty_replace=true')
  prefix+='\n[[file_rules]]\nglob='+TYPE['lit'](source.name)+'\n'
  for index,name in enumerate(names,1):
   kind='int' if name=='id' else 'decimal(10,2)' if name=='amount' else 'nvarchar(20)'
   prefix+='\n[[file_rules.columns]]\n'+'\n'.join(k+'='+TYPE['lit'](v) for k,v in dict(source_ordinal=index,target_name=name,target_type=kind).items())+'\n'
  self.case.config.write_text(prefix,encoding='utf-8');self.new_config=prefix
 def reconcile_old(self,expected=0):
  current=self.case.config;old=self.case.folder/'original-append.toml';old.write_text(self.old_config,encoding='utf-8');self.case.config=old
  try:return self.case.cli('reconcile','--run-id',self.first['run_id'],'--non-interactive',exit_code=expected)
  finally:self.case.config=current
 def verify(self,report):
  assert hashlib.sha256(self.before_path.read_bytes()).hexdigest()==self.before_sha
  item=report['files'][0];assert item['receipt']['details']['operation']=='replace'
  assert item['receipt']['details']['target_object_id']==self.first['files'][0]['receipt']['details']['target_object_id']
  expected=[] if self.empty else [(7 if self.preserve else 5,'new  ',Decimal('3.50'),None,'factory  ',Decimal('7.00'))]
  current=self.rows(self.target);assert [row[:6] for row in current]==expected
  backup=item['receipt']['details']['previous_generation'];assert backup.startswith(self.target[:-1])
  assert self.rows(backup)==self.before #ALL rows and exact generated/rowversion bytes, including unimported baseline.
  assert Path(self.first['files'][0]['artifact']['path']).read_bytes()==self.old_raw
  assert Path(item['artifact']['path']).read_bytes()==self.new_raw
  old=self.reconcile_old();assert old['files'][0]['destination']['value_digest']==self.first['files'][0]['receipt']['value_digest']
  verified=self.case.cli('reconcile','--run-id',report['run_id'],'--non-interactive');assert verified['files'][0]['destination']['state']=='VERIFIED'
  return item,backup,current

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('scenario',['generate','preserve','middelete','lost_ack','constraint','incomplete','empty','empty_approved'])
def test_business_replace_all_rows_generations_atomicity_and_same_intent_recovery(engine,scenario):
 case=ReplaceCase(engine);case.prepare(scenario)
 if scenario in ['middelete','lost_ack']:
  point='REPLACE_AFTER_DELETE_BEFORE_INSERT' if scenario=='middelete' else 'AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT'
  case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':point})
  run_id=case.case.cli('status')['runs'][0]['run_id'];before=case.case.cli('status','--run-id',run_id);item=before['files'][0]
  if scenario=='middelete':assert case.rows(case.target)==case.before
  report=case.case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive')
  assert report['run_id']==run_id and report['files'][0]['file_id']==item['file_id'] and report['files'][0]['intent_id']==item['intent_id']
 elif scenario in ['constraint','incomplete','empty']:
  result=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=4 if scenario=='constraint' else 2)
  assert case.rows(case.target)==case.before and not result['files'][0].get('receipt')
  assert Path(result['files'][0]['artifact']['path']).read_bytes()==case.new_raw
  assert case.reconcile_old()['files'][0]['destination']['state']=='VERIFIED'
  return
 else:report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive')
 case.verify(report)

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('corruption',['unimported_backup_cell','missing_generation_membership'])
def test_business_replace_backup_all_rows_and_history_corruption_blocks_receipts(engine,corruption):
 case=ReplaceCase(engine);case.prepare('generate');report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');item,backup,current=case.verify(report)
 conn=case.case.connection()
 try:
  if corruption=='unimported_backup_cell':conn.cursor().execute('UPDATE '+backup+" SET note=N'forged baseline' WHERE id=1")
  else:conn.cursor().execute('DELETE FROM [audit].[__db_ingest_business_intent_generations] WHERE intent_id=?',case.first['files'][0]['intent_id'])
  conn.commit()
 finally:conn.close()
 case.reconcile_old(expected=5)
 refused=case.case.cli('reconcile','--run-id',report['run_id'],'--non-interactive',exit_code=5)
 assert 'BUSINESS_GENERATION' in str(refused)
 assert case.rows(case.target)==current

@pytest.mark.parametrize('dependency',['foreign_key','delete_trigger','update_trigger','retention_disabled'])
def test_business_replace_dependency_and_retention_policies_block_before_mutation(dependency):
 case=ReplaceCase('odbc');case.prepare('generate')
 if dependency=='retention_disabled':
  text=case.case.config.read_text(encoding='utf-8').replace('mode = "replace"','mode = "replace"\nretain_previous_generation=false');case.case.config.write_text(text,encoding='utf-8')
 else:
  conn=case.case.connection()
  try:
   if dependency=='foreign_key':
    reference=case.target[:-1]+'__refs]';name=TYPE['q']('fk_'+case.case.source.stem[-16:])
    conn.cursor().execute('CREATE TABLE '+reference+'(id int NOT NULL PRIMARY KEY)');conn.cursor().execute('INSERT INTO '+reference+' VALUES(1),(2),(3),(4)');conn.cursor().execute('ALTER TABLE '+case.target+' ADD CONSTRAINT '+name+' FOREIGN KEY(id) REFERENCES '+reference+'(id)')
   else:
    event='DELETE' if dependency=='delete_trigger' else 'UPDATE';trigger=case.target[:-1]+'__blocked_trigger]'
    conn.cursor().execute('CREATE TRIGGER '+trigger+' ON '+case.target+' AFTER '+event+" AS BEGIN SET NOCOUNT ON; THROW 51000,'unsupported trigger must never execute',1; END")
   conn.commit()
  finally:conn.close()
  if dependency.endswith('trigger'):
   text=case.case.config.read_text(encoding='utf-8').replace('allow_target_triggers=false','allow_target_triggers=true');case.case.config.write_text(text,encoding='utf-8')
 refused=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 if dependency=='retention_disabled':
  assert refused['code']=='CONFIG_CAPABILITY_UNAVAILABLE' and 'retain_previous_generation' in str(refused)
  assert case.rows(case.target)==case.before and (case.case.folder/'replacement.csv').read_bytes()==case.new_raw
  return
 code='BUSINESS_REPLACE_DEPENDENCY' if dependency=='foreign_key' else 'BUSINESS_REPLACE_TRIGGER'
 assert code in str(refused) and not refused['files'][0].get('receipt')
 assert case.rows(case.target)==case.before
 assert Path(refused['files'][0]['artifact']['path']).read_bytes()==case.new_raw

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('corruption',['prepared_missing','prepared_changed_bytes','unimported_current_row_changed'])
def test_business_unknown_outcome_requires_original_complete_prepared_authority(engine,corruption):
 case=ReplaceCase(engine);case.prepare('generate')
 case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':'REPLACE_AFTER_DELETE_BEFORE_INSERT'})
 run_id=case.case.cli('status')['runs'][0]['run_id'];status=case.case.cli('status','--run-id',run_id);item=status['files'][0]
 assert case.rows(case.target)==case.before and not item.get('receipt')
 prepared=item['counts']['business_prepared'];path=Path(prepared['path']);assert path.resolve().is_relative_to(case.case.folder.resolve())
 assert hashlib.sha256(path.read_bytes()).hexdigest()==prepared['sha256']
 proof=json.loads(path.read_bytes());assert proof['values']['count']==4 and proof['run_id']==run_id and proof['intent_id']==item['intent_id']
 if corruption=='prepared_missing':path.rename(path.with_name('intentionally-missing-prepared.json'))
 elif corruption=='prepared_changed_bytes':
  saved=path.read_bytes();path.with_name('original-prepared-bytes.json').write_bytes(saved);path.write_bytes(saved+b' ')
 else:
  conn=case.case.connection()
  try:conn.cursor().execute('UPDATE '+case.target+" SET note=N'outside edit' WHERE id=1");conn.commit()
  finally:conn.close()
 current=case.rows(case.target)
 refused=case.case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive',exit_code=5)
 assert any(error['code'].startswith(('RECOVERY_BUSINESS','BUSINESS_PREPARED')) for error in refused.get('errors',[]))
 assert case.rows(case.target)==current and hashlib.sha256(case.before_path.read_bytes()).hexdigest()==case.before_sha
 conn=case.case.connection()
 try:
  assert conn.cursor().execute('SELECT COUNT(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',item['intent_id']).fetchval()==0
  assert conn.cursor().execute('SELECT COUNT(*) FROM [audit].[__db_ingest_business_rows] WHERE intent_id=?',item['intent_id']).fetchval()==0
 finally:conn.rollback();conn.close()
 assert case.reconcile_old()['files'][0]['destination']['state']=='VERIFIED'
 assert Path(item['artifact']['path']).read_bytes()==case.new_raw


@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_business_replace_approved_insert_audit_trigger_preserves_source_and_prior_audit(engine):
 case=ReplaceCase(engine,audit=True);case.prepare('generate');report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');case.verify(report)
 conn=case.case.connection()
 try:
  observed=[tuple(row) for row in conn.cursor().execute('SELECT id,note FROM '+case.business.audit+' ORDER BY id')]
  assert observed==[(1,'baseline A'),(2,'baseline B'),(3,'é raw  '),(4,'é raw  '),(5,'new  ')]
 finally:conn.rollback();conn.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_business_before_file_without_either_durable_seal_cannot_authorize_replay(engine):
 case=ReplaceCase(engine);case.prepare('generate')
 case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':'REPLACE_AFTER_DELETE_BEFORE_INSERT'})
 run_id=case.case.cli('status')['runs'][0]['run_id'];status=case.case.cli('status','--run-id',run_id);item=status['files'][0];pointer=item['counts']['business_prepared'];path=Path(pointer['path'])
 proof=json.loads(path.read_bytes());proof['prepared_at_utc']='2099-01-01T00:00:00+00:00';path.write_text(json.dumps(proof,ensure_ascii=True)+'\n',encoding='utf-8')
 db=sqlite3.connect(case.case.folder/'work/journal.sqlite3')
 try:
  counts=json.loads(db.execute('SELECT counts FROM files WHERE run_id=? AND file_id=?',(run_id,item['file_id'])).fetchone()[0]);del counts['business_prepared']
  db.execute('UPDATE files SET counts=? WHERE run_id=? AND file_id=?',(json.dumps(counts),run_id,item['file_id']))
  db.execute("DELETE FROM events WHERE run_id=? AND file_id=? AND category='BUSINESS_PREPARED'",(run_id,item['file_id']));db.commit()
 finally:db.close()
 refused=case.case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive',exit_code=5)
 assert 'RECOVERY_BUSINESS_BASELINE_UNSEALED' in str(refused)
 assert case.rows(case.target)==case.before
 conn=case.case.connection()
 try:
  assert conn.cursor().execute('SELECT COUNT(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',item['intent_id']).fetchval()==0
  assert conn.cursor().execute('SELECT COUNT(*) FROM [audit].[__db_ingest_business_rows] WHERE intent_id=?',item['intent_id']).fetchval()==0
 finally:conn.rollback();conn.close()
 assert hashlib.sha256(case.before_path.read_bytes()).hexdigest()==case.before_sha

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_business_prepared_event_recovers_only_original_missing_count_pointer(engine):
 case=ReplaceCase(engine);case.prepare('generate')
 case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':'REPLACE_AFTER_DELETE_BEFORE_INSERT'})
 run_id=case.case.cli('status')['runs'][0]['run_id'];status=case.case.cli('status','--run-id',run_id);item=status['files'][0];original=item['counts']['business_prepared']
 db=sqlite3.connect(case.case.folder/'work/journal.sqlite3')
 try:
  counts=json.loads(db.execute('SELECT counts FROM files WHERE run_id=? AND file_id=?',(run_id,item['file_id'])).fetchone()[0]);del counts['business_prepared']
  db.execute('UPDATE files SET counts=? WHERE run_id=? AND file_id=?',(json.dumps(counts),run_id,item['file_id']));db.commit()
 finally:db.close()
 after=case.case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive');assert after['files'][0]['intent_id']==item['intent_id'] and after['files'][0]['file_id']==item['file_id']
 assert after['files'][0]['counts']['business_prepared']==original
 case.verify(after)
 db=sqlite3.connect(case.case.folder/'work/journal.sqlite3')
 try:
  categories=[r[0] for r in db.execute("SELECT category FROM events WHERE run_id=? AND file_id=? AND category LIKE 'BUSINESS_PREPARED%'",(run_id,item['file_id']))]
  assert 'BUSINESS_PREPARED_POINTER_REPAIRED' in categories
 finally:db.close()
