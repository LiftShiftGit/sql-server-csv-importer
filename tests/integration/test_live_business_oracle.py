"""Actual mapped business tables; independent generated-key and supplied-value oracle."""
import base64,csv,hashlib,json,os,runpy,sqlite3
from decimal import Decimal
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
class BusinessCase:
 def __init__(self,engine,scenario,named=False):
  self.case=API['TypeCase'](engine=engine,fast=False,batch_rows=7)
  self.preserve=scenario=='preserve';self.null=scenario=='explicit_null';self.scenario=scenario
  headers=(['id'] if self.preserve else [])+['note','amount']+(['optional'] if self.null else [])
  rows=[]
  for i in range(2):rows.append(([str(41+i)] if self.preserve else [])+['é raw  ','12.34']+([r'\N'] if self.null else []))
  if scenario=='constraint':rows.append(['bad','-1'])
  with self.case.source.open('w',encoding='utf-8',newline='') as stream:writer=csv.writer(stream);writer.writerow(headers);writer.writerows(rows)
  self.raw=self.case.source.read_bytes();text=self.case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
  omitted=([] if self.preserve else ['id'])+['factory','twice','version']+([] if self.null else ['optional'])
  text+='\n[mapping]\nname_collision="stable_hash_suffix"\nexisting_target_strategy="mapped_business"\nomit_target_columns='+API['lit'](omitted)+'\nidentity_policy='+API['lit']('preserve' if self.preserve else 'generate')+'\nallow_target_triggers='+API['lit'](scenario in ['audit_trigger','transform_trigger'])+'\n'
  self.named_target='既存] 対象 ; $() '+self.case.source.stem[-16:] if named else None
  if named:text+='target_table='+API['lit'](self.named_target)+'\n'
  text+='\n[[file_rules]]\nglob='+API['lit'](self.case.source.name)+'\n'
  for ordinal,name in enumerate(headers,1):
   kind='int' if name=='id' else 'decimal(10,2)' if name=='amount' else 'nvarchar(20)'
   text+='\n[[file_rules.columns]]\n'+'\n'.join(k+'='+API['lit'](v) for k,v in dict(source_ordinal=ordinal,target_name=name,target_type=kind).items())+'\n'
  self.case.config.write_text(text,encoding='utf-8')
  plan=self.case.cli('plan','--new-intent')['files'][0]['plan'];self.target=API['q'](plan['schema'])+'.'+API['q'](plan['table']);self.audit=API['q'](plan['schema'])+'.'+API['q'](plan['table']+'__observed')
  conn=self.case.connection()
  try:
   cur=conn.cursor();cur.execute('CREATE TABLE '+self.target+"(id int IDENTITY(1,1) NOT NULL PRIMARY KEY,note nvarchar(20) NULL,amount decimal(10,2) NOT NULL CHECK(amount>0),optional nvarchar(20) NULL DEFAULT N'opt  ',factory nvarchar(20) NOT NULL DEFAULT N'factory  ',twice AS (amount*2),version rowversion)")
   if scenario in ['audit_trigger','transform_trigger']:
    trigger=API['q'](plan['schema'])+'.'+API['q'](plan['table']+'__trigger')
    if scenario=='audit_trigger':
     cur.execute('CREATE TABLE '+self.audit+'(id int NOT NULL,note nvarchar(20) NULL)')
     body='INSERT INTO '+self.audit+' SELECT id,note FROM inserted;'
    else:body='UPDATE target SET note=UPPER(target.note) FROM '+self.target+' target JOIN inserted incoming ON target.id=incoming.id;'
    cur.execute('CREATE TRIGGER '+trigger+' ON '+self.target+' AFTER INSERT AS BEGIN SET NOCOUNT ON; '+body+' END')
   conn.commit()
  finally:conn.close()
 def verify(self,report):
  item=report['files'][0];assert item['receipt']['committed_records']==2
  assert item['receipt']['details']['target_strategy']=='mapped_business'
  assert item['receipt']['source_hash']==hashlib.sha256(self.raw).hexdigest()
  assert Path(item['artifact']['path']).read_bytes()==self.raw
  conn=self.case.connection()
  try:
   cur=conn.cursor();actual=[tuple(r) for r in cur.execute('SELECT id,note,amount,optional,factory,twice,version FROM '+self.target+' ORDER BY id')]
   ids=[41,42] if self.preserve else [1,2]
   assert len(actual)==2
   for row,identity in zip(actual,ids,strict=True):
    assert row[:6]==(identity,'é raw  ',Decimal('12.34'),None if self.null else 'opt  ','factory  ',Decimal('24.68'))
    assert isinstance(row[6],bytes) and len(row[6])==8
   sidecars=[tuple(r) for r in cur.execute('SELECT record_id,ordinal,key_json,generated_json FROM [audit].[__db_ingest_business_rows] WHERE intent_id=? ORDER BY ordinal',item['intent_id'])]
   assert len(sidecars)==len({r[0] for r in sidecars})==2 and [r[1] for r in sidecars]==[2,3]
   local=sqlite3.connect(self.case.folder/'work/journal.sqlite3')
   try:expected_ids=list(local.execute("SELECT record_id,ordinal FROM records WHERE run_id=? AND file_id=? AND disposition='committed' ORDER BY ordinal",(report['run_id'],item['file_id'])))
   finally:local.close()
   assert [(row[0],row[1]) for row in sidecars]==expected_ids
   observed={r[0]:r for r in actual}
   for record,ordinal,key_json,generated_json in sidecars:
    key=json.loads(key_json);assert len(key)==1 and key[0]['column']['target_name']=='id'
    identity=int(base64.b64decode(key[0]['value']).decode('utf-16-le'));assert identity==ids[ordinal-2]
    generated={f['column']['target_name']:None if f['value'] is None else base64.b64decode(f['value']) if f['column']['sql_type'].startswith('varbinary') else base64.b64decode(f['value']).decode('utf-16-le') for f in json.loads(generated_json)}
    assert generated['factory']=='factory  ' and Decimal(generated['twice'])==Decimal('24.68')
    assert generated['version']==observed[identity][6]
    if not self.null:assert generated['optional']=='opt  '
   if self.scenario=='audit_trigger':assert [tuple(r) for r in cur.execute('SELECT id,note FROM '+self.audit+' ORDER BY id')]==[(i,'é raw  ') for i in ids]
  finally:conn.rollback();conn.close()
  return item

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('scenario',['generate','explicit_null','preserve','audit_trigger','transform_trigger','constraint','lost_ack'])
def test_business_generated_mapping_defaults_triggers_constraints_and_lost_ack(engine,scenario):
 case=BusinessCase(engine,scenario)
 if scenario=='lost_ack':
  case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':'AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT'})
  status=case.case.cli('status');run_id=status['runs'][0]['run_id'];before=case.case.cli('status','--run-id',run_id)
  report=case.case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive')
  assert report['files'][0]['intent_id']==before['files'][0]['intent_id']
 elif scenario in ['transform_trigger','constraint']:
  report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=5 if scenario=='transform_trigger' else 4)
  assert 'BUSINESS_SOURCE_TRANSFORMED' in str(report) if scenario=='transform_trigger' else '547' in str(report)
  conn=case.case.connection()
  try:
   assert conn.cursor().execute('SELECT COUNT_BIG(*) FROM '+case.target).fetchval()==0
   assert conn.cursor().execute('SELECT COUNT(*) FROM [audit].[__db_ingest_business_rows] WHERE intent_id=?',report['files'][0]['intent_id']).fetchval()==0
   assert conn.cursor().execute('SELECT COUNT(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',report['files'][0]['intent_id']).fetchval()==0
  finally:conn.rollback();conn.close()
  assert Path(report['files'][0]['artifact']['path']).read_bytes()==case.raw
  assert report['files'][0]['counts']['committed']==report['files'][0]['counts']['rejected']==0
  assert report['files'][0]['counts']['pending']==(3 if scenario=='constraint' else 2)
  return
 else:report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive')
 item=case.verify(report)
 assert item['receipt']['details']['transport']['engine']==engine
 reconciled=case.case.cli('reconcile','--run-id',report['run_id'],'--non-interactive')
 assert reconciled['files'][0]['destination']['state']=='VERIFIED'

@pytest.mark.parametrize('policy',['missing_omission','no_stable_key','ignore_dup_key','unapproved_trigger','instead_of_trigger'])
def test_business_unsupported_semantics_refuse_without_mutation(policy):
 case=BusinessCase('odbc','audit_trigger' if policy=='unapproved_trigger' else 'generate')
 if policy in ['missing_omission','unapproved_trigger']:
  text=case.case.config.read_text(encoding='utf-8')
  text=text.replace(',"optional"','') if policy=='missing_omission' else text.replace('allow_target_triggers=true','allow_target_triggers=false')
  case.case.config.write_text(text,encoding='utf-8')
 else:
  conn=case.case.connection()
  try:
   cur=conn.cursor()
   if policy=='no_stable_key':
    pk=cur.execute('SELECT name FROM sys.key_constraints WHERE parent_object_id=OBJECT_ID(?) AND type=?',case.target,'PK').fetchval()
    cur.execute('ALTER TABLE '+case.target+' DROP CONSTRAINT '+API['q'](pk))
   elif policy=='ignore_dup_key':cur.execute('CREATE UNIQUE INDEX [ignore_source] ON '+case.target+'(id) WITH (IGNORE_DUP_KEY=ON)')
   else:
    trigger=case.target[:-1]+'__instead]'
    cur.execute('CREATE TRIGGER '+trigger+' ON '+case.target+' INSTEAD OF INSERT AS BEGIN SET NOCOUNT ON; RETURN; END')
   conn.commit()
  finally:conn.close()
 report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 expected={'missing_omission':'BUSINESS_OMISSION_REQUIRED','no_stable_key':'BUSINESS_STABLE_KEY_REQUIRED','ignore_dup_key':'IGNORE_DUP_KEY_UNSUPPORTED','unapproved_trigger':'BUSINESS_TRIGGER_POLICY','instead_of_trigger':'BUSINESS_INSTEAD_OF_UNSUPPORTED'}[policy]
 assert expected in str(report)
 conn=case.case.connection()
 try:assert conn.cursor().execute('SELECT COUNT_BIG(*) FROM '+case.target).fetchval()==0
 finally:conn.rollback();conn.close()
 assert not report['files'][0].get('receipt')
 assert Path(report['files'][0]['artifact']['path']).read_bytes()==case.raw


@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_explicit_named_existing_business_table_unicode_quoted_identity(engine):
 case=BusinessCase(engine,'generate',named=True)
 report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive')
 item=case.verify(report)
 assert item['plan']['table']==case.named_target and item['receipt']['table']==case.named_target
 assert item['receipt']['details']['transport']['engine']==engine
 result=case.case.cli('reconcile','--run-id',report['run_id'],'--non-interactive')
 assert result['files'][0]['destination']['state']=='VERIFIED'

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_named_business_distinct_files_inferred_widths_keep_prior_contract(engine):
 case=BusinessCase(engine,'generate',named=True)
 inferred=case.case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
 inferred+='\n[[file_rules]]\nglob="**/*.csv"\n'
 for ordinal,name in enumerate(['note','amount'],1):inferred+='\n[[file_rules.columns]]\nsource_ordinal='+str(ordinal)+'\ntarget_name='+API['lit'](name)+'\n'
 case.case.config.write_text(inferred,encoding='utf-8')
 first=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');case.verify(first)
 first_types=[c['sql_type'] for c in first['files'][0]['plan']['columns']]
 old_source=case.case.source;second=case.case.folder/'second_input.csv';second_bytes='note,amount\r\nwider🙂  ,1234.5\r\n'.encode('utf-8');second.write_bytes(second_bytes)
 config=inferred.replace('path = '+API['lit'](str(old_source)),'path = '+API['lit'](str(second))).replace('include = '+API['lit']([old_source.name]),'include = '+API['lit']([second.name]))
 assert config!=inferred;case.case.config.write_text(config,encoding='utf-8')
 later=case.case.cli('run','--no-parallel','--non-interactive')
 assert later['files'][0]['plan']['table']==case.named_target
 assert [c['sql_type'] for c in later['files'][0]['plan']['columns']]!=first_types
 assert later['files'][0]['receipt']['committed_records']==1
 assert Path(later['files'][0]['artifact']['path']).read_bytes()==second_bytes
 conn=case.case.connection()
 try:
  actual=[tuple(r) for r in conn.cursor().execute('SELECT id,note,amount,optional,factory,twice FROM '+case.target+' ORDER BY id')]
  assert actual==[(1,'é raw  ',Decimal('12.34'),'opt  ','factory  ',Decimal('24.68')),(2,'é raw  ',Decimal('12.34'),'opt  ','factory  ',Decimal('24.68')),(3,'wider🙂  ',Decimal('1234.50'),'opt  ','factory  ',Decimal('2469.00'))]
  assert tuple(conn.cursor().execute('SELECT COUNT(DISTINCT intent_id),COUNT(DISTINCT record_id) FROM [audit].[__db_ingest_business_rows] WHERE target=?',case.target).fetchone())==(2,3)
 finally:conn.rollback();conn.close()
 oldconfig=case.case.folder/'first-inferred-config.toml';oldconfig.write_text(inferred,encoding='utf-8');case.case.config=oldconfig
 reconciled=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
 assert reconciled['files'][0]['destination']['state']=='VERIFIED'
 assert reconciled['files'][0]['destination']['value_digest']==first['files'][0]['receipt']['value_digest']

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_named_business_next_mapping_can_preserve_identity_and_supply_null(engine):
 case=BusinessCase(engine,'generate',named=True);first=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');case.verify(first)
 original=case.case.config.read_text(encoding='utf-8');second=case.case.folder/'preserve_next.csv';data=b'id,note,amount,optional\r\n42,explicit,3.50,\\N\r\n';second.write_bytes(data)
 prefix=original.split('[[file_rules]]')[0].replace('path = '+API['lit'](str(case.case.source)),'path = '+API['lit'](str(second))).replace('include = '+API['lit']([case.case.source.name]),'include = '+API['lit']([second.name])).replace('identity_policy="generate"','identity_policy="preserve"')
 prefix='\n'.join('omit_target_columns='+API['lit'](['factory','twice','version']) if line.startswith('omit_target_columns=') else line for line in prefix.splitlines())+'\n'
 prefix+='\n[[file_rules]]\nglob='+API['lit'](second.name)+'\n'
 for index,(name,kind) in enumerate([('id','int'),('note','nvarchar(20)'),('amount','decimal(10,2)'),('optional','nvarchar(20)')],1):prefix+='\n[[file_rules.columns]]\n'+'\n'.join(k+'='+API['lit'](v) for k,v in dict(source_ordinal=index,target_name=name,target_type=kind).items())+'\n'
 case.case.config.write_text(prefix,encoding='utf-8');later=case.case.cli('run','--no-parallel','--non-interactive')
 assert later['files'][0]['receipt']['committed_records']==1 and Path(later['files'][0]['artifact']['path']).read_bytes()==data
 conn=case.case.connection()
 try:
  actual=[tuple(row) for row in conn.cursor().execute('SELECT id,note,amount,optional,factory,twice FROM '+case.target+' ORDER BY id')]
  assert actual==[(1,'é raw  ',Decimal('12.34'),'opt  ','factory  ',Decimal('24.68')),(2,'é raw  ',Decimal('12.34'),'opt  ','factory  ',Decimal('24.68')),(42,'explicit',Decimal('3.50'),None,'factory  ',Decimal('7.00'))]
 finally:conn.rollback();conn.close()
 oldpath=case.case.folder/'first-generated-config.toml';oldpath.write_text(original,encoding='utf-8');case.case.config=oldpath
 result=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
 assert result['files'][0]['destination']['state']=='VERIFIED' and result['files'][0]['destination']['value_digest']==first['files'][0]['receipt']['value_digest']
