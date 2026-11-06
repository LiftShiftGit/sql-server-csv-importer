"""Owned schema evolution with authored values and old receipt preservation."""
import csv,json,os,runpy
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
class EvolutionCase:
 def __init__(self,engine):
  self.case=API['TypeCase'](engine=engine,fast=False);self.prefix=self.case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
 def write(self,headers,rows,columns,evolve=False):
  with self.case.source.open('w',encoding='utf-8',newline='') as stream:
   writer=csv.writer(stream);writer.writerow(headers);writer.writerows(rows)
  text=self.prefix
  if evolve:text+='\n[schema]\nallow_add_columns_on_append=true\nallow_widen_on_append=true\nallow_retype_on_append=true\n'
  text+='\n[[file_rules]]\nglob='+API['lit'](self.case.source.name)+'\n'
  for index,(name,kind) in enumerate(columns,1):text+='\n[[file_rules.columns]]\nsource_ordinal='+str(index)+'\ntarget_name='+API['lit'](name)+'\ntarget_type='+API['lit'](kind)+'\n'
  self.case.config.write_text(text,encoding='utf-8')
 def initialize(self):
  self.write(['id','note'],[['1','🙂 '],['2',r'\N']],[('id','int'),('note','nvarchar(4)')])
  first=self.case.cli('run','--no-parallel','--non-interactive');self.original_config=self.case.config.read_text(encoding='utf-8');self.original_bytes=self.case.source.read_bytes()
  self.first=first;self.target=API['q'](first['files'][0]['plan']['schema'])+'.'+API['q'](first['files'][0]['plan']['table']);return first
 def read(self,extra=False):
  conn=self.case.connection()
  try:return [tuple(row) for row in conn.cursor().execute('SELECT id,note'+(',extra' if extra else '')+',__ingest_intent,__ingest_record FROM '+self.target+' ORDER BY id')]
  finally:conn.rollback();conn.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_owned_evolution_named_reorder_add_widen_preserves_old_receipts(engine):
 case=EvolutionCase(engine);first=case.initialize();before=case.read();initial_hash=first['files'][0]['receipt']['value_digest']
 case.write(['note','id','extra'],[['long🙂  ','2147483648','new']],[('note','nvarchar(20)'),('id','bigint'),('extra','nvarchar(10)')],True)
 report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive')
 actual=case.read(True)
 assert [r[:3] for r in actual]==[(1,'🙂 ',None),(2,None,None),(2147483648,'long🙂  ','new')]
 assert [r[3:] for r in actual[:2]]==[r[2:] for r in before]
 assert Path(first['files'][0]['artifact']['path']).read_bytes()==case.original_bytes
 assert first['files'][0]['receipt']['value_digest']==initial_hash
 new_config=case.case.config;old_path=case.case.folder/'original-config.toml';old_path.write_text(case.original_config,encoding='utf-8');case.case.config=old_path
 try:
  result=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
  assert result['exit_code']==0 and result['files'][0]['destination']['state']=='VERIFIED'
  assert result['files'][0]['destination']['value_digest']==initial_hash
  resumed=case.case.cli('resume','--run-id',first['run_id'],'--non-interactive')
  assert resumed['state']=='CLEAN_VERIFIED' and resumed['files'][0]['intent_id']==first['files'][0]['intent_id']
 finally:case.case.config=new_config
 assert case.read(True)==actual

@pytest.mark.parametrize('change',['narrow','drop','index','check','default','foreign_key','drift'])
def test_owned_evolution_incompatible_or_dependent_target_unchanged(change):
 case=EvolutionCase('odbc');case.initialize();before=case.read()
 if change in ('index','check','default','foreign_key','drift'):
  statements={'index':'CREATE INDEX [independent_note_index] ON '+case.target+'(note)','check':'ALTER TABLE '+case.target+' ADD CONSTRAINT [independent_note_check] CHECK (LEN(note)<=20)','default':'ALTER TABLE '+case.target+' ADD CONSTRAINT [independent_note_default] DEFAULT N\'x\' FOR note','drift':'ALTER TABLE '+case.target+' ADD [external_drift] int NULL'}
  suffix=case.case.source.stem[-16:]
  statements={k:v.replace('[independent_note_', '[independent_'+suffix+'_note_') for k,v in statements.items()}
  conn=case.case.connection()
  try:
   if change=='foreign_key':
    reference=case.target[:-1]+'_reference]'
    conn.cursor().execute('CREATE TABLE '+reference+'(id int NOT NULL PRIMARY KEY)');conn.cursor().execute('INSERT INTO '+reference+' VALUES(1),(2)');conn.cursor().execute('ALTER TABLE '+case.target+' ADD CONSTRAINT [independent_fk_'+suffix+'] FOREIGN KEY(id) REFERENCES '+reference+'(id)')
   else:conn.cursor().execute(statements[change])
   conn.commit()
  finally:conn.close()
 columns=[('id','int'),('note','nvarchar(2)' if change=='narrow' else 'nvarchar(20)')]
 headers=['id','note'];rows=[['3','x']]
 if change=='drop':columns=columns[:1];headers=headers[:1];rows=[['3']]
 case.write(headers,rows,columns,True)
 report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 assert any(code in str(report) for code in ['SCHEMA_EVOLUTION_BLOCKED','SCHEMA_DEPENDENCY_BLOCKED','SCHEMA_DRIFT'])
 assert case.read()==before
 item=report['files'][0];assert not item.get('receipt')
 assert Path(item['artifact']['path']).read_bytes()==case.case.source.read_bytes()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_owned_decimal_and_temporal_widening_preserves_every_old_value(engine):
 case=EvolutionCase(engine)
 names=['id','amount','clock','stamp','zone']
 old_types=['int','decimal(6,2)','time(2)','datetime2(2)','datetimeoffset(2)']
 case.write(names,[['1','1234.56','12:34:56.12','2024-02-29T12:34:56.12','2024-02-29T12:34:56.12+05:30']],list(zip(names,old_types)))
 first=case.case.cli('run','--no-parallel','--non-interactive');old_config=case.case.config.read_text(encoding='utf-8');old_bytes=case.case.source.read_bytes();old_item=first['files'][0]
 target=API['q'](old_item['plan']['schema'])+'.'+API['q'](old_item['plan']['table'])
 new_types=['int','decimal(12,4)','time(7)','datetime2(7)','datetimeoffset(7)']
 case.write(names,[['2','1234567.1234','23:59:59.1234567','2024-03-01T23:59:59.1234567','2024-03-01T23:59:59.1234567-03:30']],list(zip(names,new_types)),True)
 case.case.cli('run','--new-intent','--no-parallel','--non-interactive')
 from decimal import Decimal
 conn=case.case.connection()
 try:
  actual=[tuple(r) for r in conn.cursor().execute('SELECT id,amount,CONVERT(nvarchar(64),clock,126),CONVERT(nvarchar(64),stamp,126),CONVERT(nvarchar(64),zone,126) FROM '+target+' ORDER BY id')]
  assert actual==[(1,Decimal('1234.5600'),'12:34:56.1200000','2024-02-29T12:34:56.1200000','2024-02-29T12:34:56.1200000+05:30'),(2,Decimal('1234567.1234'),'23:59:59.1234567','2024-03-01T23:59:59.1234567','2024-03-01T23:59:59.1234567-03:30')]
 finally:conn.rollback();conn.close()
 assert Path(old_item['artifact']['path']).read_bytes()==old_bytes
 oldpath=case.case.folder/'old-temporal-config.toml';oldpath.write_text(old_config,encoding='utf-8');case.case.config=oldpath
 result=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
 assert result['files'][0]['destination']['state']=='VERIFIED'
 assert result['files'][0]['destination']['value_digest']==old_item['receipt']['value_digest']

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_replace_retained_generation_reconciles_original_exact_receipt(engine):
 case=EvolutionCase(engine);first=case.initialize();before=case.read();original=case.original_config
 case.write(['id','note'],[['3','new']],[('id','int'),('note','nvarchar(4)')])
 text=case.case.config.read_text(encoding='utf-8').replace('mode = "append"','mode = "replace"');case.case.config.write_text(text,encoding='utf-8')
 replaced=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');item=replaced['files'][0]
 assert [row[:2] for row in case.read()]==[(3,'new')]
 backup=item['receipt']['details']['previous_generation'];assert backup
 conn=case.case.connection()
 try:
  actual=[tuple(r) for r in conn.cursor().execute('SELECT id,note,__ingest_intent,__ingest_record FROM '+backup+' ORDER BY id')]
  assert actual==before
 finally:conn.rollback();conn.close()
 old=case.case.folder/'original-replace-config.toml';old.write_text(original,encoding='utf-8');case.case.config=old
 result=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
 assert result['files'][0]['destination']['state']=='VERIFIED'
 assert result['files'][0]['destination']['value_digest']==first['files'][0]['receipt']['value_digest']
 assert Path(first['files'][0]['artifact']['path']).read_bytes()==case.original_bytes

 # External edits to the retained generation must invalidate the old receipt proof.
 current=case.read();conn=case.case.connection()
 try:conn.cursor().execute('UPDATE '+backup+" SET note=N'bad' WHERE id=1");conn.commit()
 finally:conn.close()
 refused=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive',exit_code=5)
 assert refused['exit_code']==5
 assert case.read()==current

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_inferred_smaller_values_keep_acknowledged_owned_storage_without_alter(engine):
 case=EvolutionCase(engine);names=['id','note','amount']
 case.write(names,[['10000','long text','123456.1234']],[('id','int'),('note','nvarchar(20)'),('amount','decimal(12,4)')])
 first=case.case.cli('run','--no-parallel','--non-interactive');original=case.case.config.read_text(encoding='utf-8');p=first['files'][0]['plan'];target=API['q'](p['schema'])+'.'+API['q'](p['table'])
 conn=case.case.connection()
 try:before=[tuple(r) for r in conn.cursor().execute('SELECT name,user_type_id,max_length,precision,scale,is_nullable FROM sys.columns WHERE object_id=OBJECT_ID(?) ORDER BY column_id',target)]
 finally:conn.rollback();conn.close()
 with case.case.source.open('w',encoding='utf-8',newline='') as stream:
  writer=csv.writer(stream);writer.writerow(names);writer.writerow(['1','x','1.2'])
 text=case.prefix+'\n[[file_rules]]\nglob='+API['lit'](case.case.source.name)+'\n'
 for ordinal,name in enumerate(names,1):text+='\n[[file_rules.columns]]\nsource_ordinal='+str(ordinal)+'\ntarget_name='+API['lit'](name)+'\n'
 case.case.config.write_text(text,encoding='utf-8');later=case.case.cli('run','--new-intent','--no-parallel','--non-interactive')
 from decimal import Decimal
 conn=case.case.connection()
 try:
  after=[tuple(r) for r in conn.cursor().execute('SELECT name,user_type_id,max_length,precision,scale,is_nullable FROM sys.columns WHERE object_id=OBJECT_ID(?) ORDER BY column_id',target)]
  assert after==before
  assert [tuple(r) for r in conn.cursor().execute('SELECT id,note,amount FROM '+target+' ORDER BY id')]==[(1,'x',Decimal('1.2000')),(10000,'long text',Decimal('123456.1234'))]
 finally:conn.rollback();conn.close()
 assert later['files'][0]['receipt']['committed_records']==1
 old=case.case.folder/'original-wider-config.toml';old.write_text(original,encoding='utf-8');case.case.config=old
 result=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
 assert result['files'][0]['destination']['state']=='VERIFIED' and result['files'][0]['destination']['value_digest']==first['files'][0]['receipt']['value_digest']
