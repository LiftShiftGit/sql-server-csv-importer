"""Independent strict/union directory group, contributor and replacement oracle."""
import hashlib,json,os,runpy,sqlite3
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
class GroupCase:
 def __init__(self,engine,policy='strict'):
  self.case=API['TypeCase'](engine=engine,fast=False)
  self.a=self.case.source;self.b=self.case.folder/'zz_b.csv';self.policy=policy
  self.raw_a=b'id,note\r\n1,n  \r\n1,n  \r\n';self.raw_b=('id,'+('extra' if policy=='union_by_name' else 'note')+'\r\n2,🙂  \r\n').encode('utf-8')
  self.a.write_bytes(self.raw_a);self.b.write_bytes(self.raw_b)
  self.prefix=self.case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
  self.prefix=self.prefix.replace('path = '+API['lit'](str(self.a)),'path = '+API['lit'](str(self.case.folder)))
  self.prefix=self.prefix.replace('include = '+API['lit']([self.a.name]),'include = ["**/*.csv"]')
  self.prefix+='\n[mapping]\nname_collision="stable_hash_suffix"\nroot_table='+API['lit']('group_'+self.a.stem[-20:])+'\ntable_strategy="per_directory"\ndirectory_merge_schema='+API['lit'](policy)+'\n'
  self.write_config()
 def write_config(self,mode='append',subset=False,scope='complete_group'):
  text=self.prefix.replace('mode = "append"','mode = '+API['lit'](mode)+'\nreplace_scope='+API['lit'](scope))
  if subset:text=text.replace('include = ["**/*.csv"]','include = '+API['lit']([self.a.name]))
  for path,label in [(self.a,'note'),(self.b,'extra' if self.policy=='union_by_name' else 'note')]:
   text+='\n[[file_rules]]\nglob='+API['lit'](path.name)+'\n'
   for ordinal,name,kind in [(1,'id','int'),(2,label,'nvarchar(20)')]:text+='\n[[file_rules.columns]]\n'+'\n'.join(k+'='+API['lit'](v) for k,v in dict(source_ordinal=ordinal,target_name=name,target_type=kind).items())+'\n'
  self.case.config.write_text(text,encoding='utf-8')
 def values(self,group):
  p=group['plan'];target=API['q'](p['schema'])+'.'+API['q'](p['table']);conn=self.case.connection()
  try:
   expr='id,note'+(',extra' if self.policy=='union_by_name' else '')+',__ingest_record,__ingest_ordinal'
   return [tuple(row) for row in conn.cursor().execute('SELECT '+expr+' FROM '+target+' ORDER BY __ingest_ordinal')]
  finally:conn.rollback();conn.close()
 def verify(self,report):
  assert len(report['files'])==2 and len(report['groups'])==1
  group=report['groups'][0];assert group['receipt']['committed_records']==3
  assert sorted(x['counts']['committed'] for x in report['files'])==[1,2]
  for file in report['files']:assert Path(file['artifact']['path']).read_bytes()==Path(file['artifact']['original_path']).read_bytes()
  manifest=group['manifest'];raw=Path(manifest['manifest_path']).read_bytes()
  assert hashlib.sha256(raw).hexdigest()==manifest['manifest_sha256']
  entries=[json.loads(line) for line in raw.splitlines()];assert len(entries)==manifest['contributor_count']==2
  assert {e['relative_path'] for e in entries}=={self.a.name,self.b.name}
  for e in entries:
   expected=self.raw_a if e['relative_path']==self.a.name else self.raw_b
   assert e['sha256']==hashlib.sha256(expected).hexdigest() and e['size']==len(expected)
  actual=self.values(group)
  expected=[(1,'n  ',None),(1,'n  ',None),(2,None,'🙂  ')] if self.policy=='union_by_name' else [(1,'n  '),(1,'n  '),(2,'🙂  ')]
  assert [r[:-2] for r in actual]==expected
  assert len({r[-2] for r in actual})==3 and [r[-1] for r in actual]==[2,3,5]
  db=sqlite3.connect(self.case.folder/'work/journal.sqlite3')
  try:
   rows=db.execute('SELECT source_file_id,source_ordinal,ordinal,payload FROM group_records WHERE run_id=? ORDER BY ordinal',(report['run_id'],)).fetchall()
   assert [r[1] for r in rows]==[2,3,2] and [r[2] for r in rows]==[2,3,5]
   assert len({r[0] for r in rows})==2
   if self.policy=='union_by_name':assert all(any(f['state']=='MISSING' for f in json.loads(r[3])['fields']) for r in rows)
  finally:db.close()
  return group,actual

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('policy',['strict','union_by_name'])
def test_directory_atomic_group_and_complete_contributor_replace(engine,policy):
 case=GroupCase(engine,policy);first=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');group,actual=case.verify(first)
 case.write_config(mode='replace',subset=True)
 refused=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 assert 'GROUP_REPLACE_OMITTED_CONTRIBUTORS' in str(refused) and case.b.name in str(refused)
 assert case.values(group)==actual
 # A fresh journal cannot erase the acknowledged destination/central contributor history.
 saved_config=case.case.config.read_text(encoding='utf-8')
 fresh_config=saved_config.replace(API['lit'](str(case.case.folder/'work')),API['lit'](str(case.case.folder/'fresh-work')))
 assert fresh_config!=saved_config
 case.case.config.write_text(fresh_config,encoding='utf-8')
 fresh_refused=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 assert 'GROUP_REPLACE_OMITTED_CONTRIBUTORS' in str(fresh_refused) and case.b.name in str(fresh_refused)
 assert case.values(group)==actual
 case.write_config(mode='replace',subset=True,scope='selected_subset')
 replaced=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');last=replaced['groups'][0]
 expected=[(1,'n  ',None),(1,'n  ',None)] if policy=='union_by_name' else [(1,'n  '),(1,'n  ')]
 # Union subset must preserve acknowledged nullable superset schema or require explicit evolution.
 assert [r[:-2] for r in case.values(last)]==expected
 assert last['receipt']['committed_records']==2
 assert last['receipt']['details']['previous_generation']


@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_strict_group_different_ordered_names_refuses_before_target(engine):
 case=GroupCase(engine,'union_by_name')
 case.prefix=case.prefix.replace('directory_merge_schema="union_by_name"','directory_merge_schema="strict"');case.write_config()
 report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 assert 'GROUP_SCHEMA_MISMATCH' in str(report)
 assert len(report['files'])==2 and not report['groups'][0].get('receipt')
 for item in report['files']:assert Path(item['artifact']['path']).read_bytes()==Path(item['artifact']['original_path']).read_bytes()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_contributor_unicode_scalar_order_matches_central_history(engine):
 case=GroupCase(engine,'strict')
 a=case.case.folder/('same_'+chr(0xE000)+'.csv');b=case.case.folder/('same_'+chr(0x10000)+'.csv')
 case.a.rename(a);case.b.rename(b);case.a=a;case.b=b;case.case.source=a;case.write_config()
 original_config=case.case.config.read_text(encoding='utf-8')
 first=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');group,actual=case.verify(first)
 lines=[json.loads(line) for line in Path(group['manifest']['manifest_path']).read_bytes().splitlines()]
 assert [item['relative_path'] for item in lines]==[a.name,b.name]
 assert a.name.encode('utf-16-le')>b.name.encode('utf-16-le') # SQL UTF16 BIN2 order differs here.
 case.write_config(mode='replace')
 replacement=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');new_group,new_actual=case.verify(replacement)
 assert [row[:-2] for row in new_actual]==[row[:-2] for row in actual]
 assert new_group['receipt']['details']['previous_generation']
 old=case.case.folder/'unicode-original-config.toml';old.write_text(original_config,encoding='utf-8');case.case.config=old
 reconciled=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
 assert reconciled['exit_code']==0

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_acknowledged_group_manifest_survives_cache_loss_and_blocks_tamper(engine):
 case=GroupCase(engine,'strict');first=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');group,actual=case.verify(first)
 db=sqlite3.connect(case.case.folder/'work/journal.sqlite3')
 try:db.execute('DELETE FROM group_contributors WHERE group_id=?',(group['group_id'],));db.commit()
 finally:db.close()
 case.write_config(mode='replace',subset=True)
 refused=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 assert 'GROUP_REPLACE_OMITTED_CONTRIBUTORS' in str(refused) and case.b.name in str(refused)
 assert case.values(group)==actual
 db=sqlite3.connect(case.case.folder/'work/journal.sqlite3')
 try:assert db.execute('SELECT COUNT(*) FROM group_contributors WHERE group_id=?',(group['group_id'],)).fetchone()[0]==2
 finally:db.close()
 manifest=Path(group['manifest']['manifest_path']);raw=manifest.read_bytes();manifest.write_bytes(raw.replace(b'\n',b' \n',1))
 # JSON values are unchanged, but approved manifest bytes are not.
 corrupted=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=5)
 assert 'GROUP_MANIFEST_HASH' in str(corrupted)
 assert case.values(group)==actual
 for item in first['files']:assert Path(item['artifact']['path']).read_bytes()==Path(item['artifact']['original_path']).read_bytes()

@pytest.mark.parametrize('omission',['excluded','missing','unreadable'])
def test_complete_group_never_erases_excluded_missing_or_unreadable_history(omission):
 case=GroupCase('odbc','strict');first=case.case.cli('run','--new-intent','--no-parallel','--non-interactive');group,actual=case.verify(first)
 if omission=='excluded':case.prefix=case.prefix.replace('include = ["**/*.csv"]','include = ["**/*.csv"]\nexclude = '+API['lit']([case.b.name]))
 elif omission=='missing':case.b.unlink()
 case.write_config(mode='replace')
 handle=None
 if omission=='unreadable':
  import ctypes
  from ctypes import wintypes
  kernel=ctypes.WinDLL('kernel32',use_last_error=True);kernel.CreateFileW.argtypes=[wintypes.LPCWSTR,wintypes.DWORD,wintypes.DWORD,wintypes.LPVOID,wintypes.DWORD,wintypes.DWORD,wintypes.HANDLE];kernel.CreateFileW.restype=wintypes.HANDLE;kernel.CloseHandle.argtypes=[wintypes.HANDLE]
  handle=kernel.CreateFileW(str(case.b),0x80000000,0,None,3,0x80,None)
  assert handle not in (None,ctypes.c_void_p(-1).value)
 try:refused=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=4 if omission=='unreadable' else 2)
 finally:
  if handle:kernel.CloseHandle(handle)
 assert case.b.name in str(refused)
 if omission!='unreadable':assert 'GROUP_REPLACE_OMITTED_CONTRIBUTORS' in str(refused)
 assert case.values(group)==actual
 assert all(not item.get('receipt') for item in refused.get('groups',[]))
 for item in first['files']:
  expected=case.raw_a if item['artifact']['relative_path']==case.a.name else case.raw_b
  assert Path(item['artifact']['path']).read_bytes()==expected

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_union_reordered_names_preserves_missing_versus_explicit_null(engine):
 case=GroupCase(engine,'union_by_name');case.raw_a=b'id,note\r\n1,\\N\r\n';case.raw_b=b'extra,id\r\n\\N,2\r\n';case.a.write_bytes(case.raw_a);case.b.write_bytes(case.raw_b);case.write_config()
 text=case.case.config.read_text(encoding='utf-8');marker='\n[[file_rules]]\nglob='+API['lit'](case.b.name)+'\n';prefix=text.split(marker)[0]+marker
 for ordinal,name,kind in [(1,'extra','nvarchar(20)'),(2,'id','int')]:prefix+='\n[[file_rules.columns]]\n'+'\n'.join(k+'='+API['lit'](v) for k,v in dict(source_ordinal=ordinal,target_name=name,target_type=kind).items())+'\n'
 case.case.config.write_text(prefix,encoding='utf-8');report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive')
 group=report['groups'][0];actual=case.values(group)
 assert [row[:-2] for row in actual]==[(1,None,None),(2,None,None)]
 assert [row[-1] for row in actual]==[2,4] and len({row[-2] for row in actual})==2
 assert group['receipt']['committed_records']==2 and [f['counts']['committed'] for f in report['files']]==[1,1]
 for item in report['files']:
  expected=case.raw_a if item['artifact']['relative_path']==case.a.name else case.raw_b
  assert Path(item['artifact']['path']).read_bytes()==expected
 db=sqlite3.connect(case.case.folder/'work/journal.sqlite3')
 try:
  payloads=[json.loads(r[0]) for r in db.execute('SELECT payload FROM group_records WHERE run_id=? ORDER BY ordinal',(report['run_id'],))]
  positions={column['target_name']:index for index,column in enumerate(group['plan']['columns'])}
  assert [[row['fields'][positions[name]]['state'] for name in ['note','extra']] for row in payloads]==[['NULL','MISSING'],['MISSING','NULL']]
 finally:db.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_group_lossy_receipt_and_original_event_seals_survive_resume_and_detect_deletion(engine):
 case=GroupCase(engine,'strict');case.raw_a='id,note\r\n1,A🙂B\r\n1,A🙂B\r\n'.encode('utf-8');case.raw_b=b'id,note\r\n2,XYZQ\r\n';case.a.write_bytes(case.raw_a);case.b.write_bytes(case.raw_b)
 case.prefix+='\n[schema]\nallow_lossy_projection=true\n';case.write_config()
 text=case.case.config.read_text(encoding='utf-8').replace('target_type="nvarchar(20)"','target_type="nvarchar(3)"\ntruncation="right"');case.case.config.write_text(text,encoding='utf-8')
 report=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=3);group=report['groups'][0]
 assert group['receipt']['assurance']=='lossy' and group['receipt']['details']['lossy_projection']['applied_cells']==3
 actual=case.values(group);assert [row[:-2] for row in actual]==[(1,'A🙂'),(1,'A🙂'),(2,'XYZ')]
 assert sorted(item['counts']['committed'] for item in report['files'])==[1,2]
 for item in report['files']:assert Path(item['artifact']['path']).read_bytes()==Path(item['artifact']['original_path']).read_bytes()
 case.case.cli('resume','--run-id',report['run_id'],'--no-parallel','--non-interactive',exit_code=3)
 case.case.cli('reconcile','--run-id',report['run_id'],'--non-interactive',exit_code=3)
 out=case.case.folder/'group-projections.jsonl';case.case.cli('export-projections','--run-id',report['run_id'],'--output',str(out))
 events=[json.loads(line) for line in out.read_text(encoding='utf-8').splitlines()]
 assert len(events)==3 and {(e['original_field']['text'],e['projected_field']['text']) for e in events}=={('A🙂B','A🙂'),('XYZQ','XYZ')}
 db=sqlite3.connect(case.case.folder/'work/journal.sqlite3')
 try:db.execute('DELETE FROM projection_events WHERE rowid=(SELECT MIN(rowid) FROM projection_events WHERE run_id=?)',(report['run_id'],));db.commit()
 finally:db.close()
 for command in ['resume','reconcile']:
  refused=case.case.cli(command,'--run-id',report['run_id'],'--non-interactive',exit_code=5)
  assert 'PROJECTION_EVIDENCE_MISMATCH' in str(refused)
 bad=case.case.folder/'missing-group-projection.jsonl';case.case.cli('export-projections','--run-id',report['run_id'],'--output',str(bad),exit_code=5)
 assert not bad.exists() and case.values(group)==actual
