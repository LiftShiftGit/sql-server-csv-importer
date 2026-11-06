"""Independent lossy opt-in and durable evidence tamper refusals."""
import hashlib,json,os,runpy,sqlite3
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
def configured(allowed):
 case=API['TypeCase'](engine='odbc',fast=False)
 raw=b'word\r\nABCDE\r\n';case.source.write_bytes(raw)
 text=case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
 text+='\n[schema]\nallow_lossy_projection='+API['lit'](allowed)+'\n[[file_rules]]\nglob='+API['lit'](case.source.name)+'\n[[file_rules.columns]]\nsource_ordinal=1\ntarget_name="word"\ntarget_type="nvarchar(3)"\ntruncation="right"\n'
 case.config.write_text(text,encoding='utf-8');return case,raw

def test_projection_column_rule_requires_global_opt_in():
 case,raw=configured(False);report=case.cli('run','--no-parallel','--non-interactive',exit_code=2)
 assert 'allow_lossy_projection' in str(report) or 'LOSSY_POLICY_REQUIRED' in str(report)
 assert not (case.folder/'work/journal.sqlite3').exists()
 assert case.source.read_bytes()==raw

@pytest.mark.parametrize('mutation',['field_hash','forged_field_and_hash','deleted_event','source_reference'])
def test_projection_evidence_corruption_blocks_resume_reconcile_and_export(mutation):
 case,raw=configured(True);report=case.cli('run','--no-parallel','--non-interactive',exit_code=3)
 item=report['files'][0];assert item['receipt']['assurance']=='lossy'
 assert Path(item['artifact']['path']).read_bytes()==raw
 clean=case.folder/'clean-projection.jsonl';case.cli('export-projections','--run-id',report['run_id'],'--output',str(clean))
 exported=[json.loads(line) for line in clean.read_text(encoding='utf-8').splitlines()]
 assert len(exported)==1 and exported[0]['original_field']['text']=='ABCDE' and exported[0]['projected_field']['text']=='ABC'
 db=sqlite3.connect(case.folder/'work/journal.sqlite3')
 try:
  forged=json.dumps({'state':'VALUE','text':'FORGED','quoted':False},ensure_ascii=False,sort_keys=True,separators=(',',':'))
  if mutation=='field_hash':db.execute('UPDATE projection_events SET original_field=? WHERE run_id=?',(forged,report['run_id']))
  elif mutation=='forged_field_and_hash':db.execute('UPDATE projection_events SET original_field=?,original_sha256=? WHERE run_id=?',(forged,hashlib.sha256(forged.encode('utf-8')).hexdigest(),report['run_id']))
  elif mutation=='deleted_event':db.execute('DELETE FROM projection_events WHERE run_id=?',(report['run_id'],))
  else:db.execute('UPDATE projection_events SET source_artifact=? WHERE run_id=?',(str(case.folder/'unrelated.bin'),report['run_id']))
  db.commit()
 finally:db.close()
 for command in ['resume','reconcile']:
  refused=case.cli(command,'--run-id',report['run_id'],'--non-interactive',exit_code=5)
  assert 'PROJECTION_EVIDENCE_MISMATCH' in str(refused)
 corrupt=case.folder/'corrupt-projection.jsonl'
 refused=case.cli('export-projections','--run-id',report['run_id'],'--output',str(corrupt),exit_code=5)
 assert 'PROJECTION_EVIDENCE_MISMATCH' in str(refused) and not corrupt.exists()
 p=item['plan'];target=API['q'](p['schema'])+'.'+API['q'](p['table']);conn=case.connection()
 try:assert conn.cursor().execute('SELECT word FROM '+target).fetchval()=='ABC'
 finally:conn.rollback();conn.close()
