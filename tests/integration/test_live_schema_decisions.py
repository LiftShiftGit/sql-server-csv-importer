"""Real SQL operator choices; expected values are authored and read directly."""
import io,os,runpy
from pathlib import Path
from types import SimpleNamespace
import pytest
from db_ingest.config import load_config
from db_ingest.decisions import resolve_schema_decision
from db_ingest.journal import Journal
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_evolution_oracle.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
class Terminal(io.StringIO):
 def isatty(self):return True

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('choice',['w','t','q','s'])
def test_owned_schema_terminal_policy_preserves_original_and_has_exact_outcome(engine,choice,monkeypatch):
 case=API['EvolutionCase'](engine);first=case.initialize();before=case.read();original=case.original_config
 rows=[['3','short'],['4','ok']] if choice=='q' else [['3','long🙂  ']]
 case.write(['id','note'],rows,[('id','int'),('note','nvarchar(20)')])
 blocked=case.case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=2)
 assert any(e['code']=='SCHEMA_DRIFT' for e in blocked['errors']),blocked
 assert case.read()==before
 raw=case.case.source.read_bytes();case.case.source.write_bytes(b'changed\nsource\n')
 config=load_config(case.case.config,environ={});config['interaction']['non_interactive']=False
 monkeypatch.setattr('sys.stdin',Terminal(choice+'\n'))
 result=resolve_schema_decision(blocked,config,SimpleNamespace(json=False),os.environ.get('DB_USERNAME'),os.environ.get('DB_PASSWORD'))
 if choice=='s':
  assert result['run_id']==blocked['run_id'] and result['exit_code']==2
  assert case.read()==before
 else:
  assert result['run_id']!=blocked['run_id'] and result['exit_code']==(3 if choice=='q' else 0),result
  item=result['files'][0]
  assert Path(item['artifact']['path']).read_bytes()==raw
  assert item['receipt']['details']['transport']['engine']==engine
  assert item['counts']['pending']==0 and item['counts']['rejected']==(1 if choice=='q' else 0)
  actual=case.read();assert actual[:2]==before
  assert [row[:2] for row in actual[2:]]==([(4,'ok')] if choice=='q' else [(3,'long🙂  ')])
  assert len({row[3] for row in actual})==len(actual)
  journal=Journal(config['preservation']['work_directory'])
  try:assert journal.run(result['run_id'])['linked_run']==blocked['run_id']
  finally:journal.close()
  conn=case.case.connection()
  try:
   cur=conn.cursor();assert cur.execute('SELECT COUNT_BIG(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',item['intent_id']).fetchone()[0]==1
   length=cur.execute("SELECT max_length FROM sys.columns WHERE object_id=OBJECT_ID(?) AND name='note'",case.target).fetchone()[0]
   assert length==(-1 if choice=='t' else 8 if choice=='q' else 40)
  finally:conn.rollback();conn.close()
 oldpath=case.case.folder/'operator-original.toml';oldpath.write_text(original,encoding='utf-8');case.case.config=oldpath
 reconciled=case.case.cli('reconcile','--run-id',first['run_id'],'--non-interactive')
 assert reconciled['files'][0]['destination']['state']=='VERIFIED'
 assert reconciled['files'][0]['destination']['value_digest']==first['files'][0]['receipt']['value_digest']
