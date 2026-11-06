"""Independent nonlossy source-ledger corruption must never become receipt success."""
import json,os,runpy,sqlite3
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
@pytest.mark.parametrize('mutation',['serialized_field','unknown_disposition','missing_committed_record','indexed_ordinal'])
def test_nonlossy_local_corruption_blocks_resume_and_reconcile_without_destination_changes(mutation):
 case=API['TypeCase'](engine='odbc',fast=False,batch_rows=7)
 before=case.cli('run','--new-intent','--no-parallel','--non-interactive');case.verify(before)
 item=before['files'][0];run_id=before['run_id'];db=sqlite3.connect(case.folder/'work/journal.sqlite3')
 try:
  record_id,payload=db.execute("SELECT record_id,payload FROM records WHERE run_id=? AND disposition='committed' ORDER BY ordinal LIMIT 1",(run_id,)).fetchone()
  if mutation=='serialized_field':
   value=json.loads(payload);index=next(i for i,c in enumerate(API['ORACLE']['columns']()) if c.name=='text_fixed');value['fields'][index]['text']='FORGED'
   db.execute('UPDATE records SET payload=? WHERE run_id=? AND record_id=?',(json.dumps(value),run_id,record_id))
  elif mutation=='unknown_disposition':db.execute('UPDATE records SET disposition=? WHERE run_id=? AND record_id=?',('silent_loss',run_id,record_id))
  elif mutation=='missing_committed_record':db.execute('DELETE FROM records WHERE run_id=? AND record_id=?',(run_id,record_id))
  else:db.execute('UPDATE records SET ordinal=ordinal+10000 WHERE run_id=? AND record_id=?',(run_id,record_id))
  db.commit()
 finally:db.close()
 for command in ['resume','reconcile']:
  refusal=case.cli(command,'--run-id',run_id,'--non-interactive',exit_code=5)
  assert any(code in str(refusal) for code in ['LEDGER_MISMATCH','LEDGER_IDENTITY_MISMATCH','DISPOSITION_INVALID','ACCOUNTING_MISMATCH','COMMITTED_COUNT_MISMATCH','BYTE_COVERAGE_MISMATCH'])
 # Before is a preserved authored oracle, not recomputed from the corrupted journal.
 case.verify(before)
 conn=case.connection()
 try:
  target=API['q'](item['plan']['schema'])+'.'+API['q'](item['plan']['table'])
  assert tuple(conn.cursor().execute('SELECT COUNT_BIG(*),COUNT(DISTINCT __ingest_record),COUNT(DISTINCT __ingest_intent) FROM '+target).fetchone())==(17,17,1)
  assert conn.cursor().execute('SELECT COUNT(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',item['intent_id']).fetchval()==1
 finally:conn.rollback();conn.close()
 assert Path(item['artifact']['path']).read_bytes()==case.source.read_bytes()
