"""Independent absence/lineage/acknowledged-history receipt-resolution regressions."""
import json
import os
from pathlib import Path
import runpy
import pytest
ROOT=Path(__file__).resolve().parents[2]
TYPE=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
CONFIG=os.environ.get('DB_INGEST_TEST_CONFIG')
ENGINES={v.strip() for v in os.environ.get('DB_INGEST_TEST_ENGINES','odbc').split(',')}
RESOLUTION=os.environ.get('DB_INGEST_TEST_RESOLUTION')=='1'
pytestmark=pytest.mark.skipif(not CONFIG,reason='Explicit disposable SQL receipt-resolution configuration required')


def crash(case,point):
    case.cli('run','--no-parallel','--non-interactive','--new-intent',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':point})
    status=case.cli('status');run_id=status['runs'][0]['run_id']
    report=case.cli('status','--run-id',run_id)
    assert report['files'][0]['receipt'] is None
    return run_id,report['files'][0]


def target(item):return TYPE['q'](item['plan']['schema'])+'.'+TYPE['q'](item['plan']['table'])


def observed(case,item):
    conn=case.connection()
    try:
        cur=conn.cursor()
        row=cur.execute('SELECT COUNT_BIG(*),COUNT(DISTINCT [__ingest_record]) FROM '+target(item)+' WHERE [__ingest_intent]=?',item['intent_id']).fetchone()
        receipt=cur.execute('SELECT payload FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',item['intent_id']).fetchone()
        return {'rows':row[0],'ids':row[1],'receipt':None if receipt is None else json.loads(receipt[0])}
    finally:conn.rollback();conn.close()


def delete_only_test_receipt(case,intent):
    conn=case.connection()
    try:
        cur=conn.cursor();cur.execute('DELETE FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',intent)
        assert cur.rowcount==1
        conn.commit()
    finally:conn.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_live_orphan_intent_rows_without_receipt_never_replayed(engine):
    if engine not in ENGINES:pytest.skip('Engine not explicitly selected')
    case=TYPE['TypeCase'](engine=engine,batch_rows=7,batch_bytes=131072)
    run_id,item=crash(case,'AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT')
    before=observed(case,item)
    assert before['rows']==before['ids']==17 and before['receipt'] is not None
    delete_only_test_receipt(case,item['intent_id'])
    blocked=case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive',exit_code=5)
    assert blocked['state']=='COMMIT_OUTCOME_UNKNOWN'
    after=observed(case,item)
    assert after['rows']==after['ids']==17 and after['receipt'] is None
    errors=case.cli('show-errors','--run-id',run_id)
    assert any('RECEIPT' in e['code'] or 'ORPHAN' in e['code'] or 'COMMIT_OUTCOME' in e['code'] for e in errors['incidents'])

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_live_missing_other_known_ack_blocks_empty_current_intent(engine):
    if engine not in ENGINES:pytest.skip('Engine not explicitly selected')
    case=TYPE['TypeCase'](engine=engine,batch_rows=7,batch_bytes=131072)
    acknowledged=case.cli('run','--no-parallel','--non-interactive')
    old=acknowledged['files'][0];case.verify(acknowledged)
    run_id,current=crash(case,'STAGED_BEFORE_PUBLICATION')
    assert observed(case,current)=={'rows':0,'ids':0,'receipt':None}
    delete_only_test_receipt(case,old['intent_id'])
    blocked=case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive',exit_code=5)
    assert blocked['state']=='COMMIT_OUTCOME_UNKNOWN'
    assert observed(case,current)=={'rows':0,'ids':0,'receipt':None}
    assert observed(case,old)['rows']==17
    errors=case.cli('show-errors','--run-id',run_id)
    assert errors['incidents'] and all(e['code']!='WORKER_FAILED' for e in errors['incidents'])

@pytest.mark.skipif(not RESOLUTION,reason='Positive missing-receipt resolution implementation readiness opt-in')
@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_live_proven_rollback_reuses_exact_original_intent(engine):
    if engine not in ENGINES:pytest.skip('Engine not explicitly selected')
    case=TYPE['TypeCase'](engine=engine,batch_rows=7,batch_bytes=131072)
    first=case.cli('run','--no-parallel','--non-interactive')
    run_id,item=crash(case,'STAGED_BEFORE_PUBLICATION')
    assert observed(case,item)=={'rows':0,'ids':0,'receipt':None}
    resumed=case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive')
    assert resumed['run_id']==run_id and resumed['files'][0]['intent_id']==item['intent_id']
    assert resumed['state']=='CLEAN_VERIFIED'
    case.verify(first);case.verify(resumed)
    conn=case.connection()
    try:
        row=conn.cursor().execute('SELECT COUNT_BIG(*),COUNT(DISTINCT [__ingest_intent]),COUNT(DISTINCT [__ingest_record]) FROM '+target(item)).fetchone()
        assert tuple(row)==(34,2,34)
    finally:conn.rollback();conn.close()


@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_live_central_ack_survives_lost_local_and_destination_projection(engine):
    if engine not in ENGINES:pytest.skip('Engine not explicitly selected')
    case=TYPE['TypeCase'](engine=engine,batch_rows=7,batch_bytes=131072)
    run_id,item=crash(case,'AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT')
    before=observed(case,item);payload=before['receipt']
    assert before['rows']==17 and payload is not None
    # Simulate destination history/projection loss after central acknowledgment,
    # without editing the absent local receipt or any local disposition.
    conn=case.connection()
    try:
        cur=conn.cursor();central=TYPE['q'](case.d['control_database'])+'.[audit].[PublicationReceipts]'
        values=(payload['intent_id'],payload['run_id'],payload['database'],target(item),payload['source_hash'],payload['interpretation_hash'],payload['mapping_hash'],payload['committed_records'],payload['value_digest'],payload['assurance'],payload['recorded_at_utc'],json.dumps(payload))
        cur.execute('INSERT INTO '+central+' (intent_id,run_id,destination_database,target,source_hash,interpretation_hash,mapping_hash,committed_records,value_digest,assurance,recorded_at_utc,receipt_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',*values)
        stored=cur.execute('SELECT receipt_json FROM '+central+' WHERE intent_id=?',item['intent_id']).fetchone()
        assert stored is not None and json.loads(stored[0])==payload
        cur.execute('DELETE FROM '+target(item)+' WHERE [__ingest_intent]=?',item['intent_id']);assert cur.rowcount==17
        cur.execute('DELETE FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',item['intent_id']);assert cur.rowcount==1
        conn.commit()
    finally:conn.close()
    assert observed(case,item)=={'rows':0,'ids':0,'receipt':None}
    blocked=case.cli('resume','--run-id',run_id,'--no-parallel','--non-interactive',exit_code=5)
    assert blocked['state']=='COMMIT_OUTCOME_UNKNOWN'
    assert observed(case,item)=={'rows':0,'ids':0,'receipt':None}
    errors=case.cli('show-errors','--run-id',run_id)
    assert any(e['code']=='RECOVERY_ACKNOWLEDGED_RECEIPT_MISSING' for e in errors['incidents'])
