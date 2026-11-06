"""Independent process-pool/concurrent-target oracle with authored SQL values."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import uuid
import pytest
ROOT=Path(__file__).resolve().parents[2]
TYPE=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
CONFIG=os.environ.get('DB_INGEST_TEST_CONFIG')
PARALLEL=os.environ.get('DB_INGEST_TEST_PARALLEL')=='1'
ENGINES={x.strip() for x in os.environ.get('DB_INGEST_TEST_ENGINES','odbc').split(',')}
pytestmark=pytest.mark.skipif(not CONFIG or not PARALLEL,reason='Explicit disposable SQL and parallel opt-in required')


def configured(engine,workers):
    case=TYPE['TypeCase'](engine=engine,batch_rows=7,batch_bytes=131072)
    original=case.source.read_bytes()
    for i in range(3):(case.folder/('other_'+str(i)+'_'+uuid.uuid4().hex+'.csv')).write_bytes(original)
    text=case.config.read_text(encoding='utf-8')
    text=text.replace('path = '+TYPE['lit'](str(case.source)),'path = '+TYPE['lit'](str(case.folder)))
    text=text.replace('include = '+TYPE['lit']([case.source.name]),'include = ["**/*.csv"]')
    text=text.replace('glob = '+TYPE['lit'](case.source.name),'glob = "**/*.csv"')
    text=text.replace('[parallel]\nenabled = false','[parallel]\nenabled = true\nmax_workers = '+str(workers))
    text+='\n[limits]\ntotal_process_tree_memory_bytes = 2147483648\nworker_memory_bytes = 134217728\n'
    case.config.write_text(text,encoding='utf-8')
    return case


def run(case,label):
    env={k:v for k,v in os.environ.items() if not k.startswith('DB_') or k in ('DB_USERNAME','DB_PASSWORD')}
    result=subprocess.run([sys.executable,'-m','db_ingest','--config',str(case.config),'run','--parallel','--non-interactive','--new-intent','--json'],cwd=ROOT,env=env,capture_output=True,text=True,timeout=300,shell=False)
    secret=os.environ.get('DB_PASSWORD');output=result.stdout+result.stderr
    sanitized=output.replace(secret,'<redacted>') if secret else output
    output_path=case.folder/(label+'.json');output_path.write_text(sanitized,encoding='utf-8')
    assert not secret or secret not in output,'Credential leaked by parallel CLI'
    assert result.returncode==0,{'actual_exit':result.returncode,'retained_output':str(output_path)}
    report=json.loads(result.stdout);assert report['state']=='CLEAN_VERIFIED'
    return report


def verify_policy(report,workers):
    policy=report['execution_policy']
    assert policy['requested_workers']==workers
    assert policy['effective_workers']==workers
    assert policy['memory_budget_bytes']==2147483648
    assert 0<policy['sampled_peak_process_tree_rss_bytes']<=policy['memory_budget_bytes']
    assert len(report['files'])==4


def verify_files(case,report,engine):
    targets=set();intents=set()
    for item in report['files']:
        case.verify({'files':[item]})
        plan=item['plan'];targets.add((plan['database'],plan['schema'],plan['table']))
        intents.add(item['intent_id'])
        assert item['receipt']['details']['transport']['engine']==engine
    assert len(targets)==4 and len(intents)==4
    return targets,intents

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('workers',[1,2,4])
def test_live_parallel_different_targets_full_type_oracle(engine,workers):
    if engine not in ENGINES:pytest.skip('Requested engine not explicitly enabled')
    case=configured(engine,workers);report=run(case,'single-run')
    verify_policy(report,workers);verify_files(case,report,engine)

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('workers',[1,2,4])
def test_live_overlapping_parallel_pools_serialize_same_targets(engine,workers):
    if engine not in ENGINES:pytest.skip('Requested engine not explicitly enabled')
    case=configured(engine,workers)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first=executor.submit(run,case,'first-pool');second=executor.submit(run,case,'second-pool')
        reports=[first.result(),second.result()]
    outputs=[]
    for report in reports:
        verify_policy(report,workers);outputs.append(verify_files(case,report,engine))
    assert outputs[0][0]==outputs[1][0]
    assert outputs[0][1].isdisjoint(outputs[1][1])
    conn=case.connection()
    try:
        cur=conn.cursor()
        for item in reports[0]['files']:
            p=item['plan'];target=TYPE['q'](p['schema'])+'.'+TYPE['q'](p['table'])
            actual=cur.execute('SELECT COUNT_BIG(*),COUNT(DISTINCT [__ingest_intent]),COUNT(DISTINCT [__ingest_record]) FROM '+target).fetchone()
            assert tuple(actual)==(34,2,34),'Concurrent append omitted/collapsed/duplicated committed source identities'
    finally:conn.rollback();conn.close()
