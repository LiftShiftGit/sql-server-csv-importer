"""Bounded receipt preview, complete independently hashed append-only chunk ledger."""
import hashlib,json,os,runpy
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_more_than_preview_limit_retains_complete_chunk_ledger(engine):
    case=API['TypeCase'](engine=engine,batch_rows=1,batch_bytes=131072,fast=engine=='odbc')
    case.manifest=API['ORACLE']['write_fixture'](case.source,41,'middle')
    case.expected=list(API['ORACLE']['authored_rows'](41,'middle'))
    report=case.cli('run','--no-parallel','--non-interactive');item=case.verify(report)
    details=item['receipt']['details']['staging_batches']
    # Native ODBC exposes canonical wire verification separately from actual array groups.
    if 'chunk_ledger' not in details:
        choices=[v for v in item['receipt']['details'].values() if isinstance(v,dict) and 'chunk_ledger' in v]
        assert len(choices)==1,item['receipt']['details'].keys()
        details=choices[0]
    ledger=details['chunk_ledger'];data=Path(ledger['path']).read_bytes()
    assert hashlib.sha256(data).hexdigest()==ledger['sha256']
    assert ledger['chunk_count']==41 and ledger['events']==82
    events=[json.loads(line) for line in data.splitlines()]
    assert len(events)==82
    for index in range(1,42):
        own=[x for x in events if x['index']==index]
        assert len(own)==2 and [x['state'] for x in own]==['SPOOLED','TYPED_STAGE_DECODED']
        assert own[0]['rows']==own[1]['rows']==1
        chunk=Path(own[1]['path']).read_bytes()
        assert len(chunk)==own[1]['bytes'] and hashlib.sha256(chunk).hexdigest()==own[1]['sha256']
    assert len(details['chunks'])<=20<ledger['chunk_count']
    assert len(json.dumps(details['chunks'],ensure_ascii=True).encode())<=65536
