"""One undecidable source must not suppress an independently safe selected file."""
import os
from pathlib import Path
import runpy
import uuid
import pytest
ROOT=Path(__file__).resolve().parents[2]
TYPE=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
CONFIG=os.environ.get('DB_INGEST_TEST_CONFIG')
ENABLED=os.environ.get('DB_INGEST_TEST_AFFECTED')=='1'
ENGINES={v.strip() for v in os.environ.get('DB_INGEST_TEST_ENGINES','odbc').split(',')}
pytestmark=pytest.mark.skipif(not CONFIG or not ENABLED,reason='Explicit disposable SQL and affected-file publication readiness opt-in required')

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('parallel',[False,True])
def test_live_good_file_publishes_while_unresolved_file_stays_unpublished(engine,parallel):
    if engine not in ENGINES:pytest.skip('Engine not explicitly selected')
    case=TYPE['TypeCase'](engine=engine,batch_rows=7,batch_bytes=131072)
    good=case.source;bad=case.folder/('unresolved_'+uuid.uuid4().hex+'.csv')
    oracle=TYPE['ORACLE']
    first=next(oracle['authored_rows']())
    malformed=oracle['encode_record']([c.name for c in oracle['columns']()])+oracle['encode_record'](first['source'],first['force_quote'])+b'2,"unclosed\r\n3,not-a-known-boundary\r\n'
    bad.write_bytes(malformed)
    text=case.config.read_text(encoding='utf-8')
    text=text.replace('path = '+TYPE['lit'](str(good)),'path = '+TYPE['lit'](str(case.folder)))
    text=text.replace('include = '+TYPE['lit']([good.name]),'include = ["**/*.csv"]')
    text=text.replace('glob = '+TYPE['lit'](good.name),'glob = "**/*.csv"')
    if parallel:text=text.replace('[parallel]\nenabled = false','[parallel]\nenabled = true\nmax_workers = 2')
    case.config.write_text(text,encoding='utf-8')
    report=case.cli('run','--parallel' if parallel else '--no-parallel','--non-interactive',exit_code=5)
    assert report['state']!='CLEAN_VERIFIED' and len(report['files'])==2
    good_item=next(f for f in report['files'] if Path(f['artifact']['original_path']).name==good.name)
    bad_item=next(f for f in report['files'] if Path(f['artifact']['original_path']).name==bad.name)
    case.verify({'files':[good_item]})
    assert good_item['state']=='RECONCILED'
    assert bad_item['receipt'] is None and bad_item['counts']['unresolved']==1
    assert Path(good_item['artifact']['path']).read_bytes()==good.read_bytes()
    assert Path(bad_item['artifact']['path']).read_bytes()==malformed
    conn=case.connection()
    try:
        count=conn.cursor().execute('SELECT COUNT_BIG(*) FROM sys.tables WHERE name LIKE ?',bad.stem+'%').fetchone()[0]
        assert count==0,'Affected unresolved file mutated a business target'
    finally:conn.rollback();conn.close()
    errors=case.cli('show-errors','--run-id',report['run_id'])
    assert any(r['file_id']==bad_item['file_id'] and any(e['code']=='FRAMING_UNRESOLVED' for e in r['errors']) for r in errors['record_errors'])
