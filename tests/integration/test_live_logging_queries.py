"""Execute the shipped operator SQL examples against independently checked live fixtures."""
import os,runpy
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Disposable SQL required')

def resultsets(cursor):
    results=[]
    while True:
        if cursor.description:
            columns=[d[0] for d in cursor.description]
            results.append([dict(zip(columns,row)) for row in cursor.fetchall()])
        if not cursor.nextset():return results

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('negative',[False,True])
def test_shipped_logging_queries(engine,negative):
    if engine not in API['ENGINES']:pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,negative=negative)
    report=case.cli('run','--non-interactive',exit_code=3 if negative else 0)
    case.verify(report)
    config=load_config(case.config,environ={})
    conn=connect(config,os.environ.get('DB_USERNAME'),os.environ.get('DB_PASSWORD'),config['destination']['control_database'],autocommit=True)
    sql=(ROOT/'examples/logging-queries.sql').read_text(encoding='utf-8-sig')
    sql=sql.replace('DECLARE @RunId nvarchar(64) = NULL;', 'DECLARE @RunId nvarchar(64) = ?;')
    try:
        results=resultsets(conn.cursor().execute(sql,report['run_id']))
        assert len(results)==10
        summary=results[1][0];counts=report['files'][0]['counts']
        assert summary['run_id']==report['run_id'] and summary['files']==1
        for key in ('committed','rejected','pending'):
            assert summary[key+'_records']==counts[key]
            assert results[2][0][key+'_records']==counts[key]
        assert summary['source_bytes']==case.source.stat().st_size
        assert bool(results[3])==negative and bool(results[4])==negative
        assert results[6] and results[7] and results[8] and len(results[9])==38
        assert results[8][0]['committed_records']==counts['committed']
        if engine=='odbc' and not negative:
            # Also execute the exact unedited default/latest-run variant.
            latest=resultsets(conn.cursor().execute((ROOT/'examples/logging-queries.sql').read_text(encoding='utf-8-sig')))
            assert latest[1][0]['run_id']==report['run_id']
    finally:conn.close()
    conn=case.connection()
    try:
        sql=(ROOT/'examples/destination-receipt-queries.sql').read_text(encoding='utf-8-sig').replace('DECLARE @RunId nvarchar(64) = NULL;', 'DECLARE @RunId nvarchar(64) = ?;')
        rows=resultsets(conn.cursor().execute(sql,report['run_id']))[0]
        assert len(rows)==1 and rows[0]['intent_id']==report['files'][0]['intent_id']
        assert rows[0]['committed_records']==counts['committed']
    finally:conn.close()
