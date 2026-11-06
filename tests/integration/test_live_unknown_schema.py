"""Real SQL, no supplied column types: large late outliers and incomplete headers."""
import hashlib,json,os,runpy,tomllib
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
GOLDEN=runpy.run_path(str(ROOT/'scripts/create_large_inference_example.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Disposable SQL required')

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('batch_rows',[257,4096])
def test_large_complete_stream_inference_without_schema_missing_header(engine,batch_rows):
    if engine not in API['ENGINES']:pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,batch_rows=batch_rows,batch_bytes=262144)
    report=GOLDEN['generate'](case.folder/'large',rows=20001,padding=2048)
    case.source=case.folder/'large'/report['source']
    original=case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
    base=tomllib.loads(original)
    base['source'].update(root=str(case.source.parent),path=str(case.source),include=[case.source.name])
    base['parsing'].update(column_names=report['column_names'],sample_records=1,sample_bytes=1024)
    # No [[file_rules.columns]] or global column types: inference alone chooses storage.
    case.config.write_text('\n'.join('['+section+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in values.items()) for section,values in base.items()),encoding='utf-8')
    result=case.cli('run','--new-intent','--no-parallel','--non-interactive')
    selected=result['files'][0];plan=selected['plan'];assert len(plan['columns'])==4
    assert [col['sql_type'] for col in plan['columns']]==report['expected_sql_types']
    assert [col['original_name'] for col in plan['columns']]==report['column_names']
    from db_ingest.journal import Journal
    from contextlib import closing
    with closing(Journal(base['preservation']['work_directory'])) as journal:
        item=next(journal.files(result['run_id']))
        assert Path(item['artifact']['path']).stat().st_size==report['bytes']
        h=hashlib.sha256()
        with Path(item['artifact']['path']).open('rb') as archive:
            for block in iter(lambda:archive.read(65536),b''):h.update(block)
        assert h.hexdigest()==report['sha256']
        assert item['counts']['committed']==report['rows']
        assert not any(item['counts'][k] for k in ('pending','rejected','unresolved'))
        header=next(journal.records(result['run_id'],item['file_id']))
        assert [field.text for field in header.fields]==['code','mixed','amount']
    connection=case.connection()
    try:
        cursor=connection.cursor();table=API['q'](plan['schema'])+'.'+API['q'](plan['table'])
        columns=','.join(API['q'](c['target_name']) for c in plan['columns'])
        cursor.execute('SELECT __ingest_ordinal,'+columns+' FROM '+table+' WHERE __ingest_intent=? ORDER BY __ingest_ordinal',selected['intent_id'])
        from decimal import Decimal
        for index in range(report['rows']):
            row=cursor.fetchone();expected=GOLDEN['values'](index,report['rows'],report['padding'])
            assert row is not None and row[0]==index+2
            assert list(row[1:])==[expected[0],expected[1],Decimal(expected[2]),expected[3]]
        assert cursor.fetchone() is None
        metadata=list(cursor.execute('SELECT t.name,c.max_length,c.precision,c.scale FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE c.object_id=OBJECT_ID(?) AND c.name NOT LIKE ? ORDER BY c.column_id',table,'__ingest[_]%'))
        assert [tuple(row) for row in metadata]==[('nvarchar',22,0,0),('nvarchar',22,0,0),('decimal',13,23,19),('nvarchar',-1,0,0)]
    finally:connection.close()
    (case.folder/'independent-result.json').write_text(json.dumps({'state':'ALL_ROWS_AND_TYPES_VERIFIED','rows':report['rows'],'source_bytes':report['bytes'],'source_sha256':report['sha256'],'engine':engine,'batch_rows':batch_rows,'batch_bytes':262144,'run_id':result['run_id'],'intent_id':selected['intent_id'],'schema_supplied':False,'raw_header_retained':True},indent=2),encoding='utf-8')

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('header,names,raw,expected_names',[
    ('present',[],b'id,,id\n001,x,12.50\n',['id','','id']),
    ('absent',[],b'001,x,12.50\n',['column_1','column_2','column_3']),
    ('absent',['account','label','amount'],b'001,x,12.50\n',['account','label','amount']),
])
def test_inferred_blank_duplicate_or_absent_headers_keep_first_data(engine,header,names,raw,expected_names):
    if engine not in API['ENGINES']:pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,batch_rows=1,batch_bytes=65536)
    case.source.write_bytes(raw)
    base=tomllib.loads(case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0])
    base['parsing'].update(header=header,column_names=names)
    case.config.write_text('\n'.join('['+section+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in values.items()) for section,values in base.items()),encoding='utf-8')
    (case.folder/'independent-manifest.json').write_text(json.dumps({'sha256':hashlib.sha256(raw).hexdigest(),'source_bytes':len(raw),'header':header,'column_names':names,'expected_original_names':expected_names,'expected_sql_types':['nvarchar(4)','nvarchar(2)','decimal(4,2)'],'expected_values':['001','x','12.50'],'schema_supplied':False},indent=2),encoding='utf-8')
    result=case.cli('run','--new-intent','--no-parallel','--non-interactive')
    item=result['files'][0];plan=item['plan']
    assert [c['original_name'] for c in plan['columns']]==expected_names
    assert [c['sql_type'] for c in plan['columns']]==['nvarchar(4)','nvarchar(2)','decimal(4,2)']
    assert len({c['target_name'] for c in plan['columns']})==3
    assert Path(item['artifact']['path']).read_bytes()==raw
    assert item['counts']['committed']==1 and item['counts']['header']==int(header=='present')
    connection=case.connection()
    try:
        table=API['q'](plan['schema'])+'.'+API['q'](plan['table'])
        columns=','.join(API['q'](c['target_name']) for c in plan['columns'])
        rows=list(connection.cursor().execute('SELECT __ingest_ordinal,'+columns+' FROM '+table+' WHERE __ingest_intent=?',item['intent_id']))
        from decimal import Decimal
        assert [tuple(row) for row in rows]==[(2 if header=='present' else 1,'001','x',Decimal('12.50'))]
    finally:connection.close()
