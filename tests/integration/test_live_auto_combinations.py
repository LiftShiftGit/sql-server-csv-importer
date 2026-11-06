"""Automatic dialect/encoding detection on unambiguous authored files."""
import os, runpy, tomllib
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Disposable SQL required')
@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('delimiter',[',','\t','|',';'])
@pytest.mark.parametrize('ending',['\r\n','\r','\n'])
@pytest.mark.parametrize('encoding',['utf-8','utf-16'])
def test_auto_encoding_delimiter_ending_quote(engine,delimiter,ending,encoding):
    if engine not in API['ENGINES']:pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,batch_rows=2)
    rows=[[str(i).zfill(3),'value'+delimiter+' quoted " detail','late' if i==11 else str(i)] for i in range(12)]
    raw=(''.join(delimiter.join('"'+v.replace('"','""')+'"' for v in row)+ending for row in [['id','label','change']]+rows)).encode(encoding)
    case.source.write_bytes(raw)
    config=tomllib.loads(case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0])
    config['source']['path']=''
    config['parsing'].update(encoding='auto',delimiter='auto',quotechar='auto',record_terminator='auto',header='present')
    case.config.write_text('\n'.join('['+s+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in fields.items()) for s,fields in config.items()),encoding='utf-8')
    result=case.cli('run','--non-interactive');item=result['files'][0];plan=item['plan']
    assert item['counts']['committed']==12 and all(item['counts'][k]==0 for k in ['pending','rejected','unresolved'])
    assert Path(item['artifact']['path']).read_bytes()==raw
    connection=case.connection()
    try:
        columns=','.join(API['q'](c['target_name']) for c in plan['columns'])
        target=API['q'](plan['schema'])+'.'+API['q'](plan['table'])
        assert [tuple(r) for r in connection.cursor().execute('SELECT '+columns+' FROM '+target+' ORDER BY __ingest_ordinal')]==[tuple(r) for r in rows]
    finally:connection.close()

