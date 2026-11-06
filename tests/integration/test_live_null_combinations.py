"""Exhaustive four-boolean NULL policy cross product through both actual transports."""
import itertools,os,runpy,tomllib
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Disposable SQL required')
@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('case_sensitive,quoted,empty,trim',itertools.product([False,True],repeat=4))
def test_all_null_policy_boolean_combinations(engine,case_sensitive,quoted,empty,trim):
    if engine not in API['ENGINES']:pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,batch_rows=1)
    raw=b'a,b,c,d,e\r\nNIL,nil,"NIL",,  x  \r\n'
    case.source.write_bytes(raw)
    config=tomllib.loads(case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0])
    config['source'].pop('path')
    config['parsing'].update(null_tokens=['NIL'],null_tokens_case_sensitive=case_sensitive,null_tokens_in_quoted_fields=quoted,empty_string_is_null=empty,trim_whitespace=trim)
    text='\n'.join('['+s+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in fields.items()) for s,fields in config.items())
    text+='\n[[file_rules]]\nglob='+API['lit'](case.source.name)+'\n'
    for i,name in enumerate('abcde',1):
        text+='[[file_rules.columns]]\nsource_ordinal='+str(i)+'\ntarget_name="'+name+'"\ntarget_type="nvarchar(16)"\n'
    case.config.write_text(text,encoding='utf-8')
    result=case.cli('run','--non-interactive');item=result['files'][0];plan=item['plan']
    assert item['counts']['committed']==1 and all(item['counts'][k]==0 for k in ('pending','rejected','unresolved'))
    assert Path(item['artifact']['path']).read_bytes()==raw
    expected=(None,'nil' if case_sensitive else None,None if quoted else 'NIL',None if empty else '', 'x' if trim else '  x  ')
    connection=case.connection()
    try:
        target=API['q'](plan['schema'])+'.'+API['q'](plan['table'])
        assert [tuple(r) for r in connection.cursor().execute('SELECT a,b,c,d,e FROM '+target)]==[expected]
    finally:connection.close()
