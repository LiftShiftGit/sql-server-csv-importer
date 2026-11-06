"""Actual SQL parity for explicit multi-token dialects; no inferred expected values."""
import hashlib,math,os,runpy,tomllib,json
from pathlib import Path
from decimal import Decimal
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Disposable SQL required')

def case_data(mode):
    if mode=='tokens':delimiter,quote,ending,encoding='||','@@','<EOR>','utf-8'
    elif mode=='angle':delimiter,quote,ending,encoding='||','<<','\r\n','utf-8'
    else:delimiter,quote,ending,encoding='\u00a7\u00a7','\u00ab\u00ab','END\r\n','utf-16-le'
    expected=[['001','literal'+delimiter+' and '+ending+' qualifier '+quote+' tail','12.50'],['002','\u4e2d\u6587 \U0001f600  ','13.00']]
    def field(value):return quote+value.replace(quote,quote+quote)+quote
    text=delimiter.join(field(s) for s in ['code','label','amount'])+ending
    text+=''.join(delimiter.join(field(s) for s in row)+ending for row in expected)
    raw=text.encode(encoding)
    if encoding=='utf-16-le':raw=b'\xff\xfe'+raw
    return raw,expected,{'encoding':encoding,'delimiter':delimiter,'quotechar':quote,'record_terminator':ending,'header':'present'}

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('mode',['tokens','angle','unicode'])
def test_explicit_token_controls_exact_types_values_and_source(engine,mode):
    if engine not in API['ENGINES']:pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,batch_rows=1,batch_bytes=65536)
    raw,expected,parsing=case_data(mode);case.source.write_bytes(raw)
    base=tomllib.loads(case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]);base['parsing'].update(parsing)
    case.config.write_text('\n'.join('['+section+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in values.items()) for section,values in base.items()),encoding='utf-8')
    (case.folder/'independent-token-oracle.json').write_text(json.dumps({'source_sha256':hashlib.sha256(raw).hexdigest(),'settings':parsing,'expected':expected},ensure_ascii=True,indent=2),encoding='utf-8')
    result=case.cli('run','--new-intent','--no-parallel','--non-interactive')
    item=result['files'][0];plan=item['plan'];assert item['receipt']['details']['transport']['engine']==engine
    assert Path(item['artifact']['path']).read_bytes()==raw and item['artifact']['sha256']==hashlib.sha256(raw).hexdigest()
    label_units=max(len(row[1].encode('utf-16-le'))//2 for row in expected)
    assert [c['sql_type'] for c in plan['columns']]==['nvarchar(4)','nvarchar('+str(math.ceil(label_units*1.2))+')','decimal(4,2)']
    assert item['counts']['committed']==2 and not any(item['counts'][k] for k in ['pending','rejected','unresolved'])
    connection=case.connection()
    try:
        table=API['q'](plan['schema'])+'.'+API['q'](plan['table']);columns=','.join(API['q'](c['target_name']) for c in plan['columns'])
        rows=list(connection.cursor().execute('SELECT __ingest_ordinal,'+columns+' FROM '+table+' WHERE __ingest_intent=? ORDER BY __ingest_ordinal',item['intent_id']))
        assert [tuple(row) for row in rows]==[(i+2,r[0],r[1],Decimal(r[2])) for i,r in enumerate(expected)]
    finally:connection.close()

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('blocked',[False,True])
def test_token_bad_width_reject_or_quote_bleed_blocks_without_fake_resync(engine,blocked):
    if engine not in API['ENGINES']:pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,batch_rows=1,batch_bytes=65536)
    raw=(b'code||label<EOR>001<EOR>002||ok<EOR>' if not blocked else b'code||label<EOR>001||@@bad\r\n002||looks valid@@<EOR>')
    case.source.write_bytes(raw)
    base=tomllib.loads(case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]);base['parsing'].update(delimiter='||',quotechar='@@',record_terminator='<EOR>',header='present',allow_multiline_fields=not blocked)
    case.config.write_text('\n'.join('['+section+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in values.items()) for section,values in base.items()),encoding='utf-8')
    result=case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=5 if blocked else 3)
    item=result['files'][0]
    assert Path(item['artifact']['path']).read_bytes()==raw
    if blocked:
        assert item['counts']['unresolved']==1 and item['counts']['committed']==0 and not item.get('receipt')
        assert item['counts']['logical_records'] is None
    else:
        assert item['counts']['rejected']==1 and item['counts']['committed']==1
        assert item['receipt']['details']['transport']['engine']==engine
        connection=case.connection()
        try:
            plan=item['plan'];table=API['q'](plan['schema'])+'.'+API['q'](plan['table']);columns=','.join(API['q'](c['target_name']) for c in plan['columns'])
            assert [tuple(r) for r in connection.cursor().execute('SELECT __ingest_ordinal,'+columns+' FROM '+table+' WHERE __ingest_intent=?',item['intent_id'])]==[(3,'002','ok')]
        finally:connection.close()
