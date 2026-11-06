"""Authored codec/dialect/batch cross product with independent SQL and archive checks."""
import hashlib, os, runpy, tomllib
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL configuration required')
# Frozen supported inventory, deliberately not imported from the parser implementation.
CODECS='utf-8 utf-16-le utf-16-be utf-32-le utf-32-be ascii cp1252 iso8859-1 cp037 cp500 cp1140 cp437 cp850 cp858 cp1250 cp1251 cp1253 cp1254 cp1255 cp1256 cp1257 cp1258 iso8859-15 koi8-r shift_jis cp932 gb18030 gbk big5 euc_jp euc_kr cp949'.split()

@pytest.mark.parametrize('encoding',CODECS)
@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('mode,batch', [('double',1),('escape',7),('plain',4096)])
def test_codec_dialect_header_batch_cross_product(encoding,engine,mode,batch):
    if engine not in API['ENGINES']: pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,batch_rows=batch,batch_bytes=65536)
    delimiter,quote,ending = {'double':(',', '"','\r\n'),'escape':('||','@@','<EOR>'),'plain':(';','none','\n')}[mode]
    # Retain a non-ASCII authored character whenever this codec represents it.
    accent=''
    for candidate in ['é','中','あ','한','Ж','Ω','א','ش']:
        try: candidate.encode(encoding)
        except UnicodeEncodeError: continue
        accent=candidate;break
    assert accent or encoding=='ascii'
    values=[]
    for i in range(9):
        label='  '+accent+' row '+str(i)+'  '
        if mode!='plain': label+=delimiter+' embedded '+quote+' line\r\nend'
        values.append([str(i).zfill(3),label,str(i) if i<8 else 'late-text'])
    def field(value):
        if mode=='plain': return value
        if mode=='double': return quote+value.replace(quote,quote+quote)+quote
        return quote+value.replace('~','~~').replace(quote,'~'+quote)+quote
    header=[] if mode=='plain' else [['code','label','changing']]
    raw=(''.join(delimiter.join(field(v) for v in row)+ending for row in header+values)).encode(encoding)
    case.source.write_bytes(raw)
    config=tomllib.loads(case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0])
    config['source'].pop('path') # exercise root-only discovery for every combination
    config['parsing'].update(encoding=encoding,delimiter=delimiter,quotechar=quote,record_terminator=ending,header='absent' if mode=='plain' else 'present',doublequote=mode=='double',escapechar='~' if mode=='escape' else 'none')
    case.config.write_text('\n'.join('['+s+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in fields.items()) for s,fields in config.items()),encoding='utf-8')
    report=case.cli('run','--non-interactive')
    item=report['files'][0];plan=item['plan']
    assert item['counts']['committed']==9
    assert all(item['counts'][k]==0 for k in ('pending','rejected','unresolved'))
    assert Path(item['artifact']['path']).read_bytes()==raw
    assert item['artifact']['sha256']==hashlib.sha256(raw).hexdigest()
    assert item['receipt']['details']['transport']['engine']==engine
    assert all(c['sql_type'].startswith('nvarchar(') for c in plan['columns'])
    connection=case.connection()
    try:
        columns=','.join(API['q'](c['target_name']) for c in plan['columns'])
        target=API['q'](plan['schema'])+'.'+API['q'](plan['table'])
        def read():return [tuple(r) for r in connection.cursor().execute('SELECT '+columns+' FROM '+target+' WHERE __ingest_intent=? ORDER BY __ingest_ordinal',item['intent_id'])]
        assert read()==[tuple(v) for v in values]
        resumed=case.cli('resume','--run-id',report['run_id'],'--non-interactive')
        assert resumed['files'][0]['intent_id']==item['intent_id']
        assert read()==[tuple(v) for v in values]
        assert connection.cursor().execute('SELECT COUNT(*) FROM audit.__db_ingest_receipts WHERE intent_id=?',item['intent_id']).fetchone()[0]==1
    finally: connection.close()
