"""Explicit VARCHAR byte policies: independent actual bytes, never inferred expectations."""
import csv,hashlib,os,runpy
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
CONFIG=os.environ.get('DB_INGEST_TEST_CONFIG')
pytestmark=pytest.mark.skipif(not CONFIG,reason='Explicit disposable SQL configuration required')

@pytest.mark.parametrize('route',['scalar','native','bcp'])
@pytest.mark.parametrize('invalid',[False,True])
def test_varchar_explicit_encoding_exact_storage_bytes(route,invalid):
    case=API['TypeCase'](engine='bcp' if route=='bcp' else 'odbc',fast=route=='native',batch_rows=7)
    good=[['é🙂  ','é  '],['',''],[r'\N',r'\N'],['x\ny  ','€ '],['abc','ß'],['é','ñ'],['🙂','A'],['ok','B']]
    good=[row+['é🙂  ','é €  '] for row in good]
    good[-1][-2:]=['é🙂 '*3000,'é € '*3000]
    rows=good+([['ok','🙂','ok','ok'],['é🙂   ','ok','ok','ok']] if invalid else [])
    with case.source.open('w',encoding='utf-8',newline='') as stream:
        writer=csv.writer(stream);writer.writerow(['utf8','western','utf8_max','western_max']);writer.writerows(rows)
    content=case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
    content+='\n[schema]\non_length_overflow="quarantine"\n'
    content+='\n[[file_rules]]\nglob='+API['lit'](case.source.name)+'\n'
    for ordinal,name,kind,coll,enc in [(1,'utf8','varchar(8)','Latin1_General_100_BIN2_UTF8','utf-8'),(2,'western','varchar(3)','Latin1_General_100_BIN2','cp1252'),(3,'utf8_max','varchar(max)','Latin1_General_100_BIN2_UTF8','utf-8'),(4,'western_max','varchar(max)','Latin1_General_100_BIN2','cp1252')]:
        content+='\n[[file_rules.columns]]\n'+'\n'.join(k+'='+API['lit'](v) for k,v in dict(source_ordinal=ordinal,target_name=name,target_type=kind,collation=coll,text_encoding=enc).items())+'\n'
    case.config.write_text(content,encoding='utf-8')
    source_bytes=case.source.read_bytes();source_hash=hashlib.sha256(source_bytes).hexdigest()
    report=case.cli('run','--no-parallel','--non-interactive',exit_code=3 if invalid else 0)
    item=report['files'][0];assert item['counts']['committed']==8
    assert item['counts']['rejected']==(2 if invalid else 0)
    assert Path(item['artifact']['path']).read_bytes()==source_bytes
    assert item['receipt']['source_hash']==source_hash
    assert item['receipt']['details']['transport']['engine']==('bcp' if route=='bcp' else 'odbc')
    if route=='native':assert item['receipt']['details']['staging_batches']['fast_array_batches']>=1
    target=API['q'](item['plan']['schema'])+'.'+API['q'](item['plan']['table'])
    conn=case.connection()
    try:
        actual=list(conn.cursor().execute('SELECT [__ingest_ordinal],[__ingest_record],[utf8],[western],[utf8_max],[western_max],DATALENGTH([utf8]),DATALENGTH([western]),DATALENGTH([utf8_max]),DATALENGTH([western_max]),CONVERT(varbinary(max),[utf8]),CONVERT(varbinary(max),[western]),CONVERT(varbinary(max),[utf8_max]),CONVERT(varbinary(max),[western_max]) FROM '+target+' ORDER BY [__ingest_ordinal]'))
        assert len(actual)==8 and len({r[1] for r in actual})==8
        for ordinal,(got,want) in enumerate(zip(actual,good,strict=True),2):
            expected=[None if value==r'\N' else value for value in want]
            encoded=[None if value is None else value.encode(enc) for value,enc in zip(expected,['utf-8','cp1252','utf-8','cp1252'],strict=True)]
            assert got[0]==ordinal
            assert tuple(got[2:])==tuple(expected+[None if b is None else len(b) for b in encoded]+encoded)
        receipts=conn.cursor().execute('SELECT COUNT(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?',item['intent_id']).fetchval()
        assert receipts==1
    finally:conn.rollback();conn.close()

@pytest.mark.parametrize('collation,expected_code',[('Cyrillic_General_100_BIN2','CONFIG_COLUMN'),('NoSuchCollation_IndependentOracle','CONFIG_COLUMN')])
def test_varchar_unsupported_collation_policy_rejected_before_capture(collation,expected_code):
    case=API['TypeCase'](engine='odbc',fast=False)
    text=case.config.read_text(encoding='utf-8')
    marker='target_name = "text_fixed"\ntarget_type = "nvarchar(32)"'
    assert marker in text
    # Use representable scalar source values so only actual server policy causes refusal.
    source=API['ORACLE']['authored_rows']
    rows=list(source(17,'middle'))
    column_index=next(i for i,c in enumerate(API['ORACLE']['columns']()) if c.name=='text_fixed')
    for row in rows:
        if row['source'][column_index] is not None:row['source'][column_index]='A'
    data=API['ORACLE']['encode_record']([c.name for c in API['ORACLE']['columns']()])+b''.join(API['ORACLE']['encode_record'](r['source'],r['force_quote']) for r in rows)
    case.source.write_bytes(data)
    text=text.replace(marker,'target_name = "text_fixed"\ntarget_type = "varchar(32)"\ntext_encoding = "cp1252"\ncollation = '+API['lit'](collation))
    case.config.write_text(text,encoding='utf-8')
    report=case.cli('run','--no-parallel','--non-interactive',exit_code=2)
    assert expected_code in str(report)
    assert not (case.folder/'work'/'journal.sqlite3').exists()
    assert case.source.read_bytes()==data
