"""Full authored type/NULL oracle across actual batching engines. No inferred expectations."""
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tomllib
import uuid
import pytest

ROOT=Path(__file__).resolve().parents[2]
ORACLE=runpy.run_path(str(ROOT/'tests/oracles/type_matrix.py'))
CONFIG=os.environ.get('DB_INGEST_TEST_CONFIG')
pytestmark=pytest.mark.skipif(not CONFIG,reason='Explicit disposable SQL configuration required for live type/batch tests')
BATCHING=os.environ.get('DB_INGEST_TEST_BATCHING')=='1'
ENGINES={x.strip() for x in os.environ.get('DB_INGEST_TEST_ENGINES','odbc').split(',')}
BCP_PATH=Path(os.environ.get('DB_INGEST_TEST_BCP_PATH',str(ROOT/'.tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/bcp.exe')))

def lit(value):
    if isinstance(value,bool):return 'true' if value else 'false'
    if isinstance(value,(int,float)):return str(value)
    if isinstance(value,list):return '['+','.join(lit(x) for x in value)+']'
    return json.dumps(str(value),ensure_ascii=False)
def q(value):return '['+value.replace(']',']]')+']'
def brace(value):return '{'+str(value).replace('}','}}')+'}'

class TypeCase:
    def __init__(self,engine='odbc',batch_rows=1,batch_bytes=65536,outlier='middle',negative=False,fast=False):
        self.d=tomllib.loads(Path(CONFIG).read_text(encoding='utf-8-sig'))['destination']
        for key in ('database','control_database'):
            assert self.d[key].casefold().startswith(('dbingesttest_','ingestfixture_')),'Refusing non-disposable type test database'
        self.folder=ROOT/'.integration'/('type-matrix-'+uuid.uuid4().hex);self.folder.mkdir(parents=True)
        self.source=self.folder/('types_'+uuid.uuid4().hex+'.csv')
        self.manifest=ORACLE['write_fixture'](self.source,17,outlier,negative)
        self.config=self.folder/'ingest.toml'
        sections={'destination':self.d,'source':{'root':str(self.folder),'path':str(self.source),'include':[self.source.name],'format':'delimited'},'parsing':{'encoding':'utf-8','delimiter':',','quotechar':'"','header':'present','null_tokens':[chr(92)+'N']},'preservation':{'work_directory':str(self.folder/'work'),'minimum_free_disk_bytes':1048576},'load':{'engine':engine,'mode':'append','fast_executemany':fast,'batch_rows':batch_rows,'batch_bytes':batch_bytes},'parallel':{'enabled':False},'verification':{'level':'full'},'interaction':{'non_interactive':True}}
        if engine=='bcp':
            assert BCP_PATH.exists(),'Selected native bcp executable missing'
            sections['tools']={'bcp_path':str(BCP_PATH)}
            sections['credentials']={'allow_password_in_child_args':True}
        content='\n'.join('['+section+']\n'+'\n'.join(k+' = '+lit(v) for k,v in values.items()) for section,values in sections.items())
        content+='\n[[file_rules]]\nglob = '+lit(self.source.name)+'\n'
        for index,column in enumerate(ORACLE['columns'](),1):
            content+='\n[[file_rules.columns]]\nsource_ordinal = '+str(index)+'\ntarget_name = '+lit(column.name)+'\ntarget_type = '+lit(column.sql_type)+'\n'
            if column.binary_encoding:content+='binary_encoding = '+lit(column.binary_encoding)+'\n'
        self.config.write_text(content,encoding='utf-8')
        self.expected=list(ORACLE['authored_rows'](17,outlier))
        (self.folder/'independent-manifest.json').write_text(json.dumps(self.manifest,indent=2),encoding='utf-8')
    def cli(self,command,*args,exit_code=0,extra_env=None):
        env={k:v for k,v in os.environ.items() if not k.startswith('DB_') or k in ('DB_USERNAME','DB_PASSWORD')}
        env.update(extra_env or {})
        result=subprocess.run([sys.executable,'-m','db_ingest','--config',str(self.config),command,*args,'--json'],cwd=ROOT,env=env,capture_output=True,text=True,timeout=240,shell=False)
        secret=os.environ.get('DB_PASSWORD');output=result.stdout+result.stderr
        sanitized=output.replace(secret,'<redacted>') if secret else output
        (self.folder/(command+'-output.json')).write_text(sanitized,encoding='utf-8')
        assert not secret or secret not in output,'Credential exposed in CLI output'
        assert result.returncode==exit_code,{'expected_exit':exit_code,'actual_exit':result.returncode,'retained_sanitized_output':str(self.folder/(command+'-output.json'))}
        return json.loads(result.stdout or result.stderr) if (result.stdout or result.stderr).strip() else None
    def connection(self):
        import pyodbc
        props={'DRIVER':'ODBC Driver 18 for SQL Server','SERVER':self.d['server'],'DATABASE':self.d['database'],'Encrypt':self.d.get('encrypt','yes'),'TrustServerCertificate':self.d.get('trust_server_certificate','no')}
        if self.d.get('authentication','sql')=='integrated':props['Trusted_Connection']='yes'
        else:props.update(UID=os.environ['DB_USERNAME'],PWD=os.environ['DB_PASSWORD'])
        return pyodbc.connect(';'.join(k+'='+brace(v) for k,v in props.items())+';',autocommit=False,timeout=15)
    def verify(self,report):
        item=report['files'][0];plan=item['plan'];receipt=item['receipt']
        captured=Path(item['artifact']['path'])
        assert captured.stat().st_size==self.manifest['source_bytes']
        assert hashlib.sha256(captured.read_bytes()).hexdigest()==self.manifest['source_sha256']
        assert captured.read_bytes()==self.source.read_bytes()
        expected_count=len(self.expected)
        assert receipt['committed_records']==expected_count
        assert item['counts']['committed']==expected_count and item['counts']['pending']==item['counts']['unresolved']==0
        assert item['counts']['rejected']==self.manifest['rejected_rows']
        conn=self.connection()
        try:
            cursor=conn.cursor();expressions=[]
            for column in ORACLE['columns']():
                name=q(column.name);base=column.sql_type.split('(')[0]
                expressions.append('CONVERT(nvarchar(64),'+name+',126)' if base in ('date','time','datetime2','datetimeoffset') else 'CONVERT(nvarchar(36),'+name+')' if base=='uniqueidentifier' else name)
            target=q(plan['schema'])+'.'+q(plan['table'])
            aggregate=cursor.execute('SELECT COUNT_BIG(*),COUNT(DISTINCT [__ingest_record]),MIN([__ingest_ordinal]),MAX([__ingest_ordinal]) FROM '+target+' WHERE [__ingest_intent]=?',item['intent_id']).fetchone()
            assert tuple(aggregate)==(expected_count,expected_count,min(r['ordinal'] for r in self.expected),max(r['ordinal'] for r in self.expected))
            cursor.execute('SELECT [__ingest_record],[__ingest_ordinal],'+','.join(expressions)+' FROM '+target+' WHERE [__ingest_intent]=? ORDER BY [row_id]',item['intent_id'])
            identities=set()
            for expected in self.expected:
                actual=cursor.fetchone();assert actual is not None
                assert actual[1]==expected['ordinal'];identities.add(actual[0])
                for column,want,got in zip(ORACLE['columns'](),expected['expected'],actual[2:],strict=True):
                    assert ORACLE['equivalent'](got,want,column),(column.name,expected['ordinal'],repr(got)[:160],repr(want)[:160])
            assert cursor.fetchone() is None and len(identities)==expected_count
            sql_receipt=cursor.execute('SELECT [payload] FROM '+q(getattr(self,'audit_schema','audit'))+'.[__db_ingest_receipts] WHERE [intent_id]=?',item['intent_id']).fetchone()
            assert sql_receipt is not None
            data=json.loads(sql_receipt[0])
            for key in ('run_id','intent_id','source_hash','interpretation_hash','mapping_hash','committed_records'):
                assert data[key]==receipt[key]
        finally:conn.rollback();conn.close()
        return item

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_live_all_advertised_types_baseline(engine):
    if engine not in ENGINES:pytest.skip('Engine not explicitly enabled by DB_INGEST_TEST_ENGINES')
    case=TypeCase(engine=engine)
    report=case.cli('run','--no-parallel','--non-interactive')
    assert report['state']=='CLEAN_VERIFIED'
    item=case.verify(report)
    assert item['receipt']['details']['transport']['engine']==engine
    case.cli('reconcile','--run-id',report['run_id'],'--non-interactive')

@pytest.mark.skipif(not BATCHING,reason='Batch controls integration opt-in pending implementation: DB_INGEST_TEST_BATCHING=1')
@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('batch_rows,batch_bytes',[(1,65536),(2,65536),(7,131072),(500,262144),(5000,1048576)])
@pytest.mark.parametrize('outlier',['first','middle','last'])
def test_live_type_batches_exact_independent_values(engine,batch_rows,batch_bytes,outlier):
    if engine not in ENGINES:pytest.skip('Engine not explicitly enabled by DB_INGEST_TEST_ENGINES')
    case=TypeCase(engine,batch_rows,batch_bytes,outlier)
    report=case.cli('run','--no-parallel','--non-interactive')
    item=case.verify(report)
    transport=item['receipt']['details']['transport'];batches=item['receipt']['details']['staging_batches']
    assert transport['engine']==engine and transport['requested']==engine
    assert batches['limit_rows']==batch_rows and batches['limit_bytes']==batch_bytes
    assert 1<=batches['max_rows']<=batch_rows
    assert batches['max_bytes']<=batch_bytes and batches['count']>=1
    if batch_rows==1:assert batches['count']==17
    if batch_rows>=7:assert batches['max_rows']>1,'Batch controls accepted but the requested multirow batching path was not exercised'

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_live_missing_and_multierror_rows_are_rejected_once(engine):
    if engine not in ENGINES:pytest.skip('Engine not explicitly enabled by DB_INGEST_TEST_ENGINES')
    case=TypeCase(engine=engine,negative=True)
    report=case.cli('run','--no-parallel','--non-interactive',exit_code=3)
    assert report['state']=='PARTIAL'
    case.verify(report)
    errors=case.cli('show-errors','--run-id',report['run_id'])
    rejected=errors['record_errors']
    assert len(rejected)==2
    assert sum(len(r['errors']) for r in rejected)>=4
    assert any(any(e['code']=='ROW_WIDTH' for e in r['errors']) for r in rejected)


@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_live_explicit_not_null_rejects_null_without_default_invention(engine):
    if engine not in ENGINES:pytest.skip('Engine not explicitly enabled by DB_INGEST_TEST_ENGINES')
    case=TypeCase(engine=engine)
    content=case.config.read_text(encoding='utf-8')
    needle='target_name = "flag"\ntarget_type = "bit"'
    assert needle in content
    case.config.write_text(content.replace(needle,needle+'\nnullable = false'),encoding='utf-8')
    case.expected=[r for r in case.expected if r['ordinal']!=3]
    case.manifest.update(accepted_rows=16,rejected_rows=1)
    report=case.cli('run','--no-parallel','--non-interactive',exit_code=3)
    case.verify(report)
    errors=case.cli('show-errors','--run-id',report['run_id'])
    assert len(errors['record_errors'])==1
    assert errors['record_errors'][0]['ordinal']==3
    assert any(e['code']=='NULLABILITY' for e in errors['record_errors'][0]['errors'])


@pytest.mark.skipif(os.environ.get('DB_INGEST_TEST_FAST')!='1',reason='Native ODBC arrays require explicit fast-path integration opt-in')
@pytest.mark.parametrize('outlier',['first','middle','last'])
def test_live_fast_full_type_oracle_with_explicit_lob_fallback(outlier):
    if 'odbc' not in ENGINES:pytest.skip('ODBC engine not explicitly enabled')
    case=TypeCase(engine='odbc',batch_rows=7,batch_bytes=131072,outlier=outlier,fast=True)
    report=case.cli('run','--no-parallel','--non-interactive')
    item=case.verify(report)
    batches=item['receipt']['details']['staging_batches']
    assert batches['fast_executemany'] is True
    assert batches['fast_array_batches']>=1
    assert batches['fallback_rows']==1 and batches['fallback_batches']==1
    assert batches['max_native_array_bytes']<=batches['limit_bytes']==131072
    assert item['receipt']['details']['transport']['engine']=='odbc'


@pytest.mark.skipif(os.environ.get('DB_INGEST_TEST_FAST')!='1',reason='Quoted audit schema canonical ODBC integration explicitly opt-in')
def test_live_default_fast_odbc_quotes_unicode_audit_schema():
    if 'odbc' not in ENGINES:pytest.skip('ODBC engine not explicitly selected')
    case=TypeCase(engine='odbc',batch_rows=7,batch_bytes=131072,fast=True)
    case.audit_schema='監査 space];--'+uuid.uuid4().hex[:12]
    with case.config.open('a',encoding='utf-8') as out:out.write('\n[audit]\ncontrol_schema = '+lit(case.audit_schema)+'\n')
    report=case.cli('run','--no-parallel','--non-interactive')
    item=case.verify(report)
    assert item['receipt']['details']['transport']['engine']=='odbc'
    assert item['receipt']['details']['staging_batches']['fast_array_batches']>0
    case.cli('reconcile','--run-id',report['run_id'],'--non-interactive')
