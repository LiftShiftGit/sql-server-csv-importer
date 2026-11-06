"""Real restore safety, only against a freshly created UUID-named test database."""
import json
import os
from pathlib import Path
import runpy
import uuid
import pytest
ROOT=Path(__file__).resolve().parents[2]
TYPE=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
CONFIG=os.environ.get('DB_INGEST_TEST_CONFIG')
ENABLED=os.environ.get('DB_INGEST_TEST_RESTORE')=='1'
ENGINES={v.strip() for v in os.environ.get('DB_INGEST_TEST_ENGINES','odbc').split(',')}
pytestmark=pytest.mark.skipif(not CONFIG or not ENABLED,reason='Explicit fresh disposable database restore test opt-in required')

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_live_restore_old_generation_blocks_acknowledged_intents(engine):
    if engine not in ENGINES:pytest.skip('Engine not explicitly selected')
    import pyodbc
    case=TYPE['TypeCase'](engine=engine,batch_rows=7,batch_bytes=131072)
    original=case.d['database'];fresh='DbIngestTest_Restore_'+uuid.uuid4().hex
    assert fresh!=original and fresh.startswith('DbIngestTest_Restore_')
    case.d={**case.d,'database':fresh}
    text=case.config.read_text(encoding='utf-8')
    needle='database = '+TYPE['lit'](original)
    assert needle in text
    case.config.write_text(text.replace(needle,'database = '+TYPE['lit'](fresh),1),encoding='utf-8')
    first=case.cli('run','--no-parallel','--non-interactive')
    case.verify(first)
    first_item=first['files'][0]
    assert first_item['receipt']['details'].get('recovery_fork_guid')
    props={'DRIVER':'ODBC Driver 18 for SQL Server','SERVER':case.d['server'],'DATABASE':'master','Encrypt':case.d.get('encrypt','yes'),'TrustServerCertificate':case.d.get('trust_server_certificate','no')}
    if case.d.get('authentication','sql')=='integrated':props['Trusted_Connection']='yes'
    else:props.update(UID=os.environ['DB_USERNAME'],PWD=os.environ['DB_PASSWORD'])
    master=pyodbc.connect(';'.join(k+'='+TYPE['brace'](v) for k,v in props.items())+';',autocommit=True,timeout=15)
    master.timeout=120
    backup='/var/opt/mssql/data/'+fresh+'.bak'
    evidence={'database':fresh,'server':case.d['server'],'backup_path_on_server':backup,'restored_shared_database':False,'engine':engine,'first_run_id':first['run_id'],'first_intent':first_item['intent_id']}
    try:
        cur=master.cursor()
        before=cur.execute('SELECT CONVERT(nvarchar(36),database_guid),CONVERT(nvarchar(36),recovery_fork_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID(?)',fresh).fetchone()
        evidence.update(database_guid_before=before[0],recovery_fork_before=before[1])
        target=TYPE['q'](fresh)+'.'+TYPE['q'](first_item['plan']['schema'])+'.'+TYPE['q'](first_item['plan']['table'])
        original_ids={row[0] for row in cur.execute('SELECT __ingest_record FROM '+target+' WHERE __ingest_intent=?',first_item['intent_id']).fetchall()}
        assert len(original_ids)==17
        cur.execute('DECLARE @f nvarchar(4000)=?; BACKUP DATABASE '+TYPE['q'](fresh)+' TO DISK=@f WITH INIT,CHECKSUM',backup)
        while cur.nextset():pass
        second=case.cli('run','--no-parallel','--non-interactive','--new-intent')
        case.verify(second)
        second_item=second['files'][0]
        target=TYPE['q'](fresh)+'.'+TYPE['q'](first_item['plan']['schema'])+'.'+TYPE['q'](first_item['plan']['table'])
        assert cur.execute('SELECT COUNT_BIG(*) FROM '+target).fetchone()[0]==34
        cur.execute('ALTER DATABASE '+TYPE['q'](fresh)+' SET SINGLE_USER WITH ROLLBACK IMMEDIATE')
        cur.execute('DECLARE @f nvarchar(4000)=?; RESTORE DATABASE '+TYPE['q'](fresh)+' FROM DISK=@f WITH REPLACE,RECOVERY',backup)
        while cur.nextset():pass
        cur.execute('ALTER DATABASE '+TYPE['q'](fresh)+' SET MULTI_USER')
        after=cur.execute('SELECT CONVERT(nvarchar(36),database_guid),CONVERT(nvarchar(36),recovery_fork_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID(?)',fresh).fetchone()
        evidence.update(database_guid_after=after[0],recovery_fork_after=after[1],second_run_id=second['run_id'],second_intent=second_item['intent_id'])
        assert after[1]!=before[1],'Actual restore did not create the recovery-fork condition this oracle requires'
        assert tuple(cur.execute('SELECT COUNT_BIG(*),COUNT(DISTINCT __ingest_intent) FROM '+target).fetchone())==(17,1)
        first_blocked=case.cli('resume','--run-id',first['run_id'],'--no-parallel','--non-interactive',exit_code=5)
        second_blocked=case.cli('resume','--run-id',second['run_id'],'--no-parallel','--non-interactive',exit_code=5)
        assert first_blocked['state']==second_blocked['state']=='COMMIT_OUTCOME_UNKNOWN'
        first_errors=case.cli('show-errors','--run-id',first['run_id'])
        assert any(e['code']=='RECEIPT_IDENTITY_MISMATCH' for e in first_errors['incidents'])
        second_errors=case.cli('show-errors','--run-id',second['run_id'])
        assert any(e['code']=='RECEIPT_MISSING' for e in second_errors['incidents'])
        count=cur.execute('SELECT COUNT_BIG(*),COUNT(DISTINCT __ingest_intent),COUNT(DISTINCT __ingest_record) FROM '+target).fetchone()
        assert tuple(count)==(17,1,17)
        assert cur.execute('SELECT COUNT_BIG(*) FROM '+target+' WHERE __ingest_intent=?',second_item['intent_id']).fetchone()[0]==0
        expressions=[]
        for column in TYPE['ORACLE']['columns']():
            name=TYPE['q'](column.name);base=column.sql_type.split('(')[0]
            expressions.append('CONVERT(nvarchar(64),'+name+',126)' if base in ('date','time','datetime2','datetimeoffset') else 'CONVERT(nvarchar(36),'+name+')' if base=='uniqueidentifier' else name)
        cur.execute('SELECT __ingest_record,__ingest_ordinal,'+','.join(expressions)+' FROM '+target+' WHERE __ingest_intent=? ORDER BY row_id',first_item['intent_id'])
        restored_ids=set()
        for expected in case.expected:
            actual=cur.fetchone();assert actual is not None
            restored_ids.add(actual[0]);assert actual[1]==expected['ordinal']
            for column,want,got in zip(TYPE['ORACLE']['columns'](),expected['expected'],actual[2:],strict=True):
                assert TYPE['ORACLE']['equivalent'](got,want,column),(column.name,expected['ordinal'])
        assert cur.fetchone() is None and restored_ids==original_ids
        evidence['restored_first_generation_full_authored_cells_and_ids_verified']=True
        for report in (first,second):
            assert Path(report['files'][0]['artifact']['path']).read_bytes()==case.source.read_bytes()
        evidence.update(result='PASSED',final_rows=count[0],final_intents=count[1],final_distinct_ids=count[2],first_ack_blocked_by_fork_identity=True,second_ack_missing_blocked=True,retained_database_and_backup=True)
    finally:
        try:master.cursor().execute('IF DB_ID(?) IS NOT NULL ALTER DATABASE '+TYPE['q'](fresh)+' SET MULTI_USER',fresh)
        finally:master.close()
        (case.folder/'restore-evidence.json').write_text(json.dumps(evidence,indent=2)+'\n',encoding='utf-8')
