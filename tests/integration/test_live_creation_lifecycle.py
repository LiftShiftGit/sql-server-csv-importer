"""Creation permission flags do not grant replacement: actual SQL lifecycle and identities."""
import itertools,json,os,runpy,tomllib,uuid
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Owned disposable SQL required')
@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_creation_then_all_flag_combinations_reuse_existing_objects(engine):
    if engine not in API['ENGINES']:pytest.skip('Engine not selected')
    case=API['TypeCase'](engine=engine,batch_rows=7)
    original=case.config.read_text(encoding='utf-8');parts=original.split('[[file_rules]]',1)
    settings=tomllib.loads(parts[0]);suffix='[[file_rules]]'+parts[1]
    token=uuid.uuid4().hex
    settings['destination'].update(database='DbIngestTest_Create_'+token,control_database='DbIngestTest_CreateControl_'+token,schema='landing_'+token[:8],create_databases=False,create_schemas=True,create_tables=True)
    settings['audit']={'control_schema':'audit_'+token[:8]}
    case.d=settings['destination']
    case.audit_schema=settings['audit']['control_schema']
    def save():case.config.write_text('\n'.join('['+s+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in fields.items()) for s,fields in settings.items())+'\n'+suffix,encoding='utf-8')
    save()
    failed=case.cli('run','--non-interactive',exit_code=4)
    assert any(e['code']=='DATABASE_ACCESS' for e in failed['errors'])
    assert not failed['files'][0].get('receipt')
    settings['destination']['create_databases']=True;save()
    baseline=case.cli('resume','--run-id',failed['run_id'],'--non-interactive')
    assert baseline['files'][0]['intent_id']==failed['files'][0]['intent_id']
    case.verify(baseline)
    p=baseline['files'][0]['plan'];target=API['q'](p['schema'])+'.'+API['q'](p['table']);sentinel=API['q'](p['schema'])+'.[unrelated_keep_me]'
    conn=case.connection()
    try:
        conn.cursor().execute('CREATE TABLE '+sentinel+'(value nvarchar(30) NOT NULL)')
        conn.cursor().execute('INSERT INTO '+sentinel+" VALUES(N'original unrelated row')");conn.commit()
    finally:conn.close()
    def inspect(expected_rows):
        conn=case.connection()
        try:
            cur=conn.cursor()
            identities=tuple(cur.execute('SELECT CONVERT(nvarchar(36),database_guid),SCHEMA_ID(?),OBJECT_ID(?),OBJECT_ID(?) FROM sys.database_recovery_status WHERE database_id=DB_ID()',p['schema'],target,sentinel).fetchone())
            assert cur.execute('SELECT COUNT_BIG(*) FROM '+target).fetchone()[0]==expected_rows
            control=API['q'](case.d['control_database'])+'.'+API['q'](case.audit_schema)
            assert cur.execute('SELECT COUNT_BIG(*) FROM '+control+'.[Runs]').fetchone()[0]==expected_rows//17
            assert cur.execute('SELECT COUNT_BIG(*) FROM '+control+'.[PublicationReceipts]').fetchone()[0]==expected_rows//17
            assert [r[0] for r in cur.execute('SELECT value FROM '+sentinel)]==['original unrelated row']
            return identities
        finally:conn.close()
    identities=inspect(17);observations=[]
    for index,flags in enumerate(itertools.product([False,True],repeat=3),2):
        settings['destination'].update(dict(zip(['create_databases','create_schemas','create_tables'],flags)))
        save();report=case.cli('run','--new-intent','--non-interactive');case.verify(report)
        assert inspect(17*index)==identities
        observations.append({'flags':flags,'rows':17*index,'object_identities_unchanged':True,'unrelated_row_unchanged':True})
    (case.folder/'creation-lifecycle-evidence.json').write_text(json.dumps({'engine':engine,'initial_database_access_refused':True,'same_intent_after_creation_enabled':True,'database':case.d['database'],'control_database':case.d['control_database'],'schema':p['schema'],'table':p['table'],'combinations':observations},indent=2),encoding='utf-8')
