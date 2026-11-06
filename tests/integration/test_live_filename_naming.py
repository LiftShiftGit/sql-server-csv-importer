"""Filename-only SQL naming: exact values, cross-run claims, concurrency and recovery."""
from concurrent.futures import ThreadPoolExecutor
import json,os,runpy,sqlite3,tomllib,uuid
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Disposable SQL required')

class Case:
    def __init__(self,engine):
        self.base=API['TypeCase'](engine=engine,batch_rows=1)
        self.settings=tomllib.loads(self.base.config.read_text().split('[[file_rules]]')[0])
        # Deliberately omit mapping.name_collision to exercise the shipped default.
        self.settings['destination']['schema']='plain_'+uuid.uuid4().hex[:16]
        self.settings['source'].update(include=['**/*.csv'])
        self.first=self.base.folder/'a/orders.csv';self.second=self.base.folder/'b/orders.csv'
        for p in (self.first,self.second):p.parent.mkdir();p.write_bytes(b'code,label\r\n001,alpha\r\n002,bravo\r\n')
        self.select(self.first)
    def select(self,path,root=None):
        self.settings['source'].update(path=str(path),root=str(root or self.base.folder))
        self.base.config.write_text('\n'.join('['+s+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in fields.items()) for s,fields in self.settings.items()))
    def sql(self,query,*params):
        conn=self.base.connection()
        try:return [tuple(r) for r in conn.cursor().execute(query,*params)]
        finally:conn.close()
    @property
    def target(self):return API['q'](self.settings['destination']['schema'])+'.[orders]'
    def verify(self,report,expected):
        item=report['files'][0];assert item['plan']['table']=='orders'
        assert self.sql('SELECT '+','.join(API['q'](c['target_name']) for c in item['plan']['columns'])+' FROM '+self.target+' ORDER BY __ingest_ordinal,__ingest_intent')==expected
        assert Path(item['artifact']['path']).read_bytes()==self.first.read_bytes()
        return item

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_filename_claim_blocks_separate_path_root_and_missing_claim(engine):
    c=Case(engine);first=c.base.cli('run','--non-interactive');item=c.verify(first,[('001','alpha'),('002','bravo')]);oid=c.sql('SELECT OBJECT_ID(?)',c.target)
    c.base.cli('resume','--run-id',first['run_id'],'--non-interactive')
    for path,root in [(c.second,None),(c.first,c.first.parent)]:
        c.select(path,root);blocked=c.base.cli('run','--new-intent','--non-interactive',exit_code=2)
        assert any(e['code']=='TABLE_NAME_COLLISION' for e in blocked['errors'])
        assert c.sql('SELECT OBJECT_ID(?)',c.target)==oid
        assert c.sql('SELECT COUNT_BIG(*) FROM '+c.target)==[(2,)]
        assert not blocked['files'][0].get('receipt')
    c.select(c.first);c.first.write_bytes(b'code,label\r\n003,delta\r\n004,gamma\r\n')
    added=c.base.cli('run','--non-interactive');assert added['files'][0]['counts']['committed']==2
    assert c.sql('SELECT COUNT_BIG(*) FROM '+c.target)==[(4,)]
    # Loss of the claim must not silently adopt the existing table.
    conn=c.base.connection()
    try:conn.cursor().execute('DELETE FROM audit.__db_ingest_name_claims WHERE target=?',c.target);conn.commit()
    finally:conn.close()
    c.first.write_bytes(b'code,label\r\n005,theta\r\n006,kappa\r\n')
    blocked=c.base.cli('run','--non-interactive',exit_code=5)
    assert any(e['code']=='TABLE_NAME_CLAIM_MISSING' for e in blocked['errors'])
    assert c.sql('SELECT COUNT_BIG(*) FROM '+c.target)==[(4,)]

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_filename_claim_atomic_commit_recovery(engine):
    c=Case(engine)
    c.base.cli('run','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':'AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT'})
    run_id=c.base.cli('status')['runs'][0]['run_id']
    recovered=c.base.cli('resume','--run-id',run_id,'--non-interactive');item=c.verify(recovered,[('001','alpha'),('002','bravo')])
    c.base.cli('resume','--run-id',run_id,'--non-interactive')
    assert c.sql('SELECT COUNT_BIG(*) FROM '+c.target)==[(2,)]
    assert c.sql('SELECT COUNT(*) FROM audit.__db_ingest_name_claims WHERE target=?',c.target)==[(1,)]
    assert c.sql('SELECT COUNT(*) FROM audit.__db_ingest_receipts WHERE intent_id=?',item['intent_id'])==[(1,)]

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_filename_claim_independent_concurrent_jobs(engine):
    c=Case(engine)
    first_config=c.base.config.read_text();second_config=c.base.folder/'second.toml'
    c.select(c.second);settings=tomllib.loads(c.base.config.read_text());settings['preservation']['work_directory']=str(c.base.folder/'second-work')
    second_config.write_text('\n'.join('['+s+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in fields.items()) for s,fields in settings.items()))
    c.base.config.write_text(first_config)
    import subprocess,sys
    env={k:v for k,v in os.environ.items() if not k.startswith('DB_') or k in ('DB_USERNAME','DB_PASSWORD')}
    def run(config):
        p=subprocess.run([sys.executable,'-m','db_ingest','--config',str(config),'run','--non-interactive','--json'],cwd=ROOT,env=env,capture_output=True,text=True,timeout=180)
        return p.returncode,json.loads(p.stdout or p.stderr)
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs=[pool.submit(run,p) for p in (c.base.config,second_config)];results=[j.result() for j in jobs]
    assert sorted(code for code,r in results)==[0,2]
    failed=next(r for code,r in results if code==2)
    assert any(e['code']=='TABLE_NAME_COLLISION' for e in failed['errors'])
    assert c.sql('SELECT COUNT_BIG(*) FROM '+c.target)==[(2,)]
    assert c.sql('SELECT COUNT(*) FROM audit.__db_ingest_name_claims WHERE target=?',c.target)==[(1,)]

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_filename_selection_collision_never_publishes(engine):
    c=Case(engine);c.select(c.base.folder)
    report=c.base.cli('run','--non-interactive',exit_code=2)
    assert any(e['code']=='TABLE_NAME_COLLISION' for e in report['errors'])
    assert c.sql('SELECT OBJECT_ID(?)',c.target)==[(None,)]
    assert c.first.read_bytes()==c.second.read_bytes()==b'code,label\r\n001,alpha\r\n002,bravo\r\n'

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_filename_existing_unowned_table_is_never_adopted(engine):
    c=Case(engine);conn=c.base.connection()
    try:
        conn.cursor().execute('CREATE SCHEMA '+API['q'](c.settings['destination']['schema']))
        conn.cursor().execute('CREATE TABLE '+c.target+'(sentinel nvarchar(40))')
        conn.cursor().execute('INSERT INTO '+c.target+" VALUES(N'keep this original row')");conn.commit()
    finally:conn.close()
    oid=c.sql('SELECT OBJECT_ID(?)',c.target)
    blocked=c.base.cli('run','--non-interactive',exit_code=5)
    assert any(e['code']=='TABLE_NAME_CLAIM_MISSING' for e in blocked['errors'])
    assert c.sql('SELECT OBJECT_ID(?)',c.target)==oid
    assert c.sql('SELECT sentinel FROM '+c.target)==[('keep this original row',)]

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_filename_claim_rolls_back_before_commit(engine):
    c=Case(engine)
    c.base.cli('run','--non-interactive',exit_code=86,extra_env={'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':'AFTER_TARGET_SCHEMA_CREATION'})
    assert c.sql('SELECT OBJECT_ID(?)',c.target)==[(None,)]
    assert c.sql('SELECT COUNT(*) FROM audit.__db_ingest_name_claims WHERE target=?',c.target)==[(0,)]
    run_id=c.base.cli('status')['runs'][0]['run_id']
    recovered=c.base.cli('resume','--run-id',run_id,'--non-interactive');item=c.verify(recovered,[('001','alpha'),('002','bravo')])
    assert c.sql('SELECT COUNT(*) FROM audit.__db_ingest_name_claims WHERE target=?',c.target)==[(1,)]
