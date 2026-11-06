"""Cross-version resume safety: old hashed default versus new filename default."""
import json,os,runpy,subprocess
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_filename_naming.py'))
OLD=Path(os.environ.get('DB_INGEST_PREVIOUS_PYTHON',''))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG') or not OLD.is_file(),reason='Disposable SQL and retained pre-default-change installed release required')

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_old_hashed_run_resumes_only_with_explicit_hash_option(engine):
    c=API['Case'](engine)
    assert 'mapping' not in c.settings
    env={k:v for k,v in os.environ.items() if not k.startswith('DB_') or k in ('DB_USERNAME','DB_PASSWORD')}
    p=subprocess.run([str(OLD),'-I','-m','db_ingest','--config',str(c.base.config),'run','--non-interactive','--json'],cwd=ROOT,env=env,capture_output=True,text=True,timeout=180)
    assert p.returncode==0,'Old-build fixture failed; inspect its retained work directory'
    old=json.loads(p.stdout);item=old['files'][0];plan=item['plan']
    assert plan['table']!='orders' and '__' in plan['table']
    target=API['API']['q'](plan['schema'])+'.'+API['API']['q'](plan['table'])
    identity=c.sql('SELECT OBJECT_ID(?)',target)
    refused=c.base.cli('resume','--run-id',old['run_id'],'--non-interactive',exit_code=2)
    assert any(e['code']=='RESUME_SEMANTIC_CHANGE' for e in refused['errors'])
    assert c.sql('SELECT COUNT_BIG(*) FROM '+target)==[(2,)]
    assert c.sql('SELECT OBJECT_ID(?)',c.target)==[(None,)]
    c.settings['mapping']={'name_collision':'stable_hash_suffix'};c.select(c.first)
    after=c.base.cli('resume','--run-id',old['run_id'],'--non-interactive')
    assert after['files'][0]['intent_id']==item['intent_id']
    assert after['files'][0]['plan']['table']==plan['table']
    assert c.sql('SELECT OBJECT_ID(?)',target)==identity
    assert c.sql('SELECT COUNT_BIG(*) FROM '+target)==[(2,)]
    assert c.sql('SELECT COUNT(*) FROM audit.__db_ingest_receipts WHERE intent_id=?',item['intent_id'])==[(1,)]
