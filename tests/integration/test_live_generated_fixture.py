"""Generated Gate1 fixture plus independent verifier; explicit disposable SQL config only."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib
import uuid
import pytest
CONFIG=os.environ.get('DB_INGEST_TEST_CONFIG')
pytestmark=pytest.mark.skipif(not CONFIG,reason='Explicit disposable SQL configuration required for generated-fixture integration')

def lit(value):
    if isinstance(value,bool):return 'true' if value else 'false'
    if isinstance(value,(int,float)):return str(value)
    if isinstance(value,list):return '['+','.join(lit(x) for x in value)+']'
    return json.dumps(str(value),ensure_ascii=True)

def test_generated_vertical_fixture_cli_independent_readback():
    root=Path(__file__).resolve().parents[2]
    d=tomllib.loads(Path(CONFIG).read_text(encoding='utf-8-sig'))['destination']
    for key in ('database','control_database'):
        assert d[key].lower().startswith(('dbingesttest_','ingestfixture_')),'Refusing non-disposable SQL fixture destination'
    assert d.get('authentication','sql')=='integrated' or (os.environ.get('DB_USERNAME') and os.environ.get('DB_PASSWORD') is not None)
    case=root/'.integration'/('generated-fixture-test-'+uuid.uuid4().hex)
    case.mkdir(parents=True)
    fixtures=case/'fixtures'
    env={k:v for k,v in os.environ.items() if not k.startswith('DB_') or k in ('DB_USERNAME','DB_PASSWORD')}
    def cli(*args):
        result=subprocess.run([sys.executable,'-m','db_ingest',*map(str,args)],cwd=root,env=env,capture_output=True,text=True,timeout=240,shell=False)
        secret=os.environ.get('DB_PASSWORD')
        sanitized=(result.stdout+result.stderr).replace(secret,'<redacted>') if secret else result.stdout+result.stderr
        if secret:assert secret not in result.stdout+result.stderr,'Credential leaked by CLI'
        assert result.returncode==0,sanitized
        return json.loads(result.stdout)
    manifest=cli('create-test-files','--output',fixtures,'--type','vertical-slice','--destination','mssql','--seed','20261029','--json')
    fixture=manifest['fixtures'][0]
    sections={'destination':d,'source':{'root':str(fixtures),'path':str(fixtures),'include':['**/*.csv'],'format':'delimited'},'parsing':{'encoding':'utf-8','delimiter':',','quotechar':'"','header':'present',**fixture['overrides']},'preservation':{'work_directory':str(case/'work'),'minimum_free_disk_bytes':1048576},'load':{'engine':'odbc','fast_executemany':False},'parallel':{'enabled':False},'interaction':{'non_interactive':True}}
    config=case/'fixture.toml'
    text='\n'.join('['+section+']\n'+'\n'.join(k+' = '+lit(v) for k,v in values.items()) for section,values in sections.items())
    text+='\n[[file_rules]]\nglob = '+lit(fixture['path'])+'\n'
    for spec in fixture['sql_schema']:
        text+='\n[[file_rules.columns]]\n'+'\n'.join(k+' = '+lit(v) for k,v in spec.items())+'\n'
    config.write_text(text,encoding='utf-8')
    run=cli('--config',config,'run','--no-parallel','--non-interactive','--new-intent','--json')
    assert run['state']=='CLEAN_VERIFIED'
    assert run['files'][0]['receipt']['committed_records']==2
    verified=cli('--config',config,'verify-test-files','--manifest',fixtures/'manifest.json','--run-id',run['run_id'],'--database',d['database'],'--non-interactive','--json')
    assert verified['state']=='FIXTURE_VERIFIED' and verified['destination_files_checked']==1
    assert verified['source_files_checked']==2 and verified['errors']==[]
    cli('--config',config,'reconcile','--run-id',run['run_id'],'--non-interactive','--json')
    resumed=cli('--config',config,'resume','--run-id',run['run_id'],'--non-interactive','--no-parallel','--json')
    assert resumed['state']=='CLEAN_VERIFIED'
    (case/'result.json').write_text(json.dumps({'run_id':run['run_id'],'verification':verified,'artifact':run['files'][0]['artifact']},indent=2),encoding='utf-8')
