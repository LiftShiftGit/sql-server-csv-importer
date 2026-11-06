"""Measured native fast arrays: 2001 rows with one MiB-scale text/BLOB outlier."""
import base64
import csv
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import time
import tomllib
import uuid
import pytest
ROOT=Path(__file__).resolve().parents[2]
TYPE=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
CONFIG=os.environ.get('DB_INGEST_TEST_CONFIG')
ENABLED=os.environ.get('DB_INGEST_TEST_RESOURCE')=='1'
pytestmark=pytest.mark.skipif(not CONFIG or not ENABLED,reason='Explicit disposable SQL/native-allocation resource test opt-in required')


def authored(index,outlier):
    return (index,'L'*1048576+'🙂  ' if index==outlier else f'tiny-{index}  ',bytes(range(256))*4096 if index==outlier else bytes([index%256]),f'note-{index}  ')

@pytest.mark.parametrize('position',['first','middle','last'])
def test_real_fast_arrays_mib_outlier_measured_and_fully_verified(position):
    import psutil
    import pyodbc
    d=tomllib.loads(Path(CONFIG).read_text(encoding='utf-8-sig'))['destination']
    for key in ('database','control_database'):assert d[key].lower().startswith('dbingesttest_'),'Refusing non-disposable resource test destination'
    folder=ROOT/'.integration'/('native-stride-'+position+'-'+uuid.uuid4().hex);folder.mkdir(parents=True)
    source=folder/('large_'+uuid.uuid4().hex+'.csv');count=2001;outlier={'first':1,'middle':1001,'last':2001}[position]
    with source.open('w',encoding='utf-8',newline='') as stream:
        writer=csv.writer(stream,lineterminator='\r\n');writer.writerow(['id','text','payload','note'])
        for index in range(1,count+1):
            identity,text,payload,note=authored(index,outlier);writer.writerow([identity,text,base64.b64encode(payload).decode('ascii'),note])
    source_hash=hashlib.sha256(source.read_bytes()).hexdigest()
    config=folder/'ingest.toml'
    sections={'destination':d,'source':{'root':str(folder),'path':str(source),'include':[source.name]},'parsing':{'encoding':'utf-8','delimiter':',','quotechar':'"','header':'present'},'preservation':{'work_directory':str(folder/'work'),'minimum_free_disk_bytes':1048576},'load':{'engine':'odbc','fast_executemany':True,'batch_rows':5000,'batch_bytes':8388608},'parallel':{'enabled':False},'verification':{'level':'full'},'interaction':{'non_interactive':True}}
    text='\n'.join('['+section+']\n'+'\n'.join(k+' = '+TYPE['lit'](v) for k,v in values.items()) for section,values in sections.items())
    text+='\n[[file_rules]]\nglob = '+TYPE['lit'](source.name)+'\n'
    for ordinal,(name,kind,binary) in enumerate([('id','int',None),('text','nvarchar(max)',None),('payload','varbinary(max)','base64'),('note','nvarchar(32)',None)],1):
        text+='\n[[file_rules.columns]]\nsource_ordinal = '+str(ordinal)+'\ntarget_name = '+TYPE['lit'](name)+'\ntarget_type = '+TYPE['lit'](kind)+'\n'
        if binary:text+='binary_encoding = '+TYPE['lit'](binary)+'\n'
    config.write_text(text,encoding='utf-8')
    env={k:v for k,v in os.environ.items() if not k.startswith('DB_') or k in ('DB_USERNAME','DB_PASSWORD')}
    stdout=folder/'run.json';stderr=folder/'stderr.txt';peak=0;samples=0;started=time.perf_counter();killed=False
    with stdout.open('w',encoding='utf-8') as out,stderr.open('w',encoding='utf-8') as err:
        child=subprocess.Popen([sys.executable,'-m','db_ingest','--config',str(config),'run','--no-parallel','--non-interactive','--json'],cwd=ROOT,env=env,stdout=out,stderr=err,shell=False)
        process=psutil.Process(child.pid)
        while child.poll() is None:
            try:
                rss=sum(p.memory_info().rss for p in [process,*process.children(recursive=True)] if p.is_running());peak=max(peak,rss);samples+=1
                if rss>536870912:
                    child.terminate();killed=True
            except psutil.Error:pass
            time.sleep(.01)
        exit_code=child.wait()
    elapsed=time.perf_counter()-started
    secret=os.environ.get('DB_PASSWORD')
    for path in (stdout,stderr):
        contents=path.read_text(encoding='utf-8')
        assert not secret or secret not in contents,'Credential leaked in resource CLI output'
    preliminary={'position':position,'rows':count,'outlier_binary_bytes':1048576,'outlier_text_utf16_bytes':2097160,'peak_cli_process_tree_rss_bytes':peak,'samples':samples,'sampling':'psutil recursive process-tree RSS every10ms; sampled, not hardpeakguarantee','elapsed_seconds':elapsed,'exit_code':exit_code,'terminated_for_512MiB_observed_budget':killed,'source_sha256':source_hash,'scope':'REAL_ODBC_FAST_ARRAYS'}
    (folder/'measurement.json').write_text(json.dumps(preliminary,indent=2),encoding='utf-8')
    assert exit_code==0 and not killed,{'measurement':str(folder/'measurement.json'),'stderr':str(stderr)}
    report=json.loads(stdout.read_text(encoding='utf-8'));assert report['state']=='CLEAN_VERIFIED'
    item=report['files'][0];batch=item['receipt']['details']['staging_batches']
    assert batch['fast_executemany'] is True
    assert batch['fast_array_batches']>=1 and batch['fallback_batches']>=1 and batch['fallback_rows']==1
    assert batch['max_native_array_bytes']<=8388608
    assert batch['count']>1 and batch['max_bytes']<=8388608
    assert item['counts']['committed']==count and item['counts']['pending']==item['counts']['rejected']==item['counts']['unresolved']==0
    assert Path(item['artifact']['path']).read_bytes()==source.read_bytes()
    props={'DRIVER':'ODBC Driver 18 for SQL Server','SERVER':d['server'],'DATABASE':d['database'],'Encrypt':d.get('encrypt','yes'),'TrustServerCertificate':d.get('trust_server_certificate','no'),'UID':os.environ['DB_USERNAME'],'PWD':os.environ['DB_PASSWORD']}
    conn=pyodbc.connect(';'.join(k+'='+TYPE['brace'](v) for k,v in props.items())+';',autocommit=False,timeout=15)
    try:
        plan=item['plan'];target=TYPE['q'](plan['schema'])+'.'+TYPE['q'](plan['table']);cur=conn.cursor()
        actual_count=cur.execute('SELECT COUNT_BIG(*),COUNT(DISTINCT [__ingest_record]) FROM '+target+' WHERE [__ingest_intent]=?',item['intent_id']).fetchone()
        assert tuple(actual_count)==(count,count)
        cur.execute('SELECT id,text,payload,note,__ingest_ordinal FROM '+target+' WHERE [__ingest_intent]=? ORDER BY id',item['intent_id'])
        for index in range(1,count+1):
            got=cur.fetchone();want=authored(index,outlier)
            assert got is not None and tuple(got[:-1])==want and got[-1]==index+1,('Full authored row mismatch',index)
        assert cur.fetchone() is None
    finally:conn.rollback();conn.close()
    preliminary.update(verification='FULL_AUTHORED_SQL_ROW_VALUES_SOURCE_BYTES_AND_DISTINCT_IDENTITIES',staging_batches=batch,run_id=report['run_id'])
    (folder/'measurement.json').write_text(json.dumps(preliminary,indent=2),encoding='utf-8')
