"""Supervised real multi-GB SQL acceptance against an immutable installed runtime.

All source/capture/journal/transport data stays on user-chosen paths/SQL instance.
Developer-only psutil samples child RSS/CPU; no app parsers/converters compute the oracle.
Reports/captures/intents are retained on every outcome. No cleanup/reset is performed.
"""
import argparse,hashlib,json,os,runpy,subprocess,sys,time,tomllib,uuid,sqlite3
from pathlib import Path
from datetime import datetime,timezone
import psutil,pyodbc

ROOT=Path(__file__).resolve().parents[1]
ORACLE=runpy.run_path(str(ROOT/'tests/oracles/multigb_csv.py'))
CASES=[('bcp',128,8*1024*1024),('odbc',128,8*1024*1024),('bcp',4096,16*1024*1024),('odbc',4096,16*1024*1024)]

def save(path,value):
    temporary=Path(str(path)+'.partial');temporary.write_text(json.dumps(value,indent=2,ensure_ascii=True)+'\n',encoding='utf-8');temporary.replace(path)

def lit(v):return str(v).lower() if isinstance(v,bool) else json.dumps(v,ensure_ascii=True)
def quote(name):return '['+name.replace(']',']]')+']'
def brace(value):return '{'+str(value).replace('}','}}')+'}'

def connect(destination,credentials,database):
    props={'DRIVER':destination.get('driver','ODBC Driver 18 for SQL Server'),'SERVER':destination['server'],'DATABASE':database,'Encrypt':destination.get('encrypt','yes'),'TrustServerCertificate':destination.get('trust_server_certificate','no'),'UID':credentials['username'],'PWD':credentials['password']}
    return pyodbc.connect(';'.join(k+'='+brace(v) for k,v in props.items()),autocommit=True,timeout=30)


def runtime_fingerprint(interpreter):
    code="import db_ingest,sys,json,hashlib,importlib.metadata;from pathlib import Path;p=Path(db_ingest.__file__).parent;files={str(f.relative_to(p)).replace(chr(92),'/'):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(p.rglob('*')) if f.is_file() and f.suffix in ('.py','.toml')};print(json.dumps({'module':str(p),'python':sys.version,'pyodbc':importlib.metadata.version('pyodbc'),'files':files}))"
    result=subprocess.run([str(interpreter),'-I','-c',code],cwd=ROOT.anchor,capture_output=True,text=True,timeout=30)
    assert result.returncode==0,'Cannot fingerprint isolated installed runtime'
    report=json.loads(result.stdout)
    report['fingerprint_sha256']=hashlib.sha256(json.dumps({k:report[k] for k in ('python','pyodbc','files')},sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return report

def hash_file(path):
    h=hashlib.sha256();size=0
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):size+=len(block);h.update(block)
    return size,h.hexdigest()

def supervise(command,folder,credentials,timeout,rss_budget):
    environment={k:v for k,v in os.environ.items() if not k.startswith(('DB_','SQLCMD'))}
    environment.update(DB_USERNAME=credentials['username'],DB_PASSWORD=credentials['password'])
    started=time.monotonic();peak=0;cpu={};samples=0;failure=None
    with (folder/'stdout.json').open('wb') as stdout,(folder/'stderr.txt').open('wb') as stderr:
        child=subprocess.Popen(command,cwd=ROOT.anchor,env=environment,stdout=stdout,stderr=stderr,shell=False)
        proc=psutil.Process(child.pid);last_progress=0;last_notice=0
        while child.poll() is None:
            current=0
            try:processes=[proc]+proc.children(recursive=True)
            except psutil.Error:processes=[]
            for process in processes:
                try:
                    identity=(process.pid,process.create_time());current+=process.memory_info().rss
                    times=process.cpu_times();cpu[identity]=times.user+times.system
                except psutil.Error:pass
            peak=max(peak,current);samples+=1;elapsed=time.monotonic()-started
            if current>rss_budget:failure='SUPERVISOR_TREE_RSS_LIMIT'
            if elapsed>timeout:failure='HARNESS_TIMEOUT'
            if failure:
                for process in reversed(processes):
                    try:process.terminate()
                    except psutil.Error:pass
                break
            if elapsed-last_progress>=15:
                save(folder/'progress.json',{'state':'IMPORT_RUNNING','pid':child.pid,'elapsed_seconds':elapsed,'sampled_peak_tree_rss_bytes':peak,'sampled_current_tree_rss_bytes':current,'samples':samples});last_progress=elapsed
            if elapsed-last_notice>=60:
                print(json.dumps({'case':folder.name,'phase':'import','seconds':round(elapsed),'peak_rss_MiB':round(peak/1048576,2)}),flush=True);last_notice=elapsed
            time.sleep(.2)
        try:code=child.wait(timeout=20)
        except subprocess.TimeoutExpired:
            child.kill();code=child.wait()
    return {'exit_code':code,'elapsed_seconds':time.monotonic()-started,'sampled_peak_process_tree_rss_bytes':peak,'rss_budget_bytes':rss_budget,'sampling_seconds':.2,'samples':samples,'sampled_process_cpu_seconds_lower_bound':sum(cpu.values()),'supervisor_failure':failure,'sampling_scope':'Importer and observed native descendants only; SQL server and test verifier excluded; sampling can miss transient peaks'}

def verify(folder,manifest,credentials,destination):
    report=json.loads((folder/'stdout.json').read_text(encoding='utf-8'))
    assert report['exit_code']==3 and len(report['files'])==1,(report.get('state'),report.get('exit_code'))
    item=report['files'][0];plan=item['plan'];counts=item['counts']
    assert [c['sql_type'] for c in plan['columns']]==manifest['expected_types']
    assert [c['original_name'] for c in plan['columns']]==manifest['column_names']
    assert counts['committed']==manifest['accepted_rows'] and counts['rejected']==len(manifest['rejected_ordinals'])
    assert not any(counts[k] for k in ['pending','unresolved','excluded'])
    assert counts['logical_records']==manifest['logical_records'] and counts['physical_lines']==manifest['physical_lines']
    assert hash_file(item['artifact']['path'])==(manifest['source_bytes'],manifest['sha256'])
    db=sqlite3.connect(folder/'work/journal.sqlite3')
    header=json.loads(db.execute('SELECT payload FROM records WHERE ordinal=1').fetchone()[0])
    assert [f['text'] for f in header['fields']]==manifest['raw_header']
    rejects=list(db.execute("SELECT ordinal,payload FROM records WHERE disposition='rejected' ORDER BY ordinal"))
    assert [r[0] for r in rejects]==manifest['rejected_ordinals']
    assert all('ROW_WIDTH' in {e['code'] for e in json.loads(r[1])['errors']} for r in rejects)
    lineage=iter(db.execute("SELECT ordinal,record_id FROM records WHERE disposition='committed' ORDER BY ordinal"))
    connection=connect(destination,credentials,plan['database']);connection.timeout=1800
    table=quote(plan['schema'])+'.'+quote(plan['table']);cursor=connection.cursor()
    columns=','.join(quote(c['target_name']) for c in plan['columns'])
    started=time.monotonic()
    cursor.execute('SELECT [__ingest_ordinal],[__ingest_record],'+columns+' FROM '+table+' WHERE [__ingest_intent]=? ORDER BY [__ingest_ordinal]',item['intent_id'])
    execute_seconds=time.monotonic()-started
    number=0;verified_digest=hashlib.sha256()
    for index in range(manifest['rows']):
        if index in manifest['rejected_indexes']:continue
        row=cursor.fetchone();expected=ORACLE['expected'](index,manifest['rows'],manifest['padding'])
        assert row is not None and row[0]==index+2,('ordinal',index)
        assert (row[0],row[1])==next(lineage),('lineage',index)
        assert list(row[2:])==expected,('cell_mismatch',index)
        verified_digest.update(str(row[0]).encode('ascii'))
        for v in expected:
            text=str(v).encode('utf-8');verified_digest.update(len(text).to_bytes(8,'big'));verified_digest.update(text)
        number+=1
        if number%10000==0:save(folder/'verification-progress.json',{'state':'INDEPENDENT_SQL_READBACK','verified_rows':number,'elapsed_seconds':time.monotonic()-started})
    assert cursor.fetchone() is None and next(lineage,None) is None
    metadata=list(cursor.execute('SELECT t.name,c.max_length,c.precision,c.scale FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE c.object_id=OBJECT_ID(?) AND c.name NOT LIKE ? ORDER BY c.column_id',table,'__ingest[_]%'))
    assert [tuple(r) for r in metadata]==[('nvarchar',22,0,0),('nvarchar',22,0,0),('decimal',13,23,19),('nvarchar',-1,0,0)]
    connection.close();db.close()
    return {'state':'ALL_ACCEPTED_SQL_CELLS_TYPES_LINEAGE_AND_EXACT_ARCHIVE_VERIFIED','verified_rows':number,'independent_readback_seconds':time.monotonic()-started,'independent_sql_execute_seconds':execute_seconds,'independent_values_sha256':verified_digest.hexdigest(),'run_id':report['run_id'],'intent_id':item['intent_id'],'database':plan['database'],'counts':counts,'loader':report.get('execution_policy',{}),'sql_types':manifest['expected_types'],'raw_header_retained':True,'row_width_reject_ordinals':manifest['rejected_ordinals']}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--folder',required=True,type=Path);parser.add_argument('--interpreter',required=True,type=Path)
    parser.add_argument('--config',required=True,type=Path);parser.add_argument('--credentials-json',required=True,type=Path)
    parser.add_argument('--bcp-path',type=Path,required=True)
    parser.add_argument('--rows',default=131073,type=int);parser.add_argument('--padding',default=16384,type=int)
    parser.add_argument('--timeout-seconds',default=14400,type=int);parser.add_argument('--only',default='all')
    parser.add_argument('--verify-only',action='store_true',help='Read-only independent verification of existing exit3 completed cases; no new load or intent')
    args=parser.parse_args()
    labels={engine+'-'+str(rows) for engine,rows,_ in CASES}
    if args.only!='all' and not set(args.only.split(','))<=labels:parser.error('Unknown requested case')
    folder=args.folder.resolve();folder.mkdir(parents=True,exist_ok=True)
    destination=tomllib.loads(args.config.read_text(encoding='utf-8'))['destination']
    assert str(destination['server']).startswith('127.0.0.1,')
    credentials=json.loads(args.credentials_json.read_text(encoding='utf-8'));interpreter=args.interpreter.resolve()
    runtime=runtime_fingerprint(interpreter)
    runtime_path=folder/'runtime.json'
    if runtime_path.exists():
        assert json.loads(runtime_path.read_text(encoding='utf-8'))['fingerprint_sha256']==runtime['fingerprint_sha256'],'Changed runtime: use a new test folder, never relabel old completed cases'
    else:save(runtime_path,runtime)
    independent=folder/'source/independent-source.json'
    source=ORACLE['generate'](folder/'source',args.rows,args.padding) if not independent.exists() else json.loads(independent.read_text(encoding='utf-8'))
    assert source['source_bytes']>2*1024**3,'Gate source must exceed2GiB'
    assert hash_file(source['source'])==(source['source_bytes'],source['sha256'])
    environment={'started_utc':datetime.now(timezone.utc).isoformat(),'source':source,'interpreter':str(interpreter),'budgets':{'worker':134217728,'tree':268435456,'record_field':131072},'cases':[],'state':'MATRIX_RUNNING','runtime_fingerprint_sha256':runtime['fingerprint_sha256'],'oracle_sha256':hashlib.sha256((ROOT/'tests/oracles/multigb_csv.py').read_bytes()).hexdigest(),'controller_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    progress=folder/'matrix.json';save(progress,environment)
    for engine,rows,bytecap in CASES:
        label=engine+'-'+str(rows)
        if args.only!='all' and label not in args.only.split(','):continue
        case=folder/label;case.mkdir(exist_ok=True)
        final=case/'verified.json'
        if final.exists():
            completed=json.loads(final.read_text(encoding='utf-8'))
            assert completed.get('runtime_fingerprint_sha256')==runtime['fingerprint_sha256'],'Completed case belongs to another or unrecorded runtime; preserve it and use a fresh folder'
            if not args.verify_only:
                environment['cases'].append(completed)
                continue
        database='DbIngestTest_MultiGB_'+folder.name[-12:].replace('-','_')+'_'+label.replace('-','_')
        settings={'destination':{**destination,'database':database,'control_database':'DbIngestTest_MultiGB_Control','statement_timeout_seconds':1800},'source':{'root':str(Path(source['source']).parent),'path':source['source'],'include':['source.csv'],'format':'delimited'},'parsing':{'encoding':'utf-8','delimiter':',','header':'present','column_names':source['column_names'],'sample_bytes':1024,'sample_records':1},'preservation':{'work_directory':str(case/'work'),'minimum_free_disk_bytes':10*1024**3},'load':{'engine':engine,'mode':'append','batch_rows':rows,'batch_bytes':bytecap,'fast_executemany':True},'parallel':{'enabled':False},'limits':{'worker_memory_bytes':134217728,'total_process_tree_memory_bytes':268435456,'max_in_memory_record_bytes':131072,'max_in_memory_field_bytes':131072},'interaction':{'non_interactive':True},'tools':{'bcp_path':str(args.bcp_path.resolve())},'credentials':{'allow_password_in_child_args':True}}
        if not args.verify_only and ((case/'execution.json').exists() or (case/'stdout.json').exists()):raise RuntimeError('Existing unverified case retained: use --verify-only for completed exit3, or resume its original intent; never blind new-intent replay')
        config=case/'ingest.toml'
        if not args.verify_only:config.write_text('\n'.join('['+section+']\n'+'\n'.join(k+' = '+lit(v) for k,v in values.items()) for section,values in settings.items())+'\n',encoding='utf-8')
        command=[str(interpreter),'-I','-m','db_ingest','--config',str(config),'run','--new-intent','--no-parallel','--non-interactive','--json']
        if args.verify_only:
            timing=json.loads((case/'execution.json').read_text(encoding='utf-8'))
            assert timing['exit_code']==3 and not timing.get('supervisor_failure'),'Only completed expected-partial imports can be independently verified'
        else:
            print(json.dumps({'case':label,'state':'STARTING','source_bytes':source['source_bytes'],'rows':source['rows'],'batch_bytes':bytecap}),flush=True)
            timing=supervise(command,case,credentials,args.timeout_seconds,268435456);save(case/'execution.json',timing)
        if timing['supervisor_failure'] or timing['exit_code']!=3:
            environment.update(state='CASE_FAILED',failed_case=label,execution=timing);save(progress,environment);return 4
        verification=verify(case,source,credentials,destination)
        result={'case':label,'engine':engine,'batch_rows':rows,'batch_bytes':bytecap,'source_bytes':source['source_bytes'],'timing':timing,'verification':verification,'runtime_fingerprint_sha256':runtime['fingerprint_sha256'],'scope':'Actual multi-GB ordinary CSV with3known wrong-width rejects, incomplete raw header and final-row inferred type/length changes; exit3 expected partialpublication'}
        save(final,result);environment['cases'].append(result);save(progress,environment)
        print(json.dumps({'case':label,'state':'VERIFIED','seconds':round(timing['elapsed_seconds'],2),'peak_rss_MiB':round(timing['sampled_peak_process_tree_rss_bytes']/1048576,2)}),flush=True)
    environment['state']='REQUESTED_CASES_VERIFIED';save(progress,environment)
    return 0

if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception as error:
        # Exception messages intentionally exclude configs/credentials/source values.
        print(json.dumps({'state':'HARNESS_ASSERTION_OR_INFRASTRUCTURE_FAILURE','type':type(error).__name__,'message':str(error)[:500]}),flush=True);raise
