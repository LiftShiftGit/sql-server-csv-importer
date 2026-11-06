"""Measured end-to-end comparison with independently authored values for every row."""
import base64,csv,hashlib,json,os,subprocess,sys,time,tomllib,uuid
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[2]
CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG or os.environ.get("DB_INGEST_TEST_BENCHMARK")!="1",reason="Explicit disposable benchmark gate required")

def lit(v):
    if isinstance(v,bool):return "true" if v else "false"
    if isinstance(v,int):return str(v)
    if isinstance(v,list):return "["+",".join(lit(i) for i in v)+"]"
    return json.dumps(str(v))

def q(v):return "["+v.replace("]","]]")+"]"
def brace(v):return "{"+str(v).replace("}","}}")+"}"

def fixture(workload):
    if workload=="wide":
        types=["int"]*32+["nvarchar(64)"]*32
        rows=[[i+j for j in range(32)]+["column "+str(j)+" Ω "+str(i)+"  " for j in range(32)] for i in range(120)]
    elif workload=="blob":
        types=["int","varbinary(max)","nvarchar(16)"]
        rows=[[i,bytes(range(256))*(4096 if i==80 else 16),"binary "+str(i)] for i in range(160)]
    else:
        types=["int","nvarchar(128)","nvarchar(128)"]
        text="plain ASCII" if workload=="narrow" else "漢字 e\u0301 😀 trailing  " if workload=="unicode" else "quoted,field\r\nsecond line\nthird\rpart"
        rows=[[i,text,"value "+str(i)] for i in range(500)]
    return types,rows


def test_real_transport_and_parallel_end_to_end_comparison():
    d=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8"))["destination"]
    assert all(d[k].lower().startswith("dbingesttest_") for k in ("database","control_database"))
    import pyodbc
    props={"DRIVER":d.get("driver","ODBC Driver 18 for SQL Server"),"SERVER":d["server"],"DATABASE":d["database"],"Encrypt":d.get("encrypt","yes"),"TrustServerCertificate":d.get("trust_server_certificate","no"),"UID":os.environ["DB_USERNAME"],"PWD":os.environ["DB_PASSWORD"]}
    conn=pyodbc.connect(";".join(k+"="+brace(v) for k,v in props.items())+";",autocommit=True,timeout=15)
    version=conn.cursor().execute("SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion'))").fetchone()[0]
    evidence={"engine_version":version,"client":"Windows Python "+sys.version.split()[0],"cases":[],"scope":"Elapsed includes child startup, capture, parsing, profiling, spool, load, full staging/destination verification and audit; sampled client RSS is not a hard allocation guarantee.","cpu_seconds":"not collected","server_log_bytes":"not collected","cache_policy":"No cache flush or server recovery/config changes; repeated cases can benefit from warm caches."}
    folder=ROOT/".integration"/("benchmark-"+uuid.uuid4().hex);folder.mkdir()
    engines=["odbc","fast","bcp"]
    env={k:v for k,v in os.environ.items() if not k.startswith("DB_") or k in ("DB_USERNAME","DB_PASSWORD")}
    try:
        for workload in ("narrow","wide","unicode","multiline","blob"):
            types,expected=fixture(workload)
            for engine in engines:
                for workers in (1,2):
                    case=folder/(workload+"-"+engine+"-"+str(workers));case.mkdir();source=case/"source";source.mkdir()
                    source_hashes={}
                    for number in (1,2):
                        path=source/(workload+"_"+engine+"_"+str(workers)+"_"+folder.name[-32:]+"_part"+str(number)+".csv")
                        with path.open("w",encoding="utf-8",newline="") as out:
                            writer=csv.writer(out);writer.writerow(["c"+str(i) for i in range(len(types))])
                            for row in expected:writer.writerow([base64.b64encode(v).decode("ascii") if isinstance(v,bytes) else v for v in row])
                        source_hashes[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
                    sections={"destination":d,"source":{"root":str(source),"path":str(source)},"parsing":{"encoding":"utf-8","delimiter":",","header":"present"},"load":{"engine":"odbc" if engine=="fast" else engine,"fast_executemany":engine=="fast","batch_rows":500,"batch_bytes":2097152},"parallel":{"enabled":workers>1,"max_workers":workers},"preservation":{"work_directory":str(case/"work"),"minimum_free_disk_bytes":1},"credentials":{"allow_password_in_child_args":True},"limits":{"total_process_tree_memory_bytes":512*1024*1024,"worker_memory_bytes":160*1024*1024}}
                    if engine=="bcp":sections["tools"]={"bcp_path":str(ROOT/".tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/bcp.exe")}
                    text="\n".join("["+k+"]\n"+"\n".join(a+"="+lit(b) for a,b in v.items()) for k,v in sections.items())
                    text+='\n[[file_rules]]\nglob="*.csv"\n'
                    for i,kind in enumerate(types,1):
                        text+='\n[[file_rules.columns]]\nsource_ordinal='+str(i)+'\ntarget_name='+lit("c"+str(i-1))+'\ntarget_type='+lit(kind)+'\n'
                        if kind.startswith("varbinary"):text+='binary_encoding="base64"\n'
                    config=case/"ingest.toml";config.write_text(text,encoding="utf-8")
                    started=time.perf_counter()
                    p=subprocess.run([sys.executable,"-m","db_ingest","--config",str(config),"run","--non-interactive","--new-intent","--json"],cwd=ROOT,env=env,capture_output=True,text=True,timeout=180,shell=False)
                    elapsed=time.perf_counter()-started
                    output=p.stdout+p.stderr
                    if env.get("DB_PASSWORD"):output=output.replace(env["DB_PASSWORD"],"<redacted>")
                    (case/"output.json").write_text(output,encoding="utf-8")
                    assert p.returncode==0,{"workload":workload,"engine":engine,"workers":workers,"output":str(case/"output.json")}
                    report=json.loads(p.stdout)
                    assert len(report["files"])==2
                    for item in report["files"]:
                        assert hashlib.sha256(Path(item["artifact"]["path"]).read_bytes()).hexdigest()==source_hashes[Path(item["artifact"]["relative_path"]).name]
                        plan=item["plan"]
                        columns=",".join(q("c"+str(i)) for i in range(len(types)))
                        actual=[list(r) for r in conn.cursor().execute("SELECT "+columns+" FROM "+q(plan["schema"])+"."+q(plan["table"])+" WHERE __ingest_intent=? ORDER BY __ingest_ordinal",item["intent_id"])]
                        assert actual==expected
                        assert item["counts"]["committed"]==len(expected) and item["counts"]["pending"]==0
                        assert item["receipt"]["details"]["transport"]["engine"]==("odbc" if engine=="fast" else engine)
                    source_bytes=sum(p.stat().st_size for p in source.iterdir())
                    artifact_bytes=sum(p.stat().st_size for p in (case/"work").rglob("*") if p.is_file())
                    item={"workload":workload,"loader":engine,"workers":workers,"rows":2*len(expected),"source_bytes":source_bytes,"elapsed_seconds":elapsed,"rows_per_second":2*len(expected)/elapsed,"source_mib_per_second":source_bytes/(1024*1024)/elapsed,"retained_work_bytes":artifact_bytes,"execution_policy":report["execution_policy"],"staging_batches":[f["receipt"]["details"]["staging_batches"] for f in report["files"]],"oracle":"all cells + exact capture SHA256 + source dispositions + receipt engine"}
                    evidence["cases"].append(item)
                    (folder/("transport-benchmark-"+version.split(".")[0]+".json")).write_text(json.dumps(evidence,indent=2),encoding="utf-8")
    finally:conn.close()
    assert len(evidence["cases"])==30
