"""Actual log-capacity failure in a fresh owned disposable database, then same-intent recovery."""
import base64,hashlib,json,os,subprocess,sys,tomllib,uuid
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG or os.environ.get("DB_INGEST_TEST_CAPACITY")!="1",reason="Explicit fresh disposable database log-capacity gate required")

def lit(v):
    if isinstance(v,bool):return "true" if v else "false"
    if isinstance(v,int):return str(v)
    if isinstance(v,list):return "["+",".join(lit(i) for i in v)+"]"
    return json.dumps(str(v))

def q(v):return "["+v.replace("]","]]")+"]"

def test_real_log9002_preserves_pending_then_recovers_same_intent(tmp_path):
    config=load_config(CONFIG,environ={});d=config["destination"]
    assert all(d[k].lower().startswith("dbingesttest_") for k in ("database","control_database"))
    database="DbIngestTest_Capacity_"+uuid.uuid4().hex
    user,password=os.environ.get("DB_USERNAME"),os.environ.get("DB_PASSWORD")
    admin=connect(config,user,password,"master",autocommit=True);cur=admin.cursor()
    cur.execute("CREATE DATABASE "+q(database))
    cur.execute("ALTER DATABASE "+q(database)+" SET RECOVERY SIMPLE")
    files=cur.execute("SELECT name,type_desc,size FROM "+q(database)+".sys.database_files").fetchall()
    for name,kind,pages in files:
        safe=name.replace("'","''")
        if kind=="ROWS":cur.execute("ALTER DATABASE "+q(database)+" MODIFY FILE(NAME=N'"+safe+"', SIZE=64MB, MAXSIZE=64MB, FILEGROWTH=0)")
        elif kind=="LOG":
            assert pages*8192<=16*1024*1024,"Fixture needs a small initial model log; do not shrink shared or existing logs"
            cur.execute("ALTER DATABASE "+q(database)+" MODIFY FILE(NAME=N'"+safe+"', FILEGROWTH=0)")
            cur.execute("ALTER DATABASE "+q(database)+" MODIFY FILE(NAME=N'"+safe+"', MAXSIZE="+str((pages*8192+1048575)//1048576)+"MB, FILEGROWTH=0)")
            log_name=safe
    source=tmp_path/("capacity_"+uuid.uuid4().hex+".csv")
    payload=bytes(range(256))*256
    count=400 #25MiB of transactional target inserts exceeds the fresh <=16MiB log.
    with source.open("wb") as out:
        out.write(b"id,payload\r\n")
        encoded=base64.b64encode(payload)
        for i in range(count):out.write(str(i).encode()+b","+encoded+b"\r\n")
    source_hash=hashlib.sha256(source.read_bytes()).hexdigest()
    sections={"destination":{**d,"database":database},"source":{"root":str(tmp_path),"path":str(source)},"parsing":{"encoding":"utf-8","delimiter":",","header":"present"},"load":{"engine":"odbc","fast_executemany":False,"batch_rows":8,"batch_bytes":1048576},"parallel":{"enabled":False},"preservation":{"work_directory":str(tmp_path/"work"),"minimum_free_disk_bytes":1}}
    text="\n".join("["+k+"]\n"+"\n".join(a+"="+lit(b) for a,b in v.items()) for k,v in sections.items())
    text+='\n[[file_rules]]\nglob='+lit(source.name)+'\n[[file_rules.columns]]\nsource_ordinal=1\ntarget_name="id"\ntarget_type="int"\n[[file_rules.columns]]\nsource_ordinal=2\ntarget_name="payload"\ntarget_type="varbinary(max)"\nbinary_encoding="base64"\n'
    path=tmp_path/"capacity.toml";path.write_text(text,encoding="utf-8")
    root=Path(__file__).resolve().parents[2]
    env={k:v for k,v in os.environ.items() if not k.startswith("DB_") or k in ("DB_USERNAME","DB_PASSWORD")}
    def cli(command,*args,expected):
        p=subprocess.run([sys.executable,"-m","db_ingest","--config",str(path),command,*args,"--non-interactive","--json"],cwd=root,env=env,capture_output=True,text=True,timeout=240)
        output=p.stdout+p.stderr
        if password:output=output.replace(password,"<redacted>")
        (tmp_path/(command+".json")).write_text(output,encoding="utf-8")
        assert p.returncode==expected,{"expected":expected,"observed":p.returncode,"retained":str(tmp_path/(command+".json"))}
        return json.loads(p.stdout or p.stderr)
    try:
        failed=cli("run",expected=4)
        item=failed["files"][0];intent=item["intent_id"]
        assert any(e["code"]=="LOG_CAPACITY" and e["context"].get("native_code")==9002 for e in failed["errors"])
        assert item["counts"]["pending"]==count and item["counts"]["committed"]==item["counts"]["rejected"]==0
        assert hashlib.sha256(Path(item["artifact"]["path"]).read_bytes()).hexdigest()==source_hash
        # Repair only this deliberately restricted fixture log; no shared/server limit changes.
        cur.execute("ALTER DATABASE "+q(database)+" MODIFY FILE(NAME=N'"+log_name+"', MAXSIZE=128MB, FILEGROWTH=16MB)")
        recovered=cli("resume","--run-id",failed["run_id"],expected=0)
        after=recovered["files"][0]
        assert after["intent_id"]==intent and after["counts"]["committed"]==count and after["counts"]["pending"]==0
        conn=connect(config,user,password,database,autocommit=True)
        try:
            plan=after["plan"]
            rows=conn.cursor().execute("SELECT id,payload,__ingest_ordinal FROM "+q(plan["schema"])+"."+q(plan["table"])+" ORDER BY __ingest_ordinal")
            seen=0
            for number,value,ordinal in rows:
                assert number==seen and value==payload and ordinal==seen+2
                seen+=1
            assert seen==count
            assert conn.cursor().execute("SELECT COUNT(*) FROM audit.__db_ingest_receipts WHERE intent_id=?",intent).fetchone()[0]==1
        finally:conn.close()
        evidence={"state":"PASSED","database":database,"native_code":9002,"same_intent":intent,"rows":count,"source_sha256":source_hash,"scope":"Only this newly created DB log was restricted and repaired; no shared/server/recovery benchmark settings changed.","artifacts":str(tmp_path)}
        (tmp_path/"real-log-capacity.json").write_text(json.dumps(evidence,indent=2),encoding="utf-8")
    finally:admin.close()

