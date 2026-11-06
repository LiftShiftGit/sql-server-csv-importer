"""Explicit lossy projections never claim lossless success; originals remain exportable."""
import json,os,subprocess,sys,tomllib,uuid
from decimal import Decimal
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect

ROOT=Path(__file__).resolve().parents[2]
CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Disposable SQL configuration required")

def lit(v):
    if isinstance(v,bool):return "true" if v else "false"
    if isinstance(v,int):return str(v)
    return json.dumps(str(v))

def q(v):return "["+v.replace("]","]]")+"]"

@pytest.mark.parametrize("engine",["odbc","bcp"])
def test_recorded_text_binary_and_decimal_projection_is_partial(engine,tmp_path):
    d=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8"))["destination"]
    assert all(d[k].lower().startswith("dbingesttest_") for k in ("database","control_database"))
    source=tmp_path/("lossy_"+uuid.uuid4().hex+".csv")
    raw=('id,utext,u8,payload,n,marker\r\n001,A😀B,A😀B,AAECAw==,1.235,NULL\r\n002,short,short,//79/A==,1.245,"NULL"\r\n003,,,"",2.000,""\r\n').encode("utf-8")
    source.write_bytes(raw)
    sections={"destination":d,"source":{"root":str(tmp_path),"path":str(source)},"parsing":{"encoding":"utf-8","delimiter":",","header":"present"},"schema":{"allow_lossy_projection":True},"load":{"engine":engine,"batch_rows":1,"batch_bytes":65536},"parallel":{"enabled":False},"preservation":{"work_directory":str(tmp_path/"work"),"minimum_free_disk_bytes":1},"credentials":{"allow_password_in_child_args":True}}
    if engine=="bcp":sections["tools"]={"bcp_path":str(ROOT/".tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/bcp.exe")}
    text="\n".join("["+k+"]\n"+"\n".join(a+"="+lit(b) for a,b in v.items()) for k,v in sections.items())
    text=text.replace('header="present"','header="present"\nnull_tokens=["NULL"]')
    columns=[{"target_name":"id","target_type":"nvarchar(3)"},{"target_name":"utext","target_type":"nvarchar(3)","truncation":"right"},{"target_name":"u8","target_type":"varchar(5)","collation":"Latin1_General_100_BIN2_UTF8","text_encoding":"utf-8","truncation":"right"},{"target_name":"payload","target_type":"varbinary(2)","binary_encoding":"base64","truncation":"right"},{"target_name":"n","target_type":"decimal(5,2)","rounding":"half_even"},{"target_name":"marker","target_type":"nvarchar(2)","truncation":"right"}]
    text+='\n[[file_rules]]\nglob='+lit(source.name)+'\n'
    for i,c in enumerate(columns,1):text+='\n[[file_rules.columns]]\nsource_ordinal='+str(i)+'\n'+"\n".join(k+"="+lit(v) for k,v in c.items())+'\n'
    path=tmp_path/"lossy.toml";path.write_text(text,encoding="utf-8")
    env={k:v for k,v in os.environ.items() if not k.startswith("DB_") or k in ("DB_USERNAME","DB_PASSWORD")}
    def cli(command,*args,expected=3):
        p=subprocess.run([sys.executable,"-m","db_ingest","--config",str(path),command,*args,"--json"],cwd=ROOT,env=env,capture_output=True,text=True,timeout=120)
        output=p.stdout+p.stderr
        if env.get("DB_PASSWORD"):output=output.replace(env["DB_PASSWORD"],"<redacted>")
        assert p.returncode==expected,output
        return json.loads(p.stdout or p.stderr)
    report=cli("run","--non-interactive")
    item=report["files"][0];receipt=item["receipt"];plan=item["plan"]
    assert report["state"]=="PARTIAL" and receipt["assurance"]=="lossy"
    assert receipt["details"]["lossy_projection"]["applied_cells"]==8
    assert receipt["details"]["lossy_projection"]["affected_records"]==2
    assert item["counts"]["committed"]==3 and item["counts"]["rejected"]==item["counts"]["pending"]==0
    assert Path(item["artifact"]["path"]).read_bytes()==raw
    config=load_config(path,environ={})
    conn=connect(config,env["DB_USERNAME"],env["DB_PASSWORD"],plan["database"],autocommit=True)
    try:
        actual=[tuple(r) for r in conn.cursor().execute("SELECT id,utext,CONVERT(nvarchar(max),u8),CONVERT(varbinary(max),u8),payload,n,marker FROM "+q(plan["schema"])+"."+q(plan["table"])+" ORDER BY __ingest_ordinal")]
        assert actual==[("001","A😀","A😀","A😀".encode("utf-8"),b"\x00\x01",Decimal("1.24"),None),("002","sho","short",b"short",b"\xff\xfe",Decimal("1.24"),"NU"),("003","","",b"",b"",Decimal("2.00"),"")]
    finally:conn.close()
    cli("resume","--run-id",report["run_id"],"--non-interactive")
    cli("reconcile","--run-id",report["run_id"],"--non-interactive")
    output=tmp_path/"projection-events.jsonl"
    exported=cli("export-projections","--run-id",report["run_id"],"--output",str(output),expected=0)
    events=[json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert exported["cells"]==len(events)==8
    assert any(e["original_field"]=={"quoted":True,"state":"VALUE","text":"NULL"} and e["projected_field"]["text"]=="NU" for e in events)
    assert any(e["original_field"]["text"]=="A😀B" and e["projected_field"]["text"]=="A😀" for e in events)
    assert all(e["source_artifact"]==item["artifact"]["path"] for e in events)
