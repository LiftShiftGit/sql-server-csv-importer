"""Independent SQL values for column policies and exact temporal inference."""
import csv,json,os,subprocess,sys,tomllib,uuid
from decimal import Decimal
from pathlib import Path
import pytest

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Disposable SQL configuration required")

def lit(value):
    if isinstance(value,bool):return "true" if value else "false"
    if isinstance(value,int):return str(value)
    if isinstance(value,list):return "["+",".join(lit(v) for v in value)+"]"
    return json.dumps(str(value))

def q(value):return "["+value.replace("]","]]")+"]"

@pytest.mark.parametrize("engine",["odbc","bcp"])
def test_explicit_column_policies_and_temporal_inference(tmp_path,engine):
    d=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8"))["destination"]
    assert all(d[k].lower().startswith("dbingesttest_") for k in ("database","control_database"))
    source=tmp_path/("controls_"+uuid.uuid4().hex+".csv")
    rows=[
      ["NULL","nil","29/02/2024T12:13:14.1234567+05:30","1.23e2","12:13:14.1234567","2024-02-29T12:13:14.1234567","2024-02-29T12:13:14.1234567-03:30"],
      ["","literal","01/01/0001T00:00:00+00:00","-0e999999","00:00:00","0001-01-01T00:00:00","2024-03-01T00:00:00Z"],
    ]
    with source.open("w",encoding="utf-8",newline="") as out:
        writer=csv.writer(out);writer.writerow(["token","nullable","calendar","amount","clock","naive","offset"]);writer.writerows(rows)
    root=Path(__file__).resolve().parents[2]
    sections={"destination":d,"source":{"root":str(tmp_path),"path":str(source)},"parsing":{"encoding":"utf-8","delimiter":",","header":"present","null_tokens":["NULL"]},"load":{"engine":engine,"batch_rows":1,"batch_bytes":65536},"parallel":{"enabled":False},"preservation":{"work_directory":str(tmp_path/"work"),"minimum_free_disk_bytes":1},"credentials":{"allow_password_in_child_args":True}}
    if engine=="bcp":sections["tools"]={"bcp_path":str(root/".tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/bcp.exe")}
    text="\n".join("["+k+"]\n"+"\n".join(a+"="+lit(b) for a,b in v.items()) for k,v in sections.items())
    columns=[{"source_ordinal":1,"target_name":"token","target_type":"nvarchar","length":10,"null_tokens":[]},{"source_ordinal":2,"target_name":"nullable","target_type":"nvarchar(10)","null_tokens":["nil"]},{"source_ordinal":3,"target_name":"calendar","target_type":"datetimeoffset(7)","date_format":"%d/%m/%Y"},{"source_ordinal":4,"target_name":"amount","target_type":"decimal","precision":5,"scale":2},{"source_ordinal":5,"target_name":"clock"},{"source_ordinal":6,"target_name":"naive"},{"source_ordinal":7,"target_name":"offset"}]
    text+='\n[[file_rules]]\nglob='+lit(source.name)+'\n'
    for c in columns:text+='\n[[file_rules.columns]]\n'+"\n".join(k+"="+lit(v) for k,v in c.items())+'\n'
    path=tmp_path/"ingest.toml";path.write_text(text,encoding="utf-8")
    env={k:v for k,v in os.environ.items() if not k.startswith("DB_") or k in ("DB_USERNAME","DB_PASSWORD")}
    result=subprocess.run([sys.executable,"-m","db_ingest","--config",str(path),"run","--non-interactive","--json"],cwd=root,env=env,capture_output=True,text=True,timeout=120)
    output=result.stdout+result.stderr
    if env.get("DB_PASSWORD"):output=output.replace(env["DB_PASSWORD"],"<redacted>")
    assert result.returncode==0,output
    report=json.loads(result.stdout);plan=report["files"][0]["plan"]
    assert [c["sql_type"] for c in plan["columns"]][-3:]==["time(7)","datetime2(7)","datetimeoffset(7)"]
    import pyodbc
    brace=lambda v:"{"+str(v).replace("}","}}")+"}"
    props={"DRIVER":d.get("driver","ODBC Driver 18 for SQL Server"),"SERVER":d["server"],"DATABASE":d["database"],"Encrypt":d.get("encrypt","yes"),"TrustServerCertificate":d.get("trust_server_certificate","no"),"UID":env.get("DB_USERNAME"),"PWD":env.get("DB_PASSWORD")}
    conn=pyodbc.connect(";".join(k+"="+brace(v) for k,v in props.items())+";",autocommit=True,timeout=15)
    try:
        expressions=["token","nullable","CONVERT(nvarchar(64),calendar,126)","amount","CONVERT(nvarchar(64),clock,126)","CONVERT(nvarchar(64),naive,126)","CONVERT(nvarchar(64),[offset],126)"]
        actual=[tuple(r) for r in conn.cursor().execute("SELECT "+",".join(expressions)+" FROM "+q(plan["schema"])+"."+q(plan["table"])+" ORDER BY __ingest_ordinal")]
        # SQL style126 omits an all-zero fractional part; nonzero seven digits remain exact.
        assert actual==[("NULL",None,"2024-02-29T12:13:14.1234567+05:30",Decimal("123.00"),"12:13:14.1234567","2024-02-29T12:13:14.1234567","2024-02-29T12:13:14.1234567-03:30"),("","literal","0001-01-01T00:00:00+00:00",Decimal("0.00"),"00:00:00","0001-01-01T00:00:00","2024-03-01T00:00:00+00:00")]
    finally:conn.close()
