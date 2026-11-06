"""Explicit sidecar BLOB imports and replay; independent actual SQL byte oracle."""
import json,os,subprocess,sys,tomllib,uuid
from pathlib import Path
import pytest

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Explicit disposable SQL test configuration required")

def literal(value):
    if isinstance(value,bool):return "true" if value else "false"
    if isinstance(value,(int,float)):return str(value)
    if isinstance(value,list):return "["+",".join(literal(x) for x in value)+"]"
    return json.dumps(str(value))

def q(value):return "["+value.replace("]","]]")+"]"

@pytest.mark.parametrize("engine",["odbc","bcp"])
def test_real_attachment_bytes_survive_original_directory_removal(tmp_path,engine):
    destination=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8"))["destination"]
    assert all(destination[k].lower().startswith("dbingesttest_") for k in ("database","control_database"))
    root=tmp_path/"source";root.mkdir();sidecars=tmp_path/"sidecars";sidecars.mkdir()
    original=bytes(range(256))*3
    (sidecars/"all.bin").write_bytes(original);(sidecars/"empty.bin").write_bytes(b"")
    source=root/("attachments_"+uuid.uuid4().hex+".csv")
    raw=b"id,payload\r\n001,all.bin\r\n002,empty.bin\r\n";source.write_bytes(raw)
    work=tmp_path/"work"
    native=Path(__file__).resolve().parents[2]/".tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/bcp.exe"
    if engine=="bcp" and not native.is_file():pytest.skip("Native TLS-capable bcp tool unavailable")
    sections={"destination":destination,"source":{"root":str(root),"path":str(source),"include":["**/*.csv"]},"parsing":{"header":"present","encoding":"utf-8","delimiter":","},"load":{"engine":engine,"batch_rows":1,"batch_bytes":65536},"preservation":{"work_directory":str(work),"minimum_free_disk_bytes":1},"parallel":{"enabled":False},"interaction":{"non_interactive":True},"credentials":{"allow_password_in_child_args":True}}
    if engine=="bcp":sections["tools"]={"bcp_path":str(native)}
    text="\n".join("["+section+"]\n"+"\n".join(k+" = "+literal(v) for k,v in values.items()) for section,values in sections.items())
    text+='\n[[file_rules]]\nglob="**/*.csv"\n[[file_rules.columns]]\nsource_ordinal=1\ntarget_name="id"\ntarget_type="nvarchar(3)"\n[[file_rules.columns]]\nsource_ordinal=2\ntarget_name="payload"\ntarget_type="varbinary(max)"\nbinary_encoding="attachment_path"\nattachment_root='+literal(str(sidecars))+'\n'
    config=tmp_path/"attachments.toml";config.write_text(text,encoding="utf-8")
    project=Path(__file__).resolve().parents[2]
    env={k:v for k,v in os.environ.items() if not k.startswith("DB_") or k in ("DB_USERNAME","DB_PASSWORD")}
    def cli(command,*args):
        p=subprocess.run([sys.executable,"-m","db_ingest","--config",str(config),command,*args,"--json"],cwd=project,env=env,capture_output=True,text=True,timeout=120)
        assert p.returncode==0,(p.stdout,p.stderr)
        secret=env.get("DB_PASSWORD")
        if secret:assert secret not in p.stdout+p.stderr
        return json.loads(p.stdout)
    first=cli("run")
    for sidecar in sidecars.iterdir():sidecar.unlink()
    sidecars.rmdir()
    cli("reconcile","--run-id",first["run_id"])
    replay=cli("reimport","--run-id",first["run_id"])
    assert replay["files"][0]["artifact"]["capture_id"]==first["files"][0]["artifact"]["capture_id"]
    import pyodbc
    def escape(v):return "{"+str(v).replace("}","}}")+"}"
    props={"DRIVER":destination.get("driver","ODBC Driver 18 for SQL Server"),"SERVER":destination["server"],"DATABASE":destination["database"],"Encrypt":destination.get("encrypt","yes"),"TrustServerCertificate":destination.get("trust_server_certificate","no"),"UID":env.get("DB_USERNAME"),"PWD":env.get("DB_PASSWORD")}
    conn=pyodbc.connect(";".join(k+"="+escape(v) for k,v in props.items())+";",autocommit=True,timeout=15)
    try:
        plan=first["files"][0]["plan"]
        rows=conn.cursor().execute("SELECT id,payload FROM "+q(plan["schema"])+"."+q(plan["table"])+" ORDER BY id").fetchall()
        assert [tuple(r) for r in rows]==[("001",original),("001",original),("002",b""),("002",b"")]
    finally:conn.close()
    assert Path(first["files"][0]["artifact"]["path"]).read_bytes()==raw
