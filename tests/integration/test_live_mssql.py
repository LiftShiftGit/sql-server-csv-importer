"""Opt-in real ODBC/CLI vertical slice; no endpoint is discovered implicitly."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib
import uuid
import pytest

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="SQL Server integration not executed: set DB_INGEST_TEST_CONFIG to an explicitly disposable configuration")

def _literal(value):
    if isinstance(value,bool):return "true" if value else "false"
    if isinstance(value,(int,float)):return str(value)
    if isinstance(value,list):return "["+",".join(_literal(x) for x in value)+"]"
    return json.dumps(str(value),ensure_ascii=True)

def _q(name):return "["+name.replace("]", "]]")+"]"
def _odbc(value):return "{"+str(value).replace("}","}}")+"}"

@pytest.mark.parametrize("crash_after_commit",[False,True],ids=["normal","hard-crash-after-commit"])
def test_real_cli_typed_duplicate_rows_and_receipt_recovery(tmp_path,crash_after_commit):
    # Both databases must be explicitly disposable before any connection or CLI invocation.
    base=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8-sig"))
    d=base.get("destination",{})
    for name in ("database","control_database"):
        assert str(d.get(name,"")).casefold().startswith("dbingesttest_"),f"Refusing integration setup: destination.{name} must start with DbIngestTest_"
    assert d.get("driver","ODBC Driver 18 for SQL Server")=="ODBC Driver 18 for SQL Server"
    pyodbc=pytest.importorskip("pyodbc",reason="Declared SQL runtime dependency unavailable")
    assert "ODBC Driver 18 for SQL Server" in pyodbc.drivers(),"Install the selected ODBC Driver 18; other installed drivers are not substituted"
    integrated=d.get("authentication","sql")=="integrated"
    assert integrated or (os.environ.get("DB_USERNAME") and os.environ.get("DB_PASSWORD") is not None),"Supply DB_USERNAME and DB_PASSWORD for SQL authentication"
    source=tmp_path/("smoke_"+uuid.uuid4().hex+".csv")
    # Authored bytes and expectations do not call the importer parser/converter.
    row='0001,123456789012345678.1234,2026-10-29T01:02:03.1234567-04:00,AAEC/w==,"é🙂  ","quoted, value"\r\n'
    original=('id,amount,moment,payload,note,note\r\n'+row+row).encode("utf-8")
    source.write_bytes(original)
    config=tmp_path/"integration.toml"
    sections={"destination":d,"source":{"root":str(tmp_path),"path":str(source),"include":[source.name],"format":"delimited"},"parsing":{"encoding":"utf-8","delimiter":",","quotechar":'"',"header":"present"},"preservation":{"work_directory":str(tmp_path/"work"),"minimum_free_disk_bytes":1},"load":{"engine":"odbc","mode":"append","fast_executemany":False},"parallel":{"enabled":False},"interaction":{"non_interactive":True}}
    # Preserve explicit test-server TLS/auth settings; unknown config keys are not accepted.
    text="\n".join("["+section+"]\n"+"\n".join(k+" = "+_literal(v) for k,v in values.items()) for section,values in sections.items())
    text+='\n[[file_rules]]\nglob = '+_literal(source.name)+'\n'
    specs=[("nvarchar(16)",None),("decimal(22,4)",None),("datetimeoffset(7)",None),("varbinary(max)","base64"),("nvarchar(64)",None),("nvarchar(64)",None)]
    for ordinal,(sql_type,binary) in enumerate(specs,1):
        text+='\n[[file_rules.columns]]\nsource_ordinal = '+str(ordinal)+'\ntarget_type = '+_literal(sql_type)+'\n'
        if binary:text+='binary_encoding = '+_literal(binary)+'\n'
    config.write_text(text,encoding="utf-8")
    project=Path(__file__).resolve().parents[2]
    def cli(command,*args,extra_env=None,expected_code=0):
        child_env={k:v for k,v in os.environ.items() if not k.startswith("DB_") or k in ("DB_USERNAME","DB_PASSWORD")}
        child_env.update(extra_env or {})
        completed=subprocess.run([sys.executable,"-m","db_ingest","--config",str(config),command,*args,"--json"],cwd=project,env=child_env,capture_output=True,text=True,timeout=240,shell=False)
        secret=os.environ.get("DB_PASSWORD")
        if secret:assert secret not in completed.stdout+completed.stderr,"Credential appeared in CLI output"
        assert completed.returncode==expected_code,(completed.stdout,completed.stderr)
        return json.loads(completed.stdout) if completed.stdout else None
    if crash_after_commit:
        cli("run","--source",str(source),"--no-parallel","--non-interactive",extra_env={"DB_INGEST_TEST_MODE":"1","DB_INGEST_TEST_FAULT":"AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT"},expected_code=86)
        crash_status=cli("status")
        assert len(crash_status["runs"])==1
        run=cli("resume","--run-id",crash_status["runs"][0]["run_id"],"--no-parallel","--non-interactive")
    else:
        run=cli("run","--source",str(source),"--no-parallel","--non-interactive")
    assert run["state"]=="CLEAN_VERIFIED"
    file=run["files"][0];receipt=file["receipt"];plan=file["plan"]
    artifact=file["artifact"]
    assert Path(artifact["path"]).read_bytes()==original
    assert artifact["sha256"]==hashlib.sha256(original).hexdigest()
    assert artifact["size"]==len(original)
    assert receipt["committed_records"]==2
    assert receipt["details"].get("control_mirror_state")=="VERIFIED"
    cli("reconcile","--run-id",run["run_id"],"--non-interactive")
    cli("resume","--run-id",run["run_id"],"--no-parallel","--non-interactive")
    props={"DRIVER":"ODBC Driver 18 for SQL Server","SERVER":d["server"],"DATABASE":d["database"],"Encrypt":d.get("encrypt","yes"),"TrustServerCertificate":d.get("trust_server_certificate","no")}
    if integrated:props["Trusted_Connection"]="yes"
    else:props.update(UID=os.environ["DB_USERNAME"],PWD=os.environ["DB_PASSWORD"])
    string=";".join(k+"="+_odbc(v) for k,v in props.items())+";"
    conn=pyodbc.connect(string,autocommit=True,timeout=15)
    try:
        names=[_q(c["target_name"]) for c in plan["columns"]]
        expressions=[names[0],"CONVERT(nvarchar(64),"+names[1]+")","CONVERT(nvarchar(64),"+names[2]+",126)",names[3],names[4],names[5]]
        target=_q(plan["schema"])+"."+_q(plan["table"])
        actual=conn.cursor().execute("SELECT [__ingest_record],"+",".join(expressions)+" FROM "+target+" WHERE [__ingest_intent]=? ORDER BY [__ingest_ordinal]",file["intent_id"]).fetchall()
        assert len(actual)==2
        assert len({r[0] for r in actual})==2,"Identical source rows collapsed or resumed publication duplicated IDs"
        expected=("0001","123456789012345678.1234","2026-10-29T01:02:03.1234567-04:00",bytes([0,1,2,255]),"é🙂  ","quoted, value")
        assert all(tuple(r[1:])==expected for r in actual)
    finally:conn.close()
