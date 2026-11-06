"""Actual ODBC sqlcmd flag adapter and environment-only password; no data-loader claim."""
import os,subprocess
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.transports.sqlcmd import diagnostic_arguments

ROOT=Path(__file__).resolve().parents[2]
CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
TOOL=ROOT/".tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/SQLCMD.EXE"
pytestmark=pytest.mark.skipif(not CONFIG or not TOOL.is_file(),reason="Disposable SQL and actual ODBC sqlcmd tool required")

@pytest.mark.parametrize("encrypt,trust,expected",[("yes","yes",0),("no","yes",0),("yes","no",1)])
def test_real_sqlcmd_tls_flags_and_literal_script_path(tmp_path,encrypt,trust,expected):
    config=load_config(CONFIG,environ={})
    assert all(config["destination"][k].lower().startswith("dbingesttest_") for k in ("database","control_database"))
    config["destination"].update(encrypt=encrypt,trust_server_certificate=trust)
    config["tools"]["sqlcmd_path"]=str(TOOL)
    script=tmp_path/"probe;$(literal).sql"
    script.write_text("SET NOCOUNT ON;\nSELECT 'SQLCMD_POLICY_OK';\nSELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id=@@SPID;\n",encoding="utf-8")
    secret=os.environ["DB_PASSWORD"]
    args,env=diagnostic_arguments(config,script,os.environ["DB_USERNAME"],secret)
    assert secret not in args and "-P" not in args and env["SQLCMDPASSWORD"]==secret
    assert str(script) in args and ("-Nm" if encrypt=="yes" else "-No") in args
    p=subprocess.run(args,env=env,cwd=ROOT,capture_output=True,text=True,encoding="utf-8",errors="backslashreplace",timeout=30,shell=False)
    output=p.stdout+p.stderr
    assert secret not in output
    (tmp_path/"tool-output.txt").write_text(output,encoding="utf-8")
    if expected==0:
        assert p.returncode==0,output
        assert "SQLCMD_POLICY_OK" in output
        if encrypt=="yes":assert "TRUE" in output
    else:
        assert p.returncode!=0
        assert "certificate" in output.lower() or "ssl provider" in output.lower()
    assert config["destination"]["trust_server_certificate"]==trust
