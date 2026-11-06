"""Real disposable SQL operations; authored readback expectations are independent.

Nothing runs without DB_INGEST_TEST_CONFIG and explicit DbIngestTest_ database names.
Each test uses a fresh uniquely named landing table; no existing objects are dropped.
"""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib
import uuid
import pytest

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="SQL Server integration not executed: explicitly disposable config required")

def lit(value):
    if isinstance(value,bool):return "true" if value else "false"
    if isinstance(value,(int,float)):return str(value)
    if isinstance(value,list):return "["+",".join(lit(x) for x in value)+"]"
    return json.dumps(str(value),ensure_ascii=True)
def q(value):return "["+value.replace("]", "]]")+"]"
def odbc(value):return "{"+str(value).replace("}","}}")+"}"

class LiveCase:
    def __init__(self,root,destination,name=None):
        self.root=root;root.mkdir(parents=True,exist_ok=True)
        self.destination=destination
        self.source=root/(name or ("operations_"+uuid.uuid4().hex+".csv"))
        self.config=root/"operations.toml"
        self.original=None
        self.configure()
    def configure(self,mode="append"):
        sections={"destination":self.destination,"source":{"root":str(self.root),"path":str(self.source),"include":[self.source.name],"format":"delimited"},"parsing":{"encoding":"utf-8","delimiter":",","quotechar":'"',"header":"present","null_tokens":["\\N"]},"preservation":{"work_directory":str(self.root/"work"),"minimum_free_disk_bytes":1},"load":{"engine":"odbc","mode":mode,"fast_executemany":False},"parallel":{"enabled":False},"interaction":{"non_interactive":True}}
        # The marker is one literal backslash plus N, not a Python NULL escape.
        sections["parsing"]["null_tokens"]=[chr(92)+"N"]
        text="\n".join("["+section+"]\n"+"\n".join(k+" = "+lit(v) for k,v in values.items()) for section,values in sections.items())
        text+='\n[[file_rules]]\nglob = '+lit(self.source.name)+'\n'
        for ordinal,(name,kind,binary) in enumerate([("id","nvarchar(12)",None),("n","int",None),("payload","varbinary(max)","base64"),("note","nvarchar(64)",None)],1):
            text+='\n[[file_rules.columns]]\nsource_ordinal = '+str(ordinal)+'\ntarget_name = '+lit(name)+'\ntarget_type = '+lit(kind)+'\n'
            if binary:text+='binary_encoding = '+lit(binary)+'\n'
        self.config.write_text(text,encoding="utf-8")
    def write(self,rows):
        self.original=("id,n,payload,note\r\n"+rows).encode("utf-8")
        self.source.write_bytes(self.original)
    def cli(self,command,*args,expected_code=0,extra_env=None):
        env={k:v for k,v in os.environ.items() if not k.startswith("DB_") or k in ("DB_USERNAME","DB_PASSWORD")}
        env.update(extra_env or {})
        completed=subprocess.run([sys.executable,"-m","db_ingest","--config",str(self.config),command,*args,"--json"],cwd=Path(__file__).resolve().parents[2],env=env,capture_output=True,text=True,timeout=240,shell=False)
        secret=os.environ.get("DB_PASSWORD")
        if secret:assert secret not in completed.stdout+completed.stderr,"Credential leaked in CLI output"
        assert completed.returncode==expected_code,(completed.stdout,completed.stderr)
        raw=completed.stdout.strip() or completed.stderr.strip()
        return json.loads(raw) if raw else None
    def run(self,**kwargs):return self.cli("run","--no-parallel","--non-interactive","--new-intent",**kwargs)
    def connection(self):
        import pyodbc
        d=self.destination
        props={"DRIVER":"ODBC Driver 18 for SQL Server","SERVER":d["server"],"DATABASE":d["database"],"Encrypt":d.get("encrypt","yes"),"TrustServerCertificate":d.get("trust_server_certificate","no")}
        if d.get("authentication","sql")=="integrated":props["Trusted_Connection"]="yes"
        else:props.update(UID=os.environ["DB_USERNAME"],PWD=os.environ["DB_PASSWORD"])
        conn=pyodbc.connect(";".join(k+"="+odbc(v) for k,v in props.items())+";",autocommit=True,timeout=15)
        conn.timeout=60
        return conn
    @staticmethod
    def table(report):
        p=report["files"][0]["plan"]
        return q(p["schema"])+"."+q(p["table"])
    def read(self,report):
        conn=self.connection()
        try:return [tuple(r) for r in conn.cursor().execute("SELECT id,n,payload,note FROM "+self.table(report)+" ORDER BY id,__ingest_intent").fetchall()]
        finally:conn.close()

@pytest.fixture
def cases(tmp_path):
    d=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8-sig")).get("destination",{})
    for key in ("database","control_database"):
        assert str(d.get(key,"")).casefold().startswith("dbingesttest_"),"Refusing non-disposable SQL test configuration"
    pyodbc=pytest.importorskip("pyodbc")
    assert "ODBC Driver 18 for SQL Server" in pyodbc.drivers()
    assert d.get("authentication","sql")=="integrated" or (os.environ.get("DB_USERNAME") and os.environ.get("DB_PASSWORD") is not None)
    created=[]
    def factory(name=None):
        case=LiveCase(tmp_path/str(len(created)),d,name)
        created.append(case)
        return case
    return factory

ROWS='001,1,,\\N\r\n002,2,\\N,""\r\n003,3,AAEC/w==,"\\N"\r\n'.replace(chr(92)*2,chr(92))
EXPECTED=[("001",1,b"",None),("002",2,None,""),("003",3,bytes([0,1,2,255]),chr(92)+"N")]

def test_real_null_empty_binary_append_replace_and_retained_generation(cases):
    case=cases();case.write(ROWS)
    first=case.run()
    assert case.read(first)==EXPECTED
    second=case.run()
    assert second["files"][0]["intent_id"]!=first["files"][0]["intent_id"]
    assert case.read(second)==[row for row in EXPECTED for _ in range(2)]
    case.configure("replace");case.write('004,4,AA==,"replacement 🙂  "\r\n')
    replacement=case.run()
    assert case.read(replacement)==[("004",4,b"\x00","replacement 🙂  ")]
    previous=replacement["files"][0]["receipt"]["details"]["previous_generation"]
    assert previous
    conn=case.connection()
    try:
        # The table name is produced by the reviewed importer; ensure it is the expected
        # qualified previous-generation table before using it as an identifier.
        assert previous.startswith("["+replacement["files"][0]["plan"]["schema"]+"]") and "__previous_" in previous
        assert conn.cursor().execute("SELECT COUNT_BIG(*) FROM "+previous).fetchone()[0]==6
    finally:conn.close()
    case.cli("reconcile","--run-id",replacement["run_id"],"--non-interactive")

def test_real_check_constraint_failure_keeps_prior_rows(cases):
    case=cases();case.write(ROWS);first=case.run()
    conn=case.connection()
    try:conn.cursor().execute("ALTER TABLE "+case.table(first)+" ADD CONSTRAINT "+q("ck_positive_"+uuid.uuid4().hex)+" CHECK (n>0)")
    finally:conn.close()
    case.write("004,-1,AA==,bad\r\n")
    failed=case.run(expected_code=4)
    assert failed["state"]=="FAILED"
    assert case.read(first)==EXPECTED
    errors=case.cli("show-errors","--run-id",failed["run_id"])
    assert any(e["code"]=="CONSTRAINT" for e in errors["incidents"])

def test_real_trigger_preflight_blocks_before_insertion(cases):
    case=cases();case.write(ROWS);first=case.run()
    conn=case.connection()
    try:conn.cursor().execute("CREATE TRIGGER "+q("dbo")+"."+q("tr_block_"+uuid.uuid4().hex)+" ON "+case.table(first)+" AFTER INSERT AS BEGIN SET NOCOUNT ON; RETURN; END")
    finally:conn.close()
    blocked=case.run(expected_code=2)
    assert case.read(first)==EXPECTED
    errors=case.cli("show-errors","--run-id",blocked["run_id"])
    assert any(e["code"]=="TRIGGER_UNSUPPORTED" for e in errors["incidents"])

@pytest.mark.parametrize("point",["STAGED_BEFORE_PUBLICATION","AFTER_TARGET_SCHEMA_CREATION"])
def test_real_precommit_process_death_leaves_no_target_rows(cases,point):
    case=cases();case.write(ROWS)
    case.run(expected_code=86,extra_env={"DB_INGEST_TEST_MODE":"1","DB_INGEST_TEST_FAULT":point})
    status=case.cli("status");run_id=status["runs"][0]["run_id"]
    report=case.cli("status","--run-id",run_id)
    target=case.table(report);file=report["files"][0]
    assert Path(file["artifact"]["path"]).read_bytes()==case.original
    conn=case.connection()
    try:
        assert conn.cursor().execute("SELECT OBJECT_ID(?)",target).fetchone()[0] is None
        assert conn.cursor().execute("SELECT COUNT_BIG(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?",file["intent_id"]).fetchone()[0]==0
    finally:conn.close()
    # A held target lock, empty current lineage and consistent local/central history
    # now provide the required positive rollback proof for this SAME intent.
    resumed=case.cli("resume","--run-id",run_id,"--non-interactive")
    assert resumed["files"][0]["intent_id"]==file["intent_id"]
    assert case.read(resumed)==EXPECTED
    import sqlite3
    with sqlite3.connect(case.root/"work"/"journal.sqlite3") as journal:
        events=journal.execute("SELECT details FROM events WHERE run_id=? AND category='COMMIT_OUTCOME_RESOLVED'",(run_id,)).fetchall()
    assert len(events)==1
    assert json.loads(events[0][0])["proof"]["current_intent_records"]==0

def test_real_replace_process_death_preserves_old_generation(cases):
    case=cases();case.write(ROWS);first=case.run()
    case.configure("replace");case.write("004,4,AA==,new\r\n")
    case.run(expected_code=86,extra_env={"DB_INGEST_TEST_MODE":"1","DB_INGEST_TEST_FAULT":"REPLACE_AFTER_DELETE_BEFORE_INSERT"})
    status=case.cli("status");run_id=status["runs"][0]["run_id"]
    assert case.read(first)==EXPECTED
    interrupted=case.cli("status","--run-id",run_id)
    intent=interrupted["files"][0]["intent_id"]
    resumed=case.cli("resume","--run-id",run_id,"--non-interactive")
    assert resumed["files"][0]["intent_id"]==intent
    assert case.read(resumed)==[("004",4,b"\x00","new")]
    backup=resumed["files"][0]["receipt"]["details"]["previous_generation"]
    conn=case.connection()
    try:assert [tuple(r) for r in conn.cursor().execute("SELECT id,n,payload,note FROM "+backup+" ORDER BY id")]==EXPECTED
    finally:conn.close()

def test_real_simultaneous_first_publishers_are_serialized(cases):
    shared="concurrent_"+uuid.uuid4().hex+".csv"
    a,b=cases(shared),cases(shared)
    a.write("001,1,AA==,first\r\n");b.write("002,2,AQ==,second\r\n")
    with ThreadPoolExecutor(max_workers=2) as pool:
        fa,fb=pool.submit(a.run),pool.submit(b.run)
        ra,rb=fa.result(),fb.result()
    assert a.table(ra)==b.table(rb)
    assert a.read(ra)==[("001",1,b"\x00","first"),("002",2,b"\x01","second")]
    a.cli("reconcile","--run-id",ra["run_id"],"--non-interactive")
    b.cli("reconcile","--run-id",rb["run_id"],"--non-interactive")


def test_real_partial_import_materializes_one_reject_with_multiple_errors(cases):
    import hashlib
    case=cases()
    case.write('001,1,,\\N\r\n002,2,\\N,""\r\n003,not-an-int,%%%invalid,rejected\r\n'.replace(chr(92)*2,chr(92)))
    result=case.run(expected_code=3)
    assert result["state"]=="PARTIAL"
    assert case.read(result)==EXPECTED[:2]
    file=result["files"][0]
    assert file["counts"]["committed"]==2
    assert file["counts"]["rejected"]==1
    failure=file["receipt"]["details"]["failure_table"]
    assert failure["state"]=="VERIFIED" and failure["rejected_records"]==1
    assert failure["database"]==case.destination["control_database"]
    assert failure["table"].startswith("[failures].[")
    conn=case.connection()
    try:
        rows=conn.cursor().execute("SELECT record_id,source_hash,logical_ordinal,payload_json,all_errors_json,artifact_path FROM "+q(failure["database"])+"."+failure["table"]).fetchall()
        assert len(rows)==1
        row=rows[0]
        assert row[1]==hashlib.sha256(case.original).hexdigest() and row[2]==4
        fields=json.loads(row[3]);errors=json.loads(row[4])
        assert fields[1]["text"]=="not-an-int" and fields[2]["text"]=="%%%invalid"
        assert len(errors)==2 and {e["column_ordinal"] for e in errors}=={2,3}
        normalized=conn.cursor().execute("SELECT COUNT_BIG(*) FROM "+q(failure["database"])+".[audit].[RecordErrors] WHERE run_id=? AND record_id=?",result["run_id"],row[0]).fetchone()[0]
        assert normalized==2
        assert Path(row[5]).read_bytes()==case.original
    finally:conn.close()
    case.cli("reconcile","--run-id",result["run_id"],"--non-interactive",expected_code=3)


def test_real_control_database_outage_is_pending_then_resume_backfills(cases):
    case=cases();case.write("001,1,AA==,baseline\r\n");baseline=case.run()
    original=dict(case.destination)
    case.destination={**original,"control_database":"DbIngestTest_Unavailable_"+uuid.uuid4().hex,"create_databases":False}
    case.configure();case.write("002,2,AQ==,during-audit-outage\r\n")
    partial=case.run(expected_code=3)
    assert partial["state"]=="PARTIAL"
    file=partial["files"][0]
    assert file["receipt"]["details"]["control_mirror_state"]=="pending"
    assert case.read(baseline)==[("001",1,b"\x00","baseline"),("002",2,b"\x01","during-audit-outage")]
    # This is real inaccessible-control-database behavior (4060), not a claim that
    # network/server outage or restore has been reproduced.
    errors=case.cli("show-errors","--run-id",partial["run_id"])
    assert any(e["code"]=="DATABASE_ACCESS" for e in errors["incidents"])
    case.destination=original;case.configure()
    resumed=case.cli("resume","--run-id",partial["run_id"],"--non-interactive")
    assert resumed["state"]=="CLEAN_VERIFIED"
    assert resumed["files"][0]["receipt"]["details"]["control_mirror_state"]=="VERIFIED"
    assert case.read(baseline)==[("001",1,b"\x00","baseline"),("002",2,b"\x01","during-audit-outage")]
    case.cli("reconcile","--run-id",partial["run_id"],"--non-interactive")


def test_real_recreated_target_object_invalidates_receipt_identity(cases):
    case=cases();case.write(ROWS);run=case.run()
    plan=run["files"][0]["plan"]
    target=case.table(run)
    retained_name=plan["table"][:90]+"__identity_original_"+uuid.uuid4().hex[:8]
    retained=q(plan["schema"])+"."+q(retained_name)
    conn=case.connection()
    try:
        before=conn.cursor().execute("SELECT OBJECT_ID(?)",target).fetchone()[0]
        conn.cursor().execute("EXEC sys.sp_rename @objname=?,@newname=?,@objtype='OBJECT'",target,retained_name)
        conn.cursor().execute("SELECT * INTO "+target+" FROM "+retained)
        after=conn.cursor().execute("SELECT OBJECT_ID(?)",target).fetchone()[0]
        assert before!=after
        assert conn.cursor().execute("SELECT COUNT_BIG(*) FROM "+retained).fetchone()[0]==3
    finally:conn.close()
    # Rows and schema bytes still match; actual object replacement alone must block.
    assert case.read(run)==EXPECTED
    mismatch=case.cli("reconcile","--run-id",run["run_id"],"--non-interactive",expected_code=5)
    assert mismatch["code"]=="RECEIPT_IDENTITY_MISMATCH"
    case.cli("resume","--run-id",run["run_id"],"--non-interactive",expected_code=5)
    assert case.read(run)==EXPECTED


def test_real_missing_acknowledged_receipt_never_replays_rows(cases):
    case=cases();case.write(ROWS);run=case.run()
    intent=run["files"][0]["intent_id"]
    conn=case.connection()
    try:
        assert conn.cursor().execute("SELECT COUNT_BIG(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?",intent).fetchone()[0]==1
        # Remove only this newly authored disposable test intent's destination receipt.
        # Its local and central receipts remain as evidence of the disagreement.
        conn.cursor().execute("DELETE FROM [audit].[__db_ingest_receipts] WHERE intent_id=?",intent)
    finally:conn.close()
    blocked=case.cli("resume","--run-id",run["run_id"],"--non-interactive",expected_code=5)
    assert blocked["state"]=="COMMIT_OUTCOME_UNKNOWN"
    assert case.read(run)==EXPECTED
    mismatch=case.cli("reconcile","--run-id",run["run_id"],"--non-interactive",expected_code=5)
    assert mismatch["code"]=="RECEIPT_MISSING"
    assert case.read(run)==EXPECTED
