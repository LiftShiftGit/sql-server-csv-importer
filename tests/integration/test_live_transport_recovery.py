"""Native transport partial commits and real SQL lock-timeout retry oracles."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import runpy
import sqlite3
import time
import tomllib
import pytest

ROOT=Path(__file__).resolve().parents[2]
BASE=runpy.run_path(str(ROOT/"tests/integration/test_live_sql_operations.py"))
CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Explicit disposable SQL config required")

@pytest.fixture
def live(tmp_path):
    d=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8-sig"))["destination"]
    assert all(d[k].casefold().startswith("dbingesttest_") for k in ("database","control_database"))
    return BASE["LiveCase"](tmp_path,d)


def test_real_sql1222_rolls_back_then_retries_same_intent(live):
    case=live;case.write("001,1,AA==,baseline\r\n");baseline=case.run()
    case.write("004,4,AA==,released\r\n")
    content=case.config.read_text(encoding="utf-8").replace("[load]\n","[load]\ntarget_lock_timeout_seconds = 1\n")
    case.config.write_text(content,encoding="utf-8")
    locker=case.connection();locker.autocommit=False
    locker.cursor().execute("IF @@TRANCOUNT=0 BEGIN TRANSACTION")
    locker.cursor().execute("SELECT COUNT_BIG(*) FROM "+case.table(baseline)+" WITH (TABLOCKX,HOLDLOCK)").fetchone()
    event=None
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(case.run)
            deadline=time.monotonic()+45
            journal=case.root/"work/journal.sqlite3"
            while time.monotonic()<deadline:
                connection=sqlite3.connect(journal,timeout=1)
                try:
                    event=connection.execute("SELECT e.run_id,e.details,f.intent_id FROM events e JOIN files f ON f.run_id=e.run_id AND f.file_id=e.file_id WHERE e.category='TRANSIENT_TRANSACTION_RESOLVED' AND e.run_id<>? ORDER BY e.id DESC LIMIT 1",(baseline["run_id"],)).fetchone()
                finally:connection.close()
                if event is not None:break
                if future.done():break
                time.sleep(.02)
            locker.rollback()
            assert event is not None,"Expected real native1222 recovery event before releasing unrelated writer"
            result=future.result(timeout=120)
    finally:
        try:locker.rollback()
        finally:locker.close()
    assert result["state"]=="CLEAN_VERIFIED"
    file=result["files"][0]
    assert file["intent_id"]==event[2]
    assert json.loads(event[1])["native_code"]==1222
    assert file["receipt"]["details"]["publication_attempt"]["number"]==2
    assert case.read(result)==[("001",1,b"\x00","baseline"),("004",4,b"\x00","released")]
    connection=case.connection()
    try:
        assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM "+case.table(result)+" WHERE __ingest_intent=?",file["intent_id"]).fetchone()[0]==1
        assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?",file["intent_id"]).fetchone()[0]==1
    finally:connection.close()
    case.cli("reconcile","--run-id",result["run_id"],"--non-interactive")


@pytest.mark.skipif("bcp" not in os.environ.get("DB_INGEST_TEST_ENGINES","odbc").split(","),reason="Native bcp integration explicitly opt-in")
def test_real_bcp_partial_batch_commits_are_reconciled_then_same_intent_resumes(live):
    case=live
    case.write("".join(f"{i:03},{i},AA==,row-{i}\r\n" for i in range(1,130)))
    tool=Path(os.environ.get("DB_INGEST_TEST_BCP_PATH",str(ROOT/".tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/bcp.exe")))
    assert tool.exists()
    content=case.config.read_text(encoding="utf-8").replace('engine = "odbc"','engine = "bcp"').replace("[load]\n","[load]\nbatch_rows = 256\nbatch_bytes = 1048576\n")
    content+='\n[tools]\nbcp_path = '+BASE["lit"](str(tool))+'\n[credentials]\nallow_password_in_child_args = true\n'
    case.config.write_text(content,encoding="utf-8")
    failed=case.run(expected_code=4,extra_env={"DB_INGEST_TEST_MODE":"1","DB_INGEST_TEST_BCP_DUPLICATE_ROW":"129"})
    assert failed["state"]=="FAILED"
    file=failed["files"][0]
    assert file["counts"]["committed"]==0 and file["counts"]["pending"]==129
    manifests=list((case.root/"work/transport"/failed["run_id"]).glob("*/manifest.json"))
    assert len(manifests)==1
    manifest=json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["state"]=="FAILED" and manifest["raw_cleanup"]=="DROPPED"
    chunk=manifest["summary"]["chunks"][0]
    assert chunk["observed_partial_rows"]==128,"Actual first128-row bcp batch must commit before the injected second-batch PK failure"
    assert Path(chunk["path"]).exists() and Path(chunk["test_fault"]["input"]).exists()
    connection=case.connection()
    try:
        assert connection.cursor().execute("SELECT OBJECT_ID(?)",case.table(failed)).fetchone()[0] is None
        assert connection.cursor().execute("SELECT OBJECT_ID(?)",manifest["raw_table"]).fetchone()[0] is None
        assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?",file["intent_id"]).fetchone()[0]==0
    finally:connection.close()
    resumed=case.cli("resume","--run-id",failed["run_id"],"--non-interactive")
    assert resumed["state"]=="CLEAN_VERIFIED"
    assert resumed["files"][0]["intent_id"]==file["intent_id"]
    assert resumed["files"][0]["receipt"]["details"]["transport"]["engine"]=="bcp"
    assert case.read(resumed)==[(f"{i:03}",i,b"\x00",f"row-{i}") for i in range(1,130)]
    case.cli("reconcile","--run-id",failed["run_id"],"--non-interactive")


def configure_native(case):
    tool=Path(os.environ.get("DB_INGEST_TEST_BCP_PATH",str(ROOT/".tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/bcp.exe")))
    assert tool.exists()
    content=case.config.read_text(encoding="utf-8").replace('engine = "odbc"','engine = "bcp"').replace("[load]\n","[load]\nbatch_rows = 256\nbatch_bytes = 1048576\n")
    content+='\n[tools]\nbcp_path = '+BASE["lit"](str(tool))+'\n[credentials]\nallow_password_in_child_args = true\n'
    case.config.write_text(content,encoding="utf-8")


def observe_interrupted(case,expected_raw):
    status=case.cli("status");run_id=status["runs"][0]["run_id"]
    report=case.cli("status","--run-id",run_id);item=report["files"][0]
    assert Path(item["artifact"]["path"]).read_bytes()==case.original
    manifests=list((case.root/"work/transport"/run_id).glob("*/manifest.json"))
    manifest=json.loads(manifests[0].read_text(encoding="utf-8")) if manifests else None
    connection=case.connection()
    try:
        assert connection.cursor().execute("SELECT OBJECT_ID(?)",case.table(report)).fetchval() is None
        assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?",item["intent_id"]).fetchval()==0
        if expected_raw is not None:
            assert len(manifests)==1
            attempt=manifests[0].parent.name
            assert len(attempt)==32 and all(c in "0123456789abcdef" for c in attempt)
            assert manifest["raw_table"]=="[audit].[__bcp_"+attempt+"]"
            assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM "+manifest["raw_table"]).fetchval()==expected_raw
            assert manifest["state"] in ("SPOOLED","TEST_FAULT_INPUT_DURABLE")
    finally:connection.close()
    return run_id,item,manifest


@pytest.mark.parametrize("engine",["odbc","bcp"])
def test_real_F04_sent_before_ack_resumes_same_intent(live,engine):
    if engine=="bcp" and "bcp" not in os.environ.get("DB_INGEST_TEST_ENGINES","odbc").split(","):pytest.skip("Native bcp explicitly opt-in")
    case=live;case.write(BASE["ROWS"])
    if engine=="bcp":configure_native(case)
    case.run(expected_code=86,extra_env={"DB_INGEST_TEST_MODE":"1","DB_INGEST_TEST_FAULT":"STAGING_SENT_BEFORE_ACK"})
    run_id,item,manifest=observe_interrupted(case,3 if engine=="bcp" else None)
    resumed=case.cli("resume","--run-id",run_id,"--non-interactive")
    assert resumed["state"]=="CLEAN_VERIFIED" and resumed["files"][0]["intent_id"]==item["intent_id"]
    assert resumed["files"][0]["receipt"]["details"]["transport"]["engine"]==engine
    assert case.read(resumed)==BASE["EXPECTED"]
    with sqlite3.connect(case.root/"work/journal.sqlite3") as journal:
        events=journal.execute("SELECT details FROM events WHERE run_id=? AND category='COMMIT_OUTCOME_RESOLVED'",(run_id,)).fetchall()
    assert len(events)==1 and json.loads(events[0][0])["proof"]["current_intent_records"]==0
    case.cli("reconcile","--run-id",run_id,"--non-interactive")


@pytest.mark.skipif("bcp" not in os.environ.get("DB_INGEST_TEST_ENGINES","odbc").split(","),reason="Native bcp integration explicitly opt-in")
def test_real_F05_partial_bcp_before_reconciliation_resumes_original_source(live):
    case=live;case.write("".join(f"{i:03},{i},AA==,row-{i}\r\n" for i in range(1,130)));configure_native(case)
    case.run(expected_code=86,extra_env={"DB_INGEST_TEST_MODE":"1","DB_INGEST_TEST_FAULT":"BCP_FAILED_BEFORE_PARTIAL_RECONCILIATION","DB_INGEST_TEST_BCP_DUPLICATE_ROW":"129"})
    run_id,item,manifest=observe_interrupted(case,128)
    assert manifest["summary"]["chunk_ledger"]["events"]==1
    resumed=case.cli("resume","--run-id",run_id,"--non-interactive")
    assert resumed["state"]=="CLEAN_VERIFIED" and resumed["files"][0]["intent_id"]==item["intent_id"]
    assert case.read(resumed)==[(f"{i:03}",i,bytes([0]),f"row-{i}") for i in range(1,130)]
    connection=case.connection()
    try:
        # Hard-crash raw rows remain isolated evidence; they are never replayed as
        # business rows or silently cleared by the successful new attempt.
        assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM "+manifest["raw_table"]).fetchval()==128
        assert tuple(connection.cursor().execute("SELECT COUNT_BIG(*),COUNT(DISTINCT __ingest_record) FROM "+case.table(resumed)).fetchone())==(129,129)
        assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?",item["intent_id"]).fetchval()==1
    finally:connection.close()
    case.cli("reconcile","--run-id",run_id,"--non-interactive")
