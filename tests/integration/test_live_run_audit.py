"""Queryable totals and idempotent local incident/event backfill on a real server."""
import importlib.util,json,os
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.journal import Journal
from db_ingest.errors import IngestError
from db_ingest.adapters.mssql.control import mirror_run_audit
from db_ingest.adapters.mssql.connect import connect

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Disposable SQL configuration required")


def test_real_run_summary_and_repeat_incident_backfill():
    module_path=Path(__file__).with_name("test_live_batch_types.py")
    spec=importlib.util.spec_from_file_location("independent_types_audit",module_path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    case=module.TypeCase("odbc",batch_rows=7)
    report=case.cli("run","--non-interactive","--new-intent")
    case.verify(report)
    config=load_config(case.config,environ={})
    run_id=report["run_id"]
    user,password=os.environ.get("DB_USERNAME"),os.environ.get("DB_PASSWORD")
    journal=Journal(config["preservation"]["work_directory"])
    try:
        journal.incident(run_id,None,IngestError("AUDIT_FIXTURE","Retained audit fixture",4,{"observation":"after publication","secret":password}))
        journal.event(run_id,None,"AUDIT_FIXTURE",{"observation":"idempotent"})
        first=mirror_run_audit(config,journal,run_id,user,password,final_state="PARTIAL")
        second=mirror_run_audit(config,journal,run_id,user,password,final_state="PARTIAL")
        assert first["incidents"]==second["incidents"]==1
        conn=connect(config,user,password,config["destination"]["control_database"],autocommit=True)
        try:
            cur=conn.cursor()
            summary=json.loads(cur.execute("SELECT summary_json FROM audit.Runs WHERE run_id=?",run_id).fetchone()[0])
            assert summary["counts"]=={"committed":17,"rejected":0,"pending":0,"excluded":0,"unresolved":0,"header":1}
            assert summary["source_bytes"]==case.source.stat().st_size and summary["logical_records"]==18
            assert summary["files"]==1 and summary["state"]=="PARTIAL"
            assert set(summary["configuration_hashes"])=={"interpretation","mapping","operational"}
            assert cur.execute("SELECT COUNT(*) FROM audit.FileIncidents WHERE run_id=? AND code='AUDIT_FIXTURE'",run_id).fetchone()[0]==1
            encoded=cur.execute("SELECT diagnostic_json FROM audit.FileIncidents WHERE run_id=? AND code='AUDIT_FIXTURE'",run_id).fetchone()[0]
            assert password not in encoded and "<redacted>" in encoded
            assert cur.execute("SELECT COUNT(*) FROM audit.Events WHERE run_id=? AND category='AUDIT_FIXTURE'",run_id).fetchone()[0]==1
        finally:conn.close()
    finally:journal.close()
