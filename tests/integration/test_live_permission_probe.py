"""Actual real-schema permission proofs; never DML the configured business target."""
import importlib,json,os,uuid
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.ddl import qualified,quote_identifier
CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Explicit disposable SQL configuration required")

@pytest.fixture
def scope():
    module=importlib.import_module("db_ingest.adapters.mssql.connect")
    config=load_config(CONFIG,environ={});d=config["destination"]
    assert all(d[k].casefold().startswith("dbingesttest_") for k in ("database","control_database"))
    username=os.environ["DB_USERNAME"];password=os.environ["DB_PASSWORD"]
    suffix=uuid.uuid4().hex
    config["destination"]["schema"]="DbIngestTest_ProbeTarget_"+suffix
    config["audit"].update(control_schema="DbIngestTest_ProbeAudit_"+suffix,failure_schema="DbIngestTest_ProbeFailure_"+suffix)
    config["mapping"].update(name_collision="stable_hash_suffix",existing_target_strategy="mapped_business",target_table="existing_probe_target")
    for database in (d["database"],d["control_database"]):
        conn=module.connect(config,username,password,database,autocommit=True)
        try:
            from db_ingest.adapters.mssql.permissions import schemas
            for schema in schemas(config,database):conn.cursor().execute("CREATE SCHEMA "+quote_identifier(schema))
            if database==d["database"]:
                target=qualified(config["destination"]["schema"],"existing_probe_target")
                conn.cursor().execute("CREATE TABLE "+target+"(id int NOT NULL PRIMARY KEY,value nvarchar(32) NOT NULL)")
                conn.cursor().execute("INSERT INTO "+target+" VALUES(1,N'keep'),(2,N'also keep')")
        finally:conn.close()
    return module,config,username,password


def assert_existing_target_unchanged(module,config,username,password):
    target=qualified(config["destination"]["schema"],"existing_probe_target")
    conn=module.connect(config,username,password,config["destination"]["database"])
    try:assert [tuple(r) for r in conn.cursor().execute("SELECT id,value FROM "+target+" ORDER BY id")]==[(1,"keep"),(2,"also keep")]
    finally:conn.close()


def test_actual_readonly_facts_and_rollback_only_real_schema_probe(scope):
    module,config,username,password=scope
    readonly=module.test_connection(config,username,password)
    assert readonly["ok"] and readonly["read_only"] and all("write_probes" not in entry for entry in readonly["databases"])
    assert all(entry["permission_observations"]["execution_proof"] is False for entry in readonly["databases"])
    report=module.test_connection(config,username,password,write_probe=True)
    assert report["ok"] and report["exit_code"]==0
    probes=[probe for entry in report["databases"] for probe in entry.get("write_probes",[])]
    assert len(probes)==4
    for probe in probes:
        assert probe["state"]=="PROVEN" and probe["cleanup"]=="VERIFIED_ABSENT_AFTER_ROLLBACK"
        assert set(probe["completed_operations"])=={"CREATE","INSERT","SELECT","ALTER","UPDATE","DELETE","DROP"}
    assert password not in json.dumps(report)
    assert_existing_target_unchanged(module,config,username,password)


def test_actual_dml_only_user_cannot_pass_create_permissions_probe(scope,monkeypatch):
    module,config,username,password=scope;real_connect=module.connect
    user="DbIngestTest_DmlOnly_"+uuid.uuid4().hex
    from db_ingest.adapters.mssql.permissions import schemas
    for database in (config["destination"]["database"],config["destination"]["control_database"]):
        conn=real_connect(config,username,password,database,autocommit=True)
        try:
            cur=conn.cursor();cur.execute("CREATE USER "+quote_identifier(user)+" WITHOUT LOGIN")
            cur.execute("GRANT CONNECT TO "+quote_identifier(user));cur.execute("DENY CREATE TABLE TO "+quote_identifier(user))
            for schema in schemas(config,database):cur.execute("GRANT SELECT,INSERT,UPDATE,DELETE ON SCHEMA::"+quote_identifier(schema)+" TO "+quote_identifier(user))
        finally:conn.close()
    class EffectiveUser:
        def __init__(self,base):self.base=base
        def __getattr__(self,key):return getattr(self.base,key)
        def close(self):
            try:self.base.cursor().execute("REVERT")
            finally:self.base.close()
    def connect_effective(config,username=None,password=None,database=None,autocommit=False):
        base=real_connect(config,username,password,database,autocommit)
        if database in (config["destination"]["database"],config["destination"]["control_database"]):
            try:base.cursor().execute("EXECUTE AS USER = N'"+user+"'")
            except Exception:base.close();raise
            return EffectiveUser(base)
        return base
    monkeypatch.setattr(module,"connect",connect_effective)
    readonly=module.test_connection(config,username,password)
    assert readonly["ok"]
    report=module.test_connection(config,username,password,write_probe=True)
    assert not report["ok"] and report["exit_code"]==4
    probes=[probe for entry in report["databases"] for probe in entry.get("write_probes",[])]
    assert len(probes)==4 and all(probe["state"]=="FAILED" and probe["failed_operation"]=="CREATE" for probe in probes)
    assert all(probe["context"]["native_code"]==262 for probe in probes)
    assert all(probe["completed_operations"]==[] for probe in probes)
    assert password not in json.dumps(report)
    monkeypatch.setattr(module,"connect",real_connect)
    assert_existing_target_unchanged(module,config,username,password)
