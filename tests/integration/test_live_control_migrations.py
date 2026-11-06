"""Real central schema version migration and future-version fail-closed behavior."""
import copy,os,uuid
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect
from db_ingest.adapters.mssql.control import _open
from db_ingest.errors import IngestError

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Disposable SQL configuration required")

@pytest.mark.parametrize("version",[1,99])
def test_control_migration_is_transactional_and_refuses_future_version(version):
    config=load_config(CONFIG,environ={})
    assert config["destination"]["control_database"].lower().startswith("dbingesttest_")
    schema="migration_"+uuid.uuid4().hex
    config["audit"]["control_schema"]=schema
    user,password=os.environ.get("DB_USERNAME"),os.environ.get("DB_PASSWORD")
    conn=connect(config,user,password,config["destination"]["control_database"],autocommit=True)
    cur=conn.cursor()
    cur.execute("CREATE SCHEMA ["+schema+"]")
    cur.execute("CREATE TABLE ["+schema+"].[SchemaMigrations] (version int NOT NULL PRIMARY KEY,applied_at_utc nvarchar(40) NOT NULL)")
    cur.execute("INSERT INTO ["+schema+"].[SchemaMigrations] VALUES (?,?)",version,"2026-01-01T00:00:00Z")
    cur.execute("CREATE TABLE ["+schema+"].[Runs] (run_id nvarchar(64) NOT NULL PRIMARY KEY,state nvarchar(40) NOT NULL,updated_at_utc nvarchar(40) NOT NULL)")
    try:
        if version==99:
            with pytest.raises(IngestError) as error:_open(config,user,password)
            assert error.value.code=="CONTROL_VERSION_UNSUPPORTED"
            assert cur.execute("SELECT COUNT(*) FROM sys.tables WHERE schema_id=SCHEMA_ID(?)",schema).fetchone()[0]==2
            assert cur.execute("SELECT COL_LENGTH(?,?)",schema+".Runs","summary_json").fetchone()[0] is None
        else:
            migrated,observed=_open(config,user,password)
            migrated.close()
            assert observed==schema
            assert cur.execute("SELECT COUNT(*) FROM sys.tables WHERE schema_id=SCHEMA_ID(?)",schema).fetchone()[0]==16
            assert cur.execute("SELECT COL_LENGTH(?,?)",schema+".Runs","summary_json").fetchone()[0]==-1
            assert cur.execute("SELECT COL_LENGTH(?,?)",schema+".Files","summary_json").fetchone()[0]==-1
            assert [r[0] for r in cur.execute("SELECT version FROM ["+schema+"].[SchemaMigrations] ORDER BY version")]==[1,2,3,4,5]
    finally:conn.close()



def test_missing_group_control_history_is_not_created(tmp_path):
    from db_ingest.adapters.mssql.control import export_group_contributors
    from db_ingest.models import SchemaPlan,ColumnProfile
    config=load_config(CONFIG,environ={})
    config["destination"]["control_database"]="DbIngestTest_MissingHistory_"+uuid.uuid4().hex
    user,password=os.environ.get("DB_USERNAME"),os.environ.get("DB_PASSWORD")
    plan=SchemaPlan(config["destination"]["database"],"dbo","never_created",[ColumnProfile(1,"x","x","int")],"a"*64,"b"*64,"c"*64)
    with pytest.raises(IngestError) as error:export_group_contributors(config,plan,tmp_path/"history.jsonl",user,password)
    assert error.value.exit_code==5 and error.value.code=="GROUP_CENTRAL_HISTORY_UNAVAILABLE"
    admin=connect(config,user,password,"master",autocommit=True)
    try:assert admin.cursor().execute("SELECT DB_ID(?)",config["destination"]["control_database"]).fetchone()[0] is None
    finally:admin.close()
    assert not (tmp_path/"history.jsonl").exists()
