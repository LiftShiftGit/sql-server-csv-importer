"""Valid component names can exceed the explicit logical-target metadata bound."""
import json,os,runpy,tomllib,uuid
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/"tests/integration/test_live_sql_operations.py"))
CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Explicit disposable SQL configuration required")


def test_qualified_name_limit_preserves_archive_and_existing_table(tmp_path):
    destination=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8-sig"))["destination"]
    assert all(destination[k].casefold().startswith("dbingesttest_") for k in ("database","control_database"))
    prefix="DbIngestTest_"+uuid.uuid4().hex
    schema=prefix+"s"*(128-len(prefix));table="t"*128
    destination={**destination,"schema":schema}
    case=API["LiveCase"](tmp_path,destination);case.write(API["ROWS"])
    target=API["q"](schema)+"."+API["q"](table)
    assert len(target.encode("utf-16-le"))//2==261
    connection=case.connection()
    try:
        connection.cursor().execute("CREATE SCHEMA "+API["q"](schema))
        connection.cursor().execute("CREATE TABLE "+target+"(id nvarchar(12) NOT NULL PRIMARY KEY,n int NULL,payload varbinary(max) NULL,note nvarchar(64) NULL)")
    finally:connection.close()
    text=case.config.read_text(encoding="utf-8")+'\n[mapping]\nname_collision="stable_hash_suffix"\nexisting_target_strategy="mapped_business"\ntarget_table='+API["lit"](table)+'\n'
    case.config.write_text(text,encoding="utf-8")
    report=case.run(expected_code=2)
    errors=case.cli("show-errors","--run-id",report["run_id"])
    assert any(row["code"]=="TARGET_METADATA_NAME_LIMIT" for row in errors["incidents"])
    assert Path(report["files"][0]["artifact"]["path"]).read_bytes()==case.original
    connection=case.connection()
    try:
        assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM "+target).fetchval()==0
        assert connection.cursor().execute("SELECT COUNT_BIG(*) FROM [audit].[__db_ingest_receipts] WHERE intent_id=?",report["files"][0]["intent_id"]).fetchval()==0
    finally:connection.close()
