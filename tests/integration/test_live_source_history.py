"""Fresh local journal must consult SQL acknowledgments before identical source replay."""
import importlib.util,json,os,tomllib
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect

ROOT=Path(__file__).resolve().parents[2]
CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Disposable SQL configuration required")

@pytest.mark.parametrize("engine",["odbc","bcp"])
@pytest.mark.parametrize("authority",["destination","central"])
def test_fresh_journal_duplicate_requires_explicit_intent(engine,authority):
    spec=importlib.util.spec_from_file_location("history_authored_types",Path(__file__).with_name("test_live_batch_types.py"))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    case=module.TypeCase(engine,batch_rows=7)
    first=case.cli("run","--non-interactive","--new-intent");case.verify(first)
    item=first["files"][0];plan=item["plan"]
    config=load_config(case.config,environ={})
    user,password=os.environ.get("DB_USERNAME"),os.environ.get("DB_PASSWORD")
    conn=connect(config,user,password,plan["database"],autocommit=True)
    target=module.q(plan["schema"])+"."+module.q(plan["table"])
    try:
        if authority=="central":
            # Only this test's fresh target/receipt is erased to model lost history.
            conn.cursor().execute("DELETE FROM "+target)
            conn.cursor().execute("DELETE FROM audit.__db_ingest_receipts WHERE intent_id=?",item["intent_id"])
        content=case.config.read_text(encoding="utf-8")
        work=tomllib.loads(content)["preservation"]["work_directory"]
        fresh=case.folder/"fresh-work"
        content=content.replace("work_directory = "+module.lit(work),"work_directory = "+module.lit(str(fresh)))
        case.config=case.folder/"fresh.toml";case.config.write_text(content,encoding="utf-8")
        blocked=case.cli("run","--non-interactive",exit_code=2)
        assert any(e["code"]=="NEW_INTENT_REQUIRED" and e["context"]["history_source"]==authority for e in blocked["errors"])
        failed=blocked["files"][0]
        assert failed["counts"]["pending"]==17 and failed["counts"]["committed"]==0
        assert conn.cursor().execute("SELECT COUNT(*) FROM "+target).fetchone()[0]==(17 if authority=="destination" else 0)
        assert Path(failed["artifact"]["path"]).read_bytes()==case.source.read_bytes()
        if authority=="destination":
            # The explicit new-intent choice permits intentional duplicate import.
            repeated=case.cli("run","--non-interactive","--new-intent")
            case.verify(repeated)
            assert tuple(conn.cursor().execute("SELECT COUNT(*),COUNT(DISTINCT __ingest_intent),COUNT(DISTINCT __ingest_record) FROM "+target).fetchone())==(34,2,34)
    finally:conn.close()
