"""Fresh-journal subset append must not duplicate acknowledged contributors silently."""
import os,runpy
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/"tests/integration/test_live_group_oracle.py"))
pytestmark=pytest.mark.skipif(not os.environ.get("DB_INGEST_TEST_CONFIG"),reason="Explicit disposable SQL configuration required")


@pytest.mark.parametrize("engine",["odbc","bcp"])
@pytest.mark.parametrize("fresh",[False,True])
def test_fresh_journal_subset_requires_explicit_new_contributor_intent(engine,fresh):
    if engine=="bcp" and "bcp" not in os.environ.get("DB_INGEST_TEST_ENGINES","odbc").split(","):pytest.skip("Native bcp explicitly opt-in")
    group=API["GroupCase"](engine,"strict")
    first=group.case.cli("run","--new-intent","--no-parallel","--non-interactive")
    original,values=group.verify(first)
    group.write_config(subset=True)
    text=group.case.config.read_text(encoding="utf-8").replace(API["API"]["lit"](str(group.case.folder/"work")),API["API"]["lit"](str(group.case.folder/"fresh-work")))
    if fresh:group.case.config.write_text(text,encoding="utf-8")
    refused=group.case.cli("run","--no-parallel","--non-interactive",exit_code=2)
    assert "NEW_INTENT_REQUIRED" in str(refused) and group.a.name in str(refused)
    assert group.values(original)==values
    permitted=group.case.cli("run","--new-intent","--no-parallel","--non-interactive")
    latest=permitted["groups"][0]
    assert latest["receipt"]["committed_records"]==2
    actual=group.values(latest)
    assert len(actual)==5 and len({r[-2] for r in actual})==5
    assert sorted(r[:-2] for r in actual)==sorted([(1,"n  ")]*4+[(2,chr(0x1F642)+"  ")])


@pytest.mark.parametrize("engine",["odbc","bcp"])
def test_fresh_journal_old_contribution_version_is_not_hidden_by_latest_cache(engine):
    if engine=="bcp" and "bcp" not in os.environ.get("DB_INGEST_TEST_ENGINES","odbc").split(","):pytest.skip("Native bcp explicitly opt-in")
    group=API["GroupCase"](engine,"strict");original_bytes=group.raw_a
    first=group.case.cli("run","--new-intent","--no-parallel","--non-interactive");group.verify(first)
    group.a.write_bytes(b"id,note\r\n1,new-a\r\n1,new-a\r\n");group.write_config(subset=True)
    changed=group.case.cli("run","--new-intent","--no-parallel","--non-interactive")
    before=sorted(group.values(changed["groups"][0]),key=lambda row:row[-2]);assert len(before)==5
    group.a.write_bytes(original_bytes);group.write_config(subset=True)
    text=group.case.config.read_text(encoding="utf-8").replace(API["API"]["lit"](str(group.case.folder/"work")),API["API"]["lit"](str(group.case.folder/"old-version-fresh")))
    group.case.config.write_text(text,encoding="utf-8")
    refused=group.case.cli("run","--no-parallel","--non-interactive",exit_code=2)
    assert "NEW_INTENT_REQUIRED" in str(refused) and group.a.name in str(refused)
    assert sorted(group.values(changed["groups"][0]),key=lambda row:row[-2])==before


@pytest.mark.parametrize("engine",["odbc","bcp"])
@pytest.mark.parametrize("loss",["destination","legacy_central"])
def test_missing_contribution_history_requires_original_ack_backfill(engine,loss):
    if engine=="bcp" and "bcp" not in os.environ.get("DB_INGEST_TEST_ENGINES","odbc").split(","):pytest.skip("Native bcp explicitly opt-in")
    import importlib,uuid
    from db_ingest.config import load_config
    from db_ingest.adapters.mssql.ddl import qualified
    connect=importlib.import_module("db_ingest.adapters.mssql.connect").connect
    group=API["GroupCase"](engine,"strict")
    schema="DbIngestTest_ContributionLegacy_"+uuid.uuid4().hex
    group.prefix+='\n[audit]\ncontrol_schema='+API["API"]["lit"](schema)+'\n';group.write_config()
    original_config=group.case.config.read_text(encoding="utf-8")
    first=group.case.cli("run","--new-intent","--no-parallel","--non-interactive");original,values=group.verify(first)
    cfg=load_config(group.case.config,environ={});d=cfg["destination"]
    username=os.environ["DB_USERNAME"];password=os.environ["DB_PASSWORD"]
    if loss=="destination":
        conn=connect(cfg,username,password,d["database"],autocommit=True)
        try:conn.cursor().execute("DELETE FROM "+qualified(schema,"__db_ingest_contribution_history")+" WHERE intent_id=?",original["intent_id"])
        finally:conn.close()
    else:
        # Touch only this case's private audit schema; shared metadata is untouched.
        conn=connect(cfg,username,password,d["control_database"],autocommit=True)
        try:
            conn.cursor().execute("DROP TABLE "+qualified(schema,"SourceGroupContributionHistory"))
            conn.cursor().execute("DELETE FROM "+qualified(schema,"SchemaMigrations")+" WHERE version>=4")
            assert conn.cursor().execute("SELECT MAX(version) FROM "+qualified(schema,"SchemaMigrations")).fetchval()==3
        finally:conn.close()
    group.a.write_bytes(b"id,note\r\n1,new-a\r\n1,new-a\r\n");group.write_config(subset=True)
    pending_config=group.case.config.read_text(encoding="utf-8").replace(API["API"]["lit"](str(group.case.folder/"work")),API["API"]["lit"](str(group.case.folder/"history-fresh")))
    group.case.config.write_text(pending_config,encoding="utf-8")
    refused=group.case.cli("run","--no-parallel","--non-interactive",exit_code=5)
    assert "GROUP_CONTRIBUTION_HISTORY_INCOMPLETE" in str(refused)
    assert group.values(original)==values
    blocked_intent=refused["groups"][0]["intent_id"]
    # Verified original receipt + original sealed manifests are the repair authority.
    group.case.config.write_text(original_config,encoding="utf-8")
    group.case.cli("resume","--run-id",first["run_id"],"--no-parallel","--non-interactive")
    conn=connect(cfg,username,password,d["database"],autocommit=True)
    try:assert conn.cursor().execute("SELECT COUNT_BIG(*) FROM "+qualified(schema,"__db_ingest_contribution_history")+" WHERE intent_id=?",original["intent_id"]).fetchval()==2
    finally:conn.close()
    conn=connect(cfg,username,password,d["control_database"],autocommit=True)
    try:
        assert conn.cursor().execute("SELECT MAX(version) FROM "+qualified(schema,"SchemaMigrations")).fetchval()>=4
        assert conn.cursor().execute("SELECT COUNT_BIG(*) FROM "+qualified(schema,"SourceGroupContributionHistory")+" WHERE intent_id=?",original["intent_id"]).fetchval()==2
    finally:conn.close()
    group.case.config.write_text(pending_config,encoding="utf-8")
    resumed=group.case.cli("resume","--run-id",refused["run_id"],"--no-parallel","--non-interactive")
    assert resumed["groups"][0]["intent_id"]==blocked_intent
    assert len(group.values(resumed["groups"][0]))==5


@pytest.mark.parametrize("engine",["odbc","bcp"])
def test_fresh_group_append_control_outage_keeps_valid_captures_pending(engine):
    if engine=="bcp" and "bcp" not in os.environ.get("DB_INGEST_TEST_ENGINES","odbc").split(","):pytest.skip("Native bcp explicitly opt-in")
    import re,uuid
    group=API["GroupCase"](engine,"strict")
    first=group.case.cli("run","--new-intent","--no-parallel","--non-interactive");original,values=group.verify(first)
    group.a.write_bytes(b"id,note\r\n1,new-a\r\n1,new-a\r\n");group.write_config(subset=True)
    text=group.case.config.read_text(encoding="utf-8")
    text=text.replace('control_database = '+API["API"]["lit"](group.case.d["control_database"]),'control_database = '+API["API"]["lit"]('DbIngestTest_UnavailableContribution_'+uuid.uuid4().hex))
    if re.search(r'^create_databases\s*=',text,re.M):text=re.sub(r'^create_databases\s*=.*$','create_databases = false',text,flags=re.M)
    else:text=text.replace('[destination]\n','[destination]\ncreate_databases = false\n')
    text=text.replace(API["API"]["lit"](str(group.case.folder/'work')),API["API"]["lit"](str(group.case.folder/'outage-fresh')))
    group.case.config.write_text(text,encoding='utf-8')
    refused=group.case.cli('run','--no-parallel','--non-interactive',exit_code=5)
    assert 'GROUP_CENTRAL_HISTORY_UNAVAILABLE' in str(refused)
    assert group.values(original)==values
    assert all(not item.get('receipt') for item in refused.get('groups',[]))
    for item in refused['files']:
        assert item['counts'].get('rejected',0)==0
        assert Path(item['artifact']['path']).read_bytes()==group.a.read_bytes()
