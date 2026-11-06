"""Policy/canonical transport tests, not substitutes for actual SQL oracles."""
import hashlib,json
from pathlib import Path
from types import SimpleNamespace
import pytest
from db_ingest.models import ColumnProfile,RecordEnvelope,FieldValue
from db_ingest.adapters.mssql.evolution import compatible
from db_ingest.transports.bcp import BcpStageLoader,encode_typed_row,typed_insert_sql
from db_ingest.errors import IngestError


def column(kind,nullable=True,**stats):return ColumnProfile(1,"a","a",kind,nullable,stats)


@pytest.mark.parametrize("old,new,allowed",[("int","bigint",True),("bigint","int",False),("nvarchar(2)","nvarchar(max)",True),("nvarchar(max)","nvarchar(4000)",False),("decimal(20,4)","decimal(38,4)",True),("decimal(20,4)","decimal(38,3)",False),("datetime2(3)","datetime2(7)",True),("datetimeoffset(7)","datetimeoffset(3)",False),("decimal(20,4)","decimal(38,10)",True),("decimal(20,4)","decimal(21,10)",False)])
def test_compatible_evolution_is_conservative(old,new,allowed):assert compatible(column(old),column(new)) is allowed


def test_bounded_chunk_preview_keeps_complete_append_only_ledger(tmp_path):
    loader=BcpStageLoader.__new__(BcpStageLoader);loader.chunks=[];loader.chunk_index=0;loader.ledger_events=0;loader.ledger_digest=hashlib.sha256();loader.chunk_ledger=tmp_path/"chunks.jsonl";loader.summary={"chunk_ledger":{}}
    for index in range(1,101):
        loader.chunk_index=index;loader._record_chunk({"index":index,"path":"x"*4000,"state":"SPOOLED"})
    assert len(loader.chunks)<=20
    assert len(json.dumps(loader.chunks).encode())<=65536
    assert loader.summary["chunk_preview_truncated"] is True
    raw=loader.chunk_ledger.read_bytes();assert len(raw.splitlines())==100
    assert loader.summary["chunk_ledger"]["sha256"]==hashlib.sha256(raw).hexdigest()
    assert loader.summary["chunk_ledger"]["chunk_count"]==100


def test_union_missing_wire_remains_distinct_and_guarded():
    record=RecordEnvelope("r",1,None,None,None,None,[FieldValue("MISSING",None)])
    with pytest.raises(IngestError,match="MISSING"):
        encode_typed_row(record,[None],[column("nvarchar(10)")])
    c=column("nvarchar(10)",missing_policy="null_with_provenance")
    document=json.loads(encode_typed_row(record,[None],[c]).split(b"\t")[2])
    assert document["s"]=="M" and document["f"]==[""]
    sql=typed_insert_sql(SimpleNamespace(columns=[c]),"[audit].[raw]")
    assert "IN ('N','M')" in sql


def test_inference_headroom_is_not_an_alter_instruction():
    from db_ingest.adapters.mssql.evolution import projection_compatible
    inferred=column("nvarchar(6)",inference="full-stream",override={},max_utf16_units=5)
    assert projection_compatible(inferred,column("nvarchar(5)"))
    declared=column("nvarchar(6)",inference="full-stream",override={"target_type":"nvarchar(6)"},max_utf16_units=5)
    assert not projection_compatible(declared,column("nvarchar(5)"))


def test_inferred_nullable_observation_does_not_change_storage_constraint():
    from db_ingest.adapters.mssql.evolution import projection_compatible
    candidate=column("nvarchar(6)",inference="full-stream",override={},max_utf16_units=5)
    assert projection_compatible(candidate,column("nvarchar(5)",nullable=False))
    assert not compatible(column("nvarchar(5)",nullable=True),column("nvarchar(5)",nullable=False))


@pytest.mark.parametrize("name",["dbo.backup; DROP TABLE victim", "[dbo].[safe]; DROP TABLE victim", "[dbo].[bad]trailer", "[dbo].unquoted", "x"*518])
def test_retained_generation_never_uses_unquoted_receipt_sql(name):
    from db_ingest.adapters.mssql.generations import generation_target
    with pytest.raises(IngestError):generation_target(name)


def test_retained_generation_identifier_round_trips_escaped_brackets():
    from db_ingest.adapters.mssql.generations import generation_target
    assert generation_target("[sch;]]ema].[table]]name]")=="[sch;]]ema].[table]]name]"


def test_business_invalid_mode_fails_before_connection():
    from unittest.mock import patch
    from db_ingest.config import load_config
    from db_ingest.models import SchemaPlan
    from db_ingest.publication import publish
    config=load_config(environ={});config["load"].update(engine="odbc",mode="merge")
    config["mapping"]["existing_target_strategy"]="mapped_business"
    plan=SchemaPlan("DbIngestTest_Data","dbo","business",[column("int")],"a"*64,"b"*64,"c"*64)
    with patch("db_ingest.publication.connect") as connect,pytest.raises(IngestError) as caught:
        publish(config,plan,[],"intent","run")
    assert caught.value.code=="MODE_INVALID"
    connect.assert_not_called()


def test_business_metadata_probe_fails_before_any_bootstrap_or_staging():
    from unittest.mock import Mock,patch
    from db_ingest.config import load_config
    from db_ingest.models import SchemaPlan
    from db_ingest.publication import publish
    config=load_config(environ={});config["load"].update(engine="odbc",mode="append")
    config["mapping"]["existing_target_strategy"]="mapped_business"
    plan=SchemaPlan("DbIngestTest_Data","dbo","business",[column("int")],"a"*64,"b"*64,"c"*64)
    connection=Mock();cursor=connection.cursor.return_value;cursor.execute.return_value=cursor;cursor.fetchone.return_value=(42,)
    with patch("db_ingest.publication.connect",return_value=connection),patch("db_ingest.publication._bootstrap_database") as create_database,patch("db_ingest.publication._bootstrap_metadata") as metadata,patch("db_ingest.adapters.mssql.business.inspect_target",side_effect=IngestError("BUSINESS_OMISSION_REQUIRED","blocked",2)),pytest.raises(IngestError) as caught:
        publish(config,plan,[],"intent","run")
    assert caught.value.code=="BUSINESS_OMISSION_REQUIRED"
    create_database.assert_not_called();metadata.assert_not_called();connection.close.assert_called_once()
    assert not any("INSERT" in call.args[0] or "CREATE" in call.args[0] for call in cursor.execute.call_args_list)


def test_qualified_target_metadata_limit_precedes_database_access():
    from db_ingest.models import SchemaPlan
    from db_ingest.adapters.mssql.metadata import validate_plan
    plan=SchemaPlan("DbIngestTest_Data","s"*128,"t"*128,[column("int")],"a"*64,"b"*64,"c"*64)
    with pytest.raises(IngestError) as caught:validate_plan(plan)
    assert caught.value.code=="TARGET_METADATA_NAME_LIMIT"
    assert caught.value.context["qualified_target_units"]==261
