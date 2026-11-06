import pytest
from db_ingest.schema_diff import compare_columns
from db_ingest.errors import IngestError


def test_complete_schema_diff_is_precise_and_never_infers_defaults():
    before=[{"name":"id","ordinal":1,"type":"int","nullable":False},{"name":"value","ordinal":2,"type":"nvarchar(4)","default":"('a')"},{"name":"removed","ordinal":3,"type":"int"}]
    after=[{"name":"value","ordinal":1,"type":"nvarchar(40)","default":None},{"name":"id","ordinal":2,"type":"bigint","nullable":True},{"name":"added","ordinal":3,"type":"date"}]
    diff=compare_columns(before,after)
    kinds={c["kind"] for c in diff["changes"]}
    assert {"added","removed","reordered","sql_type_changed","nullable_changed","default_changed"}<=kinds
    assert diff["before_hash"]!=diff["after_hash"]
    assert compare_columns(before,before)["changed"] is False


def test_duplicate_target_names_need_mapping():
    with pytest.raises(IngestError) as exc:compare_columns([{"name":"x"},{"name":"x"}],[])
    assert exc.value.code=="SCHEMA_AMBIGUOUS"
