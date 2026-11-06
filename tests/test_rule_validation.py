import pytest
from db_ingest.config import load_config,effective_for_file,resolve_columns
from db_ingest.errors import IngestError


def test_same_rule_alias_selectors_cannot_overwrite():
    c=load_config(environ={});c["file_rules"]=[{"glob":"**/*.csv","columns":[{"source_name":"x","target_type":"int"},{"source_ordinal":1,"target_type":"nvarchar(4)"}]}]
    c=effective_for_file(c,"x.csv")
    with pytest.raises(IngestError) as exc:resolve_columns(c,["x"])
    assert exc.value.code=="CONFIG_COLUMN"


def test_later_rule_can_replace_resolved_name_selector():
    c=load_config(environ={});c["file_rules"]=[{"glob":"**/*.csv","columns":[{"source_name":"x","target_type":"int"}]},{"glob":"x.csv","columns":[{"source_ordinal":1,"target_type":"nvarchar(4)","nullable":"false"}]}]
    result=resolve_columns(effective_for_file(c,"x.csv"),["x"])
    assert len(result)==1 and result[0]["target_type"]=="nvarchar(4)" and result[0]["nullable"] is False


def test_rule_binary_policy_must_match_target(tmp_path):
    p=tmp_path/"bad.toml";p.write_text('[[file_rules]]\nglob="**/*.csv"\n[[file_rules.columns]]\nsource_ordinal=1\ntarget_type="nvarchar(20)"\nbinary_encoding="base64"\n')
    with pytest.raises(IngestError) as exc:load_config(p,environ={})
    assert exc.value.code=="CONFIG_COLUMN"
