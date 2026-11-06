from pathlib import Path
import tomllib
import pytest
from db_ingest.config import EXAMPLE,DEFAULTS,load_config
from db_ingest.cli import parser,OVERRIDES
from db_ingest.errors import IngestError


def test_concise_example_uses_complete_validated_defaults():
    example=tomllib.loads(EXAMPLE.read_text(encoding="utf-8"))
    defaults=tomllib.loads(DEFAULTS.read_text(encoding="utf-8"))
    assert len(EXAMPLE.read_text(encoding="utf-8").splitlines())<90
    config=load_config(EXAMPLE,environ={})
    assert config["limits"]["max_in_memory_record_bytes"]==defaults["limits"]["max_in_memory_record_bytes"]
    assert "password" not in config["destination"]
    assert config["parsing"]["header"]=="auto"


def test_parallel_and_batch_controls_are_configurable():
    config=load_config(overrides={"parallel.enabled":True,"parallel.max_workers":4,"limits.total_process_tree_memory_bytes":1024**3,"limits.worker_memory_bytes":160*1024**2,"load.batch_rows":7,"load.batch_bytes":65536},environ={})
    assert config["parallel"]["max_workers"]==4 and config["load"]["batch_rows"]==7
    args=parser().parse_args(["run","--batch-rows","7","--batch-bytes","65536","--parallel","--workers","4"])
    assert args.batch_rows==7 and args.batch_bytes==65536 and args.parallel is True
    assert parser().parse_args(["resume","--run-id","r","--plan-only"]).plan_only


def test_tiny_worker_and_queue_budgets_fail_before_work():
    for overrides in ({"limits.worker_memory_bytes":1024},{"limits.max_queue_bytes":12},{"limits.total_process_tree_memory_bytes":1024}):
        with pytest.raises(IngestError) as exc:load_config(overrides=overrides,environ={})
        assert exc.value.code=="CONFIG_LIMIT"


def test_tab_spelling_is_convenient_cli_override():
    assert load_config(overrides={"parsing.delimiter":r"\t"},environ={})["parsing"]["delimiter"]=="\t"


@pytest.mark.parametrize("key,value",[("replace_scope","selected_subset"),("allow_empty_replace",True),("partial_publish","none")])
def test_publication_policy_changes_require_new_semantic_intent(key,value):
    from db_ingest.config import hashes
    original=load_config(environ={})
    modified=load_config(overrides={"load."+key:value},environ={})
    assert hashes(original)["mapping"]!=hashes(modified)["mapping"]
    from db_ingest.pipeline import _guard_resume_config
    with pytest.raises(IngestError) as error:_guard_resume_config(modified,original)
    assert error.value.code=="RESUME_SEMANTIC_CHANGE"


@pytest.mark.parametrize("key,value",[("statement_timeout_seconds",False),("connect_timeout_seconds",0),("statement_timeout_seconds",2147483648)])
def test_connection_timeouts_are_positive_real_integers(key,value):
    with pytest.raises(IngestError):load_config(overrides={"destination."+key:value},environ={})



def test_configuration_read_is_capped_before_toml_materialization(tmp_path):
    path=tmp_path/"large.toml"
    with path.open("wb") as stream:stream.truncate(8388609)
    with pytest.raises(IngestError) as error:load_config(path,environ={})
    assert error.value.code=="CONFIG_LIMIT"


def test_named_business_mapping_is_deliberate_and_quotes_later():
    from db_ingest.planning import map_destination
    config=load_config(overrides={"mapping.name_collision":"stable_hash_suffix","mapping.existing_target_strategy":"mapped_business","mapping.target_table":"既存] target ; $()"},environ={})
    assert map_destination(config,"a.csv")["table"]=="既存] target ; $()"
    with pytest.raises(IngestError):load_config(overrides={"mapping.target_table":"business"},environ={})


@pytest.mark.parametrize("key,value",[("schema.leading_zero_policy","typo"),("load.preferred_engine","typo"),("load.fallback_engine","none"),("schema.text_length_headroom_percent",-1),("schema.text_length_headroom_percent",True)])
def test_semantic_policy_typos_cannot_change_identifier_meaning_or_silently_fallback(key,value):
    with pytest.raises(IngestError):load_config(overrides={key:value},environ={})


@pytest.mark.parametrize("key,value",[("delimiter","none"),("delimiter","\n"),("escapechar","auto"),("quotechar",",")])
def test_known_invalid_dialect_fails_before_expensive_source_capture(key,value):
    with pytest.raises(IngestError) as error:load_config(overrides={"parsing.delimiter":",","parsing."+key:value},environ={})
    assert error.value.code=="CONFIG_DIALECT"
