from db_ingest.config import load_config
from db_ingest.pipeline import prepare,execute
from db_ingest.errors import IngestError
import pytest


def test_unavailable_policies_never_silently_accepted():
    for path,value in (("preservation.hash","sha1"),("parsing.repair_policy","guess")):
        with pytest.raises(IngestError) as exc:load_config(overrides={path:value},environ={})
        assert exc.value.code in ("CONFIG_CAPABILITY_UNAVAILABLE","CONFIG_LIMIT")


def test_explicit_length_ask_is_resumable_decision(tmp_path):
    source=tmp_path/"x.csv";source.write_text("id,name\n1,abcd\n",encoding="utf-8")
    config=load_config(environ={})
    config["source"].update(root=str(tmp_path),path=str(source))
    config["parsing"].update(header="present",delimiter=",")
    config["preservation"].update(work_directory=str(tmp_path/"work"),minimum_free_disk_bytes=1)
    config["file_rules"]=[{"glob":"x.csv","columns":[{"source_ordinal":2,"target_type":"nvarchar(2)"}]}]
    report=prepare(config)
    assert report["exit_code"]==2
    assert report["files"][0]["state"]=="NEEDS_DECISION"
    assert report["files"][0]["counts"]["pending"]==1
    result=execute(config,report["run_id"])
    assert result["exit_code"]==2


def test_explicit_length_quarantine_keeps_exact_archive(tmp_path):
    source=tmp_path/"x.csv";raw=b"id,name\n1,abcd\n";source.write_bytes(raw)
    config=load_config(environ={})
    config["source"].update(root=str(tmp_path),path=str(source))
    config["parsing"].update(header="present",delimiter=",")
    config["preservation"].update(work_directory=str(tmp_path/"work"),minimum_free_disk_bytes=1)
    config["schema"]["on_length_overflow"]="quarantine"
    config["file_rules"]=[{"glob":"x.csv","columns":[{"source_ordinal":2,"target_type":"nvarchar(2)"}]}]
    result=prepare(config)
    assert result["exit_code"]==0
    assert result["files"][0]["counts"]["rejected"]==1
    from pathlib import Path
    assert Path(result["files"][0]["artifact"]["path"]).read_bytes()==raw


def test_unmeasured_memory_budgets_do_not_silently_override():
    for path,value in (("limits.total_process_tree_memory_bytes",1),("limits.worker_memory_bytes",1),("limits.max_in_memory_field_bytes",1024**3)):
        with pytest.raises(IngestError) as exc:load_config(overrides={path:value},environ={})
        assert exc.value.code in ("CONFIG_CAPABILITY_UNAVAILABLE","CONFIG_LIMIT")
