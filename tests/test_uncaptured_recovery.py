import pytest
from db_ingest.config import load_config
from db_ingest.pipeline import prepare,execute
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest import capture,publication


def test_capture_failure_cannot_resume_to_empty_clean_run(tmp_path,monkeypatch):
    source=tmp_path/"data.csv";source.write_text("id\n1\n")
    c=load_config(overrides={"parallel.enabled":False},environ={});c["source"].update(root=str(tmp_path),path=str(source));c["parsing"].update(header="present",delimiter=",");c["preservation"].update(work_directory=str(tmp_path/"work"),minimum_free_disk_bytes=1)
    def capture_fail(*a,**k):raise IngestError("CAPTURE_IO","simulated unavailable source",4,{"source":str(source)})
    monkeypatch.setattr(capture,"capture_source",capture_fail)
    monkeypatch.setattr(publication,"publish",lambda *a,**k:(_ for _ in ()).throw(AssertionError("must not publish")))
    report=prepare(c)
    assert report["exit_code"]==4 and not report["files"]
    resumed=execute(c,report["run_id"])
    assert resumed["exit_code"]==4 and resumed["code"]=="PLANNING_INCOMPLETE"
    assert resumed["errors"][0]["code"]=="CAPTURE_IO"


def test_interrupted_inventory_with_saved_good_file_not_clean(tmp_path):
    # A durable run entry alone is not a complete inventory/profile checkpoint.
    c=load_config(overrides={"parallel.enabled":False},environ={});c["preservation"]["work_directory"]=str(tmp_path)
    from db_ingest.config import redact
    j=Journal(tmp_path);j.create_run("interrupted",redact(c));j.close()
    result=execute(c,"interrupted")
    assert result["exit_code"]==2 and result["state"]=="NEEDS_DECISION"


def test_empty_open_failure_recaptures_under_same_intent(tmp_path,monkeypatch):
    source=tmp_path/"orders.csv";raw=b"id,label\n1,original\n";source.write_bytes(raw)
    c=load_config(overrides={"parallel.enabled":False},environ={})
    c["source"].update(root=str(tmp_path),path=str(source))
    c["parsing"].update(header="present",delimiter=",",encoding="utf-8")
    c["preservation"].update(work_directory=str(tmp_path/"work"),minimum_free_disk_bytes=1)
    reader=capture._stable_reader
    def unavailable(*args,**kwargs):raise OSError("synthetic source opening failure")
    monkeypatch.setattr(capture,"_stable_reader",unavailable)
    report=prepare(c)
    assert report["exit_code"]==4
    from contextlib import closing
    with closing(Journal(c["preservation"]["work_directory"])) as journal:
        old=dict(journal.db.execute("SELECT * FROM selections").fetchone())
    monkeypatch.setattr(capture,"_stable_reader",reader)
    from db_ingest.pipeline import recover_plan
    resumed=recover_plan(c,report["run_id"])
    assert resumed["exit_code"]==0
    assert resumed["files"][0]["intent_id"]==old["intent_id"]
    from pathlib import Path
    assert Path(resumed["files"][0]["artifact"]["path"]).read_bytes()==raw
