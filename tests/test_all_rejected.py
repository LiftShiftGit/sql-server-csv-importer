from db_ingest.config import load_config
from db_ingest.pipeline import prepare,execute,reconcile
from db_ingest import publication
from db_ingest.adapters.mssql import control


def test_all_rejected_backfills_without_fake_destination_receipt(tmp_path,monkeypatch):
    source=tmp_path/"x.csv";source.write_text("id,name\n1,abc\n",encoding="utf-8")
    config=load_config(overrides={"parallel.enabled":False},environ={})
    config["source"].update(root=str(tmp_path),path=str(source))
    config["parsing"].update(header="present",delimiter=",")
    config["schema"]["on_length_overflow"]="quarantine"
    config["audit"]["mirror_receipts_after_commit"]=False #This unit covers reject materialization; central source audit has independent tests.
    config["preservation"].update(work_directory=str(tmp_path/"work"),minimum_free_disk_bytes=1)
    config["file_rules"]=[{"glob":"x.csv","columns":[{"source_ordinal":2,"target_type":"nvarchar(1)"}]}]
    plan=prepare(config);backfills=[]
    def backfill(*args,**kwargs):
        backfills.append(kwargs["table_name"])
        return {"state":"VERIFIED","rejected_records":1}
    monkeypatch.setattr(control,"materialize_failures",backfill)
    monkeypatch.setattr(publication,"publish",lambda *a,**k:(_ for _ in ()).throw(AssertionError("all rejected must not publish")))
    first=execute(config,plan["run_id"])
    assert first["exit_code"]==3
    assert first["files"][0]["receipt"] is None
    assert first["files"][0]["counts"]["plan_artifact_sha256"]==plan["files"][0]["counts"]["plan_artifact_sha256"]
    assert first["files"][0]["failures"]["state"]=="VERIFIED"
    again=execute(config,plan["run_id"])
    assert again["exit_code"]==3
    assert len(backfills)==2 and len(set(backfills))==1
    assert reconcile(config,plan["run_id"])["exit_code"]==3
