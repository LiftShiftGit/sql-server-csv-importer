"""Doctor filesystem checks use metadata only and never create work artifacts."""
import shutil
from db_ingest.adapters.mssql.filesystem import filesystem_report


def config(root,selected,work,reserve=0):
    return {"source":{"root":str(root),"path":str(selected)},"preservation":{"work_directory":str(work),"minimum_free_disk_bytes":reserve}}


def test_valid_source_and_not_yet_created_work_directory(tmp_path):
    source=tmp_path/"source";source.mkdir();selected=source/"input.csv";selected.write_bytes(b"a,b\n1,2\n")
    work=tmp_path/"new-work"/"nested"
    before=set(tmp_path.rglob("*"))
    result=filesystem_report(config(source,selected,work))
    assert result["exit_code"]==0 and result["read_only"]
    assert result["source_path"]["kind"]=="file" and result["source_root"]["read_access_observed"]
    assert result["work_directory"]["state"]=="MISSING" and not result["work_directory"]["writability_proven"]
    assert result["work_directory"]["nearest_existing_ancestor"]==str(tmp_path)
    assert result["work_directory"]["free_disk_bytes"]>0
    assert set(tmp_path.rglob("*"))==before and selected.read_bytes()==b"a,b\n1,2\n"


def test_missing_source_is_actionable_and_not_created(tmp_path):
    missing=tmp_path/"absent.csv"
    result=filesystem_report(config(tmp_path,missing,tmp_path))
    assert result["exit_code"]==2 and result["source_path"]["state"]=="MISSING"
    assert any(i["code"]=="SOURCE_PATH_UNAVAILABLE" for i in result["issues"])
    assert not missing.exists()


def test_work_path_file_and_file_ancestor_are_invalid(tmp_path):
    selected=tmp_path/"input.csv";selected.write_text("a\n1\n")
    for work in (selected,selected/"child"):
        result=filesystem_report(config(tmp_path,selected,work))
        assert result["exit_code"]==2
        assert any(i["code"]=="WORK_PATH_UNAVAILABLE" for i in result["issues"])
    assert selected.read_text()=="a\n1\n"


def test_actual_disk_reserve_greater_than_free_space(tmp_path):
    reserve=shutil.disk_usage(tmp_path).total+1
    result=filesystem_report(config(tmp_path,tmp_path,tmp_path,reserve))
    assert result["exit_code"]==2 and not result["work_directory"]["minimum_reserve_met"]
    assert any(i["code"]=="WORK_DISK_CAPACITY" for i in result["issues"])


def test_doctor_returns_exit_two_for_filesystem_failure(tmp_path,monkeypatch):
    from db_ingest.adapters.mssql import connect
    from types import SimpleNamespace
    monkeypatch.setattr(connect.importlib,"import_module",lambda name:SimpleNamespace(version="test",drivers=lambda:["ODBC Driver 18 for SQL Server"]))
    cfg=config(tmp_path,tmp_path/"missing",tmp_path)
    cfg["tools"]={"detect_versions":False}
    result=connect.doctor(cfg)
    assert result["selected_driver_available"] and result["exit_code"]==2
    assert result["filesystem"]["source_path"]["state"]=="MISSING"

def test_existing_work_directory_report_has_no_self_reference(tmp_path):
    import json
    selected=tmp_path/'input.csv';selected.write_text('a\n1\n')
    result=filesystem_report(config(tmp_path,selected,tmp_path))
    assert result['exit_code']==0
    encoded=json.dumps(result)
    decoded=json.loads(encoded)
    work=decoded['work_directory']
    assert work['ancestor']['path']==str(tmp_path)
    assert 'ancestor' not in work['ancestor']
