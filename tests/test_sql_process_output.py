"""Output limits apply while native pipes are read, not after communicate()."""
import importlib,os,sys,time
from pathlib import Path
import pytest
from db_ingest.transports.process_output import bounded_capture,text_preview


def test_combined_output_and_exact_byte_boundary():
    result=bounded_capture([sys.executable,"-c","import os;os.write(1,b'a'*100);os.write(2,b'b'*100)"],max_bytes=200)
    assert result["state"]=="COMPLETED" and result["output_complete"]
    assert result["output"]==b"a"*100+b"b"*100 and result["bytes_read"]==200


def test_noisy_child_is_killed_after_one_excess_byte():
    code="import os;block=b'x'*4096;[(os.write(1,block),os.write(2,block)) for _ in range(100000)]"
    started=time.monotonic();result=bounded_capture([sys.executable,"-c",code],max_bytes=1024)
    assert result["state"]=="OUTPUT_LIMIT" and not result["output_complete"]
    assert len(result["output"])==1024 and result["bytes_read"]==1025
    assert result["terminated"] and result["exit_code"] is not None and time.monotonic()-started<3


def test_quiet_child_timeout_is_killed_and_waited():
    result=bounded_capture([sys.executable,"-c","import time;time.sleep(60)"],max_bytes=128,timeout_seconds=.15)
    assert result["state"]=="TIMEOUT" and not result["output_complete"]
    assert result["terminated"] and result["exit_code"] is not None and result["output"]==b""


def test_credentials_and_sqlcmd_startup_variables_never_reach_probe():
    env={**os.environ,"DB_PASSWORD":"protected-probe-credential","SQLCMDPASSWORD":"protected-probe-credential","SQLCMDINI":"unexpected.sql"}
    code="import os;print(any(k.upper()=='DB_PASSWORD' or k.upper().startswith('SQLCMD') for k in os.environ))"
    result=bounded_capture([sys.executable,"-c",code],max_bytes=128,env=env)
    assert result["output_complete"] and result["output"].strip()==b"False"


def test_byte_bounded_preview_reports_truncation():
    text,truncated=text_preview(bytes([255])*100,32)
    assert len(text.encode("utf-8"))<=32 and truncated


def test_tool_info_is_unavailable_on_excess_not_detected_from_prefix():
    module=importlib.import_module("db_ingest.adapters.mssql.connect")
    info=module.tool_info(sys.executable,["-c","import os;os.write(1,b'flags -N -C '+b'x'*4096)"],128)
    assert info["state"]=="UNAVAILABLE" and info["output_truncated"]
    assert info["capture"]["state"]=="OUTPUT_LIMIT" and len(info["version_output"].encode())<=128


def test_bcp_incomplete_help_fails_closed_and_cache_key_includes_budget(monkeypatch):
    from db_ingest.transports import bcp
    from db_ingest.transports import process_output
    calls=[]
    def capture(args,max_bytes,timeout_seconds):
        calls.append((args[-1],max_bytes))
        raw=b"Version:17.0.4055.5" if args[-1]=="-v" else b"-Y[s|m|o] -u"
        complete=max_bytes>=64 or args[-1]=="-v"
        return {"state":"COMPLETED" if complete else "OUTPUT_LIMIT","output":raw[:max_bytes],"output_complete":complete,"exit_code":0,"captured_bytes":min(len(raw),max_bytes)}
    monkeypatch.setattr(process_output,"bounded_capture",capture)
    bcp.tool_capabilities.cache_clear()
    low=bcp.tool_capabilities(sys.executable,32);high=bcp.tool_capabilities(sys.executable,64)
    assert not low["available"] and high["available"] and high["tls_flags"]
    assert ("-?",32) in calls and ("-?",64) in calls
    bcp.tool_capabilities.cache_clear()


def test_actual_installed_native_bcp_and_sqlcmd_help_remain_verified():
    from db_ingest.transports.bcp import tool_capabilities
    from db_ingest.transports.sqlcmd import diagnostic_arguments
    from db_ingest.config import load_config
    root=Path(__file__).resolve().parents[1]
    folder=root/".tools/bcp18/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn"
    if not (folder/"bcp.exe").exists() or not (folder/"sqlcmd.exe").exists():pytest.skip("Project-extracted native Microsoft tools unavailable")
    cap=tool_capabilities(str(folder/"bcp.exe"),32768)
    assert cap["available"] and cap["tls_flags"] and cap["help_capture"]["output_complete"]
    config=load_config(environ={});config["tools"]["sqlcmd_path"]=str(folder/"sqlcmd.exe")
    config["destination"].update(authentication="sql",encrypt="yes",trust_server_certificate="no")
    args,env=diagnostic_arguments(config,"trusted.sql","test-user","private")
    assert "-Nm" in args and "-C" not in args and "private" not in args
    config["destination"].update(encrypt="no",trust_server_certificate="yes")
    args,env=diagnostic_arguments(config,"trusted.sql","test-user","private")
    assert "-No" in args and "-C" in args


@pytest.mark.parametrize("mode",["normal","overflow","timeout"])
def test_owned_descendants_exit_on_every_probe_boundary(mode):
    import psutil
    child="import time;time.sleep(60)"
    code=("import subprocess,sys,time,os;"
          "p=subprocess.Popen([sys.executable,'-c',"+repr(child)+"],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);"
          "print(p.pid,flush=True);")
    if mode=="overflow":code+="os.write(1,b'x'*10000);time.sleep(60)"
    elif mode=="timeout":code+="time.sleep(60)"
    result=bounded_capture([sys.executable,"-c",code],max_bytes=128,timeout_seconds=.5 if mode=="timeout" else 10)
    descendant=int(result["output"].splitlines()[0])
    deadline=time.monotonic()+3
    while psutil.pid_exists(descendant) and time.monotonic()<deadline:time.sleep(.01)
    assert not psutil.pid_exists(descendant), "Owned probe descendant survived containment cleanup"
    assert result["state"]=={"normal":"COMPLETED","overflow":"OUTPUT_LIMIT","timeout":"TIMEOUT"}[mode]


def test_tool_buffer_is_independent_of_source_field_caps():
    from db_ingest.transports.process_output import output_budget
    assert output_budget({"limits":{"max_field_bytes":1,"max_record_bytes":1,"output_buffer_bytes":32768}})==32768


@pytest.mark.skipif(os.name!="nt",reason="Windows cached API and handle lifetime regression")
def test_repeated_probes_reuse_ctypes_types_and_release_handles():
    import psutil
    from db_ingest.transports.process_output import _job_api
    process=psutil.Process();api=_job_api();thread_type=api[2]
    before=process.num_handles();hits=_job_api.cache_info().hits
    for _ in range(20):
        result=bounded_capture([sys.executable,"-c","print('ok')"],max_bytes=128)
        assert result["output_complete"] and result["output"].strip()==b"ok"
        assert _job_api()[2] is thread_type
    assert _job_api.cache_info().currsize==1 and _job_api.cache_info().hits>=hits+40
    assert process.num_handles()<=before+2
