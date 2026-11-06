"""Driver-18-only connectivity with explicit requested TLS policy."""
import importlib
import platform
import shutil
import struct
import subprocess
import sys
import uuid
from pathlib import Path
from db_ingest.errors import IngestError
from db_ingest.adapters.base import require_mssql
from .diagnostics import provider_error
from .ddl import quote_identifier

DRIVER_LINK = "https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"

def _boolean(value):
    if isinstance(value,bool): return "yes" if value else "no"
    if str(value).lower() in ("yes","true","1"): return "yes"
    if str(value).lower() in ("no","false","0"): return "no"
    raise IngestError("BOOLEAN_INVALID", "TLS booleans must be yes/no, true/false, or 1/0")

def odbc_value(value):
    text = str(value)
    if "\x00" in text: raise IngestError("CONNECTION_VALUE_INVALID", "ODBC properties cannot contain NUL")
    return "{" + text.replace("}","}}") + "}"

def build_connection_string(config, username=None, password=None, database=None):
    require_mssql(config)
    d = config.get("destination",{})
    props = [("DRIVER",d.get("driver","ODBC Driver 18 for SQL Server")),("SERVER",d.get("server","localhost,1433")),("DATABASE",database or d.get("database","LegacyLanding")),("Encrypt",_boolean(d.get("encrypt","yes"))),("TrustServerCertificate",_boolean(d.get("trust_server_certificate","no"))),("APP","db_ingest")]
    auth = d.get("authentication","sql")
    if auth == "integrated": props.append(("Trusted_Connection","yes"))
    elif auth == "sql":
        if username is None or password is None: raise IngestError("CREDENTIALS_REQUIRED", "SQL authentication requires an explicit username and password (CLI, environment, prompt or stdin)")
        props.extend([("UID",username),("PWD",password)])
    else: raise IngestError("AUTHENTICATION_UNSUPPORTED",f"Unsupported authentication mode {auth!r}")
    return ";".join(key+"="+odbc_value(value) for key,value in props)+";"

def _driver(config):
    try: module = importlib.import_module("pyodbc")
    except ImportError: raise IngestError("PYODBC_MISSING", "Install the declared pyodbc runtime dependency in the project .venv") from None
    name = config.get("destination",{}).get("driver","ODBC Driver 18 for SQL Server")
    drivers = module.drivers()
    if name != "ODBC Driver 18 for SQL Server": raise IngestError("DRIVER_UNSUPPORTED", f"This adapter requires ODBC Driver 18 for SQL Server; configured {name!r}")
    if name not in drivers: raise IngestError("DRIVER_MISSING", f"Selected driver {name!r} is unavailable for {struct.calcsize('P')*8}-bit Python",2,{"detected_drivers":drivers,"setup":DRIVER_LINK})
    return module

def connect(config, username=None, password=None, database=None, autocommit=False):
    driver = _driver(config)
    string = build_connection_string(config,username,password,database)
    conn=None
    try:
        conn = driver.connect(string,autocommit=autocommit,timeout=int(config.get("destination",{}).get("connect_timeout_seconds",15)))
        conn.timeout = int(config.get("destination",{}).get("statement_timeout_seconds",120))
        cursor=conn.cursor()
        try: cursor.execute("SET NOCOUNT ON; SET XACT_ABORT ON")
        finally: cursor.close()
        return conn
    except Exception as exc:
        if conn is not None:
            try:conn.close()
            except Exception:pass
        raise provider_error(exc,"connect",(password,string)) from None

def tool_info(path,args,max_bytes=32768,preview_bytes=16384):
    from db_ingest.transports.process_output import bounded_capture,text_preview,capture_metadata
    exe=shutil.which(path)
    if not exe:return {"path":None,"state":"UNAVAILABLE"}
    try:
        result=bounded_capture([exe,*args],max_bytes=max_bytes,timeout_seconds=10)
        output,truncated=text_preview(result["output"],min(preview_bytes,max_bytes))
        info={"path":exe,"state":"DETECTED" if result["output_complete"] else "UNAVAILABLE","version_output":output,"output_truncated":truncated or not result["output_complete"],"exit_code":result["exit_code"],"capture":capture_metadata(result)}
        if not result["output_complete"]:info["reason"]="Tool probe exceeded its output budget" if result["state"]=="OUTPUT_LIMIT" else "Tool probe exceeded its 10-second deadline"
        return info
    except OSError as error:return {"path":exe,"state":"UNAVAILABLE","reason":str(error)}


def doctor(config):
    result = {"python":sys.version,"bitness":struct.calcsize("P")*8,"platform":platform.platform(),"server_integration":"NOT_EXECUTED","selected_driver":config.get("destination",{}).get("driver","ODBC Driver 18 for SQL Server")}
    try:
        module = importlib.import_module("pyodbc")
        result.update(pyodbc_version=module.version,odbc_drivers=module.drivers())
    except ImportError: result.update(pyodbc_version=None,odbc_drivers=[],runtime_dependency="PYODBC_MISSING")
    tools = config.get("tools",{})
    if tools.get("detect_versions",True):
        from db_ingest.transports.process_output import output_budget
        result["bcp"] = tool_info(tools.get("bcp_path","bcp"),["-v"],output_budget(config))
        result["sqlcmd"] = tool_info(tools.get("sqlcmd_path","sqlcmd"),["-?"],output_budget(config))
    else: result["tool_versions"] = "Detection disabled by configuration"
    result["tls"] = {"encrypt":_boolean(config.get("destination",{}).get("encrypt","yes")),"trust_server_certificate":_boolean(config.get("destination",{}).get("trust_server_certificate","no")),"negotiated":"not independently observed"}
    result["selected_driver_available"] = result["selected_driver"] == "ODBC Driver 18 for SQL Server" and result["selected_driver"] in result["odbc_drivers"]
    from .filesystem import filesystem_report
    result["filesystem"] = filesystem_report(config)
    result["exit_code"] = 0 if result["selected_driver_available"] and result["pyodbc_version"] and result["filesystem"]["exit_code"]==0 else 2
    return result

def test_connection(config, username=None, password=None, write_probe=False):
    d=config.get("destination",{})
    databases=list(dict.fromkeys([d.get("bootstrap_database","master"),d.get("database","LegacyLanding"),d.get("control_database","IngestControl")]))
    result={"databases":[],"read_only":not write_probe,"tls":"requested policy; negotiated state not independently observed"}
    for database in databases:
        conn=None
        try:
            conn=connect(config,username,password,database,autocommit=not write_probe)
            cur=conn.cursor()
            row=cur.execute("SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion')), CONVERT(nvarchar(256),SERVERPROPERTY('Edition')), CONVERT(int,SERVERPROPERTY('EngineEdition')), DB_NAME(), compatibility_level, collation_name FROM sys.databases WHERE database_id=DB_ID()").fetchone()
            entry={"database":database,"state":"CONNECTED","product_version":row[0],"edition":row[1],"engine_edition":row[2],"observed_database":row[3],"compatibility_level":row[4],"collation":row[5],"engine_certification":"NOT_INTEGRATION_TESTED"}
            if int(str(row[0]).split('.')[0])<15: raise IngestError("ENGINE_TOO_OLD","SQL Server 2019 (15.x) or newer is required")
            try:
                provider=importlib.import_module("pyodbc")
                entry["driver"]={"name":conn.getinfo(provider.SQL_DRIVER_NAME),"version":conn.getinfo(provider.SQL_DRIVER_VER),"bitness":struct.calcsize("P")*8}
                session=cur.execute("SELECT encrypt_option,auth_scheme,net_transport,protocol_type FROM sys.dm_exec_connections WHERE session_id=@@SPID").fetchone()
                entry["session_connection"]={"state":"OBSERVED","encrypt_option":session[0],"authentication":session[1],"net_transport":session[2],"protocol_type":session[3]} if session else {"state":"NOT_OBSERVED","reason":"No visible current-session DMV row"}
            except Exception as exc:
                observed=provider_error(exc,"optional current-session TLS/driver observation",(password,))
                entry["session_connection"]={"state":"NOT_OBSERVED","reason":str(observed)}
            entry["permissions"]={}
            for permission in ("CONNECT","CREATE TABLE","CREATE SCHEMA"):
                entry["permissions"][permission]=cur.execute("SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', ?)",permission).fetchone()[0]
            from .permissions import observe,schemas,schema_probe
            entry["permission_observations"]=observe(cur,config,database)
            if write_probe:
                conn.rollback() # End read-only observation transactions before scratch DDL.
                probes=[schema_probe(conn,name,password) for name in schemas(config,database)]
                entry["write_probes"]=probes
                entry["write_probe"]="PROVEN_REAL_SCHEMA_ROLLBACK" if probes and all(p["state"]=="PROVEN" for p in probes) else "NOT_APPLICABLE" if not probes else "FAILED_OR_UNAVAILABLE"
            result["databases"].append(entry)
        except IngestError as exc: result["databases"].append({"database":database,"state":"INACCESSIBLE","code":exc.code,"message":str(exc),"context":exc.context})
        except Exception as exc:
            error=provider_error(exc,"connection capability probe",(password,))
            result["databases"].append({"database":database,"state":"INACCESSIBLE","code":error.code,"message":str(error),"context":error.context})
        finally:
            if conn is not None:
                try:
                    if write_probe:conn.rollback()
                finally:conn.close()
    if any(x.get("session_connection",{}).get("state")=="OBSERVED" for x in result["databases"]):
        result["tls"]="Server-reported session encryption observed per database; certificate policy remains explicitly configured"
    result["ok"]=all(x["state"]=="CONNECTED" and (not write_probe or x.get("write_probe") in ("PROVEN_REAL_SCHEMA_ROLLBACK","NOT_APPLICABLE")) for x in result["databases"])
    preflight={"PYODBC_MISSING","DRIVER_MISSING","DRIVER_UNSUPPORTED","CREDENTIALS_REQUIRED","ENGINE_TOO_OLD"}
    result["exit_code"] = 0 if result["ok"] else 2 if any(x.get("code") in preflight for x in result["databases"]) else 4
    return result
