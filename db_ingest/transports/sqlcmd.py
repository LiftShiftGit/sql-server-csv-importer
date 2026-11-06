"""Explicit ODBC sqlcmd argument policy; not a data loader."""
import os
from db_ingest.errors import IngestError

def diagnostic_arguments(config,script,username=None,password=None):
    tools=config.get("tools",{});d=config.get("destination",{})
    if tools.get("sqlcmd_variant","odbc")!="odbc":raise IngestError("SQLCMD_VARIANT_UNSUPPORTED","Go sqlcmd needs its own tested flag adapter")
    from db_ingest.adapters.mssql.connect import tool_info
    from .process_output import output_budget
    budget=output_budget(config)
    tool=tool_info(tools.get("sqlcmd_path","sqlcmd"),["-?"],budget,preview_bytes=budget)
    if tool.get("state")!="DETECTED" or tool.get("output_truncated",False):raise IngestError("SQLCMD_PREREQUISITE","Configured ODBC sqlcmd executable is unavailable",2)
    help_text=tool.get("version_output","")
    modern="-N[s|m|o]" in help_text
    if "-N" not in help_text or "-C" not in help_text:raise IngestError("SQLCMD_PREREQUISITE","Configured sqlcmd help does not expose tested encryption/certificate flags",2)
    args=[tool["path"],"-S",d["server"],"-d",d.get("bootstrap_database","master"),"-b","-x","-l",str(int(d.get("connect_timeout_seconds",15))),"-t",str(int(d.get("statement_timeout_seconds",120))),"-i",str(script)]
    # -X suppresses SQLCMDPASSWORD in this ODBC tool. Clear startup/config
    # variables explicitly; diagnostic scripts are generated/trusted SQL files.
    env={k:v for k,v in os.environ.items() if not k.upper().startswith("SQLCMD")}
    if d.get("authentication","sql")=="integrated":args.append("-E")
    else:
        if username is None or password is None:raise IngestError("CREDENTIALS_REQUIRED","SQL-auth sqlcmd requires explicit credentials")
        args.extend(["-U",username]);env["SQLCMDPASSWORD"]=password
    if str(d.get("encrypt","yes")).lower() in ("yes","true","1"):args.append("-Nm" if modern else "-N")
    elif modern:args.append("-No")
    if str(d.get("trust_server_certificate","no")).lower() in ("yes","true","1"):args.append("-C")
    return args,env
