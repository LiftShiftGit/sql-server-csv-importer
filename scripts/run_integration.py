"""Run opt-in integration tests with private child credentials and sanitized evidence."""
import argparse,json,os,platform,subprocess,sys,time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",required=True,type=Path)
    parser.add_argument("--credentials-json",type=Path,help="Private untracked {username,password} file; otherwise DB_USERNAME/DB_PASSWORD")
    parser.add_argument("--expected-major",type=int,choices=(15,16,17),required=True)
    parser.add_argument("--label",default="integration")
    parser.add_argument("--test-path",default="tests/integration")
    parser.add_argument("--engines",default="odbc",choices=("odbc","bcp","odbc,bcp"))
    parser.add_argument("--bcp-path",type=Path)
    for gate in ("batching","parallel","fast","resource","resolution","affected","restore","benchmark","capacity"):
        parser.add_argument("--"+gate,action="store_true",help="Enable the explicit disposable "+gate+" test gate")
    args=parser.parse_args()
    if not args.label.replace("-","").replace("_","").isalnum():parser.error("label must be letters, digits, hyphens or underscores")
    test_path=(ROOT/args.test_path).resolve()
    if not test_path.is_relative_to((ROOT/"tests/integration").resolve()):parser.error("test-path must be under tests/integration")
    config_path=args.config.resolve();config=load_config(config_path,environ={})
    d=config["destination"]
    if any(not str(d[k]).lower().startswith("dbingesttest_") for k in ("database","control_database")):parser.error("Both test databases must start with DbIngestTest_")
    credential=json.loads(args.credentials_json.read_text(encoding="utf-8")) if args.credentials_json else {"username":os.environ.get("DB_USERNAME"),"password":os.environ.get("DB_PASSWORD")}
    password=credential.get("password")
    def sanitize(text):
        if password:
            for value in (password,json.dumps(password)[1:-1],repr(password)[1:-1]):text=text.replace(value,"<redacted>")
        return text
    conn=connect(config,credential.get("username"),password,d.get("bootstrap_database","master"),autocommit=True)
    try:
        import pyodbc
        row=conn.cursor().execute("SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion')),CONVERT(nvarchar(256),SERVERPROPERTY('Edition')),CONVERT(nvarchar(128),SERVERPROPERTY('Collation'))").fetchone()
        major=int(row[0].split(".")[0])
        if major!=args.expected_major:raise RuntimeError(f"Expected actual engine major {args.expected_major}, observed {major}")
        evidence={"server_version":row[0],"edition":row[1],"collation":row[2],"client_os":platform.platform(),"python":sys.version,"pyodbc":pyodbc.version,"driver_name":conn.getinfo(pyodbc.SQL_DRIVER_NAME),"driver_version":conn.getinfo(pyodbc.SQL_DRIVER_VER),"server":d["server"],"authentication":d["authentication"],"requested_tls":{"encrypt":d["encrypt"],"trust_server_certificate":d["trust_server_certificate"]}}
    finally:conn.close()
    docs=ROOT/".integration"/"reports";docs.mkdir(parents=True,exist_ok=True)
    attempt=ROOT/".integration"/(args.label+"-attempt-"+str(time.time_ns()))
    env={k:v for k,v in os.environ.items() if not k.startswith("DB_")}
    env["DB_INGEST_TEST_CONFIG"]=str(config_path)
    env["DB_INGEST_TEST_ENGINES"]=args.engines
    if args.bcp_path:env["DB_INGEST_TEST_BCP_PATH"]=str(args.bcp_path.resolve())
    for gate in ("batching","parallel","fast","resource","resolution","affected","restore","benchmark","capacity"):
        if getattr(args,gate):env["DB_INGEST_TEST_"+gate.upper()]="1"
    if credential.get("username") is not None:env["DB_USERNAME"]=credential["username"]
    if password is not None:env["DB_PASSWORD"]=password
    command=[sys.executable,"-m","pytest",str(test_path),"-q","--tb=short","--basetemp="+str(attempt),"--junitxml="+str(docs/(args.label+"-tests.xml"))]
    started=time.monotonic()
    result=subprocess.run(command,cwd=ROOT,env=env,capture_output=True,text=True,timeout=900,shell=False)
    evidence.update(exit_code=result.returncode,elapsed_seconds=time.monotonic()-started,artifacts=str(attempt))
    output=sanitize(result.stdout+result.stderr)
    (docs/(args.label+"-output.txt")).write_text(output,encoding="utf-8")
    (docs/(args.label+"-environment.json")).write_text(json.dumps(evidence,indent=2),encoding="utf-8")
    print(output)
    return result.returncode

if __name__=="__main__":
    try:raise SystemExit(main())
    except Exception as exc:
        message=str(exc)
        for secret in (os.environ.get("DB_PASSWORD"),):
            if secret:message=message.replace(secret,"<redacted>")
        print(message,file=sys.stderr);raise SystemExit(4)
