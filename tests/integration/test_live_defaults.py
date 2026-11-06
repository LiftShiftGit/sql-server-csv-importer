"""Actual default admission/loading policy, with independent duplicate/value readback."""
import csv,json,os,subprocess,sys,tomllib,uuid
from decimal import Decimal
from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Disposable SQL configuration required")

def lit(v):
    if isinstance(v,bool):return "true" if v else "false"
    if isinstance(v,int):return str(v)
    return json.dumps(str(v))

def q(v):return "["+v.replace("]","]]")+"]"

def test_real_unmodified_parallel_fast_defaults(tmp_path):
    d=tomllib.loads(Path(CONFIG).read_text(encoding="utf-8"))["destination"]
    assert all(d[k].lower().startswith("dbingesttest_") for k in ("database","control_database"))
    rows=[[str(i),"Unicode e\u0301 😀  ",str(i)+".0000001"] for i in range(20)]
    rows.append(rows[0])
    for _ in range(2):
        source=tmp_path/("defaults_"+uuid.uuid4().hex+".csv")
        with source.open("w",encoding="utf-8",newline="") as out:
            writer=csv.writer(out);writer.writerow(["id","text","amount"]);writer.writerows(rows)
    sections={"destination":d,"source":{"root":str(tmp_path),"path":str(tmp_path)},"parsing":{"encoding":"utf-8","delimiter":",","header":"present"},"preservation":{"work_directory":str(tmp_path/"work"),"minimum_free_disk_bytes":1}}
    text="\n".join("["+k+"]\n"+"\n".join(a+"="+lit(b) for a,b in v.items()) for k,v in sections.items())
    path=tmp_path/"defaults.toml";path.write_text(text,encoding="utf-8")
    root=Path(__file__).resolve().parents[2]
    env={k:v for k,v in os.environ.items() if not k.startswith("DB_") or k in ("DB_USERNAME","DB_PASSWORD")}
    p=subprocess.run([sys.executable,"-m","db_ingest","--config",str(path),"run","--non-interactive","--json"],cwd=root,env=env,capture_output=True,text=True,timeout=120)
    output=p.stdout+p.stderr
    if env.get("DB_PASSWORD"):output=output.replace(env["DB_PASSWORD"],"<redacted>")
    assert p.returncode==0,output
    report=json.loads(p.stdout)
    assert report["execution_policy"]["requested_workers"]==2
    assert report["execution_policy"]["effective_workers"]==2
    config=load_config(path,environ={})
    assert config["parallel"]["enabled"] is True
    assert config["load"]["preferred_engine"]=="bcp" and config["load"]["fast_executemany"] is True
    assert config["credentials"]["allow_password_in_child_args"] is False
    conn=connect(config,env.get("DB_USERNAME"),env.get("DB_PASSWORD"),d["database"],autocommit=True)
    try:
        assert len(report["files"])==2
        for item in report["files"]:
            assert item["receipt"]["details"]["transport"]["engine"]=="odbc" #Default SQL password policy forbids bcp argv.
            assert item["receipt"]["details"]["staging_batches"]["fast_array_batches"]>0
            plan=item["plan"]
            cols=",".join(q(c["target_name"]) for c in plan["columns"])
            actual=[tuple(r) for r in conn.cursor().execute("SELECT "+cols+" FROM "+q(plan["schema"])+"."+q(plan["table"])+" ORDER BY __ingest_ordinal")]
            assert actual==[(int(r[0]),r[1],Decimal(r[2])) for r in rows]
            ids=[r[0] for r in conn.cursor().execute("SELECT __ingest_record FROM "+q(plan["schema"])+"."+q(plan["table"]))]
            assert len(ids)==len(set(ids))==21
    finally:conn.close()
