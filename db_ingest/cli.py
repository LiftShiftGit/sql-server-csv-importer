"""Operator CLI. Passwords never enter plans, journal snapshots or diagnostics."""
import argparse
from dataclasses import asdict
import getpass
import json
import os
from pathlib import Path
import sys
import sqlite3
from .config import EXAMPLE, load_config, redact
from .errors import IngestError

OVERRIDES={"source":"source.path","root":"source.root","include":"source.include","exclude":"source.exclude","recursive":"source.recursive","mode":"load.mode","engine":"load.engine","parallel":"parallel.enabled","workers":"parallel.max_workers","non_interactive":"interaction.non_interactive","encoding":"parsing.encoding","delimiter":"parsing.delimiter","quotechar":"parsing.quotechar","escapechar":"parsing.escapechar","header":"parsing.header","null_token":"parsing.null_tokens","username":"destination.username","batch_rows":"load.batch_rows","batch_bytes":"load.batch_bytes","on_length_overflow":"schema.on_length_overflow"}

def parser():
    p=argparse.ArgumentParser(prog="db-ingest",description="Preserve source bytes, inspect delimited files, and publish verified SQL Server landing tables.",epilog='Example: python -m db_ingest --config ingest.toml run --source D:\\Exports --header present --password-prompt')
    p.add_argument("--config",help="One TOML configuration file (before the command)")
    p.add_argument("--version",action="version",version="db-ingest 0.1.0")
    sub=p.add_subparsers(dest="command",required=True)
    commands=("init-config","doctor","test-connection","inspect","plan","run","resume","status","reconcile","show-errors","export-failures","export-projections","export-mapping","reimport","create-test-files","verify-test-files")
    for command in commands:
        s=sub.add_parser(command,help={"run":"Capture, fully profile and publish", "resume":"Resolve receipts and continue the same intent","inspect":"Bounded provisional source inspection", "plan":"Capture and fully profile, without SQL publication"}.get(command,command))
        s.add_argument("--json",action="store_true",help="Emit a structured JSON report")
        if command=="init-config":
            s.add_argument("--output",default="ingest.toml");s.add_argument("--force",action="store_true");continue
        if command=="create-test-files":
            s.add_argument("--output",required=True);s.add_argument("--type",action="append",default=None);s.add_argument("--destination",default="mssql");s.add_argument("--seed",type=int,default=20261029);s.add_argument("--rows",type=int,default=1000);s.add_argument("--encoding",default="utf-8");s.add_argument("--delimiter",default=",");s.add_argument("--size-bytes",type=int);continue
        if command=="verify-test-files":
            s.add_argument("--manifest",required=True);s.add_argument("--run-id");s.add_argument("--database");s.add_argument("--captures",help="JSON file mapping fixture paths to captured artifact paths; byte verification only")
        for name in ("source","root","encoding","delimiter","quotechar","escapechar"):
            s.add_argument("--"+name,default=None)
        s.add_argument("--header",choices=("auto","present","absent"))
        s.add_argument("--include",action="append");s.add_argument("--exclude",action="append");s.add_argument("--null-token",action="append",help="Repeated values replace configured tokens; empty string means an empty token")
        s.add_argument("--recursive",action=argparse.BooleanOptionalAction,default=None)
        s.add_argument("--parallel",action=argparse.BooleanOptionalAction,default=None)
        s.add_argument("--workers",type=int);s.add_argument("--mode",choices=("append","replace","create_only"));s.add_argument("--engine",choices=("auto","bcp","odbc"));s.add_argument("--non-interactive",action="store_true",default=None)
        s.add_argument("--on-length-overflow",choices=("ask","widen","retype_text","quarantine","abort"),help="Explicit overflow policy; semantic changes require linked reimport")
        s.add_argument("--batch-rows",type=int,help="Maximum rows per transport batch")
        s.add_argument("--batch-bytes",type=int,help="Maximum encoded bytes per transport batch")
        s.add_argument("--username")
        auth=s.add_mutually_exclusive_group();auth.add_argument("--password");auth.add_argument("--password-prompt",action="store_true");auth.add_argument("--password-stdin",action="store_true")
        if command in ("status","resume","reconcile","show-errors","export-failures","export-projections","export-mapping","reimport"):s.add_argument("--run-id",required=command not in ("status","show-errors"))
        if command in ("run","plan","reimport"):s.add_argument("--new-intent",action="store_true")
        if command in ("inspect","plan","export-failures","export-projections","export-mapping"):s.add_argument("--output")
        if command in ("plan","resume"):s.add_argument("--check-target",action="store_true",help="Read-only SQL target snapshot/diff/permissions, sealed for execution drift checks")
        if command=="resume":s.add_argument("--plan-only",action="store_true",help="Recover captured-source planning without contacting SQL Server")
        if command=="test-connection":s.add_argument("--write-probe",action="store_true")
    return p

def credentials(args,config):
    username=args.username or config["destination"].get("username")
    password=args.password if args.password is not None else config["destination"].get("password")
    if args.password_stdin:password=sys.stdin.readline().rstrip("\r\n")
    elif args.password_prompt:password=getpass.getpass("SQL password: ")
    elif password is None and config["destination"]["authentication"]=="sql":
        if sys.stdin.isatty() and not config["interaction"]["non_interactive"]:password=getpass.getpass("SQL password: ")
        else:raise IngestError("CREDENTIALS_REQUIRED","Use --password-stdin, --password-prompt or DB_PASSWORD; noninteractive input cannot prompt")
    return username,password

def dispatch(args):
    if args.command=="init-config":
        output=Path(args.output)
        if output.exists() and not args.force:raise IngestError("CONFIG_EXISTS","Configuration already exists; use --force to overwrite")
        with output.open("w" if args.force else "x",encoding="utf-8",newline="") as f:f.write(EXAMPLE.read_text(encoding="utf-8"))
        return {"path":str(output.resolve()),"exit_code":0}
    if args.command=="create-test-files":
        from .fixturegen import create_test_files
        return create_test_files(args.output,args.type or ["all"],args.destination,args.seed,args.rows,args.encoding,args.delimiter,args.size_bytes)
    overrides={path:getattr(args,name,None) for name,path in OVERRIDES.items()}
    config=load_config(args.config,overrides)
    if args.command=="export-projections":
        if not args.output:raise IngestError("OUTPUT_REQUIRED","export-projections requires an output JSONL path",2)
        from .projections import export
        from .journal import Journal
        journal=Journal(config["preservation"]["work_directory"])
        try:return export(journal,args.run_id,args.output)
        finally:journal.close()
    if args.command=="doctor":
        from .adapters.mssql.connect import doctor
        return doctor(config)
    if args.command=="inspect":
        from .discovery import discover
        from .capture import capture_source
        from .parsers import decide,parse
        # Inspection reads a captured source; archived originals remain retained.
        files=[];report_bytes=0;truncated=False;captured=0;last_capture=None
        report_limit=config["limits"]["max_queue_bytes"]
        for path in discover(config):
            if len(files)>=1000:truncated=True;break
            artifact=capture_source(path,config["source"]["root"],config["preservation"]["work_directory"],config["preservation"]["minimum_free_disk_bytes"])
            captured+=1;last_capture=artifact.capture_id
            effective=load_config(args.config,overrides,relative_path=artifact.relative_path)
            try:
                decision=decide(artifact,effective);samples=[]
                for record in parse(artifact,decision,effective):
                    samples.append({"ordinal":record.ordinal,"kind":record.kind,"field_count":len(record.fields),"errors":record.errors})
                    if len(samples)>=min(config["parsing"]["sample_records"],100):break
                item={"artifact":asdict(artifact),"decision":asdict(decision),"records":samples,"provisional":True}
            except IngestError as exc:item={"artifact":asdict(artifact),"code":exc.code,"message":str(exc),"provisional":True}
            measured=len(json.dumps(item,ensure_ascii=True,separators=(",",":")).encode("utf-8"))
            if report_bytes+measured>report_limit:truncated=True;break
            files.append(item);report_bytes+=measured
        return {"files":files,"provisional":True,"inspected_files":captured,"files_report_truncated":truncated,"last_capture_id":last_capture,"serialized_file_bytes":report_bytes,"report_byte_limit":report_limit,"scope":"Bounded provisional inspection; use plan for a complete durable source inventory","exit_code":2 if truncated or any("code" in f for f in files) or not files else 0}
    from . import pipeline
    if args.command=="status":return pipeline.status(config,args.run_id)
    if args.command=="show-errors":return pipeline.show_errors(config,args.run_id)
    if args.command=="export-mapping":
        if not args.output:raise IngestError("OUTPUT_REQUIRED","export-mapping requires --output (a new CSV filename)")
        from .reporting import export_mapping
        return export_mapping(config,args.run_id,args.output)
    if args.command=="export-failures":
        from .journal import Journal
        if not args.output:raise IngestError("OUTPUT_REQUIRED","export-failures requires --output")
        journal=Journal(config["preservation"]["work_directory"]);count=0
        try:
            journal.run(args.run_id)
            with Path(args.output).open("x",encoding="utf-8") as out:
                for item in journal.files(args.run_id):
                    for record in journal.records(args.run_id,item["file_id"],"rejected"):
                        out.write(json.dumps({"run_id":args.run_id,"artifact":item["artifact"],"record":asdict(record)},ensure_ascii=True)+"\n");count+=1
        finally:journal.close()
        return {"records":count,"output":str(Path(args.output).resolve()),"exit_code":0}
    if args.command=="plan":
        from .decisions import resolve_length_decision
        report=pipeline.prepare(config,args.config,overrides,new_intent=args.new_intent)
        report,config=resolve_length_decision(report,config,args,overrides)
        if args.check_target and report['exit_code']==0:
            from .target_review import review
            username,password=credentials(args,config)
            report=review(config,report,username,password)
        return report
    if args.command=="verify-test-files":
        from .fixturegen import verify_test_files,verify_imported_fixtures
        if args.run_id or args.database:
            if not args.run_id or not args.database or not args.database.lower().startswith(("ingestfixture_","dbingesttest_")):raise IngestError("FIXTURE_DESTINATION_REQUIRED","Supply --run-id and --database with a disposable IngestFixture_ or DbIngestTest_ prefix")
            username,password=credentials(args,config)
            return verify_imported_fixtures(args.manifest,config,args.run_id,args.database,username,password)
        return verify_test_files(args.manifest,captures=json.loads(Path(args.captures).read_text(encoding="utf-8")) if args.captures else None)
    if args.command=="run":
        report=pipeline.prepare(config,args.config,overrides,new_intent=args.new_intent)
        from .decisions import resolve_length_decision
        report,config=resolve_length_decision(report,config,args,overrides)
        if report["exit_code"] and (report.get("global_publication_blocked",True) or not report.get("publishable_files",0)):return report
        try:username,password=credentials(args,config)
        except IngestError as exc:
            exc.context.update(run_id=report["run_id"],next_command="resume --run-id "+report["run_id"])
            raise
        from .decisions import resolve_schema_decision
        return resolve_schema_decision(pipeline.execute(config,report["run_id"],username,password),config,args,username,password)
    if args.command=="resume" and args.plan_only:
        report=pipeline.recover_plan(config,args.run_id)
        if args.check_target and report['exit_code']==0:
            from .target_review import review
            username,password=credentials(args,config)
            report=review(config,report,username,password)
        return report
    if args.command=="resume" and args.check_target:raise IngestError('TARGET_REVIEW_COMMAND','Use resume --plan-only --check-target for read-only review before publication',2)
    username,password=credentials(args,config)
    if args.command=="test-connection":
        from .adapters.mssql.connect import test_connection
        return test_connection(config,username,password,args.write_probe)
    if args.command=="resume":
        from .decisions import resolve_schema_decision
        return resolve_schema_decision(pipeline.execute(config,args.run_id,username,password),config,args,username,password)
    if args.command=="reconcile":return pipeline.reconcile(config,args.run_id,username,password)
    if args.command=="reimport":
        from .journal import Journal
        journal=Journal(config["preservation"]["work_directory"])
        try:
            journal.run(args.run_id)
            report=pipeline.prepare(config,args.config,overrides,linked_run=args.run_id,new_intent=True,artifacts=(i["artifact"] for i in journal.files(args.run_id)))
        finally:journal.close()
        if report["exit_code"] and (report.get("global_publication_blocked",True) or not report.get("publishable_files",0)):return report
        from .decisions import resolve_schema_decision
        return resolve_schema_decision(pipeline.execute(config,report["run_id"],username,password),config,args,username,password)
    raise IngestError("COMMAND_UNKNOWN",args.command)

def main(argv=None):
    args=parser().parse_args(argv)
    try:
        report=dispatch(args)
        if not isinstance(report,dict):report={"result":report}
        output=getattr(args,"output",None)
        text=json.dumps(report,ensure_ascii=True,indent=2,default=str)
        if output and args.command in ("inspect","plan"):Path(output).write_text(text+"\n",encoding="utf-8")
        if sys.stdout.isatty() and not getattr(args,"json",False) and report.get("run_id") and "files" in report:
            from .reporting import console_summary
            print(console_summary(report))
        else:print(text)
        return int(report.get("exit_code",0))
    except IngestError as exc:
        context=exc.context
        def scrub(value):
            if isinstance(value,dict):return {k:scrub(v) for k,v in value.items()}
            if isinstance(value,list):return [scrub(v) for v in value]
            if isinstance(value,str):
                for secret in (getattr(args,"password",None),os.environ.get("DB_PASSWORD")):
                    if secret:value=value.replace(secret,"<redacted>")
            return value
        # Provider diagnostics may echo credentials. Redact every active supplied secret.
        secrets=[getattr(args,"password",None),os.environ.get("DB_PASSWORD")]
        text=json.dumps(scrub({"code":exc.code,"message":str(exc),"context":context,"exit_code":exc.exit_code}),ensure_ascii=True,default=str)
        for secret in secrets:
            if secret:text=text.replace(secret,"<redacted>")
        print(text,file=sys.stderr);return exc.exit_code
    except KeyboardInterrupt:
        print('{"code":"CANCELLED","exit_code":4}',file=sys.stderr);return 4
    except (OSError,ValueError,sqlite3.Error) as exc:
        print(json.dumps({"code":"LOCAL_OPERATION_FAILED","message":str(exc),"exit_code":4}),file=sys.stderr);return 4
