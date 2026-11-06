"""Versioned delimiter-safe transport codecs and isolated bcp raw staging.

Base64's alphabet excludes tab/newline. Explicit one-character state tags distinguish
NULL/MISSING/EMPTY/VALUE; a UTF-8 record ID and each field retain exact lexical content.
No raw CSV is passed to bcp. This codec is not evidence of a working bcp SQL loader.
"""
import base64
import re
from db_ingest.errors import IngestError
from db_ingest.models import FieldValue

STATES={"VALUE":"V","EMPTY":"E","NULL":"N","MISSING":"M"}

def encode_record(record):
    rid=base64.b64encode(record.record_id.encode("utf-8"))
    parts=[b"D1",rid,str(record.ordinal).encode("ascii")]
    for field in record.fields:
        tag=STATES.get(field.state)
        if tag is None:raise IngestError("FIELD_STATE","Unknown state in bcp spool")
        payload=base64.b64encode((field.text or "").encode("utf-8"))
        parts.append(tag.encode()+ (b"1" if field.quoted else b"0") + payload)
    return b"\t".join(parts)+b"\n"

def decode_record(line):
    try:
        if not line.endswith(b"\n") or b"\n" in line[:-1] or b"\r" in line:raise ValueError("Transport row framing is invalid")
        parts=line[:-1].split(b"\t")
        if parts[0]!=b"D1" or len(parts)<3:raise ValueError("Unknown transport version")
        rid=base64.b64decode(parts[1],validate=True).decode("utf-8")
        if not re.fullmatch(rb"[1-9][0-9]*",parts[2]):raise ValueError("Invalid ordinal")
        fields=[];reverse={v:k for k,v in STATES.items()}
        for raw in parts[3:]:
            if len(raw)<2 or raw[:1].decode("ascii") not in reverse or raw[1:2] not in (b"0",b"1"):raise ValueError("Invalid state tag")
            state=reverse[raw[:1].decode("ascii")]
            text=base64.b64decode(raw[2:],validate=True).decode("utf-8")
            if state in ("NULL","MISSING") and text:raise ValueError("NULL/MISSING payload must be absent")
            fields.append(FieldValue(state,None if state in ("NULL","MISSING") else text,raw[1:2]==b"1"))
        return rid,int(parts[2]),fields
    except (ValueError,UnicodeError,IndexError) as exc:raise IngestError("BCP_TRANSPORT_INVALID",f"Malformed canonical transport: {exc}") from None

def tls_flags(capability,encrypt="yes",trust_server_certificate="no"):
    supported=bool(capability.get("tls_flags")) if isinstance(capability,dict) else int(capability)>=18
    if not supported:raise IngestError("BCP_TLS_VERSION","Installed help must advertise -Y/-u; numeric package/engine versions alone do not prove the TLS adapter")
    flags=["-Ym" if encrypt=="yes" else "-Yo"]
    if trust_server_certificate=="yes":flags.append("-u")
    return flags


# D2 is a typed canonical payload. All source values are Base64 encoded; no arbitrary
# CSV or source-derived SQL is passed to bcp. Raw rows are never business rows.
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid
from functools import lru_cache
from db_ingest.adapters.mssql.ddl import qualified,quote_identifier
from db_ingest.adapters.mssql.types import parse_sql_type,parameter_value
from db_ingest.adapters.mssql.text import conversion
from db_ingest.resources import checkpoint
from db_ingest.capture import _atomic_replace,_sync_directory

@lru_cache(maxsize=16)
def tool_capabilities(path,max_bytes=32768):
    from .process_output import bounded_capture,text_preview,capture_metadata
    executable=shutil.which(path)
    if not executable:return {"available":False,"reason":"Executable not found","path":str(path)}
    try:
        version=bounded_capture([executable,"-v"],max_bytes=max_bytes,timeout_seconds=10)
        if not version["output_complete"]:return {"available":False,"path":str(executable),"reason":"bcp version probe incomplete: "+version["state"],"version_capture":capture_metadata(version)}
        help_result=bounded_capture([executable,"-?"],max_bytes=max_bytes,timeout_seconds=10)
        if not help_result["output_complete"]:return {"available":False,"path":str(executable),"reason":"bcp help probe incomplete: "+help_result["state"],"help_capture":capture_metadata(help_result)}
        version_text,version_truncated=text_preview(version["output"],min(16384,max_bytes))
        help_text=help_result["output"].decode("utf-8",errors="backslashreplace")
        match=re.search(r"Version:\s*([0-9.]+)",version_text,re.I)
        has_tls=bool(re.search(r"-Y\[?s\|m\|o",help_text)) and "-u" in help_text
        return {"available":True,"path":str(Path(executable).resolve()),"version":match[1] if match else version_text.strip(),"version_preview_truncated":version_truncated,"tls_flags":has_tls,"help_sha256":hashlib.sha256(help_result["output"]).hexdigest(),"version_capture":capture_metadata(version),"help_capture":capture_metadata(help_result)}
    except OSError as error:return {"available":False,"path":str(executable),"reason":str(error)}


def prerequisite(config):
    from .process_output import output_budget
    cap=tool_capabilities(config.get("tools",{}).get("bcp_path","bcp"),output_budget(config))
    if not cap["available"]:return False,"bcp probe is unavailable: "+cap.get("reason","unknown capability"),cap
    if not cap.get("tls_flags"):return False,"Installed bcp help does not advertise tested -Y/-u TLS controls",cap
    schema=config.get("audit",{}).get("control_schema","audit")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",schema):return False,"Native bcp raw-stage schema requires a simple ASCII identifier",cap
    auth=config.get("destination",{}).get("authentication","sql")
    if auth=="sql" and not config.get("credentials",{}).get("allow_password_in_child_args",False):
        return False,"SQL-auth bcp -P requires credentials.allow_password_in_child_args=true; SQLCMDPASSWORD is not a bcp credential path",cap
    if auth not in ("sql","integrated"):return False,"Authentication mode has no bcp adapter",cap
    return True,"Native bcp TLS/authentication prerequisites satisfied",cap


def encode_typed_row(record,values,columns):
    data=[];states=[]
    for index,(value,column) in enumerate(zip(values,columns,strict=True)):
        binary=parse_sql_type(column.sql_type).base=="varbinary"
        if value is None:raw=b""
        elif binary:raw=bytes(value)
        else:raw=str(parameter_value(value)).encode("utf-16-le")
        missing=record.fields[index].state=="MISSING"
        if missing and not (column.nullable and column.stats.get("missing_policy")=="null_with_provenance"):
            raise IngestError("MISSING_VALUE","Canonical MISSING requires explicit nullable union projection")
        states.append("M" if missing else "N" if value is None else "E" if raw==b"" else "V")
        data.append(base64.b64encode(raw).decode("ascii"))
    # One short state vector + one MAX Base64 field per source column supports the
    # 1021-business-column layout without exceeding OPENJSON's 1024 WITH columns.
    payload=json.dumps({"v":3,"s":"".join(states),"f":data},separators=(",",":"),ensure_ascii=True)
    token=base64.b64encode(record.record_id.encode("utf-16-le")).decode("ascii")
    body=(token+"\t"+str(record.ordinal)+"\t"+payload).encode("ascii")
    digest=hashlib.sha256(body).hexdigest()
    return body+b"\t"+digest.encode("ascii")+b"\n"


def typed_insert_sql(plan,raw_target,typed_target="#db_ingest_stage"):
    q=quote_identifier
    names=",".join(q(c.target_name) for c in plan.columns)
    expressions=[];json_fields=["[states] varchar(1024) '$.s'"]
    for index,column in enumerate(plan.columns):
        spec=parse_sql_type(column.sql_type)
        json_fields.append(f"[b{index}] varchar(max) '$.f[{index}]'")
        decoded=f"CAST(N'' AS xml).value('xs:base64Binary(sql:column(\"j.b{index}\"))','varbinary(max)')"
        value=decoded if spec.base=="varbinary" else f"CONVERT(nvarchar(max),{decoded})"
        null_states="'N','M'" if column.nullable and column.stats.get("missing_policy")=="null_with_provenance" else "'N'"
        expressions.append(f"CASE WHEN SUBSTRING(j.states,{index+1},1) IN ({null_states}) THEN CONVERT({spec},NULL) ELSE "+conversion(column,value)+" END")
    rid="CAST(N'' AS xml).value('xs:base64Binary(sql:column(\"r.record_token\"))','varbinary(max)')"
    validation=f"IF EXISTS (SELECT 1 FROM {raw_target} WHERE ISNULL(JSON_VALUE(payload,'$.v'),'')<>'3' OR ISNULL(LEN(JSON_VALUE(payload,'$.s')),-1)<>{len(plan.columns)}) THROW 51003,'Canonical transport version/state vector mismatch',1; "
    return validation+f"INSERT INTO {typed_target} ({names},[__ingest_intent],[__ingest_record],[__ingest_ordinal]) SELECT "+",".join(expressions)+f",?,CONVERT(nvarchar(128),{rid}),r.ordinal FROM {raw_target} AS r CROSS APPLY OPENJSON(r.payload) WITH ("+",".join(json_fields)+") AS j OPTION (MAXDOP 1, RECOMPILE)"


def _redact_artifact(path,password):
    if not password or not path.exists():return
    patterns=[password.encode("utf-8"),password.encode("utf-16-le")]
    patterns=list(dict.fromkeys(p for p in patterns if p))
    overlap=max(map(len,patterns));replacement=b"<redacted>";temporary=path.with_suffix(path.suffix+".redacting")
    with path.open("rb") as source,temporary.open("wb") as out:
        pending=b""
        while True:
            block=source.read(65536);pending+=block
            safe=len(pending) if not block else max(0,len(pending)-overlap+1)
            cursor=0
            while cursor<safe:
                hits=[(pending.find(pattern,cursor),pattern) for pattern in patterns]
                hits=[h for h in hits if h[0]>=0 and h[0]<safe]
                if not hits:out.write(pending[cursor:safe]);cursor=safe;break
                at,pattern=min(hits,key=lambda h:h[0]);out.write(pending[cursor:at]);out.write(replacement);cursor=at+len(pattern)
            pending=pending[cursor:]
            if not block:break
        out.flush();os.fsync(out.fileno())
    _atomic_replace(temporary,path);_sync_directory(path.parent)


class BcpStageLoader:
    """One attempt owns one permanent raw table and durable bounded chunk spools."""
    def __init__(self,config,plan,conn,transport,intent_id,run_id,username,password):
        self.config,self.plan,self.conn,self.transport=config,plan,conn,transport
        self.intent_id,self.username,self.password=str(intent_id),username,password
        self.limit_rows=int(config.get("load",{}).get("batch_rows",5000));self.limit_bytes=int(config.get("load",{}).get("batch_bytes",8388608))
        attempt=uuid.uuid4().hex
        self.root=Path(config["preservation"]["work_directory"])/"transport"/str(run_id)/attempt
        self.root.mkdir(parents=True,exist_ok=False)
        self.manifest=self.root/"manifest.json"
        schema=config.get("audit",{}).get("control_schema","audit")
        if transport.get("engine")=="bcp" and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",schema):raise IngestError("BCP_SCHEMA_IDENTIFIER","Native bcp raw-stage schema currently requires a simple ASCII identifier; select ODBC for this control schema")
        self.bcp_target=schema+".__bcp_"+attempt
        self.raw=qualified(schema,"__bcp_"+attempt)
        self.chunks=[];self.chunk_index=0;self.ledger_events=0;self.ledger_digest=hashlib.sha256();self.stream=None;self.chunk_path=None;self.rows=0;self.bytes=0;self.created=False;self.admission=None
        self.chunk_ledger=self.root/"chunks.jsonl"
        with self.chunk_ledger.open("xb") as ledger:ledger.flush();os.fsync(ledger.fileno())
        self.summary={"count":0,"max_rows":0,"max_bytes":0,"limit_rows":self.limit_rows,"limit_bytes":self.limit_bytes,"kind":"encoded bcp spool bytes","raw_table":self.raw,"manifest":str(self.manifest),"decode":"SQL OPENJSON/MAX + XML Base64 UTF-16LE/binary; exact typed stage readback","chunks":self.chunks,"chunk_ledger":{"path":str(self.chunk_ledger),"events":0,"chunk_count":0,"sha256":self.ledger_digest.hexdigest()},"chunk_preview_limit":20,"chunk_preview_byte_limit":65536,"server_decode_admission":1,"admission_database":config.get("destination",{}).get("bootstrap_database","master"),"decode_plan_policy":"one OPENJSON schema; MAXDOP1; RECOMPILE","wire_version":3,"bcp_commit_rows":min(self.limit_rows,128)}
        self._save("CREATING")
        cur=self.conn.cursor()
        compatibility=cur.execute("SELECT compatibility_level FROM sys.databases WHERE database_id=DB_ID()").fetchone()[0]
        self.server_decode=len(plan.columns)<=128 and compatibility>=130
        if not self.server_decode:self.summary["decode"]="Bounded client JSON/Base64/UTF16 exact decode + ordinary ODBC typed groups (wide layout or unavailable OPENJSON); full typed stage readback"
        cur.execute(f"CREATE TABLE {self.raw} (record_token varchar(1024) NOT NULL,ordinal bigint NOT NULL PRIMARY KEY,payload varchar(max) NOT NULL,row_sha256 char(64) NOT NULL)")
        self.conn.commit();self.created=True;self._save("CREATED")
    def _record_chunk(self,chunk):
        record=(json.dumps(chunk,ensure_ascii=True,separators=(",",":"))+"\n").encode("utf-8")
        with self.chunk_ledger.open("ab") as ledger:ledger.write(record);ledger.flush();os.fsync(ledger.fileno())
        self.ledger_digest.update(record);self.ledger_events+=1
        self.summary["chunk_ledger"].update(events=self.ledger_events,chunk_count=self.chunk_index,sha256=self.ledger_digest.hexdigest())
        if chunk["index"]<=20 and not any(item["index"]==chunk["index"] for item in self.chunks):self.chunks.append(chunk)
        while self.chunks and len(json.dumps(self.chunks,ensure_ascii=True).encode("utf-8"))>65536:self.chunks.pop()
        self.summary["chunk_preview_truncated"]=self.chunk_index>len(self.chunks)
    def _save(self,state,**details):
        # Per-chunk progress snapshots are constant-size; detailed events append once.
        # Only final/failure snapshots include the bounded diagnostic preview.
        summary=self.summary if state in ("VERIFIED","FAILED","TYPED_STAGE_COMPLETE") else {**self.summary,"chunks":[]}
        payload={"version":3,"intent_id":self.intent_id,"state":state,"raw_table":self.raw,"summary":summary,**details}
        temp=self.manifest.with_suffix(".tmp")
        with temp.open("w",encoding="utf-8") as stream:json.dump(payload,stream,ensure_ascii=True,indent=2);stream.flush();os.fsync(stream.fileno())
        _atomic_replace(temp,self.manifest);_sync_directory(self.manifest.parent)
    def add(self,record,values):
        checkpoint()
        row=encode_typed_row(record,values,self.plan.columns)
        if len(row)>self.limit_bytes:raise IngestError("BCP_RECORD_BUDGET","One reversible bcp record exceeds load.batch_bytes; increase that explicit bound or select ODBC",2,{"transport_bytes":len(row),"limit_bytes":self.limit_bytes})
        if self.rows and (self.rows>=self.limit_rows or self.bytes+len(row)>self.limit_bytes):self.flush()
        if self.stream is None:
            self.chunk_path=self.root/f"chunk_{self.chunk_index+1:08d}.bcp"
            self.stream=self.chunk_path.open("xb");self.rows=0;self.bytes=0
        reserve=int(self.config.get("preservation",{}).get("minimum_free_disk_bytes",0))
        if shutil.disk_usage(self.root).free<len(row)+reserve:raise IngestError("DISK_CAPACITY","Insufficient free disk reserve for durable canonical transport; no target publication",4)
        self.stream.write(row);self.rows+=1;self.bytes+=len(row)
    def _arguments(self,chunk,stdout,stderr,error_path):
        d=self.config["destination"];cap=self.transport["tool"]
        args=[cap["path"],self.bcp_target,"in",str(chunk),"-S",d["server"],"-d",self.plan.database,"-c","-t","0x09","-r","0x0a","-b",str(min(self.limit_rows,128)),"-m","1","-e",str(error_path),"-l",str(d.get("connect_timeout_seconds",15)),"-q","-Ym" if str(d.get("encrypt","yes")).lower() in ("yes","true","1") else "-Yo"]
        if str(d.get("trust_server_certificate","no")).lower() in ("yes","true","1"):args.append("-u")
        if d.get("authentication","sql")=="integrated":args.append("-T")
        else:
            if self.username is None or self.password is None:raise IngestError("CREDENTIALS_REQUIRED","SQL-auth bcp requires explicit credentials")
            if not self.config.get("credentials",{}).get("allow_password_in_child_args",False):raise IngestError("BCP_CREDENTIAL_POLICY","bcp argv password was not authorized by configuration")
            args.extend(["-U"+self.username] if self.username.startswith(("-","/")) else ["-U",self.username])
            args.extend(["-P"+self.password] if self.password.startswith(("-","/")) else ["-P",self.password])
        return args
    def _run(self,path,chunk):
        stdout=path.with_suffix(".stdout.log");stderr=path.with_suffix(".stderr.log");error=path.with_suffix(".error.log")
        chunk.update(stdout=str(stdout),stderr=str(stderr),error_file=str(error))
        actual_path=path
        fault=os.environ.get("DB_INGEST_TEST_BCP_DUPLICATE_ROW")
        if os.environ.get("DB_INGEST_TEST_MODE")=="1" and fault:
            if not self.plan.database.casefold().startswith("dbingesttest_") or not all(str(self.config.get("destination",{}).get(k,"")).casefold().startswith("dbingesttest_") for k in ("database","control_database")):raise IngestError("TEST_FAULT_DESTINATION","bcp corruption hook requires disposable destination/control database names")
            chosen=int(fault)
            if chosen<2:raise IngestError("TEST_FAULT_POINT","bcp duplicate hook requires a later row")
            actual_path=path.with_suffix(".fault-input.bcp")
            first=None
            with path.open("rb") as source,actual_path.open("xb") as target:
                for index,line in enumerate(iter(lambda:source.readline(self.limit_bytes+1),b""),1):
                    parts=line.split(b"\t")
                    if first is None:first=parts[1]
                    if index==chosen:parts[1]=first;line=b"\t".join(parts)
                    target.write(line)
                target.flush();os.fsync(target.fileno())
            chunk["test_fault"]={"kind":"derived duplicate ordinal; original canonical spool unchanged","row":chosen,"input":str(actual_path)}
            self._save("TEST_FAULT_INPUT_DURABLE")
        if len(str(actual_path))>255:raise IngestError("BCP_PATH_LIMIT","Native bcp transport path exceeds the documented255-character file limit; use a shorter durable work_directory")
        args=self._arguments(actual_path,stdout,stderr,error)
        env={k:v for k,v in os.environ.items() if k not in ("DB_PASSWORD","SQLCMDPASSWORD")}
        process=None
        try:
            with stdout.open("wb") as out,stderr.open("wb") as err:
                checkpoint(force=True)
                process=subprocess.Popen(args,shell=False,stdin=subprocess.DEVNULL,stdout=out,stderr=err,env=env,creationflags=getattr(subprocess,"CREATE_NO_WINDOW",0))
                started=time.monotonic();deadline=float(self.config.get("destination",{}).get("statement_timeout_seconds",120))
                while process.poll() is None:
                    checkpoint()
                    if shutil.disk_usage(self.root).free<int(self.config.get("preservation",{}).get("minimum_free_disk_bytes",0)):raise IngestError("DISK_CAPACITY","bcp diagnostics crossed the configured free-disk reserve; child stopped before publication",4)
                    if time.monotonic()-started>deadline:raise IngestError("BCP_TIMEOUT","bcp attempt exceeded the configured statement timeout; isolated staging will be reconciled",4)
                    time.sleep(0.05)
                return int(process.returncode)
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:process.wait(timeout=5)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
            for artifact in (stdout,stderr,error):_redact_artifact(artifact,self.password)
    def _verify_raw(self,path,expected_rows,allow_subset=False):
        cur=self.conn.cursor()
        # Waiting for TABLOCKX resolves any final child transaction before inspecting.
        cur.execute(f"SELECT record_token,ordinal,payload,row_sha256 FROM {self.raw} WITH (TABLOCKX,HOLDLOCK) ORDER BY ordinal")
        observed=0;expected=None
        with path.open("rb") as spool:
            for row in iter(cur.fetchone,None):
                if not allow_subset:checkpoint()
                actual=(row[0]+"\t"+str(row[1])+"\t"+row[2]+"\t"+row[3]+"\n").encode("ascii")
                if expected is None:expected=spool.readline(self.limit_bytes+1)
                if allow_subset:
                    while expected and int(expected.split(b"\t",2)[1])<row[1]:expected=spool.readline(self.limit_bytes+1)
                if actual!=expected:raise IngestError("BCP_RAW_MISMATCH","bcp raw row IDs or exact tagged payload differ from the durable spool",4,{"raw_table":self.raw,"observed_records":observed})
                observed+=1;expected=None
        self.conn.commit()
        if observed!=expected_rows and not allow_subset:raise IngestError("BCP_ROW_COUNT","bcp committed an incomplete isolated chunk",4,{"expected_records":expected_rows,"observed_records":observed,"raw_table":self.raw})
        return observed
    def flush(self):
        if not self.rows:return
        self.stream.flush();os.fsync(self.stream.fileno());self.stream.close();self.stream=None
        path=self.chunk_path;rows,byte_count=self.rows,self.bytes
        digest=hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda:stream.read(1048576),b""):digest.update(block)
        self.chunk_index+=1
        chunk={"index":self.chunk_index,"path":str(path),"rows":rows,"bytes":byte_count,"sha256":digest.hexdigest(),"state":"SPOOLED"}
        self._record_chunk(chunk);self._save("SPOOLED")
        try:
            try:
                code=self._run(path,chunk);chunk["exit_code"]=code
            except Exception:
                # _run always terminates/waits its child before returning/raising.
                # Mandatory bounded partial inspection continues even after cancellation.
                try:chunk["observed_partial_rows"]=self._verify_raw(path,rows,allow_subset=True)
                except Exception as inspection:chunk["partial_resolution"]="PENDING: "+getattr(inspection,"code",type(inspection).__name__)
                raise
            from .faults import test_fault
            if code:
                test_fault(self.config,"BCP_FAILED_BEFORE_PARTIAL_RECONCILIATION")
                chunk["observed_partial_rows"]=self._verify_raw(path,rows,allow_subset=True)
                raise IngestError("BCP_FAILED","bcp failed; actual isolated partial staging was reconciled and no business rows published",4,{"exit_code":code,"raw_table":self.raw,"chunk":chunk})
            test_fault(self.config,"STAGING_SENT_BEFORE_ACK")
            self._verify_raw(path,rows)
            from db_ingest.adapters.mssql.connect import connect
            from db_ingest.publication import _lock
            if self.admission is None:self.admission=connect(self.config,self.username,self.password,self.config.get("destination",{}).get("bootstrap_database","master"))
            admission_cur=self.admission.cursor()
            try:
                _lock(admission_cur,"db_ingest:bcp:typed_decode_memory",self.config.get("load",{}).get("target_lock_timeout_seconds",60))
                checkpoint(force=True)
                if self.server_decode:
                    cur=self.conn.cursor();cur.execute(typed_insert_sql(self.plan,self.raw),self.intent_id);self.conn.commit()
                else:self._decode_client(chunk)
            finally:self.admission.rollback()
            cur=self.conn.cursor();cur.execute(f"TRUNCATE TABLE {self.raw}");self.conn.commit()
            chunk["state"]="TYPED_STAGE_DECODED"
            self.summary["count"]+=1;self.summary["max_rows"]=max(self.summary["max_rows"],rows);self.summary["max_bytes"]=max(self.summary["max_bytes"],byte_count)
            self.rows=0;self.bytes=0;self.chunk_path=None;self._record_chunk(chunk);self._save("CHUNK_VERIFIED")
        except Exception:
            chunk["state"]="FAILED";self._record_chunk(chunk);self._save("FAILED");raise
    def _decode_client(self,chunk):
        from db_ingest.adapters.mssql.types import normalize_value
        from db_ingest.staging import typed_stage_parameters
        from db_ingest.transports.odbc import OdbcBatchWriter
        q=quote_identifier
        reader=self.conn.cursor();reader.execute(f"SELECT record_token,ordinal,payload FROM {self.raw} ORDER BY ordinal")
        # A raw chunk is bounded by actual encoded bytes, including every payload.
        raw_rows=[];raw_bytes=0
        for row in iter(reader.fetchone,None):
            checkpoint();size=sum(len(str(value).encode("ascii")) for value in row)+4
            raw_bytes+=size
            if raw_bytes>self.limit_bytes:raise IngestError("CANONICAL_DECODE_BUDGET","Raw typed-decode chunk exceeds its durable byte bound")
            raw_rows.append(row)
        reader.close()
        names=",".join(q(c.target_name) for c in self.plan.columns)
        markers=",".join("CASE WHEN ?=1 THEN CAST(NULL AS "+str(parse_sql_type(c.sql_type))+") ELSE "+conversion(c,"?")+" END" for c in self.plan.columns)
        sql=f"INSERT INTO #db_ingest_stage ({names},[__ingest_intent],[__ingest_record],[__ingest_ordinal]) VALUES ({markers},?,?,?)"
        direct={**self.config,"load":{**self.config.get("load",{}),"fast_executemany":False}}
        writer=OdbcBatchWriter(self.conn.cursor(),sql,direct)
        for token,ordinal,payload in raw_rows:
            checkpoint();document=json.loads(payload)
            if document.get("v")!=3 or len(document.get("s",""))!=len(self.plan.columns) or len(document.get("f",[]))!=len(self.plan.columns):raise IngestError("CANONICAL_VERSION","Typed wire state/data layout does not match the approved schema")
            values=[]
            for state,encoded,column in zip(document["s"],document["f"],self.plan.columns,strict=True):
                if state not in ("N","M","E","V") or (state in ("N","M","E") and encoded):raise IngestError("CANONICAL_STATE","Invalid typed field state/payload")
                if state=="M" and not (column.nullable and column.stats.get("missing_policy")=="null_with_provenance"):
                    raise IngestError("MISSING_VALUE","Canonical MISSING lacks explicit union policy")
                if state in ("N","M"):
                    if not column.nullable:raise IngestError("NULLABILITY","Canonical NULL reached a non-nullable typed column")
                    values.append(None);continue
                raw=base64.b64decode(encoded,validate=True)
                value=raw if parse_sql_type(column.sql_type).base=="varbinary" else raw.decode("utf-16-le",errors="strict")
                values.append(normalize_value(value,column.sql_type,text_encoding=column.stats.get("text_encoding")))
            record_id=base64.b64decode(token,validate=True).decode("utf-16-le",errors="strict")
            writer.add([*typed_stage_parameters(self.plan.columns,values),self.intent_id,record_id,ordinal])
        chunk["client_typed_decode_batches"]=writer.finish();self.conn.commit()

    def finish(self):
        self.flush();self._save("TYPED_STAGE_COMPLETE")
        return self.summary
    def close(self,success=False):
        if self.stream is not None:self.stream.close();self.stream=None
        cleanup="not_created"
        if self.created:
            try:
                self.conn.rollback();cur=self.conn.cursor()
                # Only this attempt's exact UUID-named raw object is ever dropped.
                cur.execute(f"SELECT COUNT_BIG(*) FROM {self.raw} WITH (TABLOCKX,HOLDLOCK)").fetchone()
                cur.execute(f"DROP TABLE {self.raw}");self.conn.commit();cleanup="DROPPED"
            except Exception:cleanup="PENDING; retained raw_table and manifest require explicit cleanup after connection recovery"
        if self.admission is not None:
            try:self.admission.rollback()
            finally:self.admission.close()
        self._save("VERIFIED" if success else "FAILED",raw_cleanup=cleanup)
        if success and self.config.get("preservation",{}).get("cleanup_verified_transport_spools",False):
            with os.scandir(self.root) as entries:
                for entry in entries:
                    if re.fullmatch(r"chunk_[0-9]{8,}\.bcp",entry.name) and entry.is_file(follow_symlinks=False):
                        path=Path(entry.path)
                        if path.resolve().is_relative_to(self.root.resolve()):path.unlink(missing_ok=True)
