"""Explicit, recorded lossy projections over durable source interpretations."""
from dataclasses import asdict
from decimal import Decimal,InvalidOperation,localcontext,ROUND_HALF_EVEN
import base64,binascii,hashlib,json,re
from .errors import IngestError
from .adapters.mssql.types import parse_sql_type
from .resources import checkpoint


def initialize(journal):
    journal.db.execute("""CREATE TABLE IF NOT EXISTS projection_events(run_id TEXT NOT NULL,file_id TEXT NOT NULL,record_id TEXT NOT NULL,column_ordinal INTEGER NOT NULL,rule TEXT NOT NULL,original_field TEXT NOT NULL,projected_field TEXT NOT NULL,source_artifact TEXT NOT NULL,source_byte_start INTEGER,source_byte_end INTEGER,original_sha256 TEXT NOT NULL,projected_sha256 TEXT NOT NULL,PRIMARY KEY(run_id,file_id,record_id,column_ordinal,rule))""")
    journal.db.commit()


def available(journal):return journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='projection_events'").fetchone() is not None


def _encode(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"))


def _project_text(value,spec,encoding):
    if spec.length=="max":return value
    if spec.base=="nvarchar":
        raw=value.encode("utf-16-le",errors="strict")
        if len(raw)<=spec.length*2:return value
        prefix=raw[:spec.length*2]
        if prefix and 0xD800<=int.from_bytes(prefix[-2:],"little")<=0xDBFF:prefix=prefix[:-2]
        return prefix.decode("utf-16-le",errors="strict")
    raw=value.encode(encoding,errors="strict")
    if len(raw)<=spec.length:return value
    prefix=raw[:spec.length]
    while True:
        try:return prefix.decode(encoding,errors="strict")
        except UnicodeDecodeError:
            if not prefix:raise
            prefix=prefix[:-1]


def projected_value(field,column):
    """Return a new lexical value only for the exact declared destination policy."""
    spec=parse_sql_type(column.sql_type);value=field.text if field.text is not None else ""
    if column.stats.get("truncation")=="right":
        if spec.base in ("nvarchar","varchar"):
            return _project_text(value,spec,column.stats.get("text_encoding","utf-8"))
        if spec.base=="varbinary":
            encoding=column.stats.get("binary_encoding")
            if encoding=="base64":raw=base64.b64decode(value,validate=True)
            elif encoding=="hex":raw=binascii.unhexlify(value[2:] if value.startswith("0x") else value)
            else:raise ValueError("Binary truncation requires explicit base64/hex interpretation")
            if spec.length=="max" or len(raw)<=spec.length:return value
            raw=raw[:spec.length]
            return base64.b64encode(raw).decode("ascii") if encoding=="base64" else raw.hex()
        raise ValueError("Right truncation requires a declared text or binary length")
    if column.stats.get("rounding")=="half_even":
        if spec.base!="decimal":raise ValueError("Half-even rounding requires an explicit decimal target")
        if not re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?",value):raise ValueError("Expected finite exact decimal syntax")
        with localcontext() as context:
            context.prec=80
            number=Decimal(value)
            rounded=number.quantize(Decimal(1).scaleb(-spec.scale),rounding=ROUND_HALF_EVEN)
            if number==rounded:return value
            return format(rounded,"f")
    return value


class ProjectionContext:
    def __init__(self,journal,run_id,artifact,columns,config):
        self.journal,self.run_id,self.artifact=journal,run_id,asdict(artifact) if hasattr(artifact,"capture_id") else artifact
        self.columns=[c for c in columns if c.stats.get("truncation")=="right" or c.stats.get("rounding")=="half_even"]
        self.changed=False
        if self.columns and not config["schema"]["allow_lossy_projection"]:raise IngestError("LOSSY_POLICY_REQUIRED","A projection requires intentional schema.allow_lossy_projection=true and a precise column rule",2)
        if self.columns:initialize(journal)
    def process(self,record):
        self.changed=False;errors=[]
        for column in self.columns:
            checkpoint()
            if column.ordinal>len(record.fields):continue
            field=record.fields[column.ordinal-1]
            if field.state not in ("VALUE","EMPTY"):continue
            before=asdict(field)
            try:result=projected_value(field,column)
            except (ValueError,UnicodeError,binascii.Error,InvalidOperation) as error:
                errors.append({"code":"PROJECTION_INVALID","message":"Explicit projection cannot represent this value: "+str(error),"column_ordinal":column.ordinal})
                continue
            if result==field.text:continue
            field.text=result;field.state="EMPTY" if result=="" else "VALUE"
            after=asdict(field)
            rule=("truncation:right" if column.stats.get("truncation")=="right" else "rounding:half_even")+":"+column.sql_type
            original,projected=_encode(before),_encode(after)
            original_hash=hashlib.sha256(original.encode("utf-8")).hexdigest();result_hash=hashlib.sha256(projected.encode("utf-8")).hexdigest()
            existing=self.journal.db.execute("SELECT original_sha256,projected_sha256 FROM projection_events WHERE run_id=? AND file_id=? AND record_id=? AND column_ordinal=? AND rule=?",(self.run_id,self.artifact["capture_id"],record.record_id,column.ordinal,rule)).fetchone()
            if existing is not None and tuple(existing)!=(original_hash,result_hash):raise IngestError("PROJECTION_EVIDENCE_MISMATCH","A durable projection identity has different original/result evidence",5)
            if existing is None:
                import os
                from .journal import utcnow
                details={"record_id":record.record_id,"column_ordinal":column.ordinal,"rule":rule,"original_sha256":original_hash,"projected_sha256":result_hash,"source_artifact":self.artifact["path"],"byte_start":record.byte_start,"byte_end":record.byte_end,"local_evidence_table":"projection_events"}
                with self.journal.db:
                    self.journal.db.execute("INSERT INTO projection_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",(self.run_id,self.artifact["capture_id"],record.record_id,column.ordinal,rule,original,projected,self.artifact["path"],record.byte_start,record.byte_end,original_hash,result_hash))
                    self.journal.db.execute("INSERT INTO events(run_id,file_id,category,pid,created,details) VALUES(?,?,?,?,?,?)",(self.run_id,self.artifact["capture_id"],"LOSSY_PROJECTION",os.getpid(),utcnow(),_encode(details)))
            self.changed=True
        return errors


def summary(journal,run_id,file_id=None,group_id=None):
    result={"applied_cells":0,"affected_records":0,"assurance":"no changed destination cells","evidence_journal":str(journal.work/"journal.sqlite3"),"evidence_table":"projection_events","evidence_sha256":hashlib.sha256(b"db_ingest:projection-evidence:v1\x00").hexdigest()}
    if not available(journal):return result
    if group_id is not None:
        sql="SELECT COUNT(*),COUNT(DISTINCT p.record_id) FROM projection_events p JOIN group_records r ON r.run_id=p.run_id AND r.record_id=p.record_id WHERE p.run_id=? AND r.group_id=? AND r.disposition IN ('pending','committed')"
        args=(run_id,group_id)
    else:
        sql="SELECT COUNT(*),COUNT(DISTINCT p.record_id) FROM projection_events p JOIN records r ON r.run_id=p.run_id AND r.file_id=p.file_id AND r.record_id=p.record_id WHERE p.run_id=? AND p.file_id=? AND r.disposition IN ('pending','committed')"
        args=(run_id,file_id)
    count,records=journal.db.execute(sql,args).fetchone()
    projection=hashlib.sha256(b"db_ingest:projection-evidence:v1\x00")
    detail_sql=sql.replace("SELECT COUNT(*),COUNT(DISTINCT p.record_id)","SELECT p.*")+" ORDER BY p.file_id,p.record_id,p.column_ordinal,p.rule"
    for row in journal.db.execute(detail_sql,args):
        checkpoint();item=dict(row)
        for key in ("original","projected"):
            if hashlib.sha256(item[key+"_field"].encode("utf-8")).hexdigest()!=item[key+"_sha256"]:raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Original/result projection fields differ from their durable hashes",5)
        evidence={k:v for k,v in item.items() if k not in ("original_field","projected_field")}
        encoded=_encode(evidence).encode("utf-8");projection.update(len(encoded).to_bytes(8,"big"));projection.update(encoded)
    result.update(applied_cells=count,affected_records=records,evidence_sha256=projection.hexdigest(),assurance="LOSSY_PROJECTION; preserved originals, exact declared projected values verified" if count else result["assurance"])
    return result


def verify_seal(actual,expected):
    if expected is None:return
    for key in ("applied_cells","affected_records","evidence_sha256"):
        if actual.get(key)!=expected.get(key):raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Projection evidence differs from its approved immutable seal",5)


def verify_references(journal,run_id,file_id):
    evidence=summary(journal,run_id,file_id)
    source_file=journal.file(run_id,file_id)
    verify_seal(evidence,source_file.get("counts",{}).get("projection_summary"))
    verify_seal(evidence,(source_file.get("receipt") or {}).get("details",{}).get("lossy_projection"))
    if not available(journal):return evidence
    artifact=source_file["artifact"];last_record=None;record=None
    for row in journal.db.execute("SELECT * FROM projection_events WHERE run_id=? AND file_id=? ORDER BY record_id,column_ordinal,rule",(run_id,file_id)):
        checkpoint();item=dict(row)
        for key in ("original","projected"):
            if hashlib.sha256(item[key+"_field"].encode("utf-8")).hexdigest()!=item[key+"_sha256"]:raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Original/result projection fields differ from their durable hashes",5)
        if item["record_id"]!=last_record:
            source=journal.db.execute("SELECT payload FROM records WHERE run_id=? AND file_id=? AND record_id=?",(run_id,file_id,item["record_id"])).fetchone()
            if source is None:raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Projected record is absent from the authoritative source ledger",5)
            record=json.loads(source[0]);last_record=item["record_id"]
        ordinal=item["column_ordinal"]
        if item["source_artifact"]!=artifact["path"] or item["source_byte_start"]!=record["byte_start"] or item["source_byte_end"]!=record["byte_end"]:raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Projection source artifact/span differs from authoritative source provenance",5)
        if ordinal<1 or ordinal>len(record["fields"]) or record["fields"][ordinal-1]!=json.loads(item["projected_field"]):raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Derived source ledger differs from the recorded projected field",5)
    return evidence


def export(journal,run_id,path):
    with journal.lease("run:"+run_id):return _export(journal,run_id,path)


def _export(journal,run_id,path):
    from pathlib import Path
    import os,uuid
    from .capture import _atomic_replace,_sync_directory
    journal.run(run_id)
    destination=Path(path)
    if destination.exists():raise IngestError("OUTPUT_EXISTS","Projection export already exists; choose a new output file",2)
    temporary=destination.with_name(destination.name+"."+uuid.uuid4().hex+".partial")
    # Validate sealed evidence before creating any export. This also detects a
    # deleted event or a forged original together with its newly calculated hash.
    from .pipeline import _check_capture,local_evidence
    if available(journal):
        orphan=journal.db.execute("SELECT COUNT(*) FROM projection_events p LEFT JOIN files f ON f.run_id=p.run_id AND f.file_id=p.file_id WHERE p.run_id=? AND f.file_id IS NULL",(run_id,)).fetchone()[0]
        if orphan:raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Projection events refer to missing source files",5)
    for item in journal.files(run_id):
        expected=item.get("counts",{}).get("projection_summary")
        if expected is None:
            if available(journal) and journal.db.execute("SELECT 1 FROM projection_events WHERE run_id=? AND file_id=? LIMIT 1",(run_id,item["file_id"])).fetchone():raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Projection events lack an approved source summary",5)
            continue
        _check_capture(item["artifact"])
        observed=local_evidence(journal,run_id,item["file_id"],item["artifact"],item["decision"])
        if item.get("counts",{}).get("ledger_sha256")!=observed["ledger_sha256"]:raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Authoritative source ledger changed before export",5)
        actual=observed["projection_summary"]
        for key in ("applied_cells","affected_records","evidence_sha256"):
            if expected.get(key)!=actual.get(key):raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Projection evidence differs from its approved source summary",5)
        sealed=(item.get("receipt") or {}).get("details",{}).get("lossy_projection")
        if sealed:
            for key in ("applied_cells","affected_records","evidence_sha256"):
                if sealed.get(key)!=actual.get(key):raise IngestError("PROJECTION_EVIDENCE_MISMATCH","Projection evidence differs from its immutable publication receipt",5)
    destination.parent.mkdir(parents=True,exist_ok=True);count=0;digest=hashlib.sha256()
    with temporary.open("wb") as stream:
        if available(journal):
            for row in journal.db.execute("SELECT * FROM projection_events WHERE run_id=? ORDER BY file_id,record_id,column_ordinal,rule",(run_id,)):
                checkpoint();item=dict(row)
                item["original_field"]=json.loads(item["original_field"]);item["projected_field"]=json.loads(item["projected_field"])
                raw=(_encode(item)+"\n").encode("utf-8");stream.write(raw);digest.update(raw);count+=1
        stream.flush();os.fsync(stream.fileno())
    _atomic_replace(temporary,destination);_sync_directory(destination.parent)
    return {"run_id":run_id,"path":str(destination.resolve()),"cells":count,"sha256":digest.hexdigest(),"contains_original_and_projected_values":True,"exit_code":0}
