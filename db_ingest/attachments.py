"""Explicit attachment-path BLOB capture with durable replayable asset references.

Only columns declaring binary_encoding=attachment_path access files. Source CSV
bytes keep lexical paths; derived records use Base64 for the existing binary
converter. Sidecar originals survive even when source directories disappear.
"""
from dataclasses import asdict
import base64,hashlib,json,math
from pathlib import Path
from .capture import capture_source,verify_capture
from .errors import IngestError
from .models import SourceArtifact


class AttachmentContext:
    def __init__(self,journal,run_id,artifact,columns,config):
        self.journal,self.run_id,self.artifact,self.config=journal,run_id,artifact,config
        self.columns=[]
        for column in columns:
            spec=column.stats
            if spec.get("binary_encoding")=="attachment_path" or spec.get("source_binary_encoding")=="attachment_path":
                spec["source_binary_encoding"]="attachment_path";spec["binary_encoding"]="base64"
                self.columns.append(column)
        if self.columns:
            journal.db.executescript("""CREATE TABLE IF NOT EXISTS attachment_assets(asset_key TEXT PRIMARY KEY,source_capture_id TEXT NOT NULL,source_value TEXT NOT NULL,root TEXT NOT NULL,artifact TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS attachment_links(run_id TEXT NOT NULL,file_id TEXT NOT NULL,record_id TEXT NOT NULL,column_ordinal INTEGER NOT NULL,asset_key TEXT NOT NULL,original_value TEXT NOT NULL,PRIMARY KEY(run_id,file_id,record_id,column_ordinal));""")

    def _asset(self,value,root_value):
        if len(value.encode("utf-16-le"))//2>32767:raise IngestError("ATTACHMENT_PATH_LIMIT","Attachment path exceeds the supported Windows path length; original cell remains in the source archive")
        if not isinstance(root_value,str) or not root_value:raise IngestError("ATTACHMENT_ROOT","An explicit attachment_root is required")
        root=Path(root_value).absolute()
        # The cache key does not require the original root to still exist on replay.
        key=hashlib.sha256(json.dumps([self.artifact.capture_id,str(root),value],ensure_ascii=True).encode()).hexdigest()
        existing=self.journal.db.execute("SELECT artifact FROM attachment_assets WHERE asset_key=?",(key,)).fetchone()
        if existing:
            asset=SourceArtifact(**json.loads(existing[0]));verify_capture(asset);return key,asset
        candidate=(root/Path(value)).absolute()
        try:relative=candidate.relative_to(root)
        except ValueError:raise IngestError("ATTACHMENT_ESCAPE","Attachment path leaves its explicitly configured root") from None
        if ".." in relative.parts:raise IngestError("ATTACHMENT_ESCAPE","Parent traversal is not permitted for attachment paths")
        current=root
        try:
            if root.is_symlink() or (hasattr(root,"is_junction") and root.is_junction()):raise IngestError("ATTACHMENT_SYMLINK","Attachment root must be a direct directory")
            for part in relative.parts:
                current=current/part
                if current.is_symlink() or (hasattr(current,"is_junction") and current.is_junction()):raise IngestError("ATTACHMENT_SYMLINK","Attachment symlinks/junctions are not followed")
            resolved_root=root.resolve(strict=True);resolved=candidate.resolve(strict=True)
            if not resolved.is_relative_to(resolved_root):raise IngestError("ATTACHMENT_ESCAPE","Resolved attachment path leaves its root")
            if not resolved.is_file():raise IngestError("ATTACHMENT_FORMAT","Attachment is not a regular file")
        except FileNotFoundError:raise IngestError("ATTACHMENT_MISSING","Declared attachment does not exist",context={"path":value}) from None
        except OSError as exc:raise IngestError("ATTACHMENT_ACCESS",str(exc)) from None
        asset=capture_source(resolved,resolved_root,str(self.journal.work),self.config["preservation"]["minimum_free_disk_bytes"])
        self.journal.db.execute("INSERT INTO attachment_assets VALUES(?,?,?,?,?)",(key,self.artifact.capture_id,value,str(root),json.dumps(asdict(asset))))
        self.journal.flush()
        return key,asset

    def process(self,record):
        if not self.columns or record.kind!="data":return []
        from .resources import checkpoint
        errors=[]
        for column in self.columns:
            if column.ordinal>len(record.fields):continue
            field=record.fields[column.ordinal-1]
            if field.state=="NULL":continue
            original=field.text or ""
            try:
                checkpoint()
                if field.state=="MISSING" or not original:raise IngestError("ATTACHMENT_MISSING","Empty/missing attachment paths are not empty binary or NULL")
                key,asset=self._asset(original,column.stats.get("attachment_root"))
                self.journal.db.execute("INSERT OR REPLACE INTO attachment_links VALUES(?,?,?,?,?,?)",(self.run_id,self.artifact.capture_id,record.record_id,column.ordinal,key,original))
                self.journal.flush()
                encoded_size=4*((asset.size+2)//3)
                limits=self.config["limits"]
                others=sum(len((v.text or "").encode("utf-8")) for i,v in enumerate(record.fields) if i!=column.ordinal-1)
                if encoded_size>limits["max_in_memory_field_bytes"] or encoded_size+others>limits["max_in_memory_record_bytes"]:raise IngestError("ATTACHMENT_LIMIT","Attachment bytes are captured but need a streaming BLOB path or larger supported record policy",context={"size":asset.size,"artifact":asset.path,"sha256":asset.sha256})
                with Path(asset.path).open("rb") as source:data=source.read(asset.size+1)
                if len(data)!=asset.size or hashlib.sha256(data).hexdigest()!=asset.sha256:raise IngestError("ARCHIVE_MISMATCH","Captured attachment changed before conversion",5)
                field.text=base64.b64encode(data).decode("ascii");field.state="EMPTY" if not data else "VALUE"
            except IngestError as exc:
                if exc.exit_code in (4,5):raise
                errors.append({"code":exc.code,"message":str(exc),"column_ordinal":column.ordinal,"column_name":column.original_name,"original_path":original[:256],"original_path_characters":len(original),"source_reference":{"capture_id":self.artifact.capture_id,"ordinal":record.ordinal,"column_ordinal":column.ordinal},"context":exc.context})
        return errors


def references(journal,run_id,file_id,limit=1000,max_bytes=1048576):
    if not journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachment_links'").fetchone():return {"count":0,"artifacts":[],"serialized_bytes":0}
    count=journal.db.execute("SELECT COUNT(*) FROM attachment_links WHERE run_id=? AND file_id=?",(run_id,file_id)).fetchone()[0]
    rows=journal.db.execute("SELECT l.record_id,l.column_ordinal,l.original_value,a.artifact FROM attachment_links l JOIN attachment_assets a ON l.asset_key=a.asset_key WHERE l.run_id=? AND l.file_id=? ORDER BY l.record_id,l.column_ordinal LIMIT ?",(run_id,file_id,limit))
    artifacts=[];used=0
    for r in rows:
        item={"record_id":r[0],"column_ordinal":r[1],"original_value":r[2],"artifact":json.loads(r[3])}
        length=len(json.dumps(item,ensure_ascii=True).encode("utf-8"))
        if used+length>max_bytes:break
        artifacts.append(item);used+=length
    return {"count":count,"artifacts":artifacts,"truncated":count>len(artifacts),"serialized_bytes":used}


def verify_references(journal,run_id,file_id):
    if not journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachment_links'").fetchone():return 0
    count=0
    for row in journal.db.execute("SELECT DISTINCT a.artifact FROM attachment_links l JOIN attachment_assets a ON l.asset_key=a.asset_key WHERE l.run_id=? AND l.file_id=?",(run_id,file_id)):
        verify_capture(SourceArtifact(**json.loads(row[0])));count+=1
    return count
