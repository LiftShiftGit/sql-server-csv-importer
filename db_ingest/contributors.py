"""Portable, bounded, byte-verified contributor-manifest contract."""
import hashlib
import json
from pathlib import Path,PurePosixPath
import re
from .errors import IngestError
from .resources import checkpoint


def normalized(item):
    if not isinstance(item,dict):raise IngestError("GROUP_MANIFEST_INVALID","Each contributor must be one JSON object")
    path=item.get("relative_path")
    if not isinstance(path,str) or not path or path=="." or not PurePosixPath(path).parts or PurePosixPath(path).is_absolute() or ".." in PurePosixPath(path).parts or str(PurePosixPath(path))!=path or "\\" in path:raise IngestError("GROUP_SOURCE_PATH","Contributor paths must be normalized root-relative POSIX paths")
    sha=item.get("sha256",item.get("source_hash"))
    if not isinstance(sha,str) or not re.fullmatch(r"[0-9a-fA-F]{64}",sha):raise IngestError("GROUP_SOURCE_HASH","Each contributor requires its exact source SHA256")
    size=item.get("size")
    if not isinstance(size,int) or isinstance(size,bool) or not 0<=size<=9223372036854775807:raise IngestError("GROUP_SOURCE_SIZE","Contributor byte size must fit a nonnegative signed64-bit count")
    return {**item,"relative_path":path,"sha256":sha.lower(),"size":size,"file_id":hashlib.sha256(path.encode("utf-8")).hexdigest()}


def source_manifest_hash(contributors):
    digest=hashlib.sha256(b"db_ingest:group-sources:v1\x00");last=None
    for source in contributors:
        checkpoint();item=normalized(source);path=item["relative_path"]
        if last is not None and path<=last:raise IngestError("GROUP_SOURCE_ORDER","Manifest paths must be unique and strictly ordered")
        for value in (path.encode("utf-8"),item["sha256"].encode("ascii")):
            digest.update(len(value).to_bytes(8,"big"));digest.update(value)
        digest.update(item["size"].to_bytes(8,"big"));last=path
    return digest.hexdigest()


def iter_manifest(metadata):
    """Stream one bounded JSONL file metadata record at a time; verify final bytes."""
    path=Path(metadata["manifest_path"])
    digest=hashlib.sha256();count=0;last=None
    limit=int(metadata.get("max_line_bytes",8388608))
    if not 1<=limit<=8388608:raise IngestError("GROUP_MANIFEST_LIMIT","Manifest metadata rows are limited to 8 MiB")
    with path.open("rb") as stream:
        while True:
            line=stream.readline(limit+1)
            if not line:break
            checkpoint(len(line)*4)
            if len(line)>limit or not line.endswith(b"\n"):raise IngestError("GROUP_MANIFEST_FRAMING","Manifest line exceeds its bound or lacks a complete LF frame")
            digest.update(line)
            try:item=normalized(json.loads(line.decode("utf-8",errors="strict")))
            except (ValueError,UnicodeError) as exc:raise IngestError("GROUP_MANIFEST_INVALID",f"Invalid contributor JSONL: {exc}") from None
            if last is not None and item["relative_path"]<=last:raise IngestError("GROUP_SOURCE_ORDER","Manifest paths must be unique and strictly ordered")
            count+=1;last=item["relative_path"];yield item
    if digest.hexdigest()!=metadata.get("manifest_sha256"):raise IngestError("GROUP_MANIFEST_HASH","Contributor manifest bytes differ from their durable SHA256",5)
    if count!=metadata.get("contributor_count"):raise IngestError("GROUP_MANIFEST_COUNT","Contributor manifest count differs from the approved group",5)



def contribution_descriptor(entry):
    item=normalized(entry)
    interpretation=item.get("interpretation_hash")
    mapping=item.get("mapping_hash") or ""
    if not isinstance(interpretation,str) or not re.fullmatch(r"[0-9a-fA-F]{64}",interpretation):raise IngestError("GROUP_CONTRIBUTION_SIGNATURE","Contributor interpretation fingerprint is missing/invalid",5)
    if mapping and (not isinstance(mapping,str) or not re.fullmatch(r"[0-9a-fA-F]{64}",mapping)):raise IngestError("GROUP_CONTRIBUTION_SIGNATURE","Contributor mapping fingerprint is invalid",5)
    return {"version":1,"file_id":item["file_id"],"source_sha256":item["sha256"],"size":item["size"],"interpretation_hash":interpretation.lower(),"mapping_hash":mapping.lower()}


def contribution_signature(entry):
    from .config import stable_hash
    return stable_hash(contribution_descriptor(entry))
