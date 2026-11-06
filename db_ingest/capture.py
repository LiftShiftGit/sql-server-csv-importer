"""Durable source-byte capture. Parse only the returned artifact."""
import hashlib
import json
import os
import shutil
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from .errors import IngestError
from .models import SourceArtifact


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def _atomic_replace(source, target):
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.MoveFileExW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        kernel.MoveFileExW.restype = wintypes.BOOL
        # MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH.
        if not kernel.MoveFileExW(str(source), str(target), 0x1 | 0x8):
            raise OSError(ctypes.get_last_error(), "Durable atomic artifact finalization failed")
    else:
        os.replace(source, target)


def _sync_directory(path):
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@contextmanager
def _stable_reader(path):
    if os.name == "nt":
        import ctypes
        import msvcrt
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        kernel.CreateFileW.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
        # FILE_SHARE_READ only: writers/deleters cannot race this capture.
        handle = kernel.CreateFileW(str(path), 0x80000000, 1, None, 3, 0x08000000, None)
        if handle == wintypes.HANDLE(-1).value:
            raise OSError(ctypes.get_last_error(), "Unable to acquire source read handle excluding writers/deleters", str(path))
        try:
            fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
        except BaseException:
            kernel.CloseHandle(handle)
            raise
        with os.fdopen(fd, "rb") as source:
            yield source, "Windows CreateFileW FILE_SHARE_READ (write/delete sharing denied)"
    else:
        with open(path, "rb") as source:
            yield source, "observed-stat-stability-only; caller must guarantee stable source"


def capture_source(path, root, work_dir, minimum_free_bytes=0, capture_id=None, _fault_config=None):
    path, root, work = Path(path).absolute(), Path(root).absolute(), Path(work_dir).absolute()
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError:
        raise IngestError("SOURCE_OUTSIDE_ROOT", "Capture source leaves naming root")
    if not path.resolve().is_relative_to(root.resolve()):
        raise IngestError("SOURCE_OUTSIDE_ROOT", "Resolved capture source leaves naming root")
    if work == path.resolve() or work.resolve() in path.resolve().parents:
        raise IngestError("WORK_SOURCE_OVERLAP", "A work artifact cannot be an input source")
    if _fault_config is not None:
        from .transports.faults import validate_faults
        validate_faults(_fault_config)
    capture_id = capture_id or uuid.uuid4().hex
    folder = work / "captures" / capture_id
    try:
        folder.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        raise IngestError("CAPTURE_IO", "Cannot create durable capture directory: " + str(error), exit_code=4, context={"capture_id": capture_id}) from error
    temporary, final = folder / "source.partial", folder / "source.bin"
    started = utcnow()
    sha, size = hashlib.sha256(), 0
    try:
        with _stable_reader(path) as (source, mechanism):
            before = os.fstat(source.fileno())
            path_before = path.stat()
            if shutil.disk_usage(work).free < before.st_size + minimum_free_bytes:
                raise IngestError("DISK_CAPACITY", "Insufficient free disk space for source capture and reserve", exit_code=4, context={"source_bytes": before.st_size, "minimum_free_bytes": minimum_free_bytes})
            with open(temporary, "xb") as destination:
                while block := source.read(1024 * 1024):
                    from .resources import checkpoint
                    checkpoint(estimated_bytes=len(block))
                    if shutil.disk_usage(work).free < len(block) + minimum_free_bytes:
                        raise IngestError("DISK_CAPACITY", "Free disk reserve would be crossed during capture", exit_code=4)
                    destination.write(block)
                    sha.update(block)
                    size += len(block)
                    if size==len(block) and _fault_config is not None and os.environ.get("DB_INGEST_TEST_MODE")=="1" and os.environ.get("DB_INGEST_TEST_FAULT")=="CAPTURE_FIRST_BLOCK_DURABLE":
                        destination.flush();os.fsync(destination.fileno())
                        from .transports.faults import test_fault
                        test_fault(_fault_config,"CAPTURE_FIRST_BLOCK_DURABLE")
                destination.flush()
                os.fsync(destination.fileno())
            after = os.fstat(source.fileno())
            path_after = path.stat()
            # Python on Windows can expose different st_ctime semantics via fstat
            # and stat. Compare each API to itself; compare identity/size/mtime
            # across APIs while the deny-write/delete handle remains held.
            signature = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
            if (signature(before) != signature(after) or signature(path_before) != signature(path_after)
                    or signature(after)[:4] != signature(path_after)[:4] or size != before.st_size):
                raise IngestError("SOURCE_CHANGED", "Source changed while capture was in progress; partial artifact retained", exit_code=4, context={"before": signature(before), "after_handle": signature(after), "after_path": signature(path_after), "copied_bytes": size})
            _atomic_replace(temporary, final)
            _sync_directory(folder)
        metadata = {"capture_mechanism": mechanism, "capture_started_utc": started, "capture_completed_utc": utcnow(), "source_mtime_ns": before.st_mtime_ns, "source_ctime_ns": before.st_ctime_ns, "source_device": before.st_dev, "source_inode": before.st_ino, "stable_input_required": True, "stability_scope": "Windows handle excludes ordinary write/delete opens; observed metadata checks are not a universal filesystem snapshot" if os.name == "nt" else "caller-guaranteed stable input with observed metadata checks"}
        artifact = SourceArtifact(capture_id, str(final), str(path), relative, sha.hexdigest(), size, metadata)
        from dataclasses import asdict
        with open(folder / "artifact.partial.json", "x", encoding="utf-8") as target:
            json.dump(asdict(artifact), target, ensure_ascii=False, indent=2)
            target.flush()
            os.fsync(target.fileno())
        _atomic_replace(folder / "artifact.partial.json", folder / "artifact.json")
        _sync_directory(folder)
        if _fault_config is not None:
            from .transports.faults import test_fault
            test_fault(_fault_config,"CAPTURE_FINALIZED_BEFORE_JOURNAL")
        return artifact
    except IngestError as error:
        error.context.setdefault("capture_id",capture_id)
        error.context.setdefault("source",str(path))
        error.context.setdefault("partial_artifact",str(temporary if temporary.exists() else final))
        raise
    except OSError as error:
        raise IngestError("CAPTURE_IO", str(error), exit_code=4, context={"partial_artifact": str(temporary), "capture_id": capture_id}) from error


def verify_capture(artifact):
    digest, size = hashlib.sha256(), 0
    with open(artifact.path, "rb") as source:
        while block := source.read(1024 * 1024):
            size += len(block)
            digest.update(block)
    if size != artifact.size or digest.hexdigest() != artifact.sha256:
        raise IngestError("ARCHIVE_MISMATCH", "Captured bytes no longer match their recorded size/hash", exit_code=5, context={"capture_id": artifact.capture_id})
    return {"size": size, "sha256": digest.hexdigest(), "verified": True}


def require_safe_capture_retry(journal, run_id, selection_id, current_capture_id):
    """Refuse to substitute source bytes for an interrupted, unverified snapshot."""
    import re
    from itertools import chain
    cursor = journal.db.execute(
        "SELECT file_id FROM events WHERE run_id=? AND category='CAPTURE_ATTEMPT_STARTED' "
        "AND json_extract(details,'$.selection_id')=? ORDER BY id", (run_id, selection_id))
    try:
        for capture_id in chain((current_capture_id,), (row[0] for row in cursor)):
            if capture_id is None:
                continue
            if not isinstance(capture_id, str) or not re.fullmatch(r"[0-9a-f]{32}", capture_id):
                raise IngestError("CAPTURE_RETRY_EVIDENCE_INVALID", "Invalid retained capture identity; preserve work and investigate", 5)
            folder = journal.work / "captures" / capture_id
            for name in ("source.partial", "source.bin"):
                retained = folder / name
                if retained.exists() and retained.stat().st_size:
                    raise IngestError("CAPTURE_RETRY_UNVERIFIED",
                        "An interrupted capture contains bytes but has no verified complete snapshot. "
                        "The source may have changed, including unread bytes; automatic recapture under "
                        "this intent is blocked. Preserve the run and captures for review; do not delete "
                        "evidence or use a new intent merely to bypass this error.", 5,
                        {"selection_id": selection_id, "capture_id": capture_id,
                         "retained_artifact": str(retained), "retained_bytes": retained.stat().st_size,
                         "source_identity": "unproven; size, timestamps and prefix are insufficient"})
    finally:
        cursor.close()
