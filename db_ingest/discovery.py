"""Disk-backed deterministic discovery, independent of a file's extension."""
import fnmatch
import os
import sqlite3
import tempfile
from pathlib import Path
from .errors import IngestError


def matches(relative, patterns):
    """POSIX-style relative glob: * stays within a component; ** spans zero+.

    Matching remains case sensitive even on Windows so source spelling is
    explicit; destination case collisions are handled by stable mapping names.
    """
    from functools import lru_cache
    parts = tuple(relative.replace("\\", "/").strip("/").split("/"))
    for pattern in patterns:
        tokens = tuple(pattern.replace("\\", "/").strip("/").split("/"))
        @lru_cache(None)
        def visit(i, j):
            if i == len(tokens):
                return j == len(parts)
            if tokens[i] == "**":
                return visit(i + 1, j) or (j < len(parts) and visit(i, j + 1))
            return j < len(parts) and fnmatch.fnmatchcase(parts[j], tokens[i]) and visit(i + 1, j + 1)
        if visit(0, 0):
            return True
    return False


def inventory(config):
    """Yield every encountered entry's selection result, sorted on disk.

    Symlinks/junctions are excluded unless explicitly enabled. The resolved work
    directory is always excluded, even if CLI include/exclude lists replace TOML.
    """
    settings = config.get("source", {})
    root = Path(settings.get("root", settings.get("path", "."))).absolute()
    selected = Path(settings.get("path", root)).absolute()
    work = Path(config.get("preservation", {}).get("work_directory", "ingest-work")).resolve()
    try:
        selected.relative_to(root)
    except ValueError:
        raise IngestError("SOURCE_OUTSIDE_ROOT", "Source must be inside the stable naming root", context={"root": str(root), "source": str(selected)})
    if not selected.exists():
        raise IngestError("SOURCE_MISSING", "Source path does not exist", context={"source": str(selected)})
    include = settings.get("include", ["**/*"])
    exclude = settings.get("exclude", [])
    follow = settings.get("follow_symlinks", False)
    recursive = settings.get("recursive", True)
    with tempfile.TemporaryDirectory(prefix="db-ingest-discovery-") as temp:
        db = sqlite3.connect(str(Path(temp) / "inventory.sqlite"))
        try:
            db.executescript("CREATE TABLE dirs(path TEXT PRIMARY KEY, physical TEXT UNIQUE, done INTEGER DEFAULT 0); CREATE TABLE entries(path TEXT PRIMARY KEY, relative TEXT, status TEXT, reason TEXT);")
            def add(path, is_dir=None):
                relative = path.relative_to(root).as_posix()
                try:
                    resolved = path.resolve()
                    is_link = path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction())
                    directory = path.is_dir() if is_dir is None else is_dir
                    if resolved == work or work in resolved.parents:
                        status, reason = "excluded", "mandatory work-directory exclusion"
                    elif is_link and not follow:
                        status, reason = "excluded", "symlink/junction traversal disabled"
                    elif not resolved.is_relative_to(root.resolve()):
                        status, reason = "excluded", "resolved path leaves naming root"
                    elif matches(relative, exclude) or (directory and matches(relative + "/", exclude)):
                        status, reason = "excluded", "exclude pattern"
                    elif directory:
                        if recursive or path == selected:
                            db.execute("INSERT OR IGNORE INTO dirs(path,physical) VALUES (?,?)", (str(path), str(resolved)))
                            return
                        status, reason = "excluded", "recursive traversal disabled"
                    elif not path.is_file():
                        status, reason = "unsupported", "not a regular file"
                    elif not matches(relative, include):
                        status, reason = "excluded", "include pattern did not match"
                    else:
                        status, reason = "selected", "include pattern"
                    db.execute("INSERT OR IGNORE INTO entries VALUES (?,?,?,?)", (str(path), relative, status, reason))
                except OSError as error:
                    db.execute("INSERT OR IGNORE INTO entries VALUES (?,?,?,?)", (str(path), relative, "unreadable", str(error)))
            add(selected)
            while True:
                row = db.execute("SELECT path FROM dirs WHERE done=0 ORDER BY path LIMIT 1").fetchone()
                if row is None:
                    break
                folder = Path(row[0])
                db.execute("UPDATE dirs SET done=1 WHERE path=?", row)
                try:
                    with os.scandir(folder) as entries:
                        for entry in entries:
                            add(Path(entry.path))
                except OSError as error:
                    db.execute("INSERT OR REPLACE INTO entries VALUES (?,?,?,?)", (str(folder), folder.relative_to(root).as_posix(), "unreadable", str(error)))
                db.commit()
            cursor = db.execute("SELECT path,relative,status,reason FROM entries ORDER BY relative COLLATE BINARY")
            try:
                for path, relative, status, reason in cursor:
                    yield {"path": path, "relative_path": relative, "status": status, "reason": reason}
            finally:
                cursor.close()
        finally:
            db.close()


def discover(config):
    for entry in inventory(config):
        if entry["status"] == "unreadable":
            raise IngestError("SOURCE_ACCESS", entry["reason"], exit_code=4, context=entry)
        if entry["status"] == "selected":
            yield Path(entry["path"])
