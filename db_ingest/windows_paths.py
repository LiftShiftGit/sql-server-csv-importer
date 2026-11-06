"""Windows path checks; UNC inputs are allowed, recovery journals stay local."""
import os
from pathlib import Path, PureWindowsPath
from .errors import IngestError


def validate_path_spelling(value, field):
    if os.name != "nt":
        return
    p = PureWindowsPath(value)
    if bool(p.drive) != bool(p.root):
        raise IngestError("CONFIG_PATH", field + " must be a full drive/UNC path or a relative path; drive-relative and current-drive-rooted paths are ambiguous")
    if str(p).startswith("\\\\.\\"):
        raise IngestError("CONFIG_PATH", field + " must be a filesystem path, not a Windows device path")


def _remote(path):
    p = PureWindowsPath(str(path))
    drive = p.drive
    if drive.startswith("\\\\") and not (drive.startswith("\\\\?\\") and len(drive) == 6 and drive[-1] == ":"):
        return True
    import ctypes
    from ctypes import wintypes
    get_type = ctypes.WinDLL("kernel32", use_last_error=True).GetDriveTypeW
    get_type.argtypes = [wintypes.LPCWSTR]
    get_type.restype = wintypes.UINT
    return get_type(p.anchor) == 4  # DRIVE_REMOTE, including mapped SMB drives.


def validate_local_work(value):
    if os.name != "nt":
        return
    validate_path_spelling(value, "preservation.work_directory")
    original = Path(value).absolute()
    # Check the spelling before resolving, so an unavailable UNC share fails locally.
    if _remote(original) or _remote(original.resolve()):
        raise IngestError("WORK_DIRECTORY_NETWORK", "Use a local work_directory for the SQLite recovery journal; UNC/network sources are supported, but UNC or mapped-network work directories are not")
