"""Standard-library process-tree RSS observation and cooperative resource gates.

RSS is sampled, not a hard OS allocation sandbox. Native allocations may grow
between checkpoints; reports retain that limitation rather than promising a cap.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
import os
import time
from functools import lru_cache
from .errors import IngestError


@dataclass
class MemorySnapshot:
    root_pid: int
    total_rss_bytes: int
    processes: dict[int, int] = field(default_factory=dict)
    unavailable_pids: list[int] = field(default_factory=list)
    method: str = ""
    parents: dict[int, int] = field(default_factory=dict)
    creation_identities: dict[int, int] = field(default_factory=dict)
    process_names: dict[int, str] = field(default_factory=dict)


@lru_cache(maxsize=1)
def _windows_api():
    """Build ctypes types/prototypes once, not once per RSS sample.

    ctypes.POINTER caches each structure CLASS identity globally. Creating a new
    Entry/Counters class every sample therefore leaked two permanent classes and
    pointer types per sample even after all native handles had been closed.
    """
    import ctypes
    from ctypes import wintypes
    class Entry(ctypes.Structure):
        _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD), ("th32ProcessID", wintypes.DWORD), ("th32DefaultHeapID", ctypes.c_size_t), ("th32ModuleID", wintypes.DWORD), ("cntThreads", wintypes.DWORD), ("th32ParentProcessID", wintypes.DWORD), ("pcPriClassBase", wintypes.LONG), ("dwFlags", wintypes.DWORD), ("szExeFile", wintypes.WCHAR * 260)]
    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD), ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t), ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t), ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t), ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t), ("PrivateUsage", ctypes.c_size_t)]
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(Entry)]
    kernel.Process32FirstW.restype = wintypes.BOOL
    kernel.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(Entry)]
    kernel.Process32NextW.restype = wintypes.BOOL
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    kernel.K32GetProcessMemoryInfo.restype = wintypes.BOOL
    kernel.GetProcessTimes.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.FILETIME),ctypes.POINTER(wintypes.FILETIME),ctypes.POINTER(wintypes.FILETIME),ctypes.POINTER(wintypes.FILETIME)]
    kernel.GetProcessTimes.restype=wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    return kernel,Entry,Counters


def _windows_processes(with_names=False):
    import ctypes
    from ctypes import wintypes
    kernel,Entry,_=_windows_api()
    handle = kernel.CreateToolhelp32Snapshot(2, 0)
    if handle == wintypes.HANDLE(-1).value:
        raise IngestError("RESOURCE_MEASUREMENT_UNAVAILABLE", "Cannot enumerate Windows process tree", 4)
    result, names = {}, {}
    try:
        entry = Entry();entry.dwSize = ctypes.sizeof(entry)
        exists = kernel.Process32FirstW(handle, ctypes.byref(entry))
        if not exists and ctypes.get_last_error()!=18:
            raise IngestError("RESOURCE_MEASUREMENT_UNAVAILABLE","Windows process snapshot could not be read",4)
        while exists:
            result[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            names[int(entry.th32ProcessID)] = entry.szExeFile
            exists = kernel.Process32NextW(handle, ctypes.byref(entry))
        if ctypes.get_last_error()!=18:
            raise IngestError("RESOURCE_MEASUREMENT_UNAVAILABLE","Windows process snapshot ended unexpectedly",4)
    finally:
        kernel.CloseHandle(handle)
    return (result,names) if with_names else result


def _windows_rss(pid):
    import ctypes
    kernel,_,Counters=_windows_api()
    handle = kernel.OpenProcess(0x1000 | 0x10, False, pid)
    if not handle:return None
    try:
        counters = Counters();counters.cb = ctypes.sizeof(counters)
        if not kernel.K32GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):return None
        return int(counters.WorkingSetSize)
    finally:kernel.CloseHandle(handle)


def _posix_processes():
    from pathlib import Path
    proc = Path("/proc")
    if not proc.is_dir():
        raise IngestError("RESOURCE_PLATFORM_UNSUPPORTED", "Process-tree memory observation requires Windows Toolhelp or Linux procfs; this platform is not certified", 2)
    result = {}
    for entry in proc.iterdir():
        if entry.name.isdigit():
            try:
                # Process comm may contain spaces/parentheses; split after last ).
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                result[int(entry.name)] = int(fields[1])
            except (OSError, ValueError, IndexError):
                pass
    return result


def _tree_members(root, parents, identity_lookup):
    """Reject stale ParentPID links when an unrelated process reused that PID."""
    root_identity=identity_lookup(root)
    if root_identity is None:
        raise IngestError("RESOURCE_MEASUREMENT_UNAVAILABLE","Root process creation identity could not be observed",4,{"root_pid":root})
    members,identities,unknown={root},{root:root_identity},set()
    while True:
        changed=False
        for child,parent in parents.items():
            if child in members or parent not in members:continue
            identity=identity_lookup(child)
            if identity is None:
                unknown.add(child)
                continue
            # ParentPID in PROCESSENTRY32 is not a lifetime-unique identity.
            # An older child cannot have been created by this newer parent.
            if identity<identities[parent]:continue
            identities[child]=identity;members.add(child);changed=True
        if not changed:break
    return members,identities,unknown


def process_tree_memory(root_pid=None):
    root = root_pid or os.getpid()
    if os.name=="nt":parents,names=_windows_processes(with_names=True)
    else:parents,names=_posix_processes(),{}
    members,identities,unknown=_tree_members(root,parents,process_identity)
    rss,missing={},list(unknown)
    for pid in members:
        if os.name == "nt":value=_windows_rss(pid)
        else:
            try:
                from pathlib import Path
                value=int(Path(f"/proc/{pid}/statm").read_text().split()[1])*os.sysconf("SC_PAGE_SIZE")
                names[pid]=Path(f"/proc/{pid}/comm").read_text().strip()
            except (OSError,ValueError,IndexError):value=None
        if value is None:missing.append(pid)
        else:rss[pid]=value
    if missing:
        current=_windows_processes() if os.name=="nt" else _posix_processes()
        missing=[pid for pid in missing if pid in current]
    if root not in rss:
        raise IngestError("RESOURCE_MEASUREMENT_UNAVAILABLE","Root process RSS could not be observed",4,{"root_pid":root})
    return MemorySnapshot(root,sum(rss.values()),rss,missing,"Windows Toolhelp32 + creation-time ancestry + K32GetProcessMemoryInfo working set" if os.name=="nt" else "Linux procfs RSS/creation ancestry (uncertified platform)",{pid:parents.get(pid,0) for pid in rss},{pid:identities[pid] for pid in rss},{pid:names.get(pid,"unknown") for pid in rss})


def snapshot_context(snapshot):
    return {"root_pid":snapshot.root_pid,"processes":{pid:{"rss_bytes":rss,"parent_pid":snapshot.parents.get(pid),"creation_identity":snapshot.creation_identities.get(pid),"executable":snapshot.process_names.get(pid)} for pid,rss in snapshot.processes.items()},"measurement_method":snapshot.method}


def process_identity(pid):
    """Creation identity detects an exited/reused parent PID before worker work."""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel,_,_=_windows_api()
        handle=kernel.OpenProcess(0x1000,False,pid)
        if not handle:return None
        try:
            creation,exit_time,kernel_time,user_time=[wintypes.FILETIME() for _ in range(4)]
            if not kernel.GetProcessTimes(handle,creation,exit_time,kernel_time,user_time):return None
            if exit_time.dwHighDateTime or exit_time.dwLowDateTime:return None
            return (creation.dwHighDateTime<<32)|creation.dwLowDateTime
        finally:kernel.CloseHandle(handle)
    try:
        from pathlib import Path
        return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")",1)[1].split()[19])
    except (OSError,IndexError):return None


_JOB_HANDLE = None


def _ensure_child_cleanup_job():
    """A worker's job handle dies with it, terminating its native descendants.

    No CPU/memory hard limit is imposed here; memory remains sampled. A worker
    cannot close the handle during ordinary operation because it contains itself.
    Windows closes it on process exit, including an uncatchable forced kill.
    """
    global _JOB_HANDLE
    if os.name != "nt" or _JOB_HANDLE is not None:
        return
    import ctypes
    from ctypes import wintypes
    class Basic(ctypes.Structure):
        _fields_=[("PerProcessUserTimeLimit",ctypes.c_longlong),("PerJobUserTimeLimit",ctypes.c_longlong),("LimitFlags",wintypes.DWORD),("MinimumWorkingSetSize",ctypes.c_size_t),("MaximumWorkingSetSize",ctypes.c_size_t),("ActiveProcessLimit",wintypes.DWORD),("Affinity",ctypes.c_size_t),("PriorityClass",wintypes.DWORD),("SchedulingClass",wintypes.DWORD)]
    class Io(ctypes.Structure):
        _fields_=[(name,ctypes.c_ulonglong) for name in ("ReadOperationCount","WriteOperationCount","OtherOperationCount","ReadTransferCount","WriteTransferCount","OtherTransferCount")]
    class Extended(ctypes.Structure):
        _fields_=[("BasicLimitInformation",Basic),("IoInfo",Io),("ProcessMemoryLimit",ctypes.c_size_t),("JobMemoryLimit",ctypes.c_size_t),("PeakProcessMemoryUsed",ctypes.c_size_t),("PeakJobMemoryUsed",ctypes.c_size_t)]
    kernel=ctypes.WinDLL("kernel32",use_last_error=True)
    kernel.CreateJobObjectW.argtypes=[wintypes.LPVOID,wintypes.LPCWSTR];kernel.CreateJobObjectW.restype=wintypes.HANDLE
    kernel.SetInformationJobObject.argtypes=[wintypes.HANDLE,ctypes.c_int,wintypes.LPVOID,wintypes.DWORD];kernel.SetInformationJobObject.restype=wintypes.BOOL
    kernel.AssignProcessToJobObject.argtypes=[wintypes.HANDLE,wintypes.HANDLE];kernel.AssignProcessToJobObject.restype=wintypes.BOOL
    kernel.GetCurrentProcess.restype=wintypes.HANDLE
    kernel.CloseHandle.argtypes=[wintypes.HANDLE]
    handle=kernel.CreateJobObjectW(None,None)
    if not handle:raise IngestError("PROCESS_JOB_UNAVAILABLE","Cannot create a Windows worker cleanup job",4)
    limits=Extended();limits.BasicLimitInformation.LimitFlags=0x2000 # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not kernel.SetInformationJobObject(handle,9,ctypes.byref(limits),ctypes.sizeof(limits)) or not kernel.AssignProcessToJobObject(handle,kernel.GetCurrentProcess()):
        error=ctypes.get_last_error();kernel.CloseHandle(handle)
        raise IngestError("PROCESS_JOB_UNAVAILABLE","Cannot attach this worker to a kill-on-exit child cleanup job",4,{"windows_error":error})
    _JOB_HANDLE=handle


_POLICY = None


def configure(config, cancel_event=None, root_pid=None, worker=False):
    global _POLICY
    limits = config["limits"]
    # Only spawned workers need an extra job. The coordinator supervises its own
    # pool cancellation; placing the host/test runner itself in a job is avoided.
    if root_pid is not None and root_pid!=os.getpid():_ensure_child_cleanup_job()
    _POLICY = {"global": limits["total_process_tree_memory_bytes"], "worker": limits["worker_memory_bytes"], "root": root_pid or os.getpid(), "cancel": cancel_event, "parent_identity": process_identity(root_pid or os.getpid()), "worker_scope": worker or (root_pid is not None and root_pid!=os.getpid()), "last": 0.0, "peak": 0, "last_snapshot": None}


@contextmanager
def resource_scope(config, cancel_event=None, root_pid=None, worker=False):
    global _POLICY
    previous = _POLICY
    configure(config, cancel_event, root_pid, worker)
    try:
        checkpoint(force=True)
        yield _POLICY
    finally:
        _POLICY = previous


def checkpoint(estimated_bytes=0, force=False):
    policy=_POLICY
    if policy is None:return
    if policy["cancel"] is not None and policy["cancel"].is_set():
        raise IngestError("CANCELLED","Admission stopped; this worker stopped at a durable resource checkpoint",4)
    now=time.monotonic()
    budget=min(policy["global"],policy["worker"]) if policy["worker_scope"] else policy["global"]
    own=policy["last_snapshot"]
    fresh=force or own is None or now-policy["last"]>=0.1
    if fresh:
        if os.getpid()!=policy["root"] and (policy["parent_identity"] is None or process_identity(policy["root"])!=policy["parent_identity"]):
            raise IngestError("CANCELLED","Coordinator exited; orphan worker stopped before admitting further work",4)
        own=process_tree_memory(os.getpid())
        policy["last"],policy["last_snapshot"]=now,own
        policy["peak"]=max(policy["peak"],own.total_rss_bytes)
        if own.unavailable_pids:
            raise IngestError("RESOURCE_MEASUREMENT_UNAVAILABLE","Worker descendant RSS could not be observed",4,{"pids":own.unavailable_pids})
    # Estimates are checked on EVERY call against the latest <=100ms observation;
    # force=True remeasures immediately at native send/allocation boundaries.
    if own.total_rss_bytes+estimated_bytes>budget:
        if policy["cancel"] is not None:policy["cancel"].set()
        raise IngestError("RESOURCE_BUDGET","Observed RSS plus pending allocation estimate exceeds the configured budget",4,{"observed_rss_bytes":own.total_rss_bytes,"pending_allocation_bytes":estimated_bytes,"budget_bytes":budget,"root_pid":own.root_pid,"observation_age_seconds":max(0,now-policy["last"]),"process_tree":snapshot_context(own)})
