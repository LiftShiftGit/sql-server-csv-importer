"""Bounded, credential-free help/version subprocess pipe capture.

No communicate(), reader threads, unbounded queues, or output-sized reads. stdout
and stderr share one pipe; excess output or a deadline kills and waits the child.
"""
import math,os,select,subprocess,time
from functools import lru_cache

DEFAULT_OUTPUT_BYTES=32768
MAX_OUTPUT_BYTES=1048576
READ_BYTES=4096


def output_budget(config):return int(config.get("limits",{}).get("output_buffer_bytes",DEFAULT_OUTPUT_BYTES))


@lru_cache(maxsize=1)
def _peek_api():
    import ctypes
    from ctypes import wintypes
    kernel=ctypes.WinDLL("kernel32",use_last_error=True)
    kernel.PeekNamedPipe.argtypes=[wintypes.HANDLE,wintypes.LPVOID,wintypes.DWORD,wintypes.LPVOID,ctypes.POINTER(wintypes.DWORD),wintypes.LPVOID]
    kernel.PeekNamedPipe.restype=wintypes.BOOL
    return kernel.PeekNamedPipe


def _available(pipe):
    if os.name=="nt":
        import ctypes,msvcrt
        from ctypes import wintypes
        available=wintypes.DWORD()
        if not _peek_api()(wintypes.HANDLE(msvcrt.get_osfhandle(pipe.fileno())),None,0,None,ctypes.byref(available),None):
            code=ctypes.get_last_error()
            if code in (109,232):return -1 # Broken/closing pipe: no more bytes.
            raise OSError(code,"Cannot inspect bounded probe output pipe")
        return available.value
    return READ_BYTES if select.select([pipe.fileno()],[],[],0)[0] else 0



@lru_cache(maxsize=1)
def _job_api():
    import ctypes
    from ctypes import wintypes
    class Basic(ctypes.Structure):
        _fields_=[("u",ctypes.c_longlong),("v",ctypes.c_longlong),("flags",wintypes.DWORD),("min",ctypes.c_size_t),("max",ctypes.c_size_t),("active",wintypes.DWORD),("affinity",ctypes.c_size_t),("priority",wintypes.DWORD),("scheduling",wintypes.DWORD)]
    class Extended(ctypes.Structure):
        _fields_=[("basic",Basic),("io",ctypes.c_ulonglong*6),("memory",ctypes.c_size_t*4)]
    kernel=ctypes.WinDLL("kernel32",use_last_error=True)
    k=kernel
    k.CreateJobObjectW.argtypes=[wintypes.LPVOID,wintypes.LPCWSTR];k.CreateJobObjectW.restype=wintypes.HANDLE
    k.SetInformationJobObject.argtypes=[wintypes.HANDLE,ctypes.c_int,wintypes.LPVOID,wintypes.DWORD];k.SetInformationJobObject.restype=wintypes.BOOL
    k.AssignProcessToJobObject.argtypes=[wintypes.HANDLE,wintypes.HANDLE];k.AssignProcessToJobObject.restype=wintypes.BOOL
    k.CloseHandle.argtypes=[wintypes.HANDLE]
    class ThreadEntry(ctypes.Structure):
        _fields_=[("size",wintypes.DWORD),("usage",wintypes.DWORD),("id",wintypes.DWORD),("owner",wintypes.DWORD),("base",wintypes.LONG),("delta",wintypes.LONG),("flags",wintypes.DWORD)]
    k.CreateToolhelp32Snapshot.argtypes=[wintypes.DWORD,wintypes.DWORD];k.CreateToolhelp32Snapshot.restype=wintypes.HANDLE
    k.Thread32First.argtypes=[wintypes.HANDLE,ctypes.POINTER(ThreadEntry)];k.Thread32First.restype=wintypes.BOOL
    k.Thread32Next.argtypes=[wintypes.HANDLE,ctypes.POINTER(ThreadEntry)];k.Thread32Next.restype=wintypes.BOOL
    k.OpenThread.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD];k.OpenThread.restype=wintypes.HANDLE
    k.ResumeThread.argtypes=[wintypes.HANDLE];k.ResumeThread.restype=wintypes.DWORD
    return kernel,Extended,ThreadEntry


class _ProbeTree:
    """Contain only this probe. Windows starts suspended until job attachment."""
    def __init__(self):
        self.handle=None;self.process=None
        if os.name!="nt":return
        import ctypes
        self.kernel,Extended,self.ThreadEntry=_job_api()
        k=self.kernel
        self.handle=k.CreateJobObjectW(None,None)
        if not self.handle:raise OSError(ctypes.get_last_error(),"Cannot create probe cleanup job")
        limits=Extended();limits.basic.flags=0x2000
        if not k.SetInformationJobObject(self.handle,9,ctypes.byref(limits),ctypes.sizeof(limits)):
            error=ctypes.get_last_error();self.close();raise OSError(error,"Cannot configure probe cleanup job")

    def attach_resume(self,process):
        self.process=process
        if os.name!="nt":return
        import ctypes
        from ctypes import wintypes
        k=self.kernel
        if not k.AssignProcessToJobObject(self.handle,wintypes.HANDLE(int(process._handle))):raise OSError(ctypes.get_last_error(),"Cannot contain probe process")
        # Popen closes the primary thread handle. Find its suspended primary
        # thread using documented Toolhelp APIs, then ResumeThread only after
        # successful job assignment; it cannot spawn an escaping child first.
        ThreadEntry=self.ThreadEntry
        snapshot=k.CreateToolhelp32Snapshot(4,0)
        if snapshot==wintypes.HANDLE(-1).value:raise OSError(ctypes.get_last_error(),"Cannot inspect suspended probe thread")
        found=False
        try:
            entry=ThreadEntry();entry.size=ctypes.sizeof(entry)
            more=k.Thread32First(snapshot,ctypes.byref(entry))
            while more:
                if entry.owner==process.pid:
                    thread=k.OpenThread(2,False,entry.id)
                    if not thread:raise OSError(ctypes.get_last_error(),"Cannot open suspended probe thread")
                    try:
                        if k.ResumeThread(thread)==0xffffffff:raise OSError(ctypes.get_last_error(),"Cannot resume contained probe")
                    finally:k.CloseHandle(thread)
                    found=True;break
                more=k.Thread32Next(snapshot,ctypes.byref(entry))
        finally:k.CloseHandle(snapshot)
        if not found:raise OSError("Suspended probe primary thread was not found")

    def close(self):
        if os.name=="nt":
            if self.handle:self.kernel.CloseHandle(self.handle);self.handle=None
        elif self.process is not None:
            import signal
            try:os.killpg(self.process.pid,signal.SIGKILL)
            except ProcessLookupError:pass


def bounded_capture(arguments,max_bytes=DEFAULT_OUTPUT_BYTES,timeout_seconds=10,env=None):
    if not isinstance(max_bytes,int) or isinstance(max_bytes,bool) or not 1<=max_bytes<=MAX_OUTPUT_BYTES:raise ValueError("Probe output budget must be 1..1048576 bytes")
    if not isinstance(timeout_seconds,(int,float)) or not math.isfinite(timeout_seconds) or timeout_seconds<=0:raise ValueError("Probe deadline must be positive and finite")
    source=os.environ if env is None else env
    clean_env={key:value for key,value in source.items() if key.upper()!="DB_PASSWORD" and not key.upper().startswith("SQLCMD")}
    output=bytearray();bytes_read=0;process=None;tree=None;state="COMPLETED";eof=False;terminated=False
    try:
        tree=_ProbeTree()
        process=subprocess.Popen(list(arguments),shell=False,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,bufsize=0,env=clean_env,creationflags=(getattr(subprocess,"CREATE_NO_WINDOW",0)|0x4) if os.name=="nt" else 0,start_new_session=os.name!="nt")
        tree.attach_resume(process)
        deadline=time.monotonic()+timeout_seconds
        while True:
            available=-1 if eof else _available(process.stdout)
            if available>0:
                # Read at most one excess byte to establish overflow; retain only
                # the configured budget. The OS pipe bounds unread child output.
                remaining=max_bytes-len(output)
                block=os.read(process.stdout.fileno(),min(READ_BYTES,available,remaining+1))
                bytes_read+=len(block)
                if block:
                    output.extend(block[:remaining])
                    if len(block)>remaining:state="OUTPUT_LIMIT";break
                else:eof=True
            elif available<0:eof=True
            if process.poll() is not None and eof:break
            if time.monotonic()>=deadline:state="TIMEOUT";break
            if available<=0:time.sleep(min(.01,max(0,deadline-time.monotonic())))
        if state!="COMPLETED" and process.poll() is None:
            process.kill();terminated=True
        try:process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill();terminated=True
            try:process.wait(timeout=2)
            except subprocess.TimeoutExpired:raise OSError("Probe child did not exit after forced termination") from None
        return {"state":state,"output":bytes(output),"captured_bytes":len(output),"bytes_read":bytes_read,"output_limit_bytes":max_bytes,"output_complete":state=="COMPLETED","exit_code":process.returncode,"pid":process.pid,"terminated":terminated}
    finally:
        if tree is not None:tree.close()
        if process is not None:
            try:
                if process.poll() is None:
                    process.kill()
                    try:process.wait(timeout=2)
                    except subprocess.TimeoutExpired:raise OSError("Probe cleanup could not confirm child exit") from None
            finally:
                if process.stdout is not None:process.stdout.close()


def text_preview(raw,limit_bytes=16384):
    decoded=raw.decode("utf-8",errors="backslashreplace")
    encoded=decoded.encode("utf-8")
    return encoded[:limit_bytes].decode("utf-8",errors="ignore"),len(encoded)>limit_bytes


def capture_metadata(result):
    return {key:value for key,value in result.items() if key not in ("output",)}
