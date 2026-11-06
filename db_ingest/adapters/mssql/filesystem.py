"""Read-only doctor observations; access() is not a durability/ACL proof."""
import os
from pathlib import Path
import shutil
import stat


def _inspect(path):
    item={"path":str(path),"access_basis":"os.access observation; effective ACL and future I/O success are not guaranteed"}
    try:
        info=path.stat()
    except (FileNotFoundError,NotADirectoryError):return {**item,"state":"MISSING","exists":False,"kind":None}
    except OSError as error:return {**item,"state":"UNAVAILABLE","exists":None,"kind":None,"reason":type(error).__name__}
    kind="directory" if stat.S_ISDIR(info.st_mode) else "file" if stat.S_ISREG(info.st_mode) else "other"
    return {**item,"state":"OBSERVED","exists":True,"kind":kind,"read_access_observed":os.access(path,os.R_OK),"search_access_observed":os.access(path,os.X_OK) if kind=="directory" else None}


def filesystem_report(config):
    source=config.get("source",{});preservation=config.get("preservation",{})
    root=Path(source.get("root","."));selected=Path(source.get("path",str(root)))
    work=Path(preservation.get("work_directory","."))
    issues=[];root_info=_inspect(root);source_info=_inspect(selected)
    for name,item,kinds in (("source.root",root_info,("directory",)),("source.path",source_info,("file","directory"))):
        if item["state"]!="OBSERVED" or item["kind"] not in kinds:
            issues.append({"code":"SOURCE_PATH_UNAVAILABLE","setting":name,"action":"Configure an existing readable source path and directory root"})
        elif not item["read_access_observed"]:
            issues.append({"code":"SOURCE_READ_ACCESS_NOT_OBSERVED","setting":name,"action":"Check effective read/list permissions; doctor did not open source files"})
    try:
        selected.resolve().relative_to(root.resolve())
        within_root=True
    except ValueError:
        within_root=False;issues.append({"code":"SOURCE_OUTSIDE_ROOT","action":"Select a source within the configured naming root"})
    except OSError:within_root=None
    work_info=_inspect(work);ancestor=work;ancestor_info=dict(work_info)
    while ancestor_info["state"]=="MISSING" and ancestor.parent!=ancestor:
        ancestor=ancestor.parent;ancestor_info=_inspect(ancestor)
    work_info.update(durability="NOT_VERIFIED",nearest_existing_ancestor=str(ancestor) if ancestor_info["exists"] else None,ancestor=ancestor_info,writability="UNKNOWN",writability_proven=False,minimum_free_disk_bytes=int(preservation.get("minimum_free_disk_bytes",0)),free_disk_bytes=None)
    if ancestor_info["state"]!="OBSERVED" or ancestor_info["kind"]!="directory":
        work_info["state"]="INVALID" if ancestor_info["exists"] else "UNAVAILABLE"
        issues.append({"code":"WORK_PATH_UNAVAILABLE","action":"Configure a work directory whose nearest existing ancestor is an accessible directory"})
    else:
        writable=os.access(ancestor,os.W_OK|os.X_OK)
        work_info.update(writability="INFERRED_WRITABLE" if writable else "NOT_OBSERVED",write_access_observed=writable)
        if not writable:issues.append({"code":"WORK_WRITE_ACCESS_NOT_OBSERVED","action":"Check effective create/write permissions; no scratch file was created"})
        try:
            free=shutil.disk_usage(ancestor).free;work_info["free_disk_bytes"]=free
            work_info["minimum_reserve_met"]=free>=work_info["minimum_free_disk_bytes"]
            if not work_info["minimum_reserve_met"]:issues.append({"code":"WORK_DISK_CAPACITY","action":"Free disk space or select durable storage with the configured minimum reserve"})
        except OSError as error:
            work_info["disk_state"]="UNAVAILABLE";work_info["disk_reason"]=type(error).__name__
            issues.append({"code":"WORK_DISK_UNAVAILABLE","action":"Check storage availability and permissions"})
    if work_info["state"]=="MISSING":work_info["action"]="Work directory does not yet exist; doctor did not create it"
    return {"read_only":True,"source_root":root_info,"source_path":source_info,"source_within_root":within_root,"work_directory":work_info,"issues":issues,"exit_code":2 if issues else 0,"capacity_scope":"Free-space observation and configured reserve only; source/capture/staging growth is not reserved or guaranteed"}
