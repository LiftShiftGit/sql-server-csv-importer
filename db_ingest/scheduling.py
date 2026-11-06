"""Bounded, process-isolated file scheduling with sampled RSS admission."""
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import asdict
import multiprocessing
import os
import pickle
import threading
import time
from .errors import IngestError
from .resources import checkpoint, configure, process_tree_memory, resource_scope, snapshot_context


def admission(config):
    snapshot = process_tree_memory()
    if snapshot.unavailable_pids:
        raise IngestError("RESOURCE_MEASUREMENT_UNAVAILABLE", "Cannot admit workers while descendant RSS is unknown", 4, {"pids": snapshot.unavailable_pids})
    limits = config["limits"]
    global_budget, worker_budget = limits["total_process_tree_memory_bytes"], limits["worker_memory_bytes"]
    requested = config["parallel"]["max_workers"] if config["parallel"]["enabled"] else 1
    reserve = max(16 * 1024 * 1024, limits["max_queue_bytes"])
    if not config["parallel"]["enabled"]:
        if snapshot.total_rss_bytes + reserve >= global_budget:
            raise IngestError("RESOURCE_ADMISSION", "Serial process RSS and scheduler reserve exceed the configured total budget", 4, {"rss": snapshot.total_rss_bytes, "reserve": reserve, "budget": global_budget})
        workers = 1
    else:
        workers = min(requested, max(0, (global_budget - snapshot.total_rss_bytes - reserve) // worker_budget))
        if workers < 1:
            raise IngestError("RESOURCE_ADMISSION", "No isolated worker fits the configured process-tree/worker budgets", 4, {"rss": snapshot.total_rss_bytes, "reserve": reserve, "global_budget": global_budget, "worker_budget": worker_budget, "requested_workers": requested})
    return {"root_pid": os.getpid(), "workers": int(workers), "effective_workers": int(workers), "requested_workers": requested, "parallel_requested": config["parallel"]["enabled"], "parallel_state": "process-isolated file workers" if config["parallel"]["enabled"] else "serial", "initial_process_tree_rss_bytes": snapshot.total_rss_bytes, "peak_process_tree_rss_bytes": snapshot.total_rss_bytes, "global_budget_bytes": global_budget, "memory_budget_bytes": global_budget, "sampled_peak_process_tree_rss_bytes": snapshot.total_rss_bytes, "worker_budget_bytes": worker_budget, "scheduler_reserve_bytes": reserve, "max_pending_tasks": int(workers), "max_queue_bytes": limits["max_queue_bytes"], "peak_pending_tasks": 0, "peak_queued_argument_bytes": 0, "measurement_method": snapshot.method, "memory_assurance": "RSS sampled during scheduling/checkpoints, admission reserves worker budgets; native transient allocations can exceed between samples"}


def _initialize_worker(config, cancel, root_pid):
    configure(config, cancel, root_pid)
    try:checkpoint(force=True)
    except IngestError as error:
        # A queued spawn can finish initializing after ordinary cancellation.
        # _invoke will return its durable cancellation outcome without admitting
        # a source/SQL operation; avoid an initializer traceback/BrokenProcessPool.
        if error.code!="CANCELLED":raise


def _invoke(function, argument):
    try:
        checkpoint(force=True)
        value = function(argument)
        return {"ok": True, "value": value, "pid": os.getpid()}
    except IngestError as error:
        return {"ok": False, "error": {"code": error.code, "message": str(error), "exit_code": error.exit_code, "context": error.context}, "pid": os.getpid()}
    except BaseException as error:
        if isinstance(error, (SystemExit, KeyboardInterrupt)):
            code = "CANCELLED"
        else:
            code = "WORKER_FAILED"
        return {"ok": False, "error": {"code": code, "message": str(error), "exit_code": 4, "context": {"exception_type": type(error).__name__}}, "pid": os.getpid()}



def _timed_map(function):
    from functools import wraps
    @wraps(function)
    def measured(config,task,arguments,metrics=None):
        report=metrics if metrics is not None else {}
        started=time.perf_counter()
        try:
            for argument,outcome in function(config,task,arguments,report):
                report["completed_tasks"]=report.get("completed_tasks",0)+1
                value=outcome.get("value",{}) if outcome.get("ok") else {}
                report["processed_source_bytes"]=report.get("processed_source_bytes",0)+int(value.get("source_bytes",0))
                report["processed_records"]=report.get("processed_records",0)+int(value.get("processed_records",0))
                yield argument,outcome
        finally:
            report["elapsed_seconds"]=time.perf_counter()-started
            elapsed=report["elapsed_seconds"]
            report["source_mb_per_second"]=report.get("processed_source_bytes",0)/1048576/elapsed if elapsed else 0
            report["records_per_second"]=report.get("processed_records",0)/elapsed if elapsed else 0
    return measured


@_timed_map
def bounded_map(config, function, arguments, metrics=None):
    """Yield (argument, outcome) without admitting an unbounded future queue.

    A caller persists selection before invoking this function and records outcome
    failures in its local journal. Worker exceptions never depend on exception
    picklability. Existing futures drain cooperatively before shutdown returns.
    """
    policy = admission(config)
    if metrics is not None:
        metrics.update(policy)
        policy = metrics
    iterator = iter(arguments)
    if not config["parallel"]["enabled"]:
        event = threading.Event()
        with resource_scope(config, event, worker=True):
            for argument in iterator:
                checkpoint(force=True)
                size = len(pickle.dumps(argument, protocol=5))
                if size > policy["max_queue_bytes"]:
                    raise IngestError("QUEUE_BUDGET", "One file task's serialized arguments exceed max_queue_bytes", 4)
                policy["peak_pending_tasks"] = 1
                policy["peak_queued_argument_bytes"] = max(policy["peak_queued_argument_bytes"], size)
                result = _invoke(function, argument)
                snapshot = process_tree_memory()
                policy["peak_process_tree_rss_bytes"] = max(policy["peak_process_tree_rss_bytes"], snapshot.total_rss_bytes)
                policy["sampled_peak_process_tree_rss_bytes"] = policy["peak_process_tree_rss_bytes"]
                yield argument, result
        return
    context = multiprocessing.get_context("spawn")
    cancel = context.Event()
    pending, queued_bytes, held, exhausted = {}, 0, None, False
    failure = None
    executor = ProcessPoolExecutor(max_workers=policy["workers"], mp_context=context, initializer=_initialize_worker, initargs=(config, cancel, os.getpid()))
    try:
        while pending or not exhausted or held is not None:
            snapshot = process_tree_memory()
            from .transports.faults import test_cancellation
            test_cancellation(config,function.__name__,pending,snapshot)
            policy["peak_process_tree_rss_bytes"] = max(policy["peak_process_tree_rss_bytes"], snapshot.total_rss_bytes)
            policy["sampled_peak_process_tree_rss_bytes"] = policy["peak_process_tree_rss_bytes"]
            if snapshot.unavailable_pids:
                failure = IngestError("RESOURCE_MEASUREMENT_UNAVAILABLE", "Active child RSS could not be measured", 4, {"pids": snapshot.unavailable_pids})
            elif snapshot.total_rss_bytes > policy["global_budget_bytes"]:
                failure = IngestError("RESOURCE_BUDGET", "Observed importer process-tree RSS exceeds the configured total budget", 4, {"observed_rss_bytes": snapshot.total_rss_bytes, "budget_bytes": policy["global_budget_bytes"],"process_tree":snapshot_context(snapshot)})
            else:
                # Direct child branches include Windows venv redirectors and all
                # interpreter/native descendants; each pool branch gets one
                # worker reservation. The small resource-tracker branch also fits.
                branches={}
                for pid,rss in snapshot.processes.items():
                    if pid==snapshot.root_pid:continue
                    branch=pid;seen=set()
                    while snapshot.parents.get(branch)!=snapshot.root_pid and branch not in seen:
                        seen.add(branch)
                        parent=snapshot.parents.get(branch)
                        if not parent or parent==branch:break
                        branch=parent
                    branches[branch]=branches.get(branch,0)+rss
                for branch,rss in branches.items():
                    if rss>policy["worker_budget_bytes"]:
                        failure=IngestError("RESOURCE_BUDGET","Observed worker process subtree exceeds its configured reservation",4,{"worker_root_pid":branch,"observed_rss_bytes":rss,"budget_bytes":policy["worker_budget_bytes"],"process_tree":snapshot_context(snapshot)})
                        break
            if failure:
                cancel.set()
                exhausted, held = True, None
            while not failure and len(pending) < policy["workers"] and not exhausted:
                if held is None:
                    try:
                        argument = next(iterator)
                    except StopIteration:
                        exhausted = True
                        break
                    size = len(pickle.dumps(argument, protocol=5))
                    if size > policy["max_queue_bytes"]:
                        raise IngestError("QUEUE_BUDGET", "One file task's serialized arguments exceed max_queue_bytes", 4, {"argument_bytes": size})
                    held = (argument, size)
                argument, size = held
                if queued_bytes + size > policy["max_queue_bytes"]:
                    break
                future = executor.submit(_invoke, function, argument)
                pending[future] = (argument, size)
                queued_bytes += size
                held = None
                policy["peak_pending_tasks"] = max(policy["peak_pending_tasks"], len(pending))
                policy["peak_queued_argument_bytes"] = max(policy["peak_queued_argument_bytes"], queued_bytes)
            if not pending:
                if failure:
                    raise failure
                continue
            complete, _ = wait(pending, timeout=0.05, return_when=FIRST_COMPLETED)
            for future in complete:
                argument, size = pending.pop(future)
                queued_bytes -= size
                try:
                    outcome = future.result()
                except BaseException as error:
                    outcome = {"ok": False, "error": {"code": "WORKER_LOST", "message": str(error), "exit_code": 4, "context": {}}, "pid": None}
                if not outcome["ok"] and outcome["error"]["code"] in {"RESOURCE_BUDGET","RESOURCE_MEASUREMENT_UNAVAILABLE","CANCELLED","WORKER_LOST","WORKER_FAILED"}:
                    detail=outcome["error"]
                    if failure is None or (failure.code=="CANCELLED" and detail["code"]!="CANCELLED"):
                        failure=IngestError(detail["code"],detail["message"],detail["exit_code"],detail["context"])
                    cancel.set();exhausted=True;held=None
                yield argument, outcome
            if failure and not pending:
                raise failure
    except BaseException:
        cancel.set()
        for future in pending:
            future.cancel()
        raise
    finally:
        # Workers checkpoint and close their own journal/connections. An in-flight
        # SQL transaction resolves commit/rollback before its process exits.
        cancel.set()
        executor.shutdown(wait=True, cancel_futures=True)
