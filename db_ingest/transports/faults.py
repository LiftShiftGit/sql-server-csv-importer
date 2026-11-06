"""Explicitly enabled hard-process faults restricted to disposable test databases."""
import os
from db_ingest.errors import IngestError

POINTS={"STAGED_BEFORE_PUBLICATION","AFTER_TARGET_SCHEMA_CREATION","REPLACE_AFTER_DELETE_BEFORE_INSERT","AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT","CAPTURE_FIRST_BLOCK_DURABLE","CAPTURE_FINALIZED_BEFORE_JOURNAL","AFTER_LOCAL_RECEIPT_BEFORE_AUDIT","FAILURE_TABLE_CREATED_BEFORE_COMMIT","STAGING_SENT_BEFORE_ACK","BCP_FAILED_BEFORE_PARTIAL_RECONCILIATION","CANCEL_MULTIPLE_WORKERS"}

def validate_faults(config):
    if os.environ.get("DB_INGEST_TEST_MODE")!="1" or not os.environ.get("DB_INGEST_TEST_FAULT"):return
    if os.environ["DB_INGEST_TEST_FAULT"] not in POINTS:
        raise IngestError("TEST_FAULT_POINT","Unknown explicitly requested test fault point")
    destination=config.get("destination",{})
    if not all(str(destination.get(k,"")).casefold().startswith("dbingesttest_") for k in ("database","control_database")):
        raise IngestError("TEST_FAULT_DESTINATION","Fault hooks require explicit DbIngestTest_ destination and control database names")

def test_fault(config,point):
    if os.environ.get("DB_INGEST_TEST_MODE")!="1" or os.environ.get("DB_INGEST_TEST_FAULT")!=point:return
    validate_faults(config)
    os._exit(86)


def test_cancellation(config,function_name,pending,snapshot):
    """Inject KeyboardInterrupt only with multiple active disposable publishers.

    Uses the ordinary coordinator cancellation path. Native cases first observe
    a real bcp child; the durable event retains its PID for independent cleanup
    verification. This is deliberately described as an injected interrupt.
    """
    if os.environ.get("DB_INGEST_TEST_MODE")!="1" or os.environ.get("DB_INGEST_TEST_FAULT")!="CANCEL_MULTIPLE_WORKERS":return
    validate_faults(config)
    if function_name not in ("_execute_one","execute_one") or len(pending)<2:return
    import json,sqlite3
    from pathlib import Path
    from db_ingest.journal import utcnow
    first=next(iter(pending.values()))[0];run_id=first["run_id"]
    native=[pid for pid,name in snapshot.process_names.items() if name.casefold() in ("bcp.exe","bcp")]
    if config.get("load",{}).get("engine")=="bcp" and not native:return
    connection=sqlite3.connect(Path(config["preservation"]["work_directory"])/"journal.sqlite3",timeout=.05)
    try:
        workers=[]
        sql="""SELECT e.pid FROM events e WHERE e.run_id=? AND e.category IN ('WORKER_STARTED','GROUP_WORKER_STARTED') AND json_extract(e.details,'$.phase')='publish' AND NOT EXISTS(SELECT 1 FROM events f WHERE f.run_id=e.run_id AND f.pid=e.pid AND f.id>e.id AND f.category IN ('WORKER_FINISHED','GROUP_WORKER_FINISHED') AND json_extract(f.details,'$.phase')='publish') GROUP BY e.pid"""
        workers=[row[0] for row in connection.execute(sql,(run_id,))]
        if len(workers)<2:return
        identities={str(pid):snapshot.creation_identities.get(pid) for pid in set(workers+native)}
        names={str(pid):snapshot.process_names.get(pid) for pid in set(workers+native)}
        details={"point":"CANCEL_MULTIPLE_WORKERS","phase":"publish","worker_pids":workers,"native_pids":native,"creation_identities":identities,"process_names":names,"pending_futures":len(pending),"signal":"injected KeyboardInterrupt; ordinary cooperative coordinator cancellation path"}
        with connection:connection.execute("INSERT INTO events(run_id,file_id,category,pid,created,details) VALUES(?,NULL,?,?,?,?)",(run_id,"TEST_CANCELLATION_REQUESTED",os.getpid(),utcnow(),json.dumps(details)))
    except sqlite3.OperationalError:return
    finally:connection.close()
    raise KeyboardInterrupt("Explicit disposable multi-worker cancellation fault")
