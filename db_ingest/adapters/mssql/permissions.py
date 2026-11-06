"""Observed permission facts and explicit rollback-only real-schema probes."""
import uuid
from .ddl import qualified,quote_identifier
from .diagnostics import provider_error

OPERATIONS=("CREATE","INSERT","SELECT","ALTER","UPDATE","DELETE","DROP")


def schemas(config,database):
    destination=config.get("destination",{});audit=config.get("audit",{});result=[]
    if database==destination.get("database","LegacyLanding"):result.append(destination.get("schema","dbo"))
    if database in (destination.get("database","LegacyLanding"),destination.get("control_database","IngestControl")):result.append(audit.get("control_schema","audit"))
    if database==destination.get("control_database","IngestControl"):result.append(audit.get("failure_schema","failures"))
    return list(dict.fromkeys(result))


def observe(cur,config,database):
    names=schemas(config,database);facts={"state":"OBSERVED_METADATA","execution_proof":False,"schemas":[],"objects":[]}
    privileges=("SELECT","INSERT","UPDATE","DELETE","ALTER","VIEW DEFINITION")
    for name in names:
        row=cur.execute("SELECT SCHEMA_ID(?),"+",".join("HAS_PERMS_BY_NAME(?,'SCHEMA','"+permission+"')" for permission in privileges),name,*([name]*len(privileges))).fetchone()
        facts["schemas"].append({"schema":name,"visibility":"VISIBLE" if row[0] is not None else "ABSENT_OR_HIDDEN","schema_id":row[0],"observed_permissions":dict(zip(privileges,row[1:]))})
    objects=[];destination=config.get("destination",{});audit=config.get("audit",{});schema=audit.get("control_schema","audit")
    if database==destination.get("database","LegacyLanding"):
        target=config.get("mapping",{}).get("target_table")
        if target:objects.append(qualified(destination.get("schema","dbo"),target))
        objects.extend(qualified(schema,name) for name in ("__db_ingest_receipts","__db_ingest_targets","__db_ingest_schema_versions","__db_ingest_business_targets","__db_ingest_business_rows","__db_ingest_contributors","__db_ingest_groups","__db_ingest_contribution_history"))
    if database==destination.get("control_database","IngestControl"):
        from .control import TABLES
        objects.extend(qualified(schema,name) for name in TABLES)
    for name in dict.fromkeys(objects):
        row=cur.execute("SELECT OBJECT_ID(?),"+",".join("HAS_PERMS_BY_NAME(?,'OBJECT','"+permission+"')" for permission in privileges),name,*([name]*len(privileges))).fetchone()
        facts["objects"].append({"object":name,"visibility":"VISIBLE" if row[0] is not None else "ABSENT_OR_HIDDEN","object_id":row[0],"observed_permissions":dict(zip(privileges,row[1:]))})
    return facts


def schema_probe(conn,schema,password=None):
    """Mutate ONLY a fresh UUID scratch object; never COMMIT any probe DDL/DML."""
    if getattr(conn,"autocommit",False):return {"schema":schema,"state":"UNAVAILABLE","reason":"Rollback-only real-schema probe requires autocommit=false","completed_operations":[],"cleanup":"NO_MUTATION"}
    cur=conn.cursor();visible=cur.execute("SELECT SCHEMA_ID(?)",schema).fetchone()[0]
    if visible is None:return {"schema":schema,"state":"UNAVAILABLE","reason":"Schema is absent or hidden; no schema creation or target DML attempted","completed_operations":[],"cleanup":"NOT_CREATED"}
    scratch="__db_ingest_probe_"+uuid.uuid4().hex;target=qualified(schema,scratch)
    result={"schema":schema,"object":target,"state":"FAILED","completed_operations":[],"transaction":"rollback_only","scope":"fresh scratch table; existing target data never modified"};created=False;step="CREATE"
    try:
        cur.execute("IF @@TRANCOUNT=0 BEGIN TRANSACTION")
        # A preexisting/hidden name cannot be modified: CREATE must succeed before
        # any subsequent statement, and names are never reused between probes.
        cur.execute("CREATE TABLE "+target+" ([id] int NOT NULL PRIMARY KEY,[note] nvarchar(64) NOT NULL)")
        created=True;result["completed_operations"].append("CREATE")
        step="INSERT";cur.execute("INSERT INTO "+target+" ([id],[note]) VALUES (?,?)",1,"probe");result["completed_operations"].append("INSERT")
        step="SELECT";row=cur.execute("SELECT [id],[note] FROM "+target).fetchone()
        if row is None or tuple(row)!=(1,"probe"):raise RuntimeError("Scratch inserted values do not match")
        result["completed_operations"].append("SELECT")
        step="ALTER";cur.execute("ALTER TABLE "+target+" ADD [marker] int NULL");result["completed_operations"].append("ALTER")
        step="UPDATE";cur.execute("UPDATE "+target+" SET [note]=?,[marker]=? WHERE [id]=?","changed",2,1)
        row=cur.execute("SELECT [id],[note],[marker] FROM "+target).fetchone()
        if row is None or tuple(row)!=(1,"changed",2):raise RuntimeError("Scratch updated values do not match")
        result["completed_operations"].append("UPDATE")
        step="DELETE";cur.execute("DELETE FROM "+target+" WHERE [id]=?",1)
        if cur.execute("SELECT COUNT_BIG(*) FROM "+target).fetchone()[0]!=0:raise RuntimeError("Scratch delete count does not match")
        result["completed_operations"].append("DELETE")
        step="DROP";cur.execute("DROP TABLE "+target);result["completed_operations"].append("DROP")
        if cur.execute("SELECT OBJECT_ID(?)",target).fetchone()[0] is not None:raise RuntimeError("Scratch DROP did not remove the object")
        result["state"]="PROVEN"
    except Exception as error:
        diagnostic=provider_error(error,"explicit real-schema scratch probe "+step,(password,))
        result.update(state="FAILED",failed_operation=step,code=diagnostic.code,message=str(diagnostic),context=diagnostic.context)
    finally:
        try:
            conn.rollback()
            # This is our own just-created UUID object in a visible schema, not a
            # general claim that OBJECT_ID(NULL) proves arbitrary object absence.
            if created:
                remaining=conn.cursor().execute("SELECT OBJECT_ID(?)",target).fetchone()[0]
                result["cleanup"]="VERIFIED_ABSENT_AFTER_ROLLBACK" if remaining is None else "NOT_VERIFIED"
                if remaining is not None:result["state"]="FAILED"
            else:result["cleanup"]="ROLLBACK_COMPLETED; CREATE_NOT_ACKNOWLEDGED"
        except Exception as error:
            diagnostic=provider_error(error,"scratch rollback/absence verification",(password,))
            result.update(state="FAILED",cleanup="NOT_VERIFIED",cleanup_diagnostic={"code":diagnostic.code,"message":str(diagnostic),"context":diagnostic.context})
    return result
