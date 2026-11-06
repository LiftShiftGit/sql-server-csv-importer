"""Real-schema probe control flow; actual permissions are tested independently."""
from db_ingest.adapters.mssql.permissions import schema_probe

class Cursor:
    def __init__(self,deny=False,visible=True):self.statements=[];self.value=None;self.exists=False;self.data=None;self.deny=deny;self.visible=visible
    def execute(self,sql,*values):
        self.statements.append((sql,values));self.value=None
        if sql.startswith("SELECT SCHEMA_ID"):self.value=(1 if self.visible else None,)
        elif sql.startswith("CREATE TABLE"):
            if self.deny:raise RuntimeError("[42000] CREATE TABLE permission denied (262)")
            self.exists=True
        elif sql.startswith("INSERT INTO"):self.data=(1,"probe")
        elif sql.startswith("SELECT [id],[note],[marker]"):self.value=self.data
        elif sql.startswith("SELECT [id],[note]"):self.value=self.data
        elif sql.startswith("UPDATE "):self.data=(1,"changed",2)
        elif sql.startswith("DELETE FROM"):self.data=None
        elif sql.startswith("SELECT COUNT_BIG"):self.value=(0 if self.data is None else 1,)
        elif sql.startswith("DROP TABLE"):self.exists=False
        elif sql.startswith("SELECT OBJECT_ID"):self.value=(12 if self.exists else None,)
        return self
    def fetchone(self):return self.value
class Connection:
    autocommit=False
    def __init__(self,**kw):self.c=Cursor(**kw);self.rollbacks=0
    def cursor(self):return self.c
    def rollback(self):self.rollbacks+=1;self.c.exists=False


def test_real_schema_probe_executes_all_required_operations_then_rolls_back():
    conn=Connection();result=schema_probe(conn,"permission_schema")
    assert result["state"]=="PROVEN" and result["cleanup"]=="VERIFIED_ABSENT_AFTER_ROLLBACK"
    assert set(result["completed_operations"])=={"CREATE","INSERT","SELECT","ALTER","UPDATE","DELETE","DROP"}
    assert conn.rollbacks==1
    assert result["object"].startswith("[permission_schema].[__db_ingest_probe_")
    assert not any("#db_ingest" in sql or "COMMIT" in sql for sql,values in conn.c.statements)


def test_failed_create_does_not_touch_any_existing_object():
    conn=Connection(deny=True);result=schema_probe(conn,"permission_schema","protected")
    assert result["state"]=="FAILED" and result["failed_operation"]=="CREATE"
    assert result["context"]["native_code"]==262 and result["completed_operations"]==[]
    assert not any(sql.startswith(("INSERT","ALTER","UPDATE","DELETE","DROP")) for sql,values in conn.c.statements)
    assert conn.rollbacks==1


def test_absent_or_hidden_schema_is_unavailable_not_assumed_absent():
    conn=Connection(visible=False);result=schema_probe(conn,"hidden_schema")
    assert result["state"]=="UNAVAILABLE" and result["cleanup"]=="NOT_CREATED"
    assert all(sql.startswith("SELECT") for sql,values in conn.c.statements)


def test_autocommit_probe_is_refused_before_sql():
    conn=Connection();conn.autocommit=True
    assert schema_probe(conn,"permission_schema")["state"]=="UNAVAILABLE"
    assert conn.c.statements==[]
