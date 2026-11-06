"""SQL adapter unit/simulation tests. None is a live server integration claim."""
from dataclasses import replace
from decimal import Decimal
import hashlib
import unittest
from unittest.mock import patch,Mock
from db_ingest.errors import IngestError
from db_ingest.models import ColumnProfile,FieldValue,RecordEnvelope,SchemaPlan
from db_ingest.adapters.mssql.types import normalize_value,canonical_value,parse_sql_type,convert_field
from db_ingest.adapters.mssql.connect import build_connection_string,odbc_value,test_connection as check_connection,doctor
from db_ingest.adapters.mssql.diagnostics import provider_error
from db_ingest.adapters.mssql.ddl import quote_identifier
from db_ingest.adapters.mssql.metadata import schema_hash,validate_plan
from db_ingest.staging import validate_record,typed_stage_parameters
from db_ingest.receipts import new_value_digest,update_value_digest
from db_ingest.transports.bcp import encode_record,decode_record,tls_flags
from db_ingest.transports.odbc import choose_transport
from db_ingest.transports.sqlcmd import diagnostic_arguments
from db_ingest.publication import _lock,_ownership_mapping_hash


def config():return {"destination":{"server":"localhost,1433","database":"DbIngestTest_Data","control_database":"DbIngestTest_Control","bootstrap_database":"master","authentication":"sql","driver":"ODBC Driver 18 for SQL Server","encrypt":"yes","trust_server_certificate":"no"},"load":{"engine":"auto"},"tools":{"detect_versions":False}}

def plan():return SchemaPlan("DbIngestTest_Data","dbo","test",[ColumnProfile(1,"id","id","int"),ColumnProfile(2,"name","name","nvarchar(10)")],"a"*64,"b"*64,"c"*64)

class ExactTypeTests(unittest.TestCase):
    def invalid(self,value,kind,**kwargs):
        with self.assertRaises(IngestError) as cm:normalize_value(value,kind,**kwargs)
        self.assertEqual(cm.exception.code,"VALUE_INVALID")
    def test_integer_boundaries(self):
        for kind,low,high in [("tinyint",0,255),("smallint",-32768,32767),("int",-2147483648,2147483647),("bigint",-9223372036854775808,9223372036854775807)]:
            for n in (low,high):self.assertEqual(normalize_value(str(n),kind),n)
            for n in (low-1,high+1):self.invalid(str(n),kind)
        self.invalid("1.0","int");self.invalid(" 1","int");self.invalid("True","bit")
        self.assertEqual(normalize_value(True,"bit"),1)
        self.assertEqual(normalize_value(False,"bit"),0)
    def test_decimal_exactness_and_negative_zero(self):
        self.assertEqual(normalize_value("-0.00","decimal(3,2)"),Decimal("0.00"))
        self.assertEqual(canonical_value("-0.00","decimal(3,2)"),canonical_value("0.00","decimal(3,2)"))
        self.assertEqual(normalize_value("99999999999999999999999999999999999999","decimal(38,0)"),Decimal("99999999999999999999999999999999999999"))
        for value in ("1e3","NaN","Infinity","1.001","100.00"):self.invalid(value,"decimal(4,2)")
    def test_temporal_never_rounds_seven_digits(self):
        text="2026-10-29T12:01:02.1234567-04:00"
        self.assertEqual(normalize_value(text,"datetimeoffset(7)"),text)
        self.invalid(text,"datetimeoffset(6)")
        self.invalid("0001-01-01T00:00:00.0000000+14:00","datetimeoffset(7)")
        self.invalid("9999-12-31T23:59:59.0000000-14:00","datetimeoffset(7)")
        self.invalid("2024-02-30","date")
        self.invalid("24:00:00","time(7)")
        self.assertEqual(normalize_value("12:30:01.1200000","time(2)"),"12:30:01.12")
        self.assertEqual(normalize_value("2024-01-02 01:02:03Z","datetimeoffset(7)"),"2024-01-02T01:02:03.0000000+00:00")
    def test_text_utf16_units_and_nul_gate(self):
        self.assertEqual(normalize_value("é🙂  ","nvarchar(5)"),"é🙂  ")
        self.invalid("🙂","nvarchar(1)");self.invalid("a\x00b","nvarchar(10)")
    def test_binary_declaration_and_exact_empty(self):
        self.assertEqual(normalize_value("AAEC/w==","varbinary(4)","base64"),bytes([0,1,2,255]))
        self.assertEqual(normalize_value("","varbinary(max)","hex"),b"")
        self.invalid("00gg","varbinary(max)",binary_encoding="hex")
        self.invalid("AA==\n","varbinary(max)",binary_encoding="base64")
        self.invalid("AA==","varbinary(max)")
    def test_type_allowlist_blocks_sql_injection(self):
        for kind in ("nvarchar(max)); DROP TABLE x;--","varchar(8001)","money","float","decimal(39,0)","decimal(3,4)","datetime","nvarchar(0)","nvarchar(4001)"):
            with self.subTest(kind=kind),self.assertRaises(IngestError):parse_sql_type(kind)
        self.assertEqual(str(parse_sql_type(" NUMERIC (38, 2) ")),"decimal(38,2)")
    def test_varchar_requires_verified_policy_and_counts_storage_bytes(self):
        from db_ingest.adapters.mssql.text import varchar_policy
        self.assertEqual(str(parse_sql_type("varchar(20)")),"varchar(20)")
        with self.assertRaises(IngestError) as cm:normalize_value("text","varchar(20)")
        self.assertEqual(cm.exception.code,"VARCHAR_POLICY_REQUIRED")
        self.assertEqual(normalize_value("é🙂","varchar(6)",text_encoding="utf-8"),"é🙂")
        with self.assertRaises(IngestError) as cm:normalize_value("é🙂","varchar(5)",text_encoding="utf-8")
        self.assertEqual(cm.exception.code,"VALUE_INVALID")
        with self.assertRaises(IngestError):normalize_value("🙂","varchar(10)",text_encoding="cp1252")
        self.assertEqual(normalize_value("é€ ","varchar(3)",text_encoding="cp1252"),"é€ ")
        with self.assertRaises(IngestError):varchar_policy({"text_encoding":"utf-8","collation":"Latin1_General_100_BIN2"})
        with self.assertRaises(IngestError):varchar_policy({"text_encoding":"utf-8","collation":"Latin1_General_100_BIN2_UTF8;DROP TABLE x"})
    def test_null_empty_missing_and_multiple_errors(self):
        p=plan();record=RecordEnvelope("r",2,0,0,1,1,[FieldValue("VALUE","bad"),FieldValue("VALUE","x"*11)])
        errors=validate_record(p,record)
        self.assertEqual([x["column_ordinal"] for x in errors],[1,2])
        self.assertIsNone(convert_field(FieldValue("NULL",None),p.columns[0]))
        with self.assertRaises(IngestError):convert_field(FieldValue("MISSING",None),p.columns[0])
        self.assertEqual(convert_field(FieldValue("EMPTY",""),p.columns[1]),"")
    def test_typed_stage_never_binds_python_none(self):
        columns=[ColumnProfile(i,str(i),"c"+str(i),kind) for i,kind in enumerate(("int","decimal(38,4)","nvarchar(64)","varbinary(max)","date","time(7)","datetime2(7)","datetimeoffset(7)","uniqueidentifier"),1)]
        parameters=typed_stage_parameters(columns,[None]*len(columns))
        self.assertEqual(parameters[::2],[1]*len(columns))
        self.assertNotIn(None,parameters)
        self.assertEqual(typed_stage_parameters(columns[:4],[1,Decimal("1.0000"),"",b""]),[0,1,0,"1.0000",0,"",0,b""])
    def test_golden_canonical_framing(self):
        # Independently authored protocol bytes, not an expected value built by the converter.
        self.assertEqual(canonical_value(1,"int"),b"\x00\x00\x00\x03intV\x00\x00\x00\x00\x00\x00\x00\x011")
        self.assertEqual(canonical_value(None,"int"),b"\x00\x00\x00\x03intN")
        self.assertNotEqual(canonical_value("a ","nvarchar(10)"),canonical_value("a","nvarchar(10)"))
    def test_duplicate_rows_have_distinct_identity_digest(self):
        p=plan();a=new_value_digest();b=new_value_digest()
        update_value_digest(a,"record-a",2,[1,"same"],p.columns)
        update_value_digest(b,"record-b",3,[1,"same"],p.columns)
        self.assertNotEqual(a.hexdigest(),b.hexdigest())

class ConnectionPolicyTests(unittest.TestCase):
    def test_punctuation_is_braced_without_shell(self):
        password=" ;} ' \" $() `雪"
        result=build_connection_string(config(),username="u;}",password=password)
        self.assertIn("UID={u;}}}",result)
        self.assertIn("PWD="+odbc_value(password),result)
        self.assertIn("Encrypt={yes};TrustServerCertificate={no}",result)
        self.assertEqual(odbc_value("a}b"),"{a}}b}")
    def test_false_is_not_truthy(self):
        c=config();c["destination"]["encrypt"]="false"
        self.assertIn("Encrypt={no}",build_connection_string(c,"u","p"))
    def test_password_omitted_integrated(self):
        c=config();c["destination"]["authentication"]="integrated"
        result=build_connection_string(c)
        self.assertIn("Trusted_Connection={yes}",result)
        self.assertNotIn("PWD=",result)
    def test_diagnostics_redact_and_do_not_invent_reason(self):
        secret="p;}雪$()`"
        error=provider_error(Exception("28000","[SQL Server]Login failed (18456) "+secret),"connect",(secret,))
        self.assertEqual(error.code,"AUTHENTICATION");self.assertNotIn(secret,str(error));self.assertNotIn(secret,str(error.context))
        error=provider_error(Exception("08001","Unspecified provider failure"),"connect")
        self.assertEqual(error.code,"DATABASE");self.assertNotIn("firewall",str(error).lower())
    def test_specific_database_access_precedes_generic_login_failure(self):
        error=provider_error(Exception("28000","[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Login failed for user (18456); Cannot open database requested by the login (4060)"),"connect")
        self.assertEqual(error.code,"DATABASE_ACCESS")
        self.assertEqual(error.context["native_code"],4060)
        self.assertIn("18456",error.context["diagnostic"])
        self.assertIn("4060",error.context["diagnostic"])
        self.assertIn("absence is not established",error.context["remediation"])
    def test_failed_connection_is_nonzero(self):
        with patch("db_ingest.adapters.mssql.connect.connect",side_effect=IngestError("AUTHENTICATION","login rejected",4)):
            result=check_connection(config(),"u","p")
        self.assertFalse(result["ok"]);self.assertEqual(result["exit_code"],4)
    def test_missing_driver_is_preflight_failure(self):
        with patch("db_ingest.adapters.mssql.connect.connect",side_effect=IngestError("DRIVER_MISSING","driver absent",2)):
            result=check_connection(config(),"u","p")
        self.assertEqual(result["exit_code"],2)
    def test_doctor_missing_runtime_is_nonzero(self):
        with patch("db_ingest.adapters.mssql.connect.importlib.import_module",side_effect=ImportError):result=doctor(config())
        self.assertFalse(result["selected_driver_available"]);self.assertEqual(result["exit_code"],2)
    def test_explicit_bcp_never_counts_odbc_fallback(self):
        c=config();c["load"]["engine"]="bcp"
        with patch("db_ingest.transports.bcp.prerequisite",return_value=(False,"test prerequisite unavailable",{"available":False})):
            with self.assertRaises(IngestError) as cm:choose_transport(c)
            self.assertEqual(cm.exception.code,"BCP_PREREQUISITE")
            c["load"]["engine"]="auto"
            self.assertEqual(choose_transport(c)["engine"],"odbc")
    def test_tool_flags_remain_separate(self):
        with self.assertRaises(IngestError):tls_flags(16)
        self.assertEqual(tls_flags({"tls_flags":True,"version":"17.0.4055.5"},"yes","yes"),["-Ym","-u"])
        c=config();c["destination"]["trust_server_certificate"]="yes"
        with patch("db_ingest.adapters.mssql.connect.tool_info",return_value={"state":"DETECTED","path":"sqlcmd","version_output":"-N[s|m|o] -C"}):
            args,env=diagnostic_arguments(c,"x;$(bad).sql","u","secret}")
        self.assertIn("-Nm",args);self.assertIn("-C",args);self.assertNotIn("secret}",args)
        self.assertEqual(env["SQLCMDPASSWORD"],"secret}")
        self.assertIn("x;$(bad).sql",args)

class MetadataAndTransportTests(unittest.TestCase):
    def test_identifiers_quote_closing_bracket(self):
        self.assertEqual(quote_identifier("a]b"),"[a]]b]")
        for value in ("", "🙂"*65,"a\x00b"):
            with self.assertRaises(IngestError):quote_identifier(value)
    def test_schema_fingerprint_and_operation_independent_ownership(self):
        p=plan();other=replace(p,mapping_hash="d"*64)
        self.assertEqual(_ownership_mapping_hash(p),_ownership_mapping_hash(other))
        other=replace(p,columns=[replace(p.columns[0],nullable=False),p.columns[1]])
        self.assertNotEqual(schema_hash(p),schema_hash(other))
    def test_metadata_collision_blocks(self):
        p=plan();p.columns[1].target_name="__ingest_record"
        with self.assertRaises(IngestError) as cm:validate_plan(p)
        self.assertEqual(cm.exception.code,"SCHEMA_NAME_COLLISION")
    def test_delimiter_safe_transport_round_trip(self):
        record=RecordEnvelope("a\tb\n雪",7,0,0,1,1,[FieldValue("VALUE","x\t\r\n雪",True),FieldValue("EMPTY","",True),FieldValue("NULL",None),FieldValue("MISSING",None)])
        encoded=encode_record(record)
        self.assertEqual(encoded.count(b"\n"),1)
        self.assertEqual(encoded.count(b"\t"),6)
        identity,ordinal,fields=decode_record(encoded)
        self.assertEqual((identity,ordinal,fields),(record.record_id,record.ordinal,record.fields))
    def test_invalid_transport_is_not_accepted(self):
        for raw in (b"D2\tYQ==\t1\n",b"D1\tYQ==\t1\tN0YQ==\n",b"D1\tYQ==\t0\n",b"D1\tYQ==\t1\tV0!\n"):
            with self.assertRaises(IngestError):decode_record(raw)
    def test_transaction_lock_starts_and_fails_explicitly(self):
        cur=Mock();cur.execute.return_value=cur;cur.fetchone.return_value=(-1,)
        with self.assertRaises(IngestError) as cm:_lock(cur,"target",1)
        self.assertEqual(cm.exception.code,"TARGET_LOCK")
        self.assertIn("BEGIN TRANSACTION",cur.execute.call_args_list[0].args[0])


class PublicationControlFlowSimulations(unittest.TestCase):
    """Simulated driver boundaries only; does not certify server transactions."""
    def fake(self,existing=False):
        class Cursor:
            def __init__(self):self.sql=[];self.value=None;self.exists=existing
            def execute(self,sql,*args):
                self.sql.append((sql,args))
                self.value=None
                if "ProductMajorVersion" in sql:self.value=(16,)
                elif sql.startswith("SELECT OBJECT_ID"):self.value=(42 if self.exists else None,)
                elif sql.startswith("SELECT SCHEMA_ID"):self.value=(1,)
                elif sql.startswith("CREATE TABLE [dbo].[test]"):self.exists=True
                return self
            def fetchone(self):return self.value
            def setinputsizes(self,sizes):self.sizes=sizes
            def executemany(self,sql,rows):
                for row in rows:self.execute(sql,*row)
                return self
        cur=Cursor();conn=Mock();conn.cursor.return_value=cur
        return conn,cur
    def context(self,conn,digest):
        import contextlib
        stack=contextlib.ExitStack()
        stack.enter_context(patch("db_ingest.publication._bootstrap_database",return_value=conn))
        stack.enter_context(patch("db_ingest.publication._bootstrap_metadata"))
        stack.enter_context(patch("db_ingest.adapters.mssql.evolution.snapshot"))
        stack.enter_context(patch("db_ingest.publication_history.check_source_history",return_value={"state":"SIMULATED"}))
        stack.enter_context(patch("db_ingest.publication._get_receipt",return_value=None))
        stack.enter_context(patch("db_ingest.publication._lock"))
        stack.enter_context(patch("db_ingest.publication._database_guid",return_value="01234567-89ab-cdef-0123-456789abcdef"))
        stack.enter_context(patch("db_ingest.publication._readback",return_value=(1,digest)))
        return stack
    def record(self):return RecordEnvelope("record-a",2,0,4,2,2,[FieldValue("VALUE","1"),FieldValue("VALUE","a ")])
    def test_creator_race_reprobes_online_database_after_lock(self):
        from db_ingest.publication import _bootstrap_database
        bootstrap=Mock();cur=bootstrap.cursor.return_value;cur.execute.return_value=cur;cur.fetchone.return_value=("DbIngestTest_Data","ONLINE")
        destination=Mock();c=config();c["destination"]["create_databases"]=True
        with patch("db_ingest.publication.connect",side_effect=[IngestError("DATABASE_ACCESS","initial inaccessible",4),bootstrap,destination]) as connection,patch("db_ingest.publication._lock"):
            observed=_bootstrap_database(c,"DbIngestTest_Data","u","secret")
        self.assertIs(observed,destination)
        self.assertEqual(connection.call_count,3)
        self.assertFalse(any(call.args[0].startswith("CREATE DATABASE") for call in cur.execute.call_args_list))
        bootstrap.close.assert_called_once()
    def test_lost_commit_ack_is_unknown_and_never_retried(self):
        from db_ingest.publication import publish
        p=plan();record=self.record();digest=new_value_digest();update_value_digest(digest,record.record_id,record.ordinal,[1,"a "],p.columns)
        conn,cur=self.fake();conn.commit.side_effect=[None,None,RuntimeError("network lost after COMMIT")]
        c=config();c["destination"].update(create_tables=True,create_schemas=True)
        with self.context(conn,digest.hexdigest()),self.assertRaises(IngestError) as cm:publish(c,p,[record],"intent","run","u","secret")
        self.assertEqual(cm.exception.exit_code,5)
        self.assertIn("COMMIT_OUTCOME_UNKNOWN",str(cm.exception))
        self.assertEqual(conn.commit.call_count,3)
        self.assertEqual(sum(sql.startswith("INSERT INTO [dbo].[test]") for sql,args in cur.sql),1)
        conn.close.assert_called_once()
    def test_value_mismatch_rolls_back_before_receipt(self):
        from db_ingest.publication import publish
        conn,cur=self.fake();c=config();c["destination"].update(create_tables=True,create_schemas=True)
        digest=new_value_digest();record=self.record();update_value_digest(digest,record.record_id,record.ordinal,[1,"a "],plan().columns)
        with self.context(conn,digest.hexdigest()),patch("db_ingest.publication._readback",side_effect=[(1,digest.hexdigest()),(1,"wrong-digest")]),self.assertRaises(IngestError) as cm:publish(c,plan(),[record],"intent","run","u","p")
        self.assertEqual(cm.exception.code,"VALUE_VERIFICATION_FAILED")
        self.assertEqual(conn.commit.call_count,2) # Empty session-temp DDL and isolated staging only.
        self.assertFalse(any(sql.startswith("INSERT INTO [audit].[__db_ingest_receipts]") for sql,args in cur.sql))
        conn.rollback.assert_called()
    def test_unowned_target_blocks_before_target_mutation(self):
        from db_ingest.publication import publish
        conn,cur=self.fake(existing=True);c=config()
        digest=new_value_digest();record=self.record();update_value_digest(digest,record.record_id,record.ordinal,[1,"a "],plan().columns)
        with self.context(conn,digest.hexdigest()),self.assertRaises(IngestError) as cm:publish(c,plan(),[record],"intent","run","u","p")
        self.assertEqual(cm.exception.code,"TARGET_LINEAGE_UNSUPPORTED")
        self.assertFalse(any(sql.startswith("INSERT INTO [dbo].[test]") or sql.startswith("DELETE FROM [dbo].[test]") for sql,args in cur.sql))
    def test_oversized_destination_is_not_fetched(self):
        from db_ingest.publication import _readback
        cur=Mock();cur.execute.return_value=cur;cur.fetchone.return_value=(9000000,9000000,0)
        with self.assertRaises(IngestError) as cm:_readback(cur,plan(),"[dbo].[test]","intent",config())
        self.assertEqual(cm.exception.code,"READBACK_BUDGET")
        self.assertEqual(cur.execute.call_count,1)
        self.assertIn("DATALENGTH",cur.execute.call_args.args[0])
    def test_fault_hook_refuses_nondisposable_database(self):
        from db_ingest.transports.faults import test_fault
        import os
        with patch.dict(os.environ,{"DB_INGEST_TEST_MODE":"1","DB_INGEST_TEST_FAULT":"AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT"}),patch("db_ingest.transports.faults.os._exit") as hard_exit:
            c=config();c["destination"]["database"]="Production"
            with self.assertRaises(IngestError) as cm:test_fault(c,"AFTER_DESTINATION_COMMIT_BEFORE_LOCAL_CHECKPOINT")
            self.assertEqual(cm.exception.code,"TEST_FAULT_DESTINATION")
            hard_exit.assert_not_called()

if __name__=="__main__":unittest.main()
