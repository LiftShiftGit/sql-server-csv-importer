"""Actual provider failures: no simulated native codes or lowered TLS policy."""
import copy,os,secrets,uuid
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect
from db_ingest.adapters.mssql.diagnostics import provider_error
from db_ingest.errors import IngestError

CONFIG=os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark=pytest.mark.skipif(not CONFIG,reason="Disposable SQL configuration required")

def settings():
    config=load_config(CONFIG,environ={})
    assert all(config["destination"][k].lower().startswith("dbingesttest_") for k in ("database","control_database"))
    return config,os.environ.get("DB_USERNAME"),os.environ.get("DB_PASSWORD")


def test_real_certificate_rejection_preserves_explicit_tls_policy():
    config,user,password=settings()
    config["destination"].update(encrypt="yes",trust_server_certificate="no")
    with pytest.raises(IngestError) as error:connect(config,user,password)
    assert error.value.code=="TLS"
    assert error.value.context["sqlstate"]=="08001"
    assert error.value.context["retryable"] is False
    assert config["destination"]["trust_server_certificate"]=="no"
    assert password not in str(error.value)


def test_real_bad_login_is_not_retried_or_given_an_invented_hidden_reason():
    config,user,password=settings()
    absent="DbIngestTest_Absent_"+uuid.uuid4().hex
    attempt_secret=secrets.token_hex(24)
    with pytest.raises(IngestError) as error:connect(config,absent,attempt_secret)
    assert error.value.code=="AUTHENTICATION"
    assert error.value.context["native_code"]==18456
    assert error.value.context["retryable"] is False
    assert attempt_secret not in str(error.value)


def test_real_limited_login_permission_and_hidden_database_diagnostics():
    config,user,password=settings()
    login="DbIngestTest_Limited_"+uuid.uuid4().hex
    limited_secret="Test!"+secrets.token_hex(24)
    admin=connect(config,user,password,"master",autocommit=True)
    cur=admin.cursor()
    # Generated hex punctuation-safe fixture password is never printed or persisted.
    cur.execute("CREATE LOGIN ["+login+"] WITH PASSWORD='"+limited_secret+"', CHECK_POLICY=OFF")
    try:
        cur.execute("DENY VIEW ANY DATABASE TO ["+login+"]")
        limited=connect(config,login,limited_secret,"master",autocommit=True)
        try:
            probe="DbIngestTest_DeniedCreate_"+uuid.uuid4().hex
            try:limited.cursor().execute("CREATE DATABASE ["+probe+"]")
            except Exception as exc:diagnostic=provider_error(exc,"test denied CREATE DATABASE",(limited_secret,))
            else:pytest.fail("Disposable low-privilege fixture unexpectedly acquired CREATE DATABASE")
            assert diagnostic.code=="PERMISSION" and diagnostic.context["native_code"]==262
            assert diagnostic.context["retryable"] is False
        finally:limited.close()
        with pytest.raises(IngestError) as error:connect(config,login,limited_secret,config["destination"]["database"])
        assert error.value.code=="DATABASE_ACCESS"
        assert error.value.context["native_code"]==4060
        assert error.value.context["retryable"] is False
        assert "absence is not established" in error.value.context["remediation"]
    finally:
        cur.execute("ALTER LOGIN ["+login+"] DISABLE")
        # ODBC pooling can retain closed physical sessions; kill only this fresh
        # fixture login's sessions, never unrelated publishers.
        sessions=[r[0] for r in cur.execute("SELECT session_id FROM sys.dm_exec_sessions WHERE login_name=? AND session_id<>@@SPID",login)]
        for session in sessions:
            assert isinstance(session,int) and session>50
            cur.execute("KILL "+str(session))
        cur.execute("DROP LOGIN ["+login+"]")
        admin.close()
