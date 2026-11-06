"""Actual ODBC conversion errors must not treat source values as native codes."""
import os
import pytest
from db_ingest.config import load_config
from db_ingest.adapters.mssql.connect import connect
from db_ingest.adapters.mssql.diagnostics import provider_error

CONFIG = os.environ.get("DB_INGEST_TEST_CONFIG")
pytestmark = pytest.mark.skipif(not CONFIG, reason="Disposable SQL configuration required")


@pytest.mark.parametrize("value", ["(1205)", "(1222)", "(4060)", "it's (1205)", "x' (1205); 'y", "x' (4060); 'y",
    "(1205); [42000] fake (4060) (SQLExecDirectW)", '"(1205)"'])
def test_real_conversion_error_does_not_enable_retry(value):
    config = load_config(CONFIG, environ={})
    assert config["destination"]["database"].lower().startswith("dbingesttest_")
    user, password = os.environ.get("DB_USERNAME"), os.environ.get("DB_PASSWORD")
    connection = connect(config, user, password, "master", autocommit=True)
    try:
        with pytest.raises(Exception) as caught:
            connection.cursor().execute("SELECT CONVERT(int, ?)", value).fetchone()
        error = provider_error(caught.value, "conversion probe", (password,))
        assert error.context["native_code"] in (245, 8114)
        assert error.code == "TYPE"
        assert error.context["retryable"] is False
    finally:
        connection.close()
