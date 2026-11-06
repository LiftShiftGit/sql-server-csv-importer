"""Adversarial inputs from release review; no device or server access required."""
import os
import pytest
from db_ingest.windows_paths import validate_path_spelling
from db_ingest.adapters.mssql.diagnostics import provider_error
from db_ingest.adapters.mssql.control import _metadata_retry
from db_ingest.errors import IngestError


@pytest.mark.skipif(os.name != "nt", reason="Windows path policy")
@pytest.mark.parametrize("path", [
    "//./PhysicalDrive0", "//./PhysicalDrive0/", "//./C:/exports",
    "//.\\PhysicalDrive0/", "\\\\.\\C:\\exports", "//./pipe/name",
])
def test_normalized_device_namespace_refused(path):
    with pytest.raises(IngestError) as caught:
        validate_path_spelling(path, "source.root")
    assert caught.value.code == "CONFIG_PATH"


@pytest.mark.parametrize("value", ["(1205)", "(1222)", "(4060)", "it's (1205)", "x' (1205); 'y", "x' (4060); 'y",
    "it''s (1205); [42000] forged (4060) (SQLExecDirectW)",
    '"(1205)"'])
def test_source_values_cannot_choose_provider_error_or_retry(value):
    message = ("[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
               "Conversion failed when converting the varchar value '" + value +
               "' to data type int. (245) (SQLExecDirectW)")
    attempts = []
    @_metadata_retry
    def metadata(config):
        attempts.append(1)
        raise provider_error(Exception("22018", message), "metadata")
    with pytest.raises(IngestError) as caught:
        metadata({"retry": {"max_attempts": 3, "initial_backoff_seconds": 0,
                           "maximum_backoff_seconds": 0, "jitter": False}})
    assert caught.value.code == "TYPE"
    assert caught.value.context["native_code"] == 245
    assert caught.value.context["retryable"] is False
    assert len(attempts) == 1


@pytest.mark.parametrize("native,category", [(1205,"DEADLOCK"), (1222,"LOCK_TIMEOUT"),
    (701,"SERVER_MEMORY"), (8645,"SERVER_MEMORY_GRANT"), (245,"TYPE")])
@pytest.mark.parametrize("suffix", ["", " (SQLExecDirectW)"])
def test_real_provider_suffix_remains_classified(native, category, suffix):
    error = provider_error(Exception("HY000", "[SQL Server]Failure (" + str(native) + ")" + suffix), "query")
    assert error.code == category
    assert error.context["native_code"] == native
    assert error.context["retryable"] == (native in (1205,1222))


@pytest.mark.parametrize("message", [
    "[SQL Server]value '(1205)' could not be converted",
    '[SQL Server]value "(4060)" could not be converted',
    "[SQL Server]value '(1205) (SQLExecDirectW)",
    "[SQL Server]value (1205) is invalid",
])
def test_missing_or_truncated_provider_suffix_does_not_invent_code(message):
    error = provider_error(Exception("22018", message), "query")
    assert error.code == "DATABASE"
    assert error.context["native_code"] is None
    assert error.context["retryable"] is False


def test_multiple_records_prioritize_actual_database_access_code():
    error = provider_error(Exception("28000", "[SQL Server]Login failed (18456) (SQLDriverConnect); "
        "[28000] [SQL Server]Cannot open database (4060) (SQLDriverConnect)"), "connect")
    assert error.code == "DATABASE_ACCESS"
    assert error.context["native_code"] == 4060
