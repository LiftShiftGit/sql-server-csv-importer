from db_ingest.planning import identifier,truncate_utf16
from db_ingest.profile import column_name


def test_supplementary_identifiers_measure_utf16_units():
    original="\U00010400"*200
    for value in (identifier(original,original),column_name(original,1)):
        assert len(value.encode("utf-16-le"))//2<=128
        assert "\ufffd" not in value
    assert truncate_utf16("a\U00010400b",2)=="a"
    assert truncate_utf16("a\U00010400b",3)=="a\U00010400"


def test_failure_name_reserves_utf16_suffix_units():
    from db_ingest.adapters.mssql.control import failure_table_name
    value=failure_table_name("\U00010400"*60,"run")
    assert len(value.encode("utf-16-le"))//2<=128
