from decimal import Decimal
import pytest
from db_ingest.models import FieldValue,RecordEnvelope,ColumnProfile
from db_ingest.config import load_config
from db_ingest.column_policy import target_type,normalize_record
from db_ingest.profile import profile_records
from db_ingest.adapters.mssql.types import normalize_value,convert_field
from db_ingest.errors import IngestError


def test_exact_exponents_reject_rounding_and_extreme_allocations():
    assert normalize_value("1.23e2","decimal(5,2)")==Decimal("123.00")
    assert normalize_value("-0e999999","decimal(38,7)")==Decimal("0.0000000")
    for value in ("1e999999","1e-999999","1.234e-2","NaN","Infinity"):
        with pytest.raises(IngestError):normalize_value(value,"decimal(5,2)")


def test_aliases_are_real_type_declarations():
    assert target_type({"target_type":"decimal","precision":38,"scale":7},"int")=="decimal(38,7)"
    assert target_type({"length":"max"},"nvarchar(10)")=="nvarchar(max)"
    assert target_type({"precision":3},"datetime2(7)")=="datetime2(3)"
    with pytest.raises(IngestError):target_type({"length":3},"int")


def test_column_null_replaces_global_and_respects_quotes():
    record=RecordEnvelope("x",1,0,10,1,1,[FieldValue("NULL","NULL"),FieldValue("VALUE","nil"),FieldValue("VALUE","nil",True)])
    specs=[{"source_ordinal":1,"null_tokens":[]},{"source_ordinal":2,"null_tokens":["NIL"]},{"source_ordinal":3,"null_tokens":["nil"]}]
    assert normalize_record(record,specs,{"null_tokens_case_sensitive":False})
    assert [(f.state,f.text) for f in record.fields]==[("VALUE","NULL"),("NULL",None),("VALUE","nil")]


@pytest.mark.parametrize("family,values,expected",[
    ("time",["12:13:14","00:00:00.1234567"],"time(7)"),
    ("datetime2",["2024-02-29T12:13:14.1","0001-01-01 00:00:00.1234567"],"datetime2(7)"),
    ("datetimeoffset",["2024-02-29T12:13:14Z","2024-02-29T12:13:14.1234567+14:00"],"datetimeoffset(7)"),
])
def test_full_stream_temporal_inference(family,values,expected):
    records=[RecordEnvelope(str(i),i,0,1,1,1,[FieldValue("VALUE",v)]) for i,v in enumerate(values,1)]
    result=profile_records(records,["value"],load_config(environ={}))
    assert result[0].sql_type==expected
    assert [convert_field(r.fields[0],result[0]) for r in records]


def test_explicit_day_month_date_and_exact_fraction():
    column=ColumnProfile(1,"date","date","datetimeoffset(7)",True,{"date_format":"%d/%m/%Y"})
    assert convert_field(FieldValue("VALUE","29/02/2024T12:00:00.1234567+05:30"),column)=="2024-02-29T12:00:00.1234567+05:30"
    for value in ("31/02/2024T12:00:00+00:00","02/29/2024T12:00:00+00:00"):
        with pytest.raises(IngestError):convert_field(FieldValue("VALUE",value),column)


@pytest.mark.parametrize("spec",[
    {"date_format":"%d/%m/%Y"},
    {"target_type":"time(7)","date_format":"%d/%m/%Y"},
    {"null_tokens":"NULL"},
    {"target_type":"decimal","precision":38,"scale":39},
    {"target_type":"nvarchar","length":True},
])
def test_invalid_column_control_combinations_are_rejected(spec):
    from db_ingest.column_policy import validate_spec
    with pytest.raises(IngestError):validate_spec(spec)
