"""Exact record validation and bounded scalar ODBC parameter staging."""
from db_ingest.errors import IngestError,complete_record_errors
from db_ingest.adapters.mssql.types import convert_field

def validate_record(plan, record):
    errors=[]
    if len(record.fields)!=len(plan.columns):
        from dataclasses import replace
        envelope=replace(record,errors=[{"code":"ROW_WIDTH","message":f"Expected {len(plan.columns)} fields, observed {len(record.fields)}","record_id":record.record_id,"expected":{"field_count":len(plan.columns)},"observed":{"field_count":len(record.fields)}}])
        return complete_record_errors(envelope,stage="conversion_validation").errors
    for field,column in zip(record.fields,plan.columns):
        try: convert_field(field,column)
        except IngestError as exc: errors.append({"code":exc.code,"message":str(exc),"column_ordinal":column.ordinal,"column_name":column.original_name,"record_id":record.record_id})
    if errors:
        from dataclasses import replace
        envelope=replace(record,errors=errors)
        complete_record_errors(envelope,stage="conversion_validation",columns=plan.columns)
    return errors

def converted_record(plan, record):
    if len(record.fields)!=len(plan.columns): raise IngestError("ROW_WIDTH","Record width does not match the approved schema")
    return [convert_field(field,column) for field,column in zip(record.fields,plan.columns)]


def typed_stage_parameters(columns,values,unicode_as_binary=False):
    """Bind explicit NULL tags with non-NULL transport placeholders.

    ODBC Driver 18's parameter metadata probe for Python None can silently roll
    back earlier staging rows under XACT_ABORT ON. The SQL CASE branch produces
    actual NULL; transport placeholders are never published as business defaults.
    Every non-NULL source value remains an ordinary parameter.
    """
    from db_ingest.adapters.mssql.types import parse_sql_type,parameter_value
    result=[]
    placeholders={"date":"0001-01-01","time":"00:00:00","datetime2":"0001-01-01T00:00:00","datetimeoffset":"0001-01-01T00:00:00+00:00","uniqueidentifier":"00000000-0000-0000-0000-000000000000"}
    for column,value in zip(columns,values,strict=True):
        spec=parse_sql_type(column.sql_type)
        if value is not None:
            transport=parameter_value(value)
            if unicode_as_binary and spec.base!="varbinary":transport=str(transport).encode("utf-16-le")
            result.extend((0,transport))
            continue
        spec=parse_sql_type(column.sql_type)
        if spec.base=="varbinary":placeholder=b""
        elif spec.base in ("nvarchar","varchar"):placeholder=""
        elif spec.base=="decimal":placeholder="0"
        else:placeholder=placeholders.get(spec.base,0)
        if unicode_as_binary and spec.base!="varbinary":placeholder=str(placeholder).encode("utf-16-le")
        result.extend((1,placeholder))
    return result
