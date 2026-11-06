"""Explicit VARCHAR representation policy; Unicode NVARCHAR remains the fallback."""
import codecs
import re
from db_ingest.errors import IngestError


def encoding_name(value):
    if not isinstance(value,str) or not value:raise IngestError("VARCHAR_POLICY_REQUIRED","VARCHAR requires explicit text_encoding and collation; ordinary text fallback is NVARCHAR")
    try:name=codecs.lookup(value).name
    except LookupError:raise IngestError("VARCHAR_ENCODING_UNSUPPORTED","VARCHAR text_encoding must be verified utf-8 or cp1252") from None
    if name not in ("utf-8","cp1252"):raise IngestError("VARCHAR_ENCODING_UNSUPPORTED","Only UTF8 and CP1252 VARCHAR representations are currently verified")
    return name


def varchar_policy(column):
    stats=column.stats if hasattr(column,"stats") else column
    encoding=encoding_name(stats.get("text_encoding"))
    collation=stats.get("collation")
    if not isinstance(collation,str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}",collation):raise IngestError("VARCHAR_COLLATION_REQUIRED","VARCHAR requires an explicit legal collation name; arbitrary SQL expressions are not accepted")
    if encoding=="utf-8" and not collation.upper().endswith("_UTF8"):raise IngestError("VARCHAR_COLLATION_ENCODING","UTF8 VARCHAR requires an explicit UTF8 collation")
    if encoding=="cp1252" and collation.upper().endswith("_UTF8"):raise IngestError("VARCHAR_COLLATION_ENCODING","CP1252 VARCHAR cannot use a UTF8 collation")
    return {"text_encoding":encoding,"collation":collation,"code_page":65001 if encoding=="utf-8" else 1252}


def verify_plan(cur,plan):
    from .types import parse_sql_type
    checked={};evidence=[]
    for column in plan.columns:
        if parse_sql_type(column.sql_type).base!="varchar":continue
        policy=varchar_policy(column);key=policy["collation"].casefold()
        if key not in checked:
            row=cur.execute("SELECT name,CONVERT(int,COLLATIONPROPERTY(name,'CodePage')) FROM sys.fn_helpcollations() WHERE name=?",policy["collation"]).fetchone()
            if row is None:raise IngestError("VARCHAR_COLLATION_UNAVAILABLE","Requested VARCHAR collation is not available on this actual engine",2,policy)
            checked[key]=(row[0],row[1])
        actual,code_page=checked[key]
        if code_page!=policy["code_page"]:raise IngestError("VARCHAR_COLLATION_ENCODING","Actual collation code page differs from the declared strict encoder",2,{**policy,"observed_code_page":code_page})
        evidence.append({"column":column.target_name,"collation":actual,"code_page":code_page,"text_encoding":policy["text_encoding"]})
    return evidence


def declaration(column):
    from .ddl import quote_identifier
    from .types import parse_sql_type
    spec=parse_sql_type(column.sql_type)
    collation=" COLLATE "+varchar_policy(column)["collation"] if spec.base=="varchar" else ""
    return quote_identifier(column.target_name)+" "+str(spec)+collation+(" NULL" if column.nullable else " NOT NULL")


def conversion(column,expression):
    from .types import parse_sql_type
    spec=parse_sql_type(column.sql_type)
    if spec.base=="varchar":expression="(CONVERT(nvarchar(max),"+expression+") COLLATE "+varchar_policy(column)["collation"]+")"
    return f"CONVERT({spec},{expression})"
