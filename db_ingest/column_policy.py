"""Explicit scalar column policies; original lexical bytes remain in the archive."""
import re
from .errors import IngestError

# Only numeric, fixed-width calendar layouts with an explicit day/month position.
DATE_FORMATS = {
    "%Y-%m-%d": (r"([0-9]{4})-([0-9]{2})-([0-9]{2})", (1,2,3)),
    "%Y/%m/%d": (r"([0-9]{4})/([0-9]{2})/([0-9]{2})", (1,2,3)),
    "%Y%m%d": (r"([0-9]{4})([0-9]{2})([0-9]{2})", (1,2,3)),
    "%d/%m/%Y": (r"([0-9]{2})/([0-9]{2})/([0-9]{4})", (3,2,1)),
    "%m/%d/%Y": (r"([0-9]{2})/([0-9]{2})/([0-9]{4})", (3,1,2)),
    "%d-%m-%Y": (r"([0-9]{2})-([0-9]{2})-([0-9]{4})", (3,2,1)),
    "%m-%d-%Y": (r"([0-9]{2})-([0-9]{2})-([0-9]{4})", (3,1,2)),
}


def target_type(spec, inferred):
    from .adapters.mssql.types import parse_sql_type, SqlType
    value=spec.get("target_type",inferred)
    # Bare family names are useful only when their required size is explicit.
    bare=str(value).lower().strip()
    if bare in ("nvarchar","varchar","varbinary") and "length" in spec:value=f"{bare}({spec['length']})"
    if bare in ("decimal","numeric") and "precision" in spec and "scale" in spec:value=f"decimal({spec['precision']},{spec['scale']})"
    parsed=parse_sql_type(value)
    if "length" in spec:
        if parsed.base not in ("nvarchar","varchar","varbinary"):raise IngestError("COLUMN_TYPE_OPTIONS","length requires a string/binary target")
        parsed=SqlType(parsed.base,length=spec["length"])
    if "scale" in spec and parsed.base!="decimal":raise IngestError("COLUMN_TYPE_OPTIONS","scale requires decimal")
    if "precision" in spec or "scale" in spec:
        if parsed.base=="decimal":parsed=SqlType("decimal",precision=spec.get("precision",parsed.precision),scale=spec.get("scale",parsed.scale))
        elif parsed.base in ("time","datetime2","datetimeoffset"):parsed=SqlType(parsed.base,precision=spec["precision"])
        else:raise IngestError("COLUMN_TYPE_OPTIONS","precision requires decimal or a temporal target")
    return str(parse_sql_type(str(parsed)))


def normalize_record(record, columns, settings):
    """Replace global NULL token settings for selected columns, respecting quoting."""
    changed=False
    for spec in columns:
        if "null_tokens" not in spec:continue
        index=spec["source_ordinal"]-1
        if index>=len(record.fields):continue
        field=record.fields[index]
        if field.state=="MISSING":continue
        # Opt-in parser retains the token until this policy has run.
        value=field.text
        if value is None:continue
        sensitive=settings.get("null_tokens_case_sensitive",True)
        tokens=spec["null_tokens"]
        candidate=value if sensitive else value.casefold()
        token_set=set(tokens if sensitive else [x.casefold() for x in tokens])
        is_null=(not field.quoted or settings.get("null_tokens_in_quoted_fields",False)) and candidate in token_set
        is_null=is_null or (value=="" and settings.get("empty_string_is_null",False))
        state="NULL" if is_null else "EMPTY" if value=="" else "VALUE"
        text=None if is_null else value
        if field.state!=state or field.text!=text:
            field.state=state;field.text=text;changed=True
    return changed


def format_temporal(value, spec, settings):
    fmt=settings.get("date_format",settings.get("temporal_format","iso8601"))
    if fmt in (None,"iso8601"):return value
    if fmt not in DATE_FORMATS or spec.base not in ("date","datetime2","datetimeoffset"):
        raise IngestError("TEMPORAL_FORMAT_UNSUPPORTED","Choose ISO8601 or a documented explicit numeric calendar layout")
    pattern,order=DATE_FORMATS[fmt]
    match=re.match(pattern,str(value))
    if match is None:raise IngestError("VALUE_INVALID","Date does not match the configured exact numeric layout",2)
    prefix=match.group(0)
    tail=str(value)[len(prefix):]
    if spec.base=="date" and tail:raise IngestError("VALUE_INVALID","Unexpected material after date",2)
    if tail and tail[0] not in ("T"," "):raise IngestError("VALUE_INVALID","Expected date and clock separator",2)
    return "-".join(match.group(i) for i in order)+tail


def validate_spec(spec,allow_lossy=False):
    if not isinstance(spec,dict):raise IngestError("CONFIG_COLUMN","Column policy must be a table")
    for key in ("source_name","target_name","target_type","binary_encoding","attachment_root","collation","text_encoding"):
        if key in spec and not isinstance(spec[key],str):raise IngestError("CONFIG_COLUMN",key+" must be a string")
    if "target_name" in spec and (not spec["target_name"] or "\x00" in spec["target_name"] or len(spec["target_name"].encode("utf-16-le"))//2>128):
        raise IngestError("CONFIG_COLUMN","target_name must contain 1..128 UTF16 units without NUL")
    if "null_tokens" in spec and (not isinstance(spec["null_tokens"],list) or any(not isinstance(x,str) for x in spec["null_tokens"])):
        raise IngestError("CONFIG_COLUMN","Column null_tokens must be a list of strings")
    if "length" in spec and not (spec["length"]=="max" or isinstance(spec["length"],int) and not isinstance(spec["length"],bool) and spec["length"]>0):
        raise IngestError("CONFIG_COLUMN","Column length must be a positive integer or max")
    for key in ("precision","scale"):
        if key in spec and (not isinstance(spec[key],int) or isinstance(spec[key],bool) or spec[key]<0):raise IngestError("CONFIG_COLUMN",key+" must be a nonnegative integer")
    for key in ("date_format","temporal_format"):
        if key in spec and spec[key]!="iso8601" and spec[key] not in DATE_FORMATS:raise IngestError("CONFIG_COLUMN","Unsupported explicit calendar layout")
    if "date_format" in spec or "temporal_format" in spec:
        family=str(spec.get("target_type","")).lower().split("(")[0].strip()
        if family not in ("date","time","datetime2","datetimeoffset"):raise IngestError("CONFIG_COLUMN","A calendar layout requires an explicit temporal target_type")
        fmt=spec.get("date_format",spec.get("temporal_format"))
        if family=="time" and fmt!="iso8601":raise IngestError("CONFIG_COLUMN","A date layout cannot apply to a time-only column")
    if spec.get("fractional_second_policy","preserve_exact")!="preserve_exact":raise IngestError("CONFIG_COLUMN","Only exact temporal fractions are supported")
    if spec.get("timezone_policy","preserve")!="preserve":raise IngestError("CONFIG_COLUMN","Only preserved offsets are supported")
    for key,rule,families in (("truncation","right",("nvarchar","varchar","varbinary")),("rounding","half_even",("decimal","numeric"))):
        if key not in spec or spec[key] in ("none","error","reject"):continue
        if not allow_lossy or spec[key]!=rule:raise IngestError("CONFIG_COLUMN","A projection needs schema.allow_lossy_projection=true and an exact supported column policy")
        if str(spec.get("target_type","")).lower().split("(")[0].strip() not in families:raise IngestError("CONFIG_COLUMN","Projection policy is incompatible with its explicit target_type")
    family=str(spec.get("target_type","")).lower().split("(")[0].strip()
    if family=="varchar":
        allowed={"Latin1_General_100_BIN2_UTF8":"utf-8","Latin1_General_100_BIN2":"cp1252"}
        if allowed.get(spec.get("collation"))!=spec.get("text_encoding"):raise IngestError("CONFIG_COLUMN","VARCHAR needs an explicitly supported matching collation/text_encoding pair")
    elif "collation" in spec or "text_encoding" in spec:raise IngestError("CONFIG_COLUMN","Text encoding/collation overrides currently require explicit VARCHAR")
    if "target_type" in spec:target_type(spec,spec["target_type"])
