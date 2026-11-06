"""Exact lexical validation before SQL conversion; never use float or microsecond datetime."""
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
import base64
import binascii
import re
import uuid
from db_ingest.errors import IngestError

@dataclass(frozen=True)
class SqlType:
    base: str
    length: int | str | None = None
    precision: int | None = None
    scale: int | None = None
    def __str__(self):
        if self.base == "decimal": return f"decimal({self.precision},{self.scale})"
        if self.length is not None: return f"{self.base}({self.length})"
        if self.precision is not None: return f"{self.base}({self.precision})"
        return self.base

def parse_sql_type(value):
    t = re.sub(r"\s+", "", str(value)).lower()
    if t in ("bit","tinyint","smallint","int","bigint","date","uniqueidentifier"):
        return SqlType(t)
    m = re.fullmatch(r"(nvarchar|varchar|varbinary)\((max|[0-9]+)\)", t)
    if m:
        base, size = m.groups()
        length = size if size == "max" else int(size)
        if length != "max" and not 1 <= length <= (4000 if base == "nvarchar" else 8000):
            raise IngestError("TYPE_INVALID", f"Invalid length for {base}")
        return SqlType(base,length=length)
    m = re.fullmatch(r"(?:decimal|numeric)\(([0-9]+),([0-9]+)\)", t)
    if m:
        precision, scale = map(int,m.groups())
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return SqlType("decimal",precision=precision,scale=scale)
    m = re.fullmatch(r"(time|datetime2|datetimeoffset)(?:\(([0-9]+)\))?", t)
    if m and 0 <= int(m[2] or 7) <= 7:
        return SqlType(m[1],precision=int(m[2]) if m[2] is not None else 7)
    raise IngestError("TYPE_INVALID", f"Unsupported or invalid SQL type: {value!r}")

INTEGER_RANGES = {"tinyint":(0,255),"smallint":(-32768,32767),"int":(-2147483648,2147483647),"bigint":(-9223372036854775808,9223372036854775807)}
DATE = r"([0-9]{4}-[0-9]{2}-[0-9]{2})"
CLOCK = r"([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+))?"
OFFSET = r"(Z|[+-][0-9]{2}:[0-9]{2})"

def temporal_text(value, spec):
    text = str(value)
    if spec.base == "date":
        if not re.fullmatch(DATE,text): raise ValueError("Expected ISO YYYY-MM-DD")
        return date.fromisoformat(text).isoformat()
    pattern = CLOCK if spec.base == "time" else DATE + r"[ T]" + CLOCK
    if spec.base == "datetimeoffset": pattern += r" ?" + OFFSET
    match = re.fullmatch(pattern,text)
    if not match: raise ValueError(f"Expected unambiguous ISO {spec.base}")
    groups = list(match.groups())
    day = None if spec.base == "time" else date.fromisoformat(groups.pop(0))
    hh,mm,ss = map(int,groups[:3]); fraction = groups[3] or ""
    if hh > 23 or mm > 59 or ss > 59: raise ValueError("Invalid clock component")
    precision = spec.precision
    if len(fraction) > precision and any(c != "0" for c in fraction[precision:]):
        raise ValueError(f"Fractional seconds would round at precision {precision}")
    fraction = fraction[:precision].ljust(precision,"0")
    result = f"{hh:02}:{mm:02}:{ss:02}" + (("."+fraction) if precision else "")
    if day is not None: result = day.isoformat() + "T" + result
    if spec.base == "datetimeoffset":
        offset = groups[4]
        if offset == "Z": offset = "+00:00"
        oh,om = int(offset[1:3]),int(offset[4:6])
        if oh > 14 or om > 59 or (oh == 14 and om != 0): raise ValueError("Offset outside SQL Server +/-14:00")
        minutes = (oh*60+om) * (-1 if offset[0] == "-" else 1)
        # SQL Server also checks the UTC date range. Fractional seconds remain lexical.
        datetime(day.year,day.month,day.day,hh,mm,ss) - timedelta(minutes=minutes)
        result += offset
    return result

def normalize_value(value, sql_type, binary_encoding=None, *, text_encoding=None):
    """Return a typed exact value (temporal strings retain all seven fractional digits)."""
    spec = parse_sql_type(sql_type) if not isinstance(sql_type,SqlType) else sql_type
    if value is None: return None
    base = spec.base
    try:
        if base == "varchar":
            from .text import encoding_name
            encoding=encoding_name(text_encoding)
            if not isinstance(value,str):value=str(value)
            if "\x00" in value:raise ValueError("Embedded NUL text is not certified through the ODBC text path")
            encoded=value.encode(encoding,errors="strict")
            if encoded.decode(encoding,errors="strict")!=value:raise ValueError("VARCHAR encoder is not reversible for this text")
            if spec.length!="max" and len(encoded)>spec.length:raise ValueError(f"{len(encoded)} encoded bytes exceed {spec.length}")
            return value
        if base == "nvarchar":
            if not isinstance(value,str): value = str(value)
            units = len(value.encode("utf-16-le"))//2
            if spec.length != "max" and units > spec.length: raise ValueError(f"{units} UTF-16 units exceed {spec.length}")
            if "\x00" in value: raise ValueError("Embedded NUL text is not certified through the ODBC text path")
            return value
        if base == "varbinary":
            if isinstance(value,(bytes,bytearray,memoryview)): result = bytes(value)
            elif binary_encoding == "base64": result = base64.b64decode(str(value),validate=True)
            elif binary_encoding == "hex":
                text = str(value)
                if text.startswith("0x"): text = text[2:]
                result = binascii.unhexlify(text)
            else: raise ValueError("Explicit binary_encoding=base64|hex is required")
            if spec.length != "max" and len(result) > spec.length: raise ValueError("Decoded binary exceeds destination length")
            return result
        text = str(int(value)) if base == "bit" and isinstance(value,bool) else str(value)
        if base in INTEGER_RANGES or base == "bit":
            if not re.fullmatch(r"[+-]?[0-9]+",text): raise ValueError("Expected exact integer syntax")
            number = int(text)
            low,high = (0,1) if base == "bit" else INTEGER_RANGES[base]
            if not low <= number <= high: raise ValueError(f"Integer outside {base} range")
            return number
        if base == "decimal":
            if not re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?",text): raise ValueError("Expected exact decimal syntax")
            with localcontext() as ctx:
                # SQL results contain at most38 digits; huge exponents must never
                # turn into huge allocated integers or precision contexts.
                ctx.prec = 80
                number = Decimal(text)
                if not number.is_finite():raise ValueError("Nonfinite decimal")
                if number and number.adjusted() >= spec.precision-spec.scale:raise ValueError("Decimal precision overflow")
                quantum = Decimal(1).scaleb(-spec.scale)
                normalized = number.quantize(quantum)
                if normalized != number: raise ValueError("Decimal would round")
                if abs(normalized) >= Decimal(10) ** (spec.precision-spec.scale): raise ValueError("Decimal precision overflow")
                return normalized.copy_abs() if normalized == 0 else normalized
        if base in ("date","time","datetime2","datetimeoffset"): return temporal_text(text,spec)
        if base == "uniqueidentifier":
            if not re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}",text): raise ValueError("GUID must have exactly 36 canonical characters")
            return str(uuid.UUID(text))
    except (ValueError,OverflowError,InvalidOperation,binascii.Error,UnicodeError) as exc:
        raise IngestError("VALUE_INVALID", f"Value does not fit {spec}: {exc}", 2, {"sql_type":str(spec)}) from None
    raise IngestError("TYPE_INVALID", f"No converter for {spec}")

def convert_field(field, column):
    from db_ingest.column_policy import format_temporal
    if column.stats.get("timezone_policy") not in (None,"preserve") or column.stats.get("fractional_second_policy") not in (None,"preserve_exact"):
        raise IngestError("TEMPORAL_POLICY_UNSUPPORTED", "Only exact temporal fractions and preserved offsets are implemented")
    if field.state == "MISSING":
        if column.nullable and column.stats.get("missing_policy")=="null_with_provenance":return None
        raise IngestError("MISSING_FIELD", f"Column {column.ordinal} is absent; padding/default invention is disabled")
    if field.state == "NULL":
        if not column.nullable: raise IngestError("NULLABILITY", f"Column {column.ordinal} does not allow NULL")
        return None
    if field.state not in ("VALUE","EMPTY"): raise IngestError("FIELD_STATE", f"Unknown field state {field.state}")
    value=field.text if field.text is not None else ""
    spec=parse_sql_type(column.sql_type)
    if spec.base in ("date","time","datetime2","datetimeoffset"):value=format_temporal(value,spec,column.stats)
    return normalize_value(value,spec,column.stats.get("binary_encoding"),text_encoding=column.stats.get("text_encoding"))

def canonical_value(value, sql_type, *, text_encoding=None):
    spec = parse_sql_type(sql_type)
    tag = str(spec).encode("ascii")
    if value is None: return len(tag).to_bytes(4,"big")+tag+b"N"
    normalized = normalize_value(value,spec,text_encoding=text_encoding)
    if spec.base == "varbinary": payload = normalized
    elif spec.base == "decimal": payload = format(normalized,f".{spec.scale}f").encode("ascii")
    else: payload = str(normalized).encode("utf-8")
    return len(tag).to_bytes(4,"big")+tag+b"V"+len(payload).to_bytes(8,"big")+payload

def read_expression(column):
    from .ddl import quote_identifier
    spec = parse_sql_type(column.sql_type)
    name = quote_identifier(column.target_name)
    if spec.base in ("date","time","datetime2","datetimeoffset"):
        return f"CONVERT(nvarchar(64),{name},126)"
    if spec.base == "varchar":return f"CONVERT(nvarchar(max),{name})"
    if spec.base == "uniqueidentifier": return f"CONVERT(nvarchar(36),{name})"
    return name

def parameter_value(value):
    # Bind decimals lexically so Python/driver precision choices cannot round them.
    return format(value,"f") if isinstance(value,Decimal) else value
