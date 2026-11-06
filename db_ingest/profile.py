"""Complete-stream profiles with conservative, exact SQL type proposals."""
import datetime
import hashlib
import math
import re
import uuid
from .errors import IngestError
from .models import ColumnProfile
from .planning import truncate_utf16

_INTEGER = re.compile(r"^[+-]?[0-9]+$")
_DECIMAL = re.compile(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$")
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_GUID = re.compile(r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$")
# Explicit ISO lexical validation retains all seven fractional digits.
_TEMPORAL = re.compile(r"^(?:(?:[0-9]{4}-[0-9]{2}-[0-9]{2})[ T])?[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.([0-9]+))?(?: ?(?:Z|[+-][0-9]{2}:[0-9]{2}))?$")


def column_name(label, ordinal):
    """Ordinal and hash prevent collisions with duplicates/reserved ingest names."""
    stem = re.sub(r"[^\w]", "_", label, flags=re.UNICODE).strip("_") or "column"
    if stem[0].isdigit():
        stem = "c_" + stem
    suffix = hashlib.sha256(f"{ordinal}:{label}".encode()).hexdigest()[:10]
    return f"{truncate_utf16(stem,108)}_{ordinal}_{suffix}"


def _kind(value, settings):
    if _INTEGER.fullmatch(value) and len(value.lstrip("+-")) <= 38:
        digits = value.lstrip("+-")
        if len(digits) > 1 and digits.startswith("0") and settings.get("leading_zero_policy", "text") == "text":
            return "text"
        return "integer"
    if _DECIMAL.fullmatch(value):
        unsigned = value.lstrip("+-")
        integer, _, fraction = unsigned.partition(".")
        if len(integer.lstrip("0")) + len(fraction) <= 38:
            if len(integer) > 1 and integer.startswith("0") and settings.get("leading_zero_policy", "text") == "text":
                return "text"
            return "decimal"
    if _DATE.fullmatch(value):
        try:
            datetime.date.fromisoformat(value)
            return "date"
        except ValueError:
            return "text"
    if _TEMPORAL.fullmatch(value):
        from .adapters.mssql.types import normalize_value
        family="time" if value[2:3]==":" else "datetimeoffset" if re.search(r"(?:Z|[+-][0-9]{2}:[0-9]{2})$",value) else "datetime2"
        fraction=_TEMPORAL.fullmatch(value)[1] or ""
        if len(fraction)>7:return "text"
        try:
            normalize_value(value,f"{family}({len(fraction)})")
            return family
        except IngestError:return "text"
    if _GUID.fullmatch(value):
        try:
            uuid.UUID(value)
            return "guid"
        except ValueError:
            pass
    return "text"


def profile_records(records, headers, config):
    """Consume a complete record iterator; never retain rows or distinct values.

    NULL/EMPTY/MISSING remain separate observed counts. Rejected rows do not
    narrow the published schema. Explicit overrides are schema declarations;
    the publication converter must validate each cell before mutation.
    """
    settings = config.get("schema", {})
    header_list = list(config.get("parsing", {}).get("column_names", []) or headers)
    stats = []
    for _ in header_list:
        stats.append({"records": 0, "value_count": 0, "null_count": 0, "empty_count": 0, "missing_count": 0, "max_utf16_units": 0, "max_characters": 0, "integer_min": None, "integer_max": None, "max_integer_digits": 0, "max_scale": 0, "candidate": None})
    for record in records:
        if record.kind != "data":
            continue
        if not header_list:
            header_list = [f"column_{i + 1}" for i in range(len(record.fields))]
            stats = [{"records": 0, "value_count": 0, "null_count": 0, "empty_count": 0, "missing_count": 0, "max_utf16_units": 0, "max_characters": 0, "integer_min": None, "integer_max": None, "max_integer_digits": 0, "max_scale": 0, "candidate": None} for _ in header_list]
        if len(record.fields) != len(header_list):
            raise IngestError("ROW_WIDTH", "Profile received an unquarantined ragged row", context={"record_id": record.record_id})
        for index, field in enumerate(record.fields):
            item = stats[index]
            item["records"] += 1
            if field.state in {"NULL", "MISSING"}:
                item[field.state.lower() + "_count"] += 1
                continue
            value = field.text if field.text is not None else ""
            item["value_count"] += 1
            item["max_characters"] = max(item["max_characters"], len(value))
            item["max_utf16_units"] = max(item["max_utf16_units"], len(value.encode("utf-16-le")) // 2)
            if field.state == "EMPTY":
                item["empty_count"] += 1
            kind = _kind(value, settings)
            prior = item["candidate"]
            if prior is None:
                item["candidate"] = kind
            elif prior != kind:
                item["candidate"] = "decimal" if {prior, kind} <= {"integer", "decimal"} else "text"
            if kind in {"time","datetime2","datetimeoffset"}:
                fraction=_TEMPORAL.fullmatch(value)[1] or ""
                item["max_fractional_precision"]=max(item.get("max_fractional_precision",0),len(fraction))
            if kind in {"integer", "decimal"}:
                unsigned = value.lstrip("+-")
                integer, _, fraction = unsigned.partition(".")
                item["max_integer_digits"] = max(item["max_integer_digits"], len(integer.lstrip("0")))
                item["max_scale"] = max(item["max_scale"], len(fraction))
            if kind == "integer":
                integer_value = int(value)
                item["integer_min"] = integer_value if item["integer_min"] is None else min(item["integer_min"], integer_value)
                item["integer_max"] = integer_value if item["integer_max"] is None else max(item["integer_max"], integer_value)
    overrides = {}
    for spec in config.get("columns", []):
        ordinal = spec.get("source_ordinal")
        if ordinal is None or not 1 <= ordinal <= len(header_list):
            raise IngestError("COLUMN_SELECTOR", "Column override must resolve to a valid source ordinal", context={"selector": spec})
        if ordinal in overrides:
            raise IngestError("COLUMN_SELECTOR_DUPLICATE", "Duplicate resolved override ordinal", context={"ordinal": ordinal})
        overrides[ordinal] = spec
    profiles = []
    for index, (label, item) in enumerate(zip(header_list, stats), 1):
        kind = item["candidate"]
        if kind == "integer":
            low, high = item["integer_min"], item["integer_max"]
            if low >= 0 and high <= 255:
                sql_type = "tinyint"
            elif low >= -32768 and high <= 32767:
                sql_type = "smallint"
            elif low >= -2147483648 and high <= 2147483647:
                sql_type = "int"
            elif low >= -9223372036854775808 and high <= 9223372036854775807:
                sql_type = "bigint"
            else:
                sql_type = f"decimal({max(1,item['max_integer_digits'])},0)"
        elif kind == "decimal" and item["max_integer_digits"] + item["max_scale"] <= 38:
            sql_type = f"decimal({max(1,item['max_integer_digits'] + item['max_scale'])},{item['max_scale']})"
        elif kind == "date":
            sql_type = "date"
        elif kind in ("time","datetime2","datetimeoffset"):
            sql_type=f"{kind}({item.get('max_fractional_precision',0)})"
        elif kind == "guid":
            sql_type = "uniqueidentifier"
        else:
            length = max(1, math.ceil(item["max_utf16_units"] * (1 + settings.get("text_length_headroom_percent", 20) / 100)))
            if length > 4000 and not settings.get("allow_max_types", True):
                raise IngestError("TEXT_REPRESENTATION_LIMIT", "Full-file text length requires NVARCHAR(MAX), but MAX is disabled", context={"ordinal": index, "storage_units": length})
            sql_type = f"nvarchar({length})" if length <= 4000 else "nvarchar(max)"
        override = overrides.get(index, {})
        from .column_policy import target_type
        sql_type = target_type(override,sql_type)
        item["inference"] = "full-stream"
        item["observed_nullability_is_not_constraint"] = True
        item["override"] = override
        profiles.append(ColumnProfile(index, label, override.get("target_name", column_name(label, index)), sql_type, override.get("nullable", settings.get("new_columns_nullable", True)), item))
    return profiles
