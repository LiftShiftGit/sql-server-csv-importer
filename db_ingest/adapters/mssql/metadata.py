"""Live metadata fingerprint and preflight for importer-owned landing tables."""
import hashlib
import json
from db_ingest.errors import IngestError
from .types import parse_sql_type

LINEAGE=("__ingest_intent","__ingest_record","__ingest_ordinal")

def schema_hash(plan):
    from .text import varchar_policy
    values=[]
    for c in plan.columns:
        item=(c.ordinal,c.target_name,str(parse_sql_type(c.sql_type)),bool(c.nullable))
        if parse_sql_type(c.sql_type).base=="varchar":
            policy=varchar_policy(c);item+=(policy["collation"].casefold(),policy["text_encoding"])
        values.append(item)
    return hashlib.sha256(json.dumps(values,separators=(",",":"),ensure_ascii=False).encode("utf-8")).hexdigest()

def validate_plan(plan):
    if not 1<=len(plan.columns)<=1021: raise IngestError("SCHEMA_COLUMN_LIMIT","Landing tables require 1..1021 source columns plus three lineage columns")
    if any(not isinstance(c.ordinal,int) or isinstance(c.ordinal,bool) for c in plan.columns) or [c.ordinal for c in plan.columns]!=list(range(1,len(plan.columns)+1)):raise IngestError("SCHEMA_ORDINALS","Source plan columns must retain ordered unique 1-based ordinals")
    from .ddl import qualified
    target=qualified(plan.schema,plan.table);units=len(target.encode("utf-16-le"))//2
    if units>260:raise IngestError("TARGET_METADATA_NAME_LIMIT","Qualified target exceeds the current 260 UTF-16-unit metadata contract; choose a shorter schema/table name",2,{"qualified_target_units":units,"supported_units":260,"remediation":"shorter schema/table; future hash-key registry migration required for wider labels"})
    names=[c.target_name.casefold() for c in plan.columns]
    if len(set(names))!=len(names) or any(n in LINEAGE for n in names): raise IngestError("SCHEMA_NAME_COLLISION","Column names collide with each other or reserved __ingest_* lineage names")
    for column in plan.columns:
        if parse_sql_type(column.sql_type).base=="varchar":
            from .text import varchar_policy
            varchar_policy(column)

def live_columns(cur, target):
    rows=cur.execute("SELECT c.name,t.name,c.max_length,c.precision,c.scale,c.is_nullable,c.is_identity,c.is_computed,c.generated_always_type FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id",target).fetchall()
    result=[]
    for row in rows:
        name,base,length,precision,scale,nullable,identity,computed,generated=row
        if identity or computed or generated or base in ("timestamp","rowversion"):
            raise IngestError("GENERATED_COLUMN_UNSUPPORTED",f"Target column {name!r} has unsupported generated/identity semantics")
        if base in ("nvarchar","varchar","varbinary"):
            size="max" if length==-1 else (length//2 if base=="nvarchar" else length)
            sql_type=f"{base}({size})"
        elif base in ("decimal","numeric"): sql_type=f"decimal({precision},{scale})"
        elif base in ("time","datetime2","datetimeoffset"): sql_type=f"{base}({scale})"
        else: sql_type=base
        result.append((name,sql_type,bool(nullable)))
    return result

def require_supported_target(cur,plan,target,replace=False,config=None):
    if cur.execute("SELECT COUNT(*) FROM sys.triggers WHERE parent_id=OBJECT_ID(?) AND is_disabled=0",target).fetchone()[0]:
        raise IngestError("TRIGGER_UNSUPPORTED","Full verification of existing target triggers is not certified; publication blocked before mutation")
    if cur.execute("SELECT COUNT(*) FROM sys.indexes WHERE object_id=OBJECT_ID(?) AND ignore_dup_key=1",target).fetchone()[0]:
        raise IngestError("IGNORE_DUP_KEY_UNSUPPORTED","IGNORE_DUP_KEY can suppress rows; target publication is blocked")
    if replace:
        properties=cur.execute("SELECT CONVERT(int,OBJECTPROPERTYEX(OBJECT_ID(?),'TableHasForeignKey')),CONVERT(int,OBJECTPROPERTYEX(OBJECT_ID(?),'TableHasForeignRef'))",target,target).fetchone()
        if properties is None or any(value is None for value in properties):raise IngestError("SCHEMA_METADATA_VISIBILITY","Foreign-key properties are unavailable; no dependency absence inferred")
        if any(properties):raise IngestError("REPLACE_DEPENDENCY","Replacement of foreign-key dependent targets requires a separately reviewed manual plan")
    require_collations(cur,plan,target)
    expected=[(c.target_name,str(parse_sql_type(c.sql_type)),bool(c.nullable)) for c in plan.columns]+[("__ingest_intent","nvarchar(64)",False),("__ingest_record","nvarchar(128)",False),("__ingest_ordinal","bigint",False)]
    actual=live_columns(cur,target)
    if dict((name,(kind,nullable)) for name,kind,nullable in actual)!=dict((name,(kind,nullable)) for name,kind,nullable in expected):
        if config is not None:
            from db_ingest.publication import _tables
            _,_,registry=_tables(config)
            row=cur.execute(f"SELECT target_object_id,schema_hash FROM {registry} WITH (READCOMMITTEDLOCK) WHERE target=?",target).fetchone()
            if row is not None:
                from .evolution import require_historical_projection
                require_historical_projection(cur,config,plan,target,row[0],row[1])
                return
        raise IngestError("SCHEMA_DRIFT","Live target differs from the approved named column types/nullability",2,{"expected":expected,"actual":actual})


def require_collations(cur,plan,target):
    from .text import varchar_policy
    columns=[c for c in plan.columns if parse_sql_type(c.sql_type).base=="varchar"]
    if not columns:return
    actual={row[0]:row[1] for row in cur.execute("SELECT name,collation_name FROM sys.columns WHERE object_id=OBJECT_ID(?)",target).fetchall()}
    for column in columns:
        expected=varchar_policy(column)["collation"]
        if (actual.get(column.target_name) or "").casefold()!=expected.casefold():raise IngestError("VARCHAR_COLLATION_DRIFT","Live VARCHAR collation differs from the declared verified representation",2,{"column":column.target_name,"expected":expected,"actual":actual.get(column.target_name)})
