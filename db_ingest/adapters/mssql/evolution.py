"""Transactional, explicitly permitted owned-schema evolution and old projection proofs."""
from dataclasses import asdict
import hashlib,json
from db_ingest.errors import IngestError
from db_ingest.models import ColumnProfile,SchemaPlan
from .ddl import qualified,quote_identifier
from .metadata import live_columns,schema_hash,LINEAGE
from .text import declaration
from .types import parse_sql_type


def table(config):return qualified(config.get("audit",{}).get("control_schema","audit"),"__db_ingest_schema_versions")


def bootstrap(cur,config):
    name=table(config)
    if cur.execute("SELECT OBJECT_ID(?)",name).fetchone()[0] is None:
        if not config.get("destination",{}).get("create_tables",False):raise IngestError("SCHEMA_HISTORY_UNAVAILABLE","Owned schema history is absent and creation disabled",5)
        cur.execute(f"CREATE TABLE {name}(target nvarchar(260) NOT NULL,schema_hash char(64) NOT NULL,target_object_id int NOT NULL,columns_json nvarchar(max) NOT NULL,details_json nvarchar(max) NOT NULL,PRIMARY KEY(target,schema_hash))")


def inspection(cur,target):
    # Definition text is MAX data, so row-count caps alone cannot bound fetching.
    # Check aggregate server-side bytes before any definitions enter the client.
    from db_ingest.resources import checkpoint
    defaults=cur.execute("SELECT COALESCE(SUM(CONVERT(bigint,DATALENGTH(d.definition))),0) FROM sys.default_constraints d WHERE d.parent_object_id=OBJECT_ID(?)",target).fetchone()[0]
    checks=cur.execute("SELECT COALESCE(SUM(CONVERT(bigint,DATALENGTH(definition))),0) FROM sys.check_constraints WHERE parent_object_id=OBJECT_ID(?)",target).fetchone()[0]
    metadata_bytes=int(defaults or 0)+int(checks or 0)
    if metadata_bytes>4194304:raise IngestError("SCHEMA_METADATA_BUDGET","Target constraint/default definitions exceed the bounded 4 MiB inspection policy",2,{"definition_bytes":metadata_bytes,"limit_bytes":4194304})
    checkpoint(metadata_bytes*4,force=True)
    queries={
      "columns":"SELECT c.name,c.column_id,t.name,c.max_length,c.precision,c.scale,c.is_nullable,c.is_identity,c.is_computed,c.generated_always_type,c.collation_name,d.name,d.definition FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id LEFT JOIN sys.default_constraints d ON c.default_object_id=d.object_id WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id",
      "indexes":"SELECT i.name,i.is_unique,i.is_primary_key,i.ignore_dup_key,i.is_disabled,c.name FROM sys.indexes i LEFT JOIN sys.index_columns x ON i.object_id=x.object_id AND i.index_id=x.index_id LEFT JOIN sys.columns c ON x.object_id=c.object_id AND x.column_id=c.column_id WHERE i.object_id=OBJECT_ID(?) ORDER BY i.index_id,x.index_column_id",
      "checks":"SELECT name,definition,is_disabled,is_not_trusted FROM sys.check_constraints WHERE parent_object_id=OBJECT_ID(?) ORDER BY name",
      "foreign_keys":"SELECT name,parent_object_id,referenced_object_id,is_disabled,is_not_trusted FROM sys.foreign_keys WHERE parent_object_id=OBJECT_ID(?) OR referenced_object_id=OBJECT_ID(?) ORDER BY name",
      "triggers":"SELECT name,is_disabled,is_instead_of_trigger FROM sys.triggers WHERE parent_id=OBJECT_ID(?) ORDER BY name"}
    result={};total_bytes=0
    for key,sql in queries.items():
        args=(target,target) if key=="foreign_keys" else (target,)
        if key=="foreign_keys":
            properties=cur.execute("SELECT CONVERT(int,OBJECTPROPERTYEX(OBJECT_ID(?),'TableHasForeignKey')),CONVERT(int,OBJECTPROPERTYEX(OBJECT_ID(?),'TableHasForeignRef'))",target,target).fetchone()
            if properties is None or any(value is None for value in properties):raise IngestError("SCHEMA_METADATA_VISIBILITY","Foreign-key properties are hidden/unavailable; no empty dependency list inferred",2)
            if not any(properties):
                result[key]=[]
                continue
            # Avoid the OR plan that scans metadata for unrelated uncommitted new
            # targets; object-scoped properties resolve the common no-FK case.
            sql="SELECT name,parent_object_id,referenced_object_id,is_disabled,is_not_trusted FROM sys.foreign_keys WHERE parent_object_id=OBJECT_ID(?) UNION SELECT name,parent_object_id,referenced_object_id,is_disabled,is_not_trusted FROM sys.foreign_keys WHERE referenced_object_id=OBJECT_ID(?) ORDER BY name"
        cur.execute(sql,*args);rows=[];category_bytes=0
        while True:
            row=cur.fetchone()
            if row is None:break
            # Only one pre-length-checked definition row is fetched at a time.
            # Include both wire encodings plus fixed object/list overhead in the
            # accounting rather than treating each SQL metadata row as tiny.
            cost=64+sum(len(value.encode("utf-8"))+len(value.encode("utf-16-le"))+32 if isinstance(value,str) else 48 for value in row)
            category_bytes+=cost;total_bytes+=cost
            if len(rows)>=8192 or category_bytes>8388608 or total_bytes>16777216:raise IngestError("SCHEMA_METADATA_BUDGET","Target metadata exceeds bounded streamed inspection policy",2,{"category":key,"rows":len(rows)+1,"category_bytes":category_bytes,"total_bytes":total_bytes,"category_limit_bytes":8388608,"total_limit_bytes":16777216,"row_limit":8192})
            checkpoint(total_bytes*2)
            rows.append(list(row))
        result[key]=rows
    return result


def snapshot(cur,config,plan,target,object_id,details=None):
    fingerprint=schema_hash(plan);name=table(config)
    row=cur.execute(f"SELECT target_object_id FROM {name} WITH (READCOMMITTEDLOCK) WHERE target=? AND schema_hash=?",target,fingerprint).fetchone()
    columns=json.dumps([asdict(c) for c in plan.columns],ensure_ascii=False,separators=(",",":"))
    if row is not None:
        if row[0]!=object_id:raise IngestError("SCHEMA_HISTORY_IDENTITY","Published schema snapshot belongs to another object",5)
        return
    proof={"shape":live_columns(cur,target),"inspection":inspection(cur,target),**(details or {})}
    details_json=json.dumps(proof,ensure_ascii=False,separators=(",",":"))
    size=len(columns.encode("utf-16-le"))+len(details_json.encode("utf-16-le"))
    if size>33554432:raise IngestError("SCHEMA_METADATA_BUDGET","Schema version metadata exceeds bounded 32 MiB storage/readback policy; publication rolled back",2,{"bytes":size,"limit_bytes":33554432})
    cur.execute(f"INSERT INTO {name}(target,schema_hash,target_object_id,columns_json,details_json) VALUES(?,?,?,?,?)",target,fingerprint,object_id,columns,details_json)


def load(cur,config,plan,target,fingerprint,object_id):
    from db_ingest.resources import checkpoint
    sizes=cur.execute(f"SELECT DATALENGTH(columns_json),DATALENGTH(details_json) FROM {table(config)} WITH (READCOMMITTEDLOCK) WHERE target=? AND schema_hash=?",target,fingerprint).fetchone()
    if sizes is None:return None
    size=sum(n or 0 for n in sizes)
    if size>33554432:raise IngestError("SCHEMA_METADATA_BUDGET","Stored schema version exceeds bounded 32 MiB readback policy",5,{"bytes":size,"limit_bytes":33554432})
    checkpoint(size*3,force=True)
    row=cur.execute(f"SELECT target_object_id,columns_json,details_json FROM {table(config)} WITH (READCOMMITTEDLOCK) WHERE target=? AND schema_hash=?",target,fingerprint).fetchone()
    if row is None:return None
    if row[0]!=object_id:raise IngestError("SCHEMA_HISTORY_IDENTITY","Owned schema version object identity differs",5)
    raw=json.loads(row[1])
    if not isinstance(raw,list) or not 1<=len(raw)<=1021:raise IngestError("SCHEMA_HISTORY_CONFLICT","Stored schema source-column count is outside 1..1021",5)
    columns=[ColumnProfile(**item) for item in raw]
    result=SchemaPlan(plan.database,plan.schema,plan.table,columns,plan.source_hash,plan.interpretation_hash,plan.mapping_hash)
    if schema_hash(result)!=fingerprint:raise IngestError("SCHEMA_HISTORY_CONFLICT","Owned schema version does not match its recorded fingerprint",5)
    return result,json.loads(row[2])


def compatible(old,new):
    a,b=parse_sql_type(old.sql_type),parse_sql_type(new.sql_type)
    if old.nullable and not new.nullable:return False
    if a==b:
        return a.base!="varchar" or all(old.stats.get(k)==new.stats.get(k) for k in ("collation","text_encoding"))
    if a.base==b.base and a.base in ("nvarchar","varchar","varbinary"):
        if a.base=="varchar" and any(old.stats.get(k)!=new.stats.get(k) for k in ("collation","text_encoding")):return False
        return b.length=="max" or a.length!="max" and int(b.length)>=int(a.length)
    if a.base==b.base=="decimal":return b.scale>=a.scale and b.precision-b.scale>=a.precision-a.scale
    if a.base==b.base and a.base in ("time","datetime2","datetimeoffset"):return b.precision>=a.precision
    order={"tinyint":0,"smallint":1,"int":2,"bigint":3}
    return a.base in order and b.base in order and order[b.base]>=order[a.base]



def projection_compatible(source,storage):
    spec=source.stats.get("override") or {}
    observed_constraint=source.stats.get("inference")=="full-stream" and "nullable" not in spec
    typed=ColumnProfile(source.ordinal,source.original_name,source.target_name,source.sql_type,storage.nullable,source.stats) if observed_constraint else source
    if compatible(typed,storage):return True
    a,b=parse_sql_type(source.sql_type),parse_sql_type(storage.sql_type)
    inferred=source.stats.get("inference")=="full-stream" and not any(key in spec for key in ("target_type","length","precision","scale"))
    # Inference headroom is a proposed capacity, not an instruction to ALTER a
    # target that already holds every fully profiled source value exactly.
    observed=source.stats.get("max_utf16_units")
    return inferred and a.base==b.base=="nvarchar" and isinstance(observed,int) and b.length!="max" and observed<=b.length and not(typed.nullable and not storage.nullable)

def _old_values(cur,config,plan,target):
    from db_ingest.publication import _readback
    digest=hashlib.sha256(b"db_ingest:evolution-old-projection:v1\0");count=0;intents=0;last="";started=0
    while True:
        row=cur.execute(f"SELECT TOP(1) __ingest_intent FROM {target} WHERE (?=0 OR __ingest_intent>?) ORDER BY __ingest_intent",started,last).fetchone()
        if row is None:break
        started=1;last=str(row[0]);n,value=_readback(cur,plan,target,last,config)
        raw=last.encode("utf-8");digest.update(len(raw).to_bytes(4,"big"));digest.update(raw);digest.update(n.to_bytes(8,"big"));digest.update(bytes.fromhex(value))
        count+=n;intents+=1
    return {"count":count,"intents":intents,"sha256":digest.hexdigest(),"schema_hash":schema_hash(plan)}


def prepare(cur,config,plan,target,object_id,registered,dry_run=False):
    """Returns a concrete diff; every ALTER and both proofs share publication TX."""
    if cur.execute("SELECT HAS_PERMS_BY_NAME(?,'OBJECT','VIEW DEFINITION')",target).fetchone()[0]!=1:
        raise IngestError("SCHEMA_METADATA_VISIBILITY","Owned ALTER requires observable full target definitions/dependencies; request VIEW DEFINITION",2)
    actual=live_columns(cur,target);observed=inspection(cur,target)
    prior=load(cur,config,plan,target,registered[1],object_id)
    if prior is None:
        columns=[]
        collations={r[0]:r[10] for r in observed["columns"]}
        for name,kind,nullable in actual:
            if name in LINEAGE:continue
            stats={}
            if parse_sql_type(kind).base=="varchar":
                collation=collations[name];stats={"collation":collation,"text_encoding":"utf-8" if collation.upper().endswith("_UTF8") else "cp1252"}
            columns.append(ColumnProfile(len(columns)+1,name,name,kind,nullable,stats))
        old=SchemaPlan(plan.database,plan.schema,plan.table,columns,plan.source_hash,plan.interpretation_hash,plan.mapping_hash)
        if schema_hash(old)!=registered[1]:raise IngestError("SCHEMA_DRIFT","Legacy live target does not match its published registry",2,{"published_hash":registered[1],"live":actual,"proposed":[asdict(c) for c in plan.columns],"inspection":observed})
        if not dry_run:snapshot(cur,config,old,target,object_id,{"legacy_registry_verified":True})
        prior=(old,{"shape":actual})
    old,proof=prior
    if actual!=[tuple(row) for row in proof["shape"]]:raise IngestError("SCHEMA_DRIFT","Live target differs from its last acknowledged schema snapshot",2,{"published":proof["shape"],"live":actual,"proposed":[asdict(c) for c in plan.columns],"inspection":observed})
    oldmap={c.target_name:c for c in old.columns};newmap={c.target_name:c for c in plan.columns};actions=[];blocked=[]
    for name,column in oldmap.items():
        new=newmap.get(name)
        if new is None:blocked.append({"column":name,"reason":"dropping or omitting published columns is unsupported"});continue
        if parse_sql_type(column.sql_type)!=parse_sql_type(new.sql_type) or column.nullable!=new.nullable or (parse_sql_type(column.sql_type).base=="varchar" and any(column.stats.get(k)!=new.stats.get(k) for k in ("collation","text_encoding"))):
            permitted=config.get("schema",{}).get("allow_widen_on_append",False) if parse_sql_type(column.sql_type).base==parse_sql_type(new.sql_type).base else config.get("schema",{}).get("allow_retype_on_append",False)
            if not permitted or not compatible(column,new):blocked.append({"column":name,"reason":"conversion is not an explicitly enabled compatible widening","proposed_sql":f"ALTER TABLE {target} ALTER COLUMN "+declaration(new)})
            else:actions.append({"kind":"widen","column":name,"sql":f"ALTER TABLE {target} ALTER COLUMN "+declaration(new)})
    for name,column in newmap.items():
        if name in oldmap:continue
        if not config.get("schema",{}).get("allow_add_columns_on_append",False) or not column.nullable:blocked.append({"column":name,"reason":"only explicitly enabled nullable additions are supported","proposed_sql":f"ALTER TABLE {target} ADD "+declaration(column)})
        else:actions.append({"kind":"add","column":name,"sql":f"ALTER TABLE {target} ADD "+declaration(column)})
    widened={a["column"] for a in actions if a["kind"]=="widen"}
    dependencies=[row for row in observed["indexes"] if row[5] in widened]
    dependencies+=observed["foreign_keys"]+observed["checks"] if widened else []
    dependencies += [row for row in observed["columns"] if row[0] in widened and row[11] is not None]
    if actions and any(not row[1] for row in observed["triggers"]):blocked.append({"reason":"active target triggers require an explicit reviewed migration"})
    if dependencies:blocked.append({"reason":"ALTER has index/check/foreign-key dependencies requiring an explicit manual migration","dependencies":dependencies})
    if actions and config.get("load",{}).get("mode","append")!="append":blocked.append({"reason":"automatic schema evolution is enabled only for append"})
    diff={"published_hash":registered[1],"proposed_hash":schema_hash(plan),"published_columns":[asdict(c) for c in old.columns],"live_columns":actual,"proposed_columns":[asdict(c) for c in plan.columns],"inspection":observed,"actions":actions,"blocked":blocked}
    if blocked:raise IngestError("SCHEMA_DRIFT","Owned schema change requires resolution before publication",2,{"schema_diff":diff})
    if dry_run:return diff
    before=_old_values(cur,config,old,target)
    for action in actions:cur.execute(action["sql"])
    after=_old_values(cur,config,old,target)
    if before!=after:raise IngestError("SCHEMA_EVOLUTION_VALUE_MISMATCH","Old typed values changed during ALTER; transaction rolled back",5,{"before":before,"after":after,"schema_diff":diff})
    diff.update(old_projection_before=before,old_projection_after=after,verification="every old value under its recorded old SQL type")
    return diff


def require_historical_projection(cur,config,plan,target,object_id,current_hash):
    historical=load(cur,config,plan,target,schema_hash(plan),object_id)
    current=load(cur,config,plan,target,current_hash,object_id)
    if historical is None or current is None:raise IngestError("SCHEMA_DRIFT","Historical projection lacks acknowledged schema-version evidence",5)
    latest,proof=current
    if live_columns(cur,target)!=[tuple(row) for row in proof["shape"]]:raise IngestError("SCHEMA_DRIFT","Live target differs from acknowledged evolved schema",5)
    latestmap={c.target_name:c for c in latest.columns}
    if any(c.target_name not in latestmap or not projection_compatible(c,latestmap[c.target_name]) for c in plan.columns):raise IngestError("SCHEMA_DRIFT","Historical projection is incompatible with current acknowledged storage",5)


def adapt_inferred(cur,config,plan,target,object_id,registered):
    """Keep wider acknowledged storage for narrower FULL inference candidates.

    Explicit target_type/length/precision/scale requests remain declarations and
    therefore cannot silently become inference-only projections.
    """
    prior=load(cur,config,plan,target,registered[1],object_id)
    if prior is None:
        actual=live_columns(cur,target);collations={r[0]:r[10] for r in inspection(cur,target)["columns"]};columns=[]
        for name,kind,nullable in actual:
            if name in LINEAGE:continue
            stats={}
            if parse_sql_type(kind).base=="varchar":stats={"collation":collations[name],"text_encoding":"utf-8" if collations[name].upper().endswith("_UTF8") else "cp1252"}
            columns.append(ColumnProfile(len(columns)+1,name,name,kind,nullable,stats))
        old=SchemaPlan(plan.database,plan.schema,plan.table,columns,plan.source_hash,plan.interpretation_hash,plan.mapping_hash)
        if schema_hash(old)!=registered[1]:raise IngestError("SCHEMA_DRIFT","Live legacy target differs from its acknowledged storage fingerprint",2,{"published_hash":registered[1],"live":actual})
    else:
        old,proof=prior
        if live_columns(cur,target)!=[tuple(r) for r in proof["shape"]]:raise IngestError("SCHEMA_DRIFT","Live target differs from acknowledged storage snapshot",2,{"published":proof["shape"],"live":live_columns(cur,target)})
    by_name={c.target_name:c for c in old.columns};columns=[];changes=[]
    for candidate in plan.columns:
        previous=by_name.get(candidate.target_name);spec=candidate.stats.get("override") or {}
        inferred=candidate.stats.get("inference")=="full-stream" and not any(key in spec for key in ("target_type","length","precision","scale"))
        kind=candidate.sql_type;nullable=candidate.nullable
        if previous is not None and inferred:
            comparable=ColumnProfile(candidate.ordinal,candidate.original_name,candidate.target_name,candidate.sql_type,previous.nullable,candidate.stats)
            if projection_compatible(comparable,previous):kind=previous.sql_type
        if previous is not None and candidate.stats.get("inference")=="full-stream" and "nullable" not in spec:nullable=previous.nullable
        effective=ColumnProfile(candidate.ordinal,candidate.original_name,candidate.target_name,kind,nullable,candidate.stats)
        if kind!=candidate.sql_type or nullable!=candidate.nullable:changes.append({"column":candidate.target_name,"inferred_type":candidate.sql_type,"storage_type":kind,"candidate_nullable":candidate.nullable,"storage_nullable":nullable,"action":"preserve acknowledged storage; complete observed source fits without ALTER"})
        columns.append(effective)
    return SchemaPlan(plan.database,plan.schema,plan.table,columns,plan.source_hash,plan.interpretation_hash,plan.mapping_hash),changes
