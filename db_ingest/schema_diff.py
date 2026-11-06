"""Reviewable ordered schema changes; observations do not invent business defaults."""
from dataclasses import asdict,is_dataclass
from .config import stable_hash


def _column(value):
    c=asdict(value) if is_dataclass(value) else dict(value)
    stats=c.get("stats",{})
    return {"ordinal":c.get("ordinal"),"original_name":c.get("original_name",c.get("name")),"target_name":c.get("target_name",c.get("name")),"sql_type":c.get("sql_type",c.get("type")),"nullable":c.get("nullable",True),"default":c.get("default"),"collation":c.get("collation",stats.get("collation")),"generated":c.get("generated",False)}


def compare_columns(previous,proposed):
    """Match explicit target names and retain source ordinal/name changes separately.

    Duplicate names are a schema error for a writable SQL target, so a comparison
    never silently overwrites one entry in a name-keyed dictionary.
    """
    from .errors import IngestError
    before=[_column(c) for c in previous];after=[_column(c) for c in proposed]
    def index(columns):
        result={}
        for c in columns:
            name=c["target_name"]
            if name in result:raise IngestError("SCHEMA_AMBIGUOUS","Duplicate target names cannot be compared without an explicit mapping")
            result[name]=c
        return result
    old,new=index(before),index(after)
    changes=[]
    for c in before:
        if c["target_name"] not in new:changes.append({"kind":"removed","before":c})
    for c in after:
        name=c["target_name"]
        if name not in old:changes.append({"kind":"added","after":c});continue
        for key in ("ordinal","original_name","sql_type","nullable","default","collation","generated"):
            if old[name][key]!=c[key]:changes.append({"kind":"reordered" if key=="ordinal" else "source_renamed" if key=="original_name" else key+"_changed","column":name,"before":old[name][key],"after":c[key]})
    return {"changed":bool(changes),"before_hash":stable_hash(before),"after_hash":stable_hash(after),"changes":changes}
