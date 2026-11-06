"""Versioned length/type framed digest, separate from source-byte hashes."""
import hashlib
from db_ingest.adapters.mssql.types import canonical_value

def new_value_digest():
    return hashlib.sha256(b"db_ingest:typed-values:v1\x00")

def update_value_digest(digest, record_id, ordinal, values, columns):
    rid = str(record_id).encode("utf-8")
    digest.update(len(rid).to_bytes(4,"big")); digest.update(rid)
    digest.update(int(ordinal).to_bytes(8,"big")); digest.update(len(columns).to_bytes(4,"big"))
    for column,value in zip(columns,values,strict=True): digest.update(canonical_value(value,column.sql_type,text_encoding=column.stats.get("text_encoding")))
