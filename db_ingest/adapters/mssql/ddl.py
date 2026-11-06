"""Identifier quoting only; type strings are validated separately."""
from db_ingest.errors import IngestError

def quote_identifier(name):
    if not isinstance(name, str) or not name or len(name.encode("utf-16-le")) // 2 > 128 or "\x00" in name:
        raise IngestError("IDENTIFIER_INVALID", "SQL Server identifiers must contain 1..128 UTF-16 units and no NUL")
    return "[" + name.replace("]", "]]") + "]"

def qualified(schema, table):
    return quote_identifier(schema) + "." + quote_identifier(table)
