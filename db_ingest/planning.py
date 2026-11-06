"""Stable path-derived mapping, independent of the current source selection.

Keep hash inputs and separators stable: saved runs and SQL ownership claims depend
on these identities. Readable names require separate collision/ownership checks.
"""
import re
from pathlib import PurePosixPath

from .config import stable_hash
from .errors import IngestError

READABLE_NAME_POLICIES = frozenset({"filename_error", "path_error"})


def uses_name_claims(config):
    """Readable names need source ownership claims; hashed names keep legacy behavior."""
    return config.get("mapping", {}).get("name_collision") in READABLE_NAME_POLICIES


def truncate_utf16(text, units):
    prefix = text.encode("utf-16-le")[:units * 2]
    if prefix and 0xD800 <= int.from_bytes(prefix[-2:], "little") <= 0xDBFF:
        prefix = prefix[:-2]
    return prefix.decode("utf-16-le", errors="strict")


def identifier(label, identity):
    friendly = re.sub(r"[^\w]", "_", label, flags=re.UNICODE).strip("_") or "unnamed"
    if friendly[0].isdigit():
        friendly = "n_" + friendly
    return truncate_utf16(friendly, 105) + "__" + stable_hash(identity)[:16]


def filename_table(stem):
    """Portable SQL name. Never truncate or silently suffix a readable collision."""
    name = re.sub(r"[^A-Za-z0-9_]", "_", stem).strip("_") or "unnamed"
    if name[0].isdigit():
        name = "n_" + name
    if len(name) > 128:
        raise IngestError(
            "TABLE_NAME_LENGTH",
            "Filename-only table names must fit 128 characters; rename the source or use stable_hash_suffix",
        )
    return name


def _database_mapping(mapping, destination, path):
    """Return the database and path components used by legacy per-file table names."""
    strategy = mapping["database_strategy"]
    parent = path.parts[:-1]
    if strategy == "single":
        return destination["database"], (*parent, path.stem)
    if strategy == "per_top_level_directory":
        database = (identifier(mapping["database_prefix"] + parent[0], "db-top:" + parent[0])
                    if parent else mapping["root_database"])
        return database, (*parent[1:], path.stem)
    if strategy == "per_directory":
        database = (identifier(mapping["database_prefix"] + "__".join(parent), "db-dir:" + "/".join(parent))
                    if parent else mapping["root_database"])
        return database, (path.stem,)
    database = identifier(mapping["database_prefix"] + "__".join((*parent, path.stem)), "db-file:" + str(path))
    return database, ()


def _table_mapping(mapping, path, table_parts):
    parent = path.parts[:-1]
    if mapping["table_strategy"] == "per_directory":
        table = identifier("__".join(parent) or mapping["root_table"], "table-dir:" + "/".join(parent))
    elif mapping["database_strategy"] == "per_file":
        table = mapping["per_file_database_table"]
    else:
        table = identifier("__".join(table_parts), "table-file:" + str(path))

    policy = mapping.get("name_collision")
    if policy == "filename_error":
        table = filename_table(path.stem)
    elif policy == "path_error":
        if mapping["table_strategy"] == "per_directory":
            label = "_".join(parent) or mapping["root_table"]
        else:
            label = "_".join((*parent, path.stem))
        table = filename_table(label)

    if mapping.get("existing_target_strategy") == "mapped_business" and mapping.get("target_table"):
        table = mapping["target_table"]
    return table


def map_destination(config, relative_path):
    path = PurePosixPath(relative_path.replace("\\", "/"))
    mapping = config["mapping"]
    destination = config["destination"]
    if not path.parts[:-1] and mapping["root_files"] == "error":
        raise IngestError("ROOT_FILE_POLICY", f"Root file is disallowed: {relative_path}")
    database, table_parts = _database_mapping(mapping, destination, path)
    return {
        "database": database,
        "schema": destination["schema"],
        "table": _table_mapping(mapping, path, table_parts),
        "relative_path": str(path),
    }


def name_claim_source(config, relative_path):
    """Directory claims include a trailing slash to distinguish file identities."""
    path = PurePosixPath(relative_path.replace("\\", "/"))
    if config["mapping"]["table_strategy"] == "per_directory":
        return path.parent.as_posix() + "/"
    return path.as_posix()
