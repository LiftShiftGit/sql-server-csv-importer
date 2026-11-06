"""Data transferred between capture, planning, journals and SQL publication."""
import copy
from dataclasses import dataclass, field
from typing import Any

@dataclass
class SourceArtifact:
    capture_id: str
    path: str
    original_path: str
    relative_path: str
    sha256: str
    size: int
    metadata: dict = field(default_factory=dict)

@dataclass
class DialectDecision:
    encoding: str
    delimiter: str
    quotechar: str | None
    escapechar: str | None
    doublequote: bool
    header: str
    evidence: dict = field(default_factory=dict)

@dataclass
class FieldValue:
    state: str
    text: str | None
    quoted: bool = False

@dataclass
class RecordEnvelope:
    record_id: str
    ordinal: int
    byte_start: int | None
    byte_end: int | None
    line_start: int | None
    line_end: int | None
    fields: list[FieldValue]
    kind: str = "data"
    errors: list[dict] = field(default_factory=list)

@dataclass
class ColumnProfile:
    ordinal: int
    original_name: str
    target_name: str
    sql_type: str
    nullable: bool = True
    stats: dict = field(default_factory=dict)

@dataclass
class SchemaPlan:
    database: str
    schema: str
    table: str
    columns: list[ColumnProfile]
    source_hash: str
    interpretation_hash: str
    mapping_hash: str

@dataclass
class FailureEvent:
    code: str
    message: str
    context: dict = field(default_factory=dict)

@dataclass
class ChunkManifest:
    chunk_id: str
    count: int
    digest: str

@dataclass
class PublicationReceipt:
    intent_id: str
    run_id: str
    database: str
    schema: str
    table: str
    source_hash: str
    interpretation_hash: str
    mapping_hash: str
    committed_records: int
    value_digest: str
    assurance: str
    recorded_at_utc: str
    details: dict = field(default_factory=dict)

@dataclass
class RunReport:
    run_id: str
    state: str
    files: list = field(default_factory=list)
    exit_code: int = 0


def schema_plan_from_dict(value):
    """Rebuild a journal plan without sharing mutable column statistics with it.

    This accepts the serialized journal shape, not partially constructed models.
    Dataclass constructors retain their strict rejection of unknown/missing fields.
    """
    saved = copy.deepcopy(value)
    saved["columns"] = [ColumnProfile(**column) for column in saved["columns"]]
    return SchemaPlan(**saved)
