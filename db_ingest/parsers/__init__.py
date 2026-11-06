"""Content-aware dispatch with explicit structured-source mapping contracts."""
import codecs
from ..errors import IngestError
from ..models import DialectDecision
from . import delimited


def decide(artifact, config):
    with open(artifact.path, "rb") as source:
        sample = source.read(8192)
    encoding, bom = delimited._encoding(sample, config.get("parsing", {}).get("encoding", "auto"))
    try:
        text = codecs.getincrementaldecoder(encoding)(errors="strict").decode(sample[bom:], final=artifact.size <= len(sample)).lstrip()
    except UnicodeDecodeError:
        return delimited.decide(artifact, config)
    requested = config.get("source", {}).get("format", "auto")
    settings=config.get("parsing",{})
    explicit_tokens = requested == "delimited" and isinstance(settings.get("quotechar"),str) and settings["quotechar"].startswith("<") and text.startswith(settings["quotechar"])
    if explicit_tokens:return delimited.decide(artifact,config)
    actual = None
    lower = text.lower()
    if lower.startswith(("<!doctype html", "<html", "<table")):
        actual = "html"
    elif text.startswith("<") and ">" in text[:2048]:
        actual = "xml"
    adapter = requested if requested in {"xml", "html"} else actual
    if requested == "delimited" and actual:
        raise IngestError("FORMAT_CONFLICT", "Declared delimited source contains structured markup")
    if not adapter:
        return delimited.decide(artifact, config)
    if adapter == "xml":
        from .xml_records import validate
    else:
        from .html_table import validate
    validate(config)
    import copy
    semantic_keys=("header","null_tokens","null_tokens_case_sensitive","null_tokens_in_quoted_fields","empty_string_is_null","trim_whitespace")
    evidence={"version":2,"record_identity_policy_version":2,"adapter":adapter,"bom_bytes":bom,"header_source":"declared scalar mapping metadata; synthetic envelope ordinal 0","span_assurance":"exact original archive; XML/HTML row byte/line spans unavailable","mapping":copy.deepcopy(config[adapter]),"parsing_policy":{key:copy.deepcopy(config.get("parsing",{}).get(key)) for key in semantic_keys},"column_policies":copy.deepcopy(config.get("columns",[]))}
    return DialectDecision(encoding,",",None,None,True,"present",evidence)


def parse(artifact, decision, config):
    adapter = decision.evidence.get("adapter", "delimited")
    if adapter == "xml":
        from .xml_records import parse as parse_adapter
    elif adapter == "html":
        from .html_table import parse as parse_adapter
    else:
        parse_adapter = delimited.parse
    yield from parse_adapter(artifact, decision, config)
