"""Explicit flat XML record/attribute extraction, with no inferred flattening."""
import io
from xml.sax.handler import ContentHandler, feature_namespaces
from ..errors import IngestError
from .structured import declared_header, envelope, fields_from_values, guarded_chunks


def _expanded(name):
    uri, local = name
    return f"{{{uri}}}{local}" if uri else local


def validate(config):
    xml = config.get("xml", {})
    if not xml.get("enabled", False):
        raise IngestError("XML_DISABLED", "XML adapter is opt-in; enable it and declare scalar mappings")
    selector, fields = xml.get("record_selector"), xml.get("fields", [])
    if selector and len(selector) > 2048:
        raise IngestError("XML_SELECTOR", "XML selector exceeds bounded name length")
    if not selector or "/" in selector.replace("http://", "").replace("https://", ""):
        # Expanded namespace URIs may contain slashes; only local selector syntax
        # is limited. No XPath evaluation or external access is performed.
        if not (selector and selector.startswith("{") and "}" in selector and "/" not in selector.split("}", 1)[1]):
            raise IngestError("XML_SELECTOR", "record_selector must be one element name or {namespace}name")
    if not fields or len(fields) > 1024:
        raise IngestError("XML_FIELDS", "Declare between 1 and 1024 ordered scalar XML fields")
    seen = set()
    for item in fields:
        if not isinstance(item, dict) or set(item) != {"name", "path"} or not isinstance(item["name"], str) or not isinstance(item["path"], str):
            raise IngestError("XML_FIELDS", "Each XML field must have exactly name and path strings")
        path = item["path"]
        if len(path) > 2048 or len(item["name"]) > 128:
            raise IngestError("XML_FIELDS", "XML field name/path exceeds bounded length")
        local = path.lstrip("@").split("}", 1)[-1]
        if not local or "/" in local or "[" in local or path in seen:
            raise IngestError("XML_FIELDS", "XML fields must be unique direct scalar children or @attributes")
        seen.add(path)
    return selector, fields


def parse(artifact, decision, config):
    selector, specs = validate(config)
    try:
        from defusedxml.sax import make_parser
    except ImportError as error:
        raise IngestError("XML_DEPENDENCY", "Install the declared xml extra (defusedxml) before using XML") from error
    settings, limits = config["xml"], config.get("limits", {})
    field_limit = limits.get("max_in_memory_field_bytes", 8388608)
    record_limit = limits.get("max_in_memory_record_bytes", 8388608)
    output = []

    class Handler(ContentHandler):
        def __init__(self):
            self.depth = 0
            self.record_depth = None
            self.ordinal = 0
            self.values = {}
            self.scalar = None
            self.text = io.StringIO()
            self.field_bytes = self.record_bytes = 0
            self.errors = []
            self.seen = set()

        def startElementNS(self, name, qname, attrs):
            self.depth += 1
            if self.depth > settings.get("max_depth", 64):
                raise IngestError("XML_DEPTH", "XML depth exceeds configured limit")
            tag = _expanded(name)
            if tag == selector:
                if self.record_depth is not None:
                    raise IngestError("XML_NESTED_RECORD", "Nested selected records are outside the declared flat XML model")
                self.record_depth = self.depth
                self.values, self.errors, self.seen = {}, [], set()
                self.record_bytes = 0
                attributes = {_expanded(k): v for k, v in attrs.items()}
                for spec in specs:
                    if spec["path"].startswith("@"):
                        value = attributes.get(spec["path"][1:])
                        self.values[spec["path"]] = value
                        if value is not None:
                            self.record_bytes += len(value.encode(decision.encoding))
                            if len(value.encode(decision.encoding)) > field_limit:
                                self.errors.append({"code": "XML_FIELD_LIMIT", "message": "Attribute exceeds field budget"})
                if self.record_bytes > record_limit:
                    self.errors.append({"code": "XML_RECORD_LIMIT", "message": "Mapped attributes exceed record budget"})
                return
            if self.record_depth is not None:
                if self.depth != self.record_depth + 1:
                    raise IngestError("XML_NESTED_SCALAR", "Nested XML inside a scalar field is unsupported")
                if tag in self.seen:
                    raise IngestError("XML_REPEATED_SCALAR", "Repeated direct XML children require a different declared mapping")
                self.seen.add(tag)
                if tag not in {spec["path"] for spec in specs if not spec["path"].startswith("@")}:
                    raise IngestError("XML_UNMAPPED_CHILD", "Selected XML record has an undeclared child", context={"element": tag})
                if attrs.getLength():
                    raise IngestError("XML_SCALAR_ATTRIBUTES", "Attributes on a scalar child are outside the declared flat mapping")
                self.scalar, self.text, self.field_bytes = tag, io.StringIO(), 0

        def characters(self, content):
            if self.record_depth is None:
                return
            length = len(content.encode(decision.encoding))
            self.record_bytes += length
            if self.record_bytes > record_limit and not self.errors:
                self.errors.append({"code": "XML_RECORD_LIMIT", "message": "Selected XML record exceeds record budget"})
            if self.scalar is None:
                if content.strip():
                    raise IngestError("XML_RECORD_TEXT", "Mixed record text outside mapped scalar fields is unsupported")
                return
            self.field_bytes += length
            if self.field_bytes > field_limit or self.record_bytes > record_limit:
                if not self.errors:
                    self.errors.append({"code": "XML_RECORD_LIMIT", "message": "Selected XML record exceeds field/record budget"})
                self.text = io.StringIO()
            elif not self.errors:
                self.text.write(content)

        def endElementNS(self, name, qname):
            tag = _expanded(name)
            if self.record_depth == self.depth and tag == selector:
                self.ordinal += 1
                values = [self.values.get(spec["path"]) for spec in specs]
                if any(v is None for v in values):
                    self.errors.append({"code": "XML_MISSING_FIELD", "message": "A declared XML scalar field/attribute is missing"})
                fields = fields_from_values(values, config) if not self.errors else []
                output.append(envelope(artifact, decision, self.ordinal, fields, "rejected" if self.errors else "data", self.errors.copy()))
                self.record_depth, self.scalar = None, None
            elif self.scalar == tag and self.record_depth is not None and self.depth == self.record_depth + 1:
                if not self.errors:
                    self.values[tag] = self.text.getvalue()
                self.scalar, self.text = None, io.StringIO()
            self.depth -= 1

    handler = Handler()
    parser = make_parser()
    parser.forbid_dtd = parser.forbid_entities = parser.forbid_external = True
    parser.setFeature(feature_namespaces, True)
    parser.setContentHandler(handler)
    yield declared_header(artifact, decision, [spec["name"] for spec in specs])
    try:
        for chunk in guarded_chunks(artifact, decision, config):
            parser.feed(chunk)
            yield from output
            output.clear()
        parser.close()
        yield from output
    except IngestError:
        raise
    except Exception as error:
        # The entire affected file is blocked by the pipeline; no partial XML
        # publication claims are derived from a structurally invalid document.
        raise IngestError("XML_INVALID", str(error), context={"archive": artifact.path}) from error
    if handler.ordinal == 0:
        raise IngestError("XML_NO_RECORDS", "Declared XML record selector matched no elements")
