"""Opt-in single-table HTML extraction with explicit ordered field names."""
import io
from html.parser import HTMLParser
from ..errors import IngestError
from .structured import declared_header, envelope, fields_from_values, guarded_chunks


def validate(config):
    settings = config.get("html", {})
    if not settings.get("enabled", False):
        raise IngestError("HTML_DISABLED", "HTML adapter is opt-in; declare one table index and ordered fields")
    selector = settings.get("table_selector", "")
    if not str(selector).isdigit():
        raise IngestError("HTML_SELECTOR", "table_selector must be an explicit zero-based table index string")
    names = settings.get("fields", [])
    if not names or len(names) > 1024 or any(not isinstance(n, str) or len(n) > 128 for n in names):
        raise IngestError("HTML_FIELDS", "Declare between 1 and 1024 ordered HTML field names, each at most 128 characters")
    if settings.get("text_policy", "preserve") != "preserve":
        raise IngestError("HTML_TEXT_POLICY", "Only preserve decoded cell text is supported")
    if settings.get("allow_nested_tables", False) or settings.get("allow_rowspan_colspan", False):
        raise IngestError("HTML_STRUCTURE_UNSUPPORTED", "Nested tables and rowspan/colspan expansion lack a verified adapter")
    if config.get("parsing", {}).get("header", "auto") == "auto":
        raise IngestError("HEADER_AMBIGUOUS", "Declare whether the selected HTML table has a header row")
    return int(selector), names


def parse(artifact, decision, config):
    index, names = validate(config)
    limits = config.get("limits", {})
    field_limit = limits.get("max_in_memory_field_bytes", 8388608)
    record_limit = limits.get("max_in_memory_record_bytes", 8388608)
    output = []

    class Parser(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.table_index = -1
            self.table_depth = 0
            self.selected = self.in_row = self.in_cell = False
            self.rows = 0
            self.ordinal = 0
            self.values = []
            self.cell = io.StringIO()
            self.field_bytes = self.record_bytes = 0
            self.errors = []

        def handle_starttag(self, tag, attrs):
            if tag == "table":
                if self.table_depth:
                    if self.selected:
                        raise IngestError("HTML_NESTED_TABLE", "Nested table inside selected table is unsupported")
                    self.table_depth += 1
                    return
                self.table_index += 1
                self.selected = self.table_index == index
                self.table_depth = 1
                return
            if not self.selected:
                return
            attrs = dict(attrs)
            if any(attrs.get(k, "1") != "1" for k in ("rowspan", "colspan")):
                raise IngestError("HTML_SPAN_UNSUPPORTED", "Rowspan/colspan expansion is outside the simple-table model")
            if tag == "tr":
                if self.in_row:
                    raise IngestError("HTML_ROW_NESTING", "Nested/unclosed HTML rows require a corrected interpretation")
                self.in_row, self.values, self.record_bytes, self.errors = True, [], 0, []
            elif tag in {"td", "th"}:
                if not self.in_row or self.in_cell:
                    raise IngestError("HTML_CELL_NESTING", "HTML cells require explicit nonnested rows/cells")
                self.in_cell, self.cell, self.field_bytes = True, io.StringIO(), 0
            elif self.in_cell and tag in {"script", "style", "iframe", "object", "img", "br"}:
                raise IngestError("HTML_CELL_CONTENT", "Cell contains markup requiring an explicit text extraction extension", context={"tag": tag})

        def handle_startendtag(self, tag, attrs):
            self.handle_starttag(tag, attrs)
            self.handle_endtag(tag)

        def handle_data(self, data):
            if not self.selected:
                return
            if not self.in_cell:
                if data.strip():
                    raise IngestError("HTML_UNMAPPED_TEXT", "Selected table contains text outside declared cells")
                return
            size = len(data.encode(decision.encoding))
            self.field_bytes += size
            self.record_bytes += size
            if self.field_bytes > field_limit or self.record_bytes > record_limit:
                if not self.errors:
                    self.errors.append({"code": "HTML_RECORD_LIMIT", "message": "HTML row exceeds field/record memory budget"})
                self.cell = io.StringIO()
            elif not self.errors:
                self.cell.write(data)

        def handle_endtag(self, tag):
            if tag == "table":
                if self.table_depth == 0:
                    raise IngestError("HTML_TABLE_UNBALANCED", "HTML table closing tag has no matching start")
                if self.selected and (self.in_row or self.in_cell):
                    raise IngestError("HTML_UNCLOSED_ROW", "Selected table ends inside a row/cell")
                self.table_depth -= 1
                if self.table_depth == 0:
                    self.selected = False
                return
            if not self.selected:
                return
            if tag in {"td", "th"}:
                if not self.in_cell:
                    raise IngestError("HTML_CELL_UNBALANCED", "HTML cell closing tag has no matching start")
                self.values.append(self.cell.getvalue() if not self.errors else "")
                self.in_cell, self.cell = False, io.StringIO()
                if len(self.values) > 1024:
                    raise IngestError("HTML_COLUMNS_LIMIT", "HTML row exceeds bounded column count")
            elif tag == "tr":
                if not self.in_row or self.in_cell:
                    raise IngestError("HTML_ROW_UNBALANCED", "HTML row is not explicitly balanced")
                self.rows += 1
                if self.rows == 1 and config.get("parsing", {}).get("header") == "present":
                    if self.errors or self.values != names:
                        raise IngestError("HTML_HEADER_MISMATCH", "Extracted header differs from declared ordered HTML fields")
                else:
                    self.ordinal += 1
                    if len(self.values) != len(names):
                        self.errors.append({"code": "ROW_WIDTH", "message": "HTML row width differs from declared fields", "expected": len(names), "observed": len(self.values)})
                    output.append(envelope(artifact, decision, self.ordinal, fields_from_values(self.values, config) if not self.errors else [], "rejected" if self.errors else "data", self.errors.copy()))
                self.in_row, self.values = False, []

    parser = Parser()
    yield declared_header(artifact, decision, names)
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
        raise IngestError("HTML_INVALID", str(error), context={"archive": artifact.path}) from error
    if parser.table_depth or parser.in_row or parser.in_cell:
        raise IngestError("HTML_UNCLOSED_STRUCTURE", "HTML ends inside an unclosed table/row/cell")
    if parser.table_index < index:
        raise IngestError("HTML_TABLE_MISSING", "Declared HTML table index was not found")
    if parser.rows == 0:
        raise IngestError("HTML_NO_ROWS", "Selected HTML table contains no explicit rows")
