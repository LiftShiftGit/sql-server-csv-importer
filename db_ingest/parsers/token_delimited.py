"""Bounded explicit-token framing; default CSV remains on the legacy fast path."""
from collections import deque
from dataclasses import asdict, replace
import hashlib
import io
import json
from ..errors import IngestError
from ..models import FieldValue, RecordEnvelope


def alternate(decision, settings):
    return (any(len(x or "") > 1 for x in (decision.delimiter, decision.quotechar, decision.escapechar))
            or settings.get("record_terminator", "auto") != "auto"
            or not settings.get("allow_multiline_fields", True))


def validate_tokens(settings):
    """Reject dialects whose control prefixes cannot have a unique meaning."""
    for key in ("delimiter", "quotechar", "escapechar", "record_terminator"):
        value = settings.get(key, "auto" if key in {"delimiter", "quotechar", "record_terminator"} else "none")
        if key=="delimiter" and value=="none" or key=="escapechar" and value=="auto":
            raise IngestError("CONFIG_DIALECT", "Unsupported sentinel for parsing."+key, 2)
        if value == "\\t":
            settings[key] = value = "\t"
        allowed = {"auto"} if key in {"delimiter", "record_terminator"} else {"auto", "none", ""} if key == "quotechar" else {"none", ""}
        if not isinstance(value, str) or (value not in allowed and (not 1 <= len(value) <= 32 or "\x00" in value)):
            raise IngestError("CONFIG_DIALECT", "parsing." + key + " requires a token of1..32 characters or its documented sentinel", 2)
        if key != "record_terminator" and value not in allowed and any(c in value for c in "\r\n" + ("\x85" if settings.get("nel_is_newline",False) else "")):
            raise IngestError("CONFIG_DIALECT", "Field control tokens cannot contain CR, LF or NEL", 2)
    if not isinstance(settings.get("allow_multiline_fields", True), bool):
        raise IngestError("CONFIG_DIALECT", "allow_multiline_fields must be boolean", 2)
    delimiter = settings.get("delimiter", "auto")
    quote = settings.get("quotechar", "auto")
    quote = '"' if quote == "auto" else None if quote in {"none", ""} else quote
    escape = settings.get("escapechar", "none")
    escape = None if escape in {"none", ""} else escape
    term = settings.get("record_terminator", "auto")
    controls = [("delimiter", delimiter)] if delimiter != "auto" else []
    controls += [(k, v) for k, v in (("quotechar", quote), ("escapechar", escape)) if v]
    if term != "auto": controls.append(("record_terminator", term))
    use_alternate = any(len(v)>1 for name,v in controls if name!="record_terminator") or term!="auto" or not settings.get("allow_multiline_fields",True)
    for i, (name, value) in enumerate(controls):
        for other, candidate in controls[i+1:]:
            if value == candidate or use_alternate and (value.startswith(candidate) or candidate.startswith(value)):
                # Legacy CSV permits equal quote and escape characters.
                if not use_alternate and {name, other} == {"quotechar", "escapechar"}: continue
                raise IngestError("CONFIG_DIALECT_PREFIX" if use_alternate else "CONFIG_DIALECT", "Control tokens must differ and cannot prefix one another", 2, context={"controls":[name,other]})


def _lexemes(artifact, decision, settings, chunk_bytes=65536):
    """At most one decode chunk plus32 scalars lookahead, including at seams."""
    from .delimited import _chars
    endings = [settings["record_terminator"]] if settings.get("record_terminator", "auto") != "auto" else ["\r\n", "\r", "\n"] + (["\x85"] if decision.evidence.get("nel_is_newline",False) else [])
    meanings = {decision.delimiter:"delimiter", **{t:"ending" for t in endings}}
    if decision.quotechar: meanings[decision.quotechar]="quote"
    if decision.escapechar: meanings[decision.escapechar]="escape"
    # Physical line boundaries remain visible even when they are not record ends.
    for token in ["\r", "\n"] + (["\x85"] if decision.evidence.get("nel_is_newline",False) else []): meanings.setdefault(token,"physical")
    controls = tuple(sorted(meanings, key=len, reverse=True))
    starters = {t[0] for t in controls}
    proxy = replace(decision, delimiter="".join(set("".join(controls))))
    source = iter(_chars(artifact,proxy,runs=True,chunk_bytes=chunk_bytes))
    pending = deque()
    def take():
        value, start, end = pending.popleft() if pending else next(source)
        if len(value)>1:
            first_end=start+len(value[0].encode(decision.encoding))
            pending.appendleft((value[1:], first_end, end))
            return value[0],start,first_end
        return value,start,end
    while True:
        try:
            item=pending.popleft() if pending else next(source)
        except StopIteration:return
        value,start,end=item
        if value[0] not in starters:
            yield "ordinary",value,start,end
            continue
        pending.appendleft(item)
        pieces=[take()]; prefix=pieces[0][0]
        candidates=[t for t in controls if t.startswith(prefix)]
        best=prefix if prefix in meanings else None
        while candidates and any(len(t)>len(prefix) for t in candidates):
            try:pieces.append(take())
            except StopIteration:break
            prefix+=pieces[-1][0]
            candidates=[t for t in candidates if t.startswith(prefix)]
            if prefix in meanings:best=prefix
        used=len(best) if best else 1
        for remainder in reversed(pieces[used:]):pending.appendleft(remainder)
        yield meanings[best] if best else "ordinary", best or pieces[0][0],start,pieces[used-1][2]


def _field(value, quoted, settings):
    if settings.get("trim_whitespace",False):value=value.strip()
    match=value if settings.get("null_tokens_case_sensitive",True) else value.casefold()
    tokens=settings.get("null_tokens",[])
    if not settings.get("null_tokens_case_sensitive",True):tokens=[t.casefold() for t in tokens]
    null=(not quoted or settings.get("null_tokens_in_quoted_fields",False)) and match in tokens
    null=null or value=="" and settings.get("empty_string_is_null",False)
    return FieldValue("NULL" if null else "EMPTY" if value=="" else "VALUE", None if null and not settings.get("retain_null_lexical",False) else value,quoted)


def parse(artifact, decision, config):
    settings=dict(config.get("parsing",{})); validate_tokens({**settings,"delimiter":decision.delimiter,"quotechar":decision.quotechar or "none","escapechar":decision.escapechar or "none"})
    if any("null_tokens" in c for c in config.get("columns",[])):settings["retain_null_lexical"]=True
    limits=config.get("limits",{}); cap=limits.get("max_in_memory_record_bytes",8388608); field_cap=limits.get("max_in_memory_field_bytes",cap)
    identity={"decision":asdict(decision),"parsing":{k:v for k,v in settings.items() if not (k=="column_names" and not v or k=="nel_is_newline" and not v or k=="record_terminator" and v=="auto" or k=="allow_multiline_fields" and v is True)}}
    if config.get("columns"):identity["column_policies"]=config["columns"]
    policy=hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(",",":")).encode()).hexdigest()
    start=decision.evidence.get("bom_bytes",0); line=line_start=1; previous_cr=False; last=""; ordinal=0
    expected=len(settings.get("column_names",[])) or None
    fields=[]; buffer=io.StringIO(); state="start"; quoted=False; escaped=False; oversized=False; malformed=False; field_bytes=0; payload=False; tail_reason=None
    def append(value):
        if not oversized:buffer.write(value)
    def finish_field(header=None):
        nonlocal buffer,quoted
        if not oversized:
            is_header=(ordinal==0 and decision.header=="present") if header is None else header
            field_settings={} if is_header else settings
            fields.append(_field(buffer.getvalue(),quoted,field_settings))
        buffer=io.StringIO();quoted=False
    def envelope(end, unresolved=False):
        nonlocal ordinal,expected
        ordinal+=1; errors=[]; result=[]
        if unresolved:
            kind="unresolved";errors=[{"code":"FRAMING_UNRESOLVED","message":tail_reason or "Unclosed quoted field or dangling escape; logical count for preserved tail is unknown"}]
        elif oversized or len(fields) >= 1024:
            # The current field is not finalized yet, including an empty EOF field.
            kind="rejected";errors=[{"code":"RECORD_LIMIT","message":"Record/field/column-count budget exceeded; exact bytes retained in artifact","byte_start":start,"byte_end":end}]
        elif malformed:
            kind="rejected";errors=[{"code":"CSV_INVALID","message":"Unexpected content after closing qualifier; exact record retained"}]
        elif not payload:
            kind="excluded" if settings.get("blank_record_policy")=="explicitly_skip" else "rejected"
            code="BLANK_EXPLICITLY_EXCLUDED" if kind=="excluded" else "BLANK_PRESERVED_UNIMPORTED" if settings.get("blank_record_policy")=="preserve" else "BLANK_RECORD"
            errors=[{"code":code,"message":"Blank record retained under declared policy"}]
        else:
            finish_field(header=ordinal==1 and decision.header=="present");result=fields[:];kind="header" if ordinal==1 and decision.header=="present" else "data"
            if expected is None:expected=len(result)
            elif kind=="data" and len(result)!=expected:
                kind="rejected";errors=[{"code":"ROW_WIDTH","message":"Field count differs from declared header/first record","expected":expected,"observed":len(result)}]
        rid=hashlib.sha256(f"{artifact.capture_id}:{artifact.sha256}:{ordinal}:{policy}".encode()).hexdigest()
        return RecordEnvelope(rid,ordinal,start,end,line_start,max(line_start,line-1 if last in ("\r\n"+("\x85" if decision.evidence.get("nel_is_newline",False) else "")) else line),result,kind,errors)
    try:
        for token,value,byte_start,byte_end in _lexemes(artifact,decision,settings):
            # Count physical lines regardless of logical record terminator.
            for char in value if token!="ordinary" else ():
                if char=="\r" or char=="\n" and not previous_cr or char=="\x85" and decision.evidence.get("nel_is_newline",False):line+=1
                previous_cr=char=="\r"
            if token=="ordinary":previous_cr=False
            last=value[-1]
            field_bytes+=byte_end-byte_start
            if not oversized and (byte_end-start>cap or field_bytes>field_cap or len(fields)>=1024):
                oversized=True;fields=[];buffer=io.StringIO()
            physical=any(c in value for c in "\r\n") or decision.evidence.get("nel_is_newline",False) and "\x85" in value
            if not settings.get("allow_multiline_fields",True) and physical and (state=="quoted" or escaped or token!="ending"):
                tail_reason="Physical newline violates allow_multiline_fields=false; no resynchronization attempted"
                # Keep scanning only for strict decoding and honest physical span.
            if tail_reason:continue
            if escaped:
                # Ordinary runs contain no token starts: escape only first scalar.
                append(value);escaped=False;payload=True
                if state!="quoted":state="unquoted"
                continue
            if token=="escape":
                if state=="closed":malformed=True
                escaped=True;payload=True;continue
            if state=="start" and token=="quote":state="quoted";quoted=True;payload=True;continue
            if state=="quoted":
                if token=="quote":state="closed"
                else:append(value)
                payload=True;continue
            if state=="closed" and token=="quote" and decision.doublequote:
                append(value);state="quoted";continue
            if token=="ending":
                yield envelope(byte_end)
                start=byte_end;line_start=line;fields=[];buffer=io.StringIO();state="start";quoted=escaped=oversized=malformed=payload=False;field_bytes=0
            elif token=="delimiter":
                finish_field();state="start";field_bytes=0;payload=True
            else:
                if state=="closed":malformed=True
                append(value);state="unquoted";payload=True
    except IngestError as exc:
        if exc.code!="ENCODING_INVALID":raise
        record=envelope(artifact.size,True);record.errors.append({"code":exc.code,"message":str(exc),**exc.context});record.line_end=None;yield record;return
    if start<artifact.size:yield envelope(artifact.size,bool(tail_reason or state=="quoted" or escaped))
