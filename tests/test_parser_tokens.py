"""Independent authored token grammars, exact byte partitions and decode seams."""
import copy
import hashlib
import tracemalloc
from pathlib import Path
from unittest.mock import patch
import pytest
from db_ingest.config import load_config, hashes, _validate
from db_ingest.errors import IngestError
from db_ingest.models import SourceArtifact
from db_ingest.parsers import decide, parse
from db_ingest.parsers import delimited
from db_ingest.parsers.token_delimited import validate_tokens


def setup(tmp_path, text, encoding="utf-8", **settings):
    cfg=load_config(environ={})
    cfg["source"]["format"]="delimited"
    cfg["parsing"].update({"delimiter":"||","quotechar":"@@","escapechar":"!!","header":"present","record_terminator":"<END>",**settings})
    cfg["parsing"]["encoding"]=encoding
    raw=text.encode(encoding);path=tmp_path/"source.txt";path.write_bytes(raw)
    artifact=SourceArtifact("capture",str(path),str(path),"source.txt",hashlib.sha256(raw).hexdigest(),len(raw))
    return artifact,cfg


def values(record):return [(f.state,f.text,f.quoted) for f in record.fields]


@pytest.mark.parametrize("encoding",["utf-8","utf-16-le","utf-16-be","utf-32-le","utf-32-be","cp037","cp1252","shift_jis"])
@pytest.mark.parametrize("chunk",[1,2,3,7,31,65536])
def test_seams_qualifiers_escapes_null_and_spans(tmp_path,encoding,chunk):
    pieces=["left||right<END>","@@A||B@@||@@a!!@@b@@@@c@@<END>","NULL||@@NULL@@<END>","||tail<END>"]
    artifact,cfg=setup(tmp_path,"".join(pieces),encoding)
    cfg["parsing"]["null_tokens"]=["NULL"]
    original=delimited._chars
    with patch.object(delimited,"_chars",lambda a,d,runs=False,chunk_bytes=65536: original(a,d,runs,chunk)):
        records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","data","data","data"]
    assert values(records[1])==[("VALUE","A||B",True),("VALUE",'a@@b@@c',True)]
    assert values(records[2])==[("NULL",None,False),("VALUE","NULL",True)]
    assert values(records[3])==[("EMPTY","",False),("VALUE","tail",False)]
    offset=0
    for record,piece in zip(records,pieces):
        assert (record.byte_start,record.byte_end)==(offset,offset+len(piece.encode(encoding)))
        assert (record.line_start,record.line_end)==(1,1)
        offset=record.byte_end
    assert offset==artifact.size


@pytest.mark.parametrize("term",["\r\n","\n","\r","<EOR>","ENDEND"])
def test_explicit_record_endings_and_unmatched_newline_data(tmp_path,term):
    artifact,cfg=setup(tmp_path,"a||b"+term+"1||@@two\nlines@@"+term+"2||z",record_terminator=term)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","data","data"]
    assert records[1].fields[1].text=="two\nlines"
    assert records[1].line_end>=records[1].line_start
    assert records[-1].byte_end==artifact.size


def test_explicit_terminator_bare_newline_is_data(tmp_path):
    artifact,cfg=setup(tmp_path,"a||b<END>1||x\r\ny<END>")
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert records[1].fields[1].text=="x\r\ny"
    assert (records[1].line_start,records[1].line_end)==(1,2)


@pytest.mark.parametrize("ending",["\n","\r","\r\n"])
@pytest.mark.parametrize("quoted",[True,False])
def test_line_oriented_no_guessed_resynchronization(tmp_path,ending,quoted):
    tail=("@@broken" if quoted else "broken")+ending+"9||must_not_be_row<END>"
    artifact,cfg=setup(tmp_path,"a||b<END>1||"+tail,allow_multiline_fields=False)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","unresolved"]
    assert records[1].byte_start==len("a||b<END>")
    assert records[1].byte_end==artifact.size
    assert "no resynchronization" in records[1].errors[0]["message"]


def test_auto_crlf_and_multiline_guard(tmp_path):
    artifact,cfg=setup(tmp_path,"a||b\r\n1||ok\n2||@@bad\r\n3||bleed\n",record_terminator="auto",allow_multiline_fields=False)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","data","unresolved"]
    assert [(r.line_start,r.line_end) for r in records]==[(1,1),(2,2),(3,4)]
    assert records[-1].byte_end==artifact.size


@pytest.mark.parametrize("tail",["1||@@unclosed<END>9||bleed<END>","1||dangling!!","1||@@dangling!!"])
def test_malformed_tail_never_becomes_following_row(tmp_path,tail):
    artifact,cfg=setup(tmp_path,"a||b<END>"+tail)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","unresolved"]
    assert records[-1].byte_end==artifact.size


def test_bad_close_width_and_no_padding(tmp_path):
    artifact,cfg=setup(tmp_path,"a||b<END>1||@@x@@junk<END>one<END>2||ok<END>")
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","rejected","rejected","data"]
    assert records[1].errors[0]["code"]=="CSV_INVALID"
    assert len(records[2].fields)==1
    assert records[2].errors[0]["code"]=="ROW_WIDTH"


@pytest.mark.parametrize("controls",[{"delimiter":"|","quotechar":"||"},{"delimiter":"||","escapechar":"|"},{"quotechar":"@@","escapechar":"@@"},{"record_terminator":"||END"},{"delimiter":"x"*33},{"record_terminator":""},{"escapechar":"x\ny"}])
def test_prefix_conflicts_and_token_bounds(controls):
    settings={"delimiter":"||","quotechar":"@@","escapechar":"!!","record_terminator":"<END>",**controls}
    with pytest.raises(IngestError):validate_tokens(settings)


def test_overlapping_prefixes_are_deterministic_and_angle_quote_not_xml(tmp_path):
    artifact,cfg=setup(tmp_path,"<<a<<::<<b<<<EOR>1::<<x::y<<<EOR>",delimiter="::",quotechar="<<",record_terminator="<EOR>")
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","data"]
    assert records[1].fields[1].text=="x::y"


def test_self_overlap_and_escaped_full_tokens(tmp_path):
    artifact,cfg=setup(tmp_path,"aabaabababEND1aba!!abaXababEND",delimiter="aba",quotechar="@@",record_terminator="ababEND")
    # Delimiter prefixes terminator; this grammar has no safe unambiguous contract.
    with pytest.raises(IngestError,match="prefix"):decide(artifact,cfg)
    artifact,cfg=setup(tmp_path,"a||b<END>1||x!!<END>y!!||z!!!!q<END>")
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert records[1].fields[1].text=="x<END>y||z!!q"


def test_limits_clear_payload_but_keep_quote_state_and_following_rows(tmp_path):
    artifact,cfg=setup(tmp_path,"a||b<END>1||@@"+"x"*10000+"<END>not_a_row@@<END>2||ok<END>")
    cfg["limits"]["max_in_memory_record_bytes"]=128
    cfg["limits"]["max_in_memory_field_bytes"]=64
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","rejected","data"]
    assert records[1].errors[0]["code"]=="RECORD_LIMIT"
    assert not records[1].fields
    assert records[2].fields[1].text=="ok"
    assert records[0].byte_end==records[1].byte_start
    assert records[1].byte_end==records[2].byte_start


def test_strict_invalid_encoding_tail(tmp_path):
    artifact,cfg=setup(tmp_path,"a||b<END>1||@@x")
    with open(artifact.path,"ab") as handle:handle.write(b"\xff<END>2||bad<END>")
    artifact.size=Path(artifact.path).stat().st_size
    # Small decision sample avoids deciding over the deliberately invalid tail.
    cfg["parsing"]["sample_bytes"]=8
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert records[-1].kind=="unresolved"
    assert records[-1].line_end is None
    assert records[-1].byte_end==artifact.size


def test_defaults_and_saved_old_decision_keep_record_identity(tmp_path):
    cfg=load_config(environ={});cfg["parsing"].update(delimiter=",",header="present",encoding="utf-8")
    raw=b"a,b\r\n1,2\r\n";path=tmp_path/"old.csv";path.write_bytes(raw)
    artifact=SourceArtifact("old",str(path),str(path),"old.csv",hashlib.sha256(raw).hexdigest(),len(raw))
    old=copy.deepcopy(cfg)
    old["parsing"].pop("record_terminator");old["parsing"].pop("allow_multiline_fields")
    assert hashes(cfg)["interpretation"]==hashes(old)["interpretation"]
    assert decide(artifact,cfg)==decide(artifact,old)
    assert list(parse(artifact,decide(artifact,old),old))==list(parse(artifact,decide(artifact,cfg),cfg))


def test_field_token_budget_peak_is_independent_of_oversized_length(tmp_path):
    artifact,cfg=setup(tmp_path,"a||b<END>1||@@"+"x"*(4*1024*1024)+"@@<END>2||ok<END>")
    cfg["limits"]["max_in_memory_record_bytes"]=4096;cfg["limits"]["max_in_memory_field_bytes"]=2048
    tracemalloc.start()
    try:
        records=list(parse(artifact,decide(artifact,cfg),cfg));_,peak=tracemalloc.get_traced_memory()
    finally:tracemalloc.stop()
    assert [r.kind for r in records]==["header","rejected","data"]
    assert peak<2*1024*1024

@pytest.mark.parametrize("encoding",["utf-8","utf-16-le","utf-16-be","utf-32-le","utf-32-be"])
@pytest.mark.parametrize("chunk",[1,3,17])
def test_unicode_tokens_exact_encoded_spans(tmp_path,encoding,chunk):
    text="name🌸::value終止符««😀🌸::é««🌸::««x««終止符"
    artifact,cfg=setup(tmp_path,text,encoding,delimiter="🌸::",quotechar="««",escapechar="‼",record_terminator="終止符")
    original=delimited._chars
    with patch.object(delimited,"_chars",lambda a,d,runs=False,chunk_bytes=65536:original(a,d,runs,chunk)):
        records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","data"]
    assert [f.text for f in records[1].fields]==["😀🌸::é","x"]
    assert records[0].byte_end==len("name🌸::value終止符".encode(encoding))
    assert records[1].byte_start==records[0].byte_end
    assert records[1].byte_end==len(text.encode(encoding))


@pytest.mark.parametrize("term,second",[("\r","\n1||x\r"),("\n","1||x\r\n"),("\r\n","1||x\n\r\n")])
def test_explicit_cr_lf_does_not_let_physical_pair_hide_terminator(tmp_path,term,second):
    artifact,cfg=setup(tmp_path,"a||b"+term+second,record_terminator=term)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert len(records)==2
    assert records[0].byte_end==len("a||b"+term)
    assert records[1].byte_end==artifact.size
    assert records[1].kind=="data"


@pytest.mark.parametrize("escaped",["!!\n","!!\r\n"])
def test_line_oriented_escape_cannot_bypass_guard(tmp_path,escaped):
    artifact,cfg=setup(tmp_path,"a||b\n1||x"+escaped+"2||bleed\n",record_terminator="auto",allow_multiline_fields=False)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","unresolved"]
    assert records[-1].byte_end==artifact.size


def test_nel_opt_in_with_custom_tokens(tmp_path):
    artifact,cfg=setup(tmp_path,"a||b\x851||@@x\x85y@@\x852||z\x85",encoding="cp037",record_terminator="auto",nel_is_newline=True)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","data","data"]
    assert records[1].fields[1].text=="x\x85y"
    assert [(r.line_start,r.line_end) for r in records]==[(1,1),(2,3),(4,4)]


def test_full_32_character_tokens_and_1024_columns(tmp_path):
    delimiter="D"*32;quote="Q"*32;escape="X"*32;ending="E"*32
    artifact,cfg=setup(tmp_path,delimiter.join(["h"]*1024)+ending+delimiter.join(["v"]*1024)+ending,delimiter=delimiter,quotechar=quote,escapechar=escape,record_terminator=ending)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","data"]
    assert len(records[1].fields)==1024
    artifact,cfg=setup(tmp_path,"a||b<END>"+"||".join(["v"]*1025)+"<END>2||ok<END>")
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [r.kind for r in records]==["header","rejected","data"]
    assert records[1].errors[0]["code"]=="RECORD_LIMIT"


def test_raw_header_labels_are_not_null_trimmed(tmp_path):
    artifact,cfg=setup(tmp_path," NULL ||@@ NULL @@<END>NULL||@@NULL@@<END>",trim_whitespace=True,null_tokens=["NULL"])
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert [f.text for f in records[0].fields]==[" NULL "," NULL "]
    assert values(records[1])==[("NULL",None,False),("VALUE","NULL",True)]


def test_custom_delimiter_does_not_disable_real_xml_conflict(tmp_path):
    artifact,cfg=setup(tmp_path,"<row><a>1</a></row>")
    with pytest.raises(IngestError,match="structured"):decide(artifact,cfg)


def test_new_settings_per_file_validation_and_hash_semantics(tmp_path):
    from db_ingest.config import effective_for_file
    cfg=load_config(environ={});base=hashes(cfg)["interpretation"]
    cfg["file_rules"]=[{"glob":"*.txt","delimiter":"||","quotechar":"@@","record_terminator":"<END>","allow_multiline_fields":False}]
    effective=effective_for_file(cfg,"source.txt")
    assert effective["parsing"]["record_terminator"]=="<END>"
    assert effective["parsing"]["allow_multiline_fields"] is False
    assert hashes(effective)["interpretation"]!=base
    cfg["file_rules"][0]["quotechar"]="|"
    with pytest.raises(IngestError):effective_for_file(cfg,"source.txt")


def test_random_authored_round_trip(tmp_path):
    import random
    rng=random.Random(1707)
    choices=["", "plain", "a||b", "@@", "!!", "<END>", "a\r\nb", "NULL", "a@b", "||@@!!<END>"]
    def quote(value):return "@@"+value.replace("!!","!!!!").replace("@@","!!@@")+"@@"
    expected=[[rng.choice(choices) for _ in range(4)] for _ in range(150)]
    text="||".join(["a","b","c","d"])+"<END>"+"".join("||".join(quote(v) for v in row)+"<END>" for row in expected)
    artifact,cfg=setup(tmp_path,text)
    records=list(parse(artifact,decide(artifact,cfg),cfg))
    assert all(r.kind=="data" for r in records[1:])
    assert [[f.text for f in r.fields] for r in records[1:]]==expected
    assert all(a.byte_end==b.byte_start for a,b in zip(records,records[1:]))
    assert records[-1].byte_end==artifact.size

@pytest.mark.parametrize("delimiter,ending",[(",","\n"),("||","<END>")])
@pytest.mark.parametrize("header",["present","absent"])
@pytest.mark.parametrize("terminated",[True,False])
@pytest.mark.parametrize("trailing_empty",[True,False])
@pytest.mark.parametrize("count",[1024,1025])
def test_column_budget_at_exact_eof_boundary(tmp_path,delimiter,ending,header,terminated,trailing_empty,count):
    # With trailing_empty=True the final delimiter itself creates the final field.
    row=delimiter*(count-1) if trailing_empty else delimiter.join(["x"]*count)
    artifact,cfg=setup(tmp_path,row+(ending if terminated else ""),delimiter=delimiter,record_terminator="auto" if delimiter=="," else ending,quotechar="none",escapechar="none",header=header)
    decision=decide(artifact,cfg)
    original=delimited._field_values
    decoded_widths=[]
    def bounded_decode(text,decision,settings,nel_terminated=False):
        fields=original(text,decision,settings,nel_terminated=nel_terminated)
        decoded_widths.append(len(fields))
        assert len(fields)<=1024
        return fields
    with patch.object(delimited,"_field_values",bounded_decode):
        records=list(parse(artifact,decision,cfg))
    assert len(records)==1
    record=records[0]
    assert (record.byte_start,record.byte_end)==(0,artifact.size)
    if count==1024:
        assert record.kind==("header" if header=="present" else "data")
        assert len(record.fields)==1024
        assert all(f.state==("EMPTY" if trailing_empty else "VALUE") for f in record.fields)
    else:
        assert record.kind=="rejected"
        assert record.errors[0]["code"]=="RECORD_LIMIT"
        assert not record.fields
        assert not decoded_widths
