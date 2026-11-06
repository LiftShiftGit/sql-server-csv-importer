"""Exact values/bytes/outcomes from authored exports; no parser creates the oracle."""
from dataclasses import asdict
import codecs
import hashlib
import json
from pathlib import Path
from functools import partial
import pytest
from db_ingest.capture import capture_source
from db_ingest.config import load_config, effective_for_file
from db_ingest.errors import IngestError
from db_ingest.fixturegen import create_test_files, verify_fixture_outcome
from db_ingest.legacy_examples import examples, LEGACY_TEXT
from db_ingest.models import SourceArtifact, DialectDecision
from db_ingest.parsers import delimited

CATEGORIES=("csv_escaping","csv_dirty","csv_legacy_encodings","mainframe_ebcdic")
NAMES=[item['name'] for category in CATEGORIES for item in examples(category)]

@pytest.fixture(scope='module')
def corpus(tmp_path_factory):
    root=tmp_path_factory.mktemp('legacy-corpus')
    manifest=create_test_files(root,CATEGORIES)
    return root,{f['path']:f for f in manifest['fixtures']}

@pytest.mark.parametrize('name',NAMES)
def test_exact_corpus_bytes_values_dispositions_and_spans(corpus,tmp_path,name):
    root,fixtures=corpus; fixture=fixtures[name]
    config=load_config(environ={})
    config['parsing'].update(encoding=fixture['encoding'],delimiter=fixture['delimiter'],quotechar=fixture['quotechar'],header=fixture['header'])
    config['parsing'].update(fixture['overrides'])
    artifact=capture_source(root/name,root,tmp_path/'work')
    assert artifact.sha256==fixture['sha256']==hashlib.sha256((root/name).read_bytes()).hexdigest()
    assert Path(artifact.path).read_bytes()==(root/name).read_bytes()
    try:
        decision=delimited.decide(artifact,config)
    except IngestError as error:
        assert fixture['expected_outcome'] in ('encoding_failure','unsupported_format')
        assert error.code in fixture['expected_reason_codes']
        return
    records=list(delimited.parse(artifact,decision,config))
    expected=[json.loads(line) for line in (root/fixture['expected_path']).open(encoding='utf-8')]
    data=[r for r in records if r.kind=='data']
    assert [(r.ordinal,[v.text for v in r.fields]) for r in data]==[(r['ordinal'],[v['value'] for v in r['values']]) for r in expected]
    assert [r.ordinal for r in records if r.kind=='rejected']==fixture['rejected_ordinals']
    assert any(r.kind=='unresolved' for r in records)==(fixture['expected_outcome']=='unresolved')
    cursor=decision.evidence['bom_bytes']
    for record in records:
        assert record.byte_start==cursor and record.byte_end>cursor
        cursor=record.byte_end
    assert cursor==artifact.size
    dispositions=[(r.ordinal,('pending' if fixture['expected_outcome']=='unresolved' else 'committed') if r.kind=='data' else r.kind,asdict(r)) for r in records]
    outcome=verify_fixture_outcome(fixture,{},dispositions)
    assert outcome['errors']==[]

@pytest.mark.parametrize('encoding',list(LEGACY_TEXT)+['cp037','cp500','cp1140'])
def test_every_valid_single_byte_has_exact_reversible_offset(tmp_path,encoding):
    raw=bytearray(); expected=[]
    for value in range(256):
        try:text=bytes([value]).decode(encoding,errors='strict')
        except UnicodeDecodeError:continue
        assert text.encode(encoding)==bytes([value])
        raw.append(value);expected.append(text)
    p=tmp_path/'all.bin';p.write_bytes(raw)
    artifact=SourceArtifact('single-byte',str(p),str(p),p.name,hashlib.sha256(raw).hexdigest(),len(raw))
    decision=DialectDecision(codecs.lookup(encoding).name,',','"',None,True,'absent')
    for chunk_size in (1,7,65536):
        actual=list(delimited._chars(artifact,decision,chunk_bytes=chunk_size))
        assert actual==[(text,i,i+1) for i,text in enumerate(expected)]

@pytest.mark.parametrize('chunk_size',[1,2,7,65536])
def test_ebcdic_known_hex_nel_multiline_offsets_and_no_escape_loss(tmp_path,monkeypatch,chunk_size):
    # Literal EBCDIC037 golden: id,value NEL 001,"a NEL b" NEL.
    raw=bytes.fromhex('89846ba58193a48515f0f0f16b7f8115827f15')
    path=tmp_path/'known.csv';path.write_bytes(raw)
    config=load_config(environ={});config['parsing'].update(encoding='cp037',delimiter=',',header='present',nel_is_newline=True)
    artifact=capture_source(path,tmp_path,tmp_path/'work')
    decision=delimited.decide(artifact,config)
    original=delimited._chars
    monkeypatch.setattr(delimited,'_chars',partial(original,chunk_bytes=chunk_size))
    records=list(delimited.parse(artifact,decision,config))
    assert [[f.text for f in r.fields] for r in records]==[['id','value'],['001','a\x85b']]
    assert [(r.byte_start,r.byte_end,r.line_start,r.line_end) for r in records]==[(0,9,1,1),(9,len(raw),2,3)]


def test_explicit_nel_policy_rule_and_no_auto_guessing(tmp_path):
    config=load_config(environ={});config['file_rules']=[{'glob':'*.ebc','encoding':'cp037','delimiter':';','nel_is_newline':True}]
    selected=effective_for_file(config,'input.ebc')
    assert selected['parsing']['nel_is_newline'] and selected['parsing']['encoding']=='cp037'
    assert not config['parsing']['nel_is_newline']
    path=tmp_path/'x';path.write_bytes('a;b\x851;ok\x85'.encode('cp037'))
    artifact=capture_source(path,tmp_path,tmp_path/'work')
    selected['parsing']['delimiter']='auto'
    with pytest.raises(IngestError,match='NEL record separators'):
        delimited.decide(artifact,selected)


def test_escaped_terminal_nel_is_literal_field_at_eof(tmp_path):
    p=tmp_path/'literal';p.write_bytes('id,value\x85001,a\\\x85'.encode('cp037'))
    c=load_config(environ={});c['parsing'].update(encoding='cp037',delimiter=',',header='present',escapechar='\\',nel_is_newline=True)
    a=capture_source(p,tmp_path,tmp_path/'work');records=list(delimited.parse(a,delimited.decide(a,c),c))
    assert [v.text for v in records[-1].fields]==['001','a\x85']

@pytest.mark.parametrize('encoding',['iso2022_jp','utf-7','hz'])
def test_untested_stateful_or_dbcs_codecs_cannot_claim_safe_spans(encoding):
    with pytest.raises(IngestError) as failure:delimited._encoding(b'a,b\n1,x\n',encoding)
    assert failure.value.code=='ENCODING_UNSUPPORTED'


@pytest.mark.parametrize('encoding,text',[('cp932','東京 ①'),('shift_jis','東京 ソフト'),('gb18030','中文 😀'),('gbk','中文'),('big5','臺北'),('euc_jp','東京'),('euc_kr','서울'),('cp949','서울')])
@pytest.mark.parametrize('chunk_size',[1,2,7,65536])
def test_multibyte_exact_offsets_across_every_decode_seam(tmp_path,encoding,text,chunk_size):
    raw=('id,label\r\n001,"'+text+'"\r\n').encode(encoding)
    p=tmp_path/'dbcs.csv';p.write_bytes(raw)
    a=SourceArtifact('dbcs',str(p),str(p),p.name,hashlib.sha256(raw).hexdigest(),len(raw))
    c=load_config(environ={});c['parsing'].update(encoding=encoding,delimiter=',',header='present')
    decision=delimited.decide(a,c)
    chars=list(delimited._chars(a,decision,chunk_bytes=chunk_size))
    assert ''.join(v[0] for v in chars)==raw.decode(encoding)
    cursor=0
    for char,start,end in chars:
        assert start==cursor and raw[start:end].decode(encoding)==char
        cursor=end
    assert cursor==len(raw)


def test_noncanonical_cp932_sequence_retained_with_unknown_span(tmp_path):
    # Both8790 and81e0 decode U+2252, but standard encoding selects81e0.
    raw=b'id,label\n001,\x87\x90\n';p=tmp_path/'alias.csv';p.write_bytes(raw)
    a=capture_source(p,tmp_path,tmp_path/'work');c=load_config(environ={})
    c['parsing'].update(encoding='cp932',delimiter=',',header='present')
    rows=list(delimited.parse(a,delimited.decide(a,c),c))
    assert len(rows)==1 and rows[0].kind=='unresolved'
    assert {e['code'] for e in rows[0].errors}=={'FRAMING_UNRESOLVED','ENCODING_INVALID'}
    assert rows[0].byte_start==0 and rows[0].byte_end==len(raw)
    assert Path(a.path).read_bytes()==raw

def test_oversized_ebcdic_nel_record_keeps_following_data_and_exact_coverage(tmp_path):
    p=tmp_path/'large.ebc'
    prefix='id,label\x85001,"'.encode('cp037');suffix='"\x85002,after\x85'.encode('cp037')
    size=32*1024*1024
    with p.open('wb') as output:
        output.write(prefix)
        for _ in range(size//65536):output.write(bytes.fromhex('c1')*65536)
        output.write(suffix)
    a=capture_source(p,tmp_path,tmp_path/'work');c=load_config(environ={})
    c['parsing'].update(encoding='cp037',delimiter=',',header='present',nel_is_newline=True)
    c['limits'].update(max_in_memory_record_bytes=1024,max_in_memory_field_bytes=512)
    records=list(delimited.parse(a,delimited.decide(a,c),c))
    assert [r.kind for r in records]==['header','rejected','data']
    assert records[1].errors[0]['code']=='RECORD_LIMIT' and records[1].fields==[]
    assert [v.text for v in records[2].fields]==['002','after']
    assert [(r.byte_start,r.byte_end) for r in records]==[(0,9),(9,len(prefix)+size+2),(len(prefix)+size+2,p.stat().st_size)]
