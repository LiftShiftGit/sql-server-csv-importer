"""The scalable token fixture's writer must agree with its authored values/spans."""
from pathlib import Path
import runpy,hashlib
from db_ingest.capture import capture_source
from db_ingest.config import load_config
from db_ingest.parsers import decide,parse
from db_ingest.profile import profile_records
ROOT=Path(__file__).resolve().parents[1]
ORACLE=runpy.run_path(str(ROOT/'tests/oracles/multigb_tokens.py'))

def test_scalable_multitoken_oracle_quotes_tokens_escapes_and_line_counts(tmp_path):
    manifest=ORACLE['generate'](tmp_path/'source',rows=11,padding=256)
    source=Path(manifest['source']);raw=source.read_bytes()
    assert hashlib.sha256(raw).hexdigest()==manifest['sha256']
    config=load_config(environ={});config['source']['format']='delimited'
    config['parsing'].update(encoding='utf-8',delimiter='||',header='present',column_names=manifest['column_names'])
    config['parsing'].update(manifest['parsing'])
    artifact=capture_source(source,source.parent,tmp_path/'work');decision=decide(artifact,config)
    records=list(parse(artifact,decision,config))
    assert [r.kind for r in records]==['header']+['data']*11
    assert [f.text for f in records[0].fields]==manifest['raw_header']
    assert [[f.text for f in r.fields] for r in records[1:]]==[ORACLE['cells'](i,11,256) for i in range(11)]
    assert 'literal||delimiter @@qualifier@@ escaped!!token <EOR>' in records[1].fields[3].text
    assert [c.sql_type for c in profile_records(iter(records),manifest['raw_header'],config)]==manifest['expected_types']
    assert records[0].byte_start==0 and records[-1].byte_end==len(raw)
    assert all(a.byte_end==b.byte_start for a,b in zip(records,records[1:]))
    assert manifest['logical_records']==12 and manifest['physical_lines']==3
