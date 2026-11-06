"""Independent headerless large-file writer retains first data and exact spans."""
import hashlib,runpy
from pathlib import Path
from db_ingest.capture import capture_source
from db_ingest.config import load_config
from db_ingest.parsers import decide,parse
from db_ingest.profile import profile_records
ORACLE=runpy.run_path(str(Path(__file__).resolve().parent/'oracles/multigb_headerless.py'))
def test_headerless_multigb_oracle_first_row_and_inferred_types(tmp_path):
    m=ORACLE['generate'](tmp_path/'source',rows=11,padding=256);source=Path(m['source']);raw=source.read_bytes()
    assert hashlib.sha256(raw).hexdigest()==m['sha256'] and m['raw_header']==[]
    c=load_config(environ={});c['source']['format']='delimited';c['parsing'].update(encoding='utf-8',delimiter=',',header='absent',column_names=m['column_names'])
    a=capture_source(source,source.parent,tmp_path/'work');records=list(parse(a,decide(a,c),c))
    assert len(records)==11 and all(r.kind=='data' for r in records)
    assert [[f.text for f in r.fields] for r in records]==[ORACLE['cells'](i,11,256) for i in range(11)]
    assert records[0].ordinal==1 and records[0].fields[0].text=='000000000'
    assert [p.sql_type for p in profile_records(iter(records),[],c)]==m['expected_types']
    assert records[0].byte_start==0 and records[-1].byte_end==len(raw)
    assert all(a.byte_end==b.byte_start for a,b in zip(records,records[1:]))
    assert m['logical_records']==11 and m['physical_lines']==13
