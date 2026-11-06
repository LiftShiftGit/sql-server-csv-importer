"""Complete-file inference and explicit missing-header interpretation oracles."""
from contextlib import closing
from pathlib import Path
import hashlib
import pytest
from db_ingest.capture import capture_source
from db_ingest.config import load_config, hashes
from db_ingest.errors import IngestError
from db_ingest.models import FieldValue,RecordEnvelope
from db_ingest.parsers.delimited import decide,parse
from db_ingest.profile import profile_records
from db_ingest.pipeline import prepare
from db_ingest.journal import Journal

@pytest.mark.parametrize('raw,header,names,expected_header',[
    (b'id,label\n001,x,12.50\n002,y,13.00\n','present',['id','label','amount'],['id','label']),
    (b'001,x,12.50\n002,y,13.00\n','absent',['id','label','amount'],None),
    (b'id,,id\n001,x,12.50\n','present',[],['id','','id']),
])
def test_missing_duplicate_absent_header_exact_values_and_stable_names(tmp_path,raw,header,names,expected_header):
    source=tmp_path/'source';source.mkdir();p=source/'input.csv';p.write_bytes(raw)
    c=load_config(environ={});c['parsing'].update(encoding='ascii',delimiter=',',header=header,column_names=names)
    a=capture_source(p,source,tmp_path/'archive');records=list(parse(a,decide(a,c),c))
    assert all(r.kind in ('header','data') for r in records)
    observed=[r for r in records if r.kind=='header']
    assert ([f.text for f in observed[0].fields] if observed else None)==expected_header
    profiles=profile_records(iter(records),expected_header or [],c)
    assert len(profiles)==3 and len({p.target_name for p in profiles})==3
    assert [p.sql_type for p in profiles]==['nvarchar(4)','nvarchar(2)','decimal(4,2)']
    assert [p.original_name for p in profiles]==(names or expected_header)
    assert Path(a.path).read_bytes()==raw
    assert [f.text for f in next(r for r in records if r.kind=='data').fields]==['001','x','12.50']


def test_short_header_without_override_quarantines_not_guesses(tmp_path):
    p=tmp_path/'short.csv';p.write_bytes(b'id,label\n001,x,12.50\n')
    c=load_config(environ={});c['parsing'].update(encoding='ascii',delimiter=',',header='present')
    a=capture_source(p,tmp_path,tmp_path/'work');rows=list(parse(a,decide(a,c),c))
    assert rows[-1].kind=='rejected' and rows[-1].errors[0]['code']=='ROW_WIDTH'
    assert rows[-1].errors[0]['expected']==2 and rows[-1].errors[0]['observed']==3


def test_pipeline_seals_override_preserves_original_and_profiles_missing_name(tmp_path):
    source=tmp_path/'source';source.mkdir();p=source/'short.csv';p.write_bytes(b'id,label\n001,x,12.50\n002,y,13.00\n')
    c=load_config(environ={});c['source'].update(root=str(source),path=str(p),include=['*.csv'])
    c['preservation'].update(work_directory=str(tmp_path/'work'),minimum_free_disk_bytes=1048576)
    c['parsing'].update(encoding='ascii',delimiter=',',header='present',column_names=['id','label','amount'])
    c['parallel']['enabled']=False;c['load']['engine']='odbc'
    result=prepare(c)
    assert result['exit_code']==0
    with closing(Journal(c['preservation']['work_directory'])) as journal:
        item=next(journal.files(result['run_id']))
        assert [col['original_name'] for col in item['plan']['columns']]==['id','label','amount']
        header=next(journal.records(result['run_id'],item['file_id']))
        assert [f.text for f in header.fields]==['id','label']
        assert item['plan']['interpretation_hash']==hashes(c)['interpretation']
        assert item['counts']['physical_lines']==3


def test_entire_stream_not_sample_decides_types_and_precision():
    c=load_config(environ={});c['parsing']['sample_records']=1
    tail=['late text','9'*39,'1.234567890123456789','\U0001f600'*2400,'000123','2024-02-30']
    def records():
        for index in range(30001):
            values=['1','2','3.50','short','123','2024-02-29'] if index<30000 else tail
            yield RecordEnvelope(str(index),index+1,None,None,None,None,[FieldValue('VALUE',v) for v in values])
    profiles=profile_records(records(),['mixed','too_precise','scale','unicode','identifier','date'],c)
    assert [p.sql_type for p in profiles]==['nvarchar(11)','nvarchar(47)','decimal(19,18)','nvarchar(max)','nvarchar(8)','nvarchar(12)']
    assert all(p.stats['records']==30001 for p in profiles)
    assert profiles[3].stats['max_utf16_units']==4800

@pytest.mark.parametrize('names',[[1],['x']*1025,['a\x00b'],['a'*1025]])
def test_invalid_header_override_refuses_configuration(tmp_path,names):
    p=tmp_path/'invalid.toml'
    import json
    p.write_text('[parsing]\ncolumn_names='+json.dumps(names)+'\n',encoding='utf-8')
    with pytest.raises(IngestError):load_config(p,environ={})

def test_disabled_new_policies_preserve_existing_interpretation_identity():
    import copy
    config=load_config(environ={})
    old=copy.deepcopy(config)
    old['parsing'].pop('column_names');old['parsing'].pop('nel_is_newline')
    assert hashes(config)['interpretation']==hashes(old)['interpretation']


def test_new_policy_changes_are_semantic_and_record_identity_stable_when_disabled(tmp_path):
    import copy
    p=tmp_path/'old.csv';p.write_bytes(b'id,label\n001,alpha\n')
    config=load_config(environ={});config['parsing'].update(encoding='ascii',delimiter=',',header='present')
    old=copy.deepcopy(config);old['parsing'].pop('column_names');old['parsing'].pop('nel_is_newline')
    artifact=capture_source(p,tmp_path,tmp_path/'work')
    decision=decide(artifact,config)
    from dataclasses import replace
    old_decision=replace(decision,evidence={k:v for k,v in decision.evidence.items() if k!='nel_is_newline'})
    current=list(parse(artifact,decision,config));prior=list(parse(artifact,old_decision,old))
    assert [row.record_id for row in current]==[row.record_id for row in prior]
    config['parsing']['column_names']=['account','description']
    assert hashes(config)['interpretation']!=hashes(old)['interpretation']
