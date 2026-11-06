"""Existing-schema operator policy choices use real captures and linked journals.

SQL execution is explicitly mocked here; live independent cases are separate.
"""
from types import SimpleNamespace
import io
import pytest
from db_ingest.config import load_config
from db_ingest.decisions import resolve_schema_decision
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.pipeline import prepare

class Terminal(io.StringIO):
    def isatty(self):return True

@pytest.fixture
def drift(tmp_path):
    source=tmp_path/'source';source.mkdir();path=source/'input.csv';path.write_bytes(b'value\na\nabcdef\n')
    config=load_config(environ={});config['parallel']['enabled']=False
    config['source'].update(root=str(source),path=str(source),include=['**/*.csv'])
    config['preservation'].update(work_directory=str(tmp_path/'work'),minimum_free_disk_bytes=0)
    config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
    config['columns']=[{'source_ordinal':1,'target_type':'nvarchar(8)'}]
    report=prepare(config);assert report['exit_code']==0
    item=report['files'][0];name=item['plan']['columns'][0]['target_name']
    metadata={'columns':[[name,1,'nvarchar',4,0,0,True,False,False,0,'Latin1_General_100_BIN2']],'indexes':[],'checks':[],'foreign_keys':[],'triggers':[]}
    journal=Journal(config['preservation']['work_directory'])
    journal.incident(report['run_id'],item['file_id'],IngestError('SCHEMA_DRIFT','explicit policy required',2,{'schema_diff':{'inspection':metadata,'blocked':['schema policy disabled']}}))
    journal.update_file(report['run_id'],item['file_id'],state='FAILED')
    journal.close();path.write_bytes(b'changed\noriginal\n')
    return config,{**report,'exit_code':2},item

@pytest.mark.parametrize('choice',['w','t','q'])
def test_supported_schema_choice_reprofiles_original_capture(drift,monkeypatch,choice):
    config,report,old=drift;seen=[]
    monkeypatch.setattr('sys.stdin',Terminal(choice+'\n'))
    def execute(changed,run_id,username,password):
        journal=Journal(changed['preservation']['work_directory'])
        try:
            item=next(journal.files(run_id));seen.append((changed,item))
            assert journal.run(run_id)['linked_run']==report['run_id']
            from pathlib import Path
            assert Path(item['artifact']['path']).read_bytes()==b'value\na\nabcdef\n'
            return {'run_id':run_id,'exit_code':3 if choice=='q' else 0}
        finally:journal.close()
    monkeypatch.setattr('db_ingest.pipeline.execute',execute)
    result=resolve_schema_decision(report,config,SimpleNamespace(json=False),'user','secret')
    assert result['run_id']!=report['run_id'] and len(seen)==1
    changed,item=seen[0]
    if choice=='w':assert changed['schema']['allow_widen_on_append'] is True and item['plan']['columns'][0]['sql_type']=='nvarchar(8)'
    elif choice=='t':assert changed['schema']['allow_retype_on_append'] is True and item['plan']['columns'][0]['sql_type']=='nvarchar(max)'
    else:assert item['plan']['columns'][0]['sql_type']=='nvarchar(2)' and item['counts']['rejected']==1 and item['counts']['pending']==1
    assert config['schema']['allow_widen_on_append'] is False

@pytest.mark.parametrize('choice',['s\n','\n','wrong\nwrong\nwrong\n'])
def test_stop_never_creates_a_linked_operation(drift,monkeypatch,choice):
    config,report,_=drift;monkeypatch.setattr('sys.stdin',Terminal(choice))
    assert resolve_schema_decision(report,config,SimpleNamespace(json=False)) is report
    journal=Journal(config['preservation']['work_directory'])
    try:assert journal.db.execute('SELECT COUNT(*) FROM runs').fetchone()[0]==1
    finally:journal.close()

def test_manual_drift_is_not_an_implicit_alter_menu(drift,monkeypatch):
    config,report,item=drift;terminal=Terminal('w\n');monkeypatch.setattr('sys.stdin',terminal)
    journal=Journal(config['preservation']['work_directory'])
    with journal.db:journal.db.execute("UPDATE incidents SET context=?",('{"schema_diff":{"blocked":["externally changed schema"]}}',))
    journal.close()
    assert resolve_schema_decision(report,config,SimpleNamespace(json=False)) is report
    assert terminal.tell()==0
