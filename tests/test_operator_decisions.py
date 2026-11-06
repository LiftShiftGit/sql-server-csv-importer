"""Operator choices exercise real preserved-source reprofile and durable linkage."""
import io
import json
from types import SimpleNamespace
import pytest
from db_ingest.config import load_config
from db_ingest.decisions import resolve_length_decision
from db_ingest.journal import Journal
from db_ingest.pipeline import prepare

class Terminal(io.StringIO):
    def isatty(self): return True

@pytest.fixture
def overflow(tmp_path):
    source=tmp_path/'source';source.mkdir();path=source/'input.csv'
    raw='value\n😀ab\n'.encode();path.write_bytes(raw)
    config=load_config(environ={});config['parallel']['enabled']=False
    config['source'].update(root=str(source),path=str(source),include=['**/*.csv'])
    config['preservation'].update(work_directory=str(tmp_path/'work'),minimum_free_disk_bytes=0)
    config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
    config['columns']=[{'source_ordinal':1,'target_type':'nvarchar(2)'}]
    report=prepare(config)
    assert report['exit_code']==2,report
    # A decision must use the immutable snapshot, never a changed live file.
    path.write_bytes(b'changed\nsource\n')
    return config,report,raw

@pytest.mark.parametrize('answer,policy,rejected',[('w','widen',0),('t','retype_text',0),('q','quarantine',1)])
def test_terminal_choice_creates_linked_preserved_interpretation(overflow,monkeypatch,capsys,answer,policy,rejected):
    config,report,raw=overflow;monkeypatch.setattr('sys.stdin',Terminal(answer+'\n'))
    linked,changed=resolve_length_decision(report,config,SimpleNamespace(json=False),{})
    assert changed['schema']['on_length_overflow']==policy
    assert config['schema']['on_length_overflow']=='ask'
    assert linked['run_id']!=report['run_id'] and linked['exit_code']==0
    assert linked['decision']['resume_option']=='--on-length-overflow '+policy
    journal=Journal(config['preservation']['work_directory'])
    try:
        assert journal.run(linked['run_id'])['linked_run']==report['run_id']
        item=next(journal.files(linked['run_id']))
        assert journal.counts(linked['run_id'],item['file_id'])['rejected']==rejected
        from pathlib import Path
        assert Path(item['artifact']['path']).read_bytes()==raw
        assert journal.db.execute("SELECT COUNT(*) FROM events WHERE category LIKE 'INTERPRETATION_DECISION%' ").fetchone()[0]==2
        assert journal.run(report['run_id'])['state']=='NEEDS_DECISION'
    finally:journal.close()
    output=capsys.readouterr().err
    assert 'current_type' in output and 'nvarchar(2)' in output and 'value_preview' in output
    assert '\\ud83d' in output and 'Existing-target ALTER' in output

@pytest.mark.parametrize('answer',['s\n','\n','oops\nno\ninvalid\n'])
def test_stop_and_invalid_choices_do_not_create_new_intent(overflow,monkeypatch,answer):
    config,report,_=overflow;monkeypatch.setattr('sys.stdin',Terminal(answer))
    result,changed=resolve_length_decision(report,config,SimpleNamespace(json=False),{})
    assert result is report and changed is config
    journal=Journal(config['preservation']['work_directory'])
    try:assert journal.db.execute('SELECT COUNT(*) FROM runs').fetchone()[0]==1
    finally:journal.close()

@pytest.mark.parametrize('mode',['json','noninteractive','pipe'])
def test_machine_workflow_never_reads_a_choice(overflow,monkeypatch,mode):
    config,report,_=overflow
    terminal=Terminal('w\n') if mode!='pipe' else io.StringIO('w\n')
    monkeypatch.setattr('sys.stdin',terminal)
    config['interaction']['non_interactive']=mode=='noninteractive'
    result,_=resolve_length_decision(report,config,SimpleNamespace(json=mode=='json'),{})
    assert result is report and terminal.tell()==0

def test_cli_explicit_overflow_policy_is_semantic():
    from db_ingest.cli import parser,OVERRIDES
    from db_ingest.config import hashes
    args=parser().parse_args(['run','--on-length-overflow','widen'])
    a=load_config(environ={});b=load_config(overrides={OVERRIDES['on_length_overflow']:args.on_length_overflow},environ={})
    assert b['schema']['on_length_overflow']=='widen'
    assert hashes(a)['interpretation']!=hashes(b)['interpretation']
