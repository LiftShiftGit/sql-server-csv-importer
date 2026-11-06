"""Queryable summaries keep current dispositions distinct from attempt metrics."""
from db_ingest.config import load_config
from db_ingest.pipeline import prepare
from db_ingest.journal import Journal
from db_ingest.reporting import file_summary


def test_source_only_summary_has_unknown_sql_counts_and_exact_local_measures(tmp_path):
    source=tmp_path/'input.csv';raw=b'value\n1\nbad\n';source.write_bytes(raw)
    config=load_config(environ={});config['parallel']['enabled']=False
    config['source'].update(root=str(tmp_path),path=str(source),include=[source.name])
    config['preservation'].update(work_directory=str(tmp_path/'work'),minimum_free_disk_bytes=0)
    config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
    config['columns']=[{'source_ordinal':1,'target_type':'int'}]
    report=prepare(config);journal=Journal(config['preservation']['work_directory'])
    try:
        item=next(journal.files(report['run_id']));summary=file_summary(journal,config,item)
        assert summary['source_only_profile'] is True
        assert summary['measures']['rejected']==1 and summary['measures']['pending']==1
        assert summary['measures']['source_bytes']==len(raw)
        assert summary['measures']['logical_records']==3 and summary['measures']['header_records']==1
        assert summary['measures']['sql_staged_records'] is None
        assert summary['measures']['sql_validated_records'] is None
        assert summary['target_before_records'] is summary['target_after_records'] is None
        assert summary['destination']['table']==item['plan']['table']
        assert summary['format']['encoding']=='utf-8'
        assert any(p['phase']=='parse' and p['succeeded_attempts']==1 for p in summary['phase_timings'])
        assert all(report['run_id'] in command for command in summary['next_commands'])
        journal.event(report['run_id'],item['file_id'],'PHASE_TIMING',{'phase':'sql_stage_validate_publish','elapsed_seconds':1.5,'succeeded':False})
        journal.event(report['run_id'],item['file_id'],'PHASE_TIMING',{'phase':'sql_stage_validate_publish','elapsed_seconds':2.5,'succeeded':True})
        refreshed=file_summary(journal,config,item)
        phase=next(p for p in refreshed['phase_timings'] if p['phase']=='sql_stage_validate_publish')
        assert phase=={'phase':'sql_stage_validate_publish','elapsed_seconds_all_attempts':4.0,'attempts':2,'succeeded_attempts':1}
        assert refreshed['measures']['pending']==1 # attempts never become dispositions
    finally:journal.close()

def test_console_summary_never_labels_unpublished_plan_clean_and_escapes_controls():
    from db_ingest.reporting import console_summary
    report={'run_id':'run','state':'PROFILED','exit_code':0,'files':[{'artifact':{'relative_path':'input.csv','path':'D:/archive/source.bin'},'plan':{'database':'db','schema':'dbo','table':'x'},'counts':{'pending':2,'committed':0,'rejected':0,'unresolved':0},'receipt':None}],'errors':[{'code':'LOCAL','message':'observed\x1b[31m\ntext'}]}
    text=console_summary(report)
    assert text.startswith('PROFILED (exit 0)') and 'CLEAN_VERIFIED' not in text
    assert 'SQL staged ?; validated ?' in text and 'not acknowledged' in text
    assert '\x1b' not in text and '\\x1b' in text and '\\x0a' in text
    assert 'db.dbo.x' in text and 'D:/archive/source.bin' in text
