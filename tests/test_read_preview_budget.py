"""Console preview bounds; full traversal oracles deliberately simulate SQL only."""
from dataclasses import asdict
import json
from pathlib import Path
from unittest.mock import patch
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.models import RecordEnvelope,FieldValue,PublicationReceipt
from db_ingest.pipeline import status,show_errors,prepare,execute,reconcile


def config_for(root):
    config=load_config(environ={});config['parallel']['enabled']=False
    config['preservation'].update(work_directory=str(root/'work'),minimum_free_disk_bytes=0)
    config['audit']['mirror_receipts_after_commit']=False
    config['load']['engine']='odbc'
    return config


def test_status_all_runs_has_count_and_byte_bounded_preview(tmp_path):
    config=config_for(tmp_path);config['limits']['max_queue_bytes']=4096
    journal=Journal(config['preservation']['work_directory'])
    try:
        for index in range(1100):journal.create_run('run-'+str(index),{})
    finally:journal.close()
    result=status(config)
    assert result['run_count']==1100 and result['runs_report_truncated']
    assert len(result['runs'])<1000
    assert len(json.dumps(result,ensure_ascii=True).encode('ascii'))<=4096


def test_show_errors_never_fetches_full_fields_or_large_incident_context(tmp_path):
    config=config_for(tmp_path);config['limits']['max_queue_bytes']=4096
    journal=Journal(config['preservation']['work_directory']);journal.create_run('run',config)
    try:
        journal.incident('run','file',IngestError('SCHEMA_DRIFT','schema differs',2,{'inspection':'x'*16777216}))
        record=RecordEnvelope('record',1,0,2097152,1,1,[FieldValue('VALUE','x'*2097152)],'rejected',[{'code':'TEST','message':'bad'*5000,'column_ordinal':1}])
        journal.save_record('run','file',record);journal.flush()
    finally:journal.close()
    for run_id in ('run',None):
        result=show_errors(config,run_id)
        assert len(json.dumps(result,ensure_ascii=True).encode('ascii'))<=4096
        assert result['incidents'][0]['context_truncated']
        row=result['record_errors'][0];assert row['errors_truncated'] and row['retained_in_journal']
        assert 'fields' not in row and result['record_error_count']==1
    journal=Journal(config['preservation']['work_directory'])
    try:assert journal.db.execute("SELECT length(json_extract(payload,'$.fields[0].text')) FROM records").fetchone()[0]==2097152
    finally:journal.close()


def test_reconcile_verifies_omitted_files_and_keeps_their_partial_status(tmp_path):
    config=config_for(tmp_path);source=tmp_path/'source';source.mkdir()
    config['source'].update(root=str(source),path=str(source),include=['**/*.csv'])
    config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
    config['columns']=[{'source_ordinal':1,'target_type':'int'},{'source_ordinal':2,'target_type':'nvarchar(40)'}]
    for index in range(7):(source/f'f{index}.csv').write_text('id,text\n1,good\n'+('bad,rejected\n' if index==6 else ''),encoding='utf-8')
    planned=prepare(config);assert planned['exit_code']==0
    receipts={}
    def simulated_publish(config,plan,records,intent_id,run_id,*args):
        receipt=asdict(PublicationReceipt(intent_id,run_id,plan.database,plan.schema,plan.table,plan.source_hash,plan.interpretation_hash,plan.mapping_hash,len(list(records())),'a'*64,'full','2026-10-29T00:00:00Z',{}));receipts[intent_id]=receipt;return receipt
    with patch('db_ingest.publication.publish',side_effect=simulated_publish),patch('db_ingest.adapters.mssql.control.materialize_failures',return_value={'state':'VERIFIED','rejected_records':1}):published=execute(config,planned['run_id'])
    assert published['exit_code']==3
    config['limits']['max_queue_bytes']=4096
    with patch('db_ingest.publication.find_receipt',side_effect=lambda config,plan,intent_id,*a:receipts[intent_id]),patch('db_ingest.reconciliation.verify_destination',return_value={'state':'VERIFIED','assurance':'full','simulated_padding':'x'*1000}) as verify:
        checked=reconcile(config,planned['run_id'])
    assert verify.call_count==7 and checked['file_count']==7 and checked['all_files_verified']
    assert checked['files_report_truncated'] and len(checked['files'])<7
    assert checked['exit_code']==3 #The omitted final source has one actual retained reject.
    assert len(json.dumps(checked,ensure_ascii=True).encode('ascii'))<=4096
