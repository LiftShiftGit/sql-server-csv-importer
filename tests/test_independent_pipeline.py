"""Cross-role lifecycle regressions. SQL calls mocked; not live integration evidence."""
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest import pipeline
from db_ingest.models import PublicationReceipt

class IndependentPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.source=self.root/'source';self.source.mkdir()
        self.config=load_config(environ={})
        self.config['source'].update(root=str(self.source),path=str(self.source),include=['**/*.csv'])
        self.config['preservation'].update(work_directory=str(self.root/'work'),minimum_free_disk_bytes=0)
        self.config['parsing'].update(encoding='utf-8',delimiter=',',header='present',quotechar='"')
        self.config['parallel']['enabled']=False
        (self.source/'data.csv').write_bytes(b'id,text\n1,ok\n2\n')
    def tearDown(self):self.temp.cleanup()
    def prepared(self):
        result=pipeline.prepare(self.config)
        self.assertEqual(result['exit_code'],0,result)
        return result['run_id']
    def receipt(self,run_id,item):
        plan=item['plan']
        return asdict(PublicationReceipt(item['intent_id'],run_id,plan['database'],plan['schema'],plan['table'],plan['source_hash'],plan['interpretation_hash'],plan['mapping_hash'],1,'test-double-digest','full','2026-10-29T00:00:00Z',{'audit_mirror_state':'mirrored','control_mirror_state':'mirrored','failure_table_state':'materialized'}))
    def mark_reconciled(self,run_id):
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            item=next(journal.files(run_id))
            receipt=self.receipt(run_id,item)
            journal.committed(run_id,item['file_id'])
            journal.update_file(run_id,item['file_id'],state='RECONCILED',receipt=receipt)
            journal.state(run_id,'PARTIAL',3)
            return item,receipt
        finally:journal.close()
    def test_direct_prepare_keeps_explicit_caller_parsing_and_source(self):
        result=pipeline.prepare(self.config)
        self.assertEqual(result['exit_code'],0,result)
        self.assertEqual(result['files'][0]['decision']['header'],'present')
        self.assertEqual(result['files'][0]['counts']['pending'],1)
        self.assertEqual(result['files'][0]['counts']['rejected'],1)
    def test_resume_partial_never_upgrades_to_clean_without_new_work(self):
        run_id=self.prepared();item,receipt=self.mark_reconciled(run_id)
        with patch('db_ingest.publication.find_receipt',return_value=receipt),patch('db_ingest.reconciliation.verify_destination',return_value={'verified':True}),patch('db_ingest.publication.publish') as publish:
            result=pipeline.execute(self.config,run_id)
        self.assertEqual(result['exit_code'],3,result)
        self.assertEqual(result['state'],'PARTIAL')
        publish.assert_not_called()
    def test_resume_completed_detects_corrupt_archive_before_sql(self):
        run_id=self.prepared();item,receipt=self.mark_reconciled(run_id)
        Path(item['artifact']['path']).write_bytes(b'corrupted')
        with patch('db_ingest.publication.find_receipt') as find,patch('db_ingest.publication.publish') as publish:
            result=pipeline.execute(self.config,run_id)
        self.assertEqual(result['exit_code'],5,result)
        find.assert_not_called();publish.assert_not_called()
    def test_no_partial_policy_blocks_before_publication(self):
        self.config['load']['partial_publish']='none'
        run_id=self.prepared()
        with patch('db_ingest.publication.publish') as publish:
            result=pipeline.execute(self.config,run_id)
        self.assertEqual(result['exit_code'],2,result)
        publish.assert_not_called()
    def test_pending_control_mirror_cannot_be_clean(self):
        (self.source/'data.csv').write_bytes(b'id,text\n1,ok\n')
        run_id=self.prepared()
        journal=Journal(self.config['preservation']['work_directory'])
        try:item=next(journal.files(run_id))
        finally:journal.close()
        receipt=self.receipt(run_id,item)
        receipt['details']={'control_mirror_state':'pending'}
        with patch('db_ingest.publication.publish',return_value=receipt):
            result=pipeline.execute(self.config,run_id)
        self.assertEqual(result['exit_code'],3,result)
    def test_declared_header_over_record_cap_cannot_become_empty_schema(self):
        (self.source/'data.csv').write_bytes(b'header_too_long_for_limit,b\n1,ok\n')
        self.config['limits'].update(max_in_memory_record_bytes=16,max_in_memory_field_bytes=16)
        result=pipeline.prepare(self.config)
        self.assertNotEqual(result['exit_code'],0,result)
        self.assertIsNone(result['files'][0]['plan'])

class IndependentSecretTests(unittest.TestCase):
    def test_escaped_password_not_disclosed_in_json_diagnostic(self):
        from db_ingest.cli import main
        secret='semi;brace}quote"backslash\\$(value)`'
        diagnostic=IngestError('TEST_PROVIDER','Provider echoed '+secret,context={'raw':'Error '+secret})
        err=io.StringIO()
        with patch('db_ingest.cli.dispatch',side_effect=diagnostic),redirect_stderr(err):
            code=main(['test-connection','--username','test','--password',secret])
        self.assertEqual(code,2)
        output=json.loads(err.getvalue())
        self.assertNotIn(secret,output['message'])
        self.assertNotIn(secret,output['context']['raw'])

if __name__=='__main__':unittest.main()
