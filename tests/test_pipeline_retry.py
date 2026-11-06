"""Retry decision simulations; these do not substitute for SQL fault integration."""
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import hashlib,json
from db_ingest.config import stable_hash
from unittest.mock import patch
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.models import PublicationReceipt
from db_ingest.pipeline import prepare,_plan_from_dict,_publish_with_retry


class RetryDecisionTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.root=Path(self.temp.name)
        source=self.root/'data.csv';source.write_bytes(b'id,text\n1,ok\n')
        self.config=load_config(environ={})
        self.config['source'].update(root=str(self.root),path=str(source),include=['**/*.csv'])
        self.config['preservation'].update(work_directory=str(self.root/'work'),minimum_free_disk_bytes=0)
        self.config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
        self.config['parallel']['enabled']=False
        report=prepare(self.config);self.assertEqual(report['exit_code'],0,report)
        self.config['retry'].update(max_attempts=3,initial_backoff_seconds=0,maximum_backoff_seconds=0,jitter=False)
        self.run_id=report['run_id'];self.item=report['files'][0]
        self.journal=Journal(self.config['preservation']['work_directory'])
        self.plan=_plan_from_dict(self.item['plan'])
        p=self.plan
        self.receipt=asdict(PublicationReceipt(self.item['intent_id'],self.run_id,p.database,p.schema,p.table,p.source_hash,p.interpretation_hash,p.mapping_hash,1,'test-simulation-digest','full','2026-10-29T00:00:00Z',{'control_mirror_state':'VERIFIED'}))

    def tearDown(self):self.journal.close();self.temp.cleanup()

    def call(self):
        return _publish_with_retry(self.journal,self.config,self.plan,lambda:self.journal.records(self.run_id,self.item['file_id'],'pending'),self.item['intent_id'],self.run_id,'test-user','test-secret',self.item['file_id'])

    def transient(self,native=1205):
        return IngestError('DEADLOCK' if native==1205 else 'LOCK_TIMEOUT','simulated explicit native transaction rollback',4,{'retryable':True,'native_code':native})

    def test_explicit_deadlock_resolves_absent_receipt_then_retries_same_intent(self):
        with patch('db_ingest.publication.publish',side_effect=[self.transient(),self.receipt]) as publish,patch('db_ingest.publication.find_receipt',return_value=None) as find:
            actual=self.call()
        self.assertEqual(actual['intent_id'],self.item['intent_id'])
        self.assertEqual(publish.call_count,2);self.assertEqual(find.call_count,1)
        events=self.journal.events(self.run_id)
        self.assertEqual(sum(e['category']=='LOAD_ATTEMPT' for e in events),2)
        self.assertEqual(sum(e['category']=='TRANSIENT_TRANSACTION_RESOLVED' for e in events),1)

    def test_committed_receipt_is_verified_without_replaying_timeout(self):
        with patch('db_ingest.publication.publish',side_effect=self.transient(1222)) as publish,patch('db_ingest.publication.find_receipt',return_value=self.receipt),patch('db_ingest.reconciliation.verify_destination',return_value={'verified':True}) as verify:
            actual=self.call()
        self.assertEqual(actual,self.receipt);self.assertEqual(publish.call_count,1);verify.assert_called_once()

    def test_permission_error_never_retries_or_resolves_as_transient(self):
        with patch('db_ingest.publication.publish',side_effect=IngestError('PERMISSION','simulated denied operation',4,{'retryable':False})) as publish,patch('db_ingest.publication.find_receipt') as find:
            with self.assertRaises(IngestError) as cm:self.call()
        self.assertEqual(cm.exception.code,'PERMISSION');self.assertEqual(publish.call_count,1);find.assert_not_called()

    def test_unknown_commit_absent_receipt_never_replays(self):
        error=IngestError('TIMEOUT','simulated lost COMMIT acknowledgment',5,{'retryable':True,'native_code':1205})
        with patch('db_ingest.publication.publish',side_effect=error) as publish,patch('db_ingest.publication.find_receipt',return_value=None):
            with self.assertRaises(IngestError) as cm:self.call()
        self.assertEqual(cm.exception.exit_code,5);self.assertEqual(publish.call_count,1)
        self.assertIn('no replay',cm.exception.context['receipt_resolution'])

    def test_unknown_commit_existing_receipt_reads_back_without_replay(self):
        error=IngestError('TIMEOUT','simulated lost COMMIT acknowledgment',5)
        with patch('db_ingest.publication.publish',side_effect=error) as publish,patch('db_ingest.publication.find_receipt',return_value=self.receipt),patch('db_ingest.reconciliation.verify_destination',return_value={'verified':True}) as verify:
            actual=self.call()
        self.assertEqual(actual,self.receipt);self.assertEqual(publish.call_count,1);verify.assert_called_once()

    def test_attempt_limit_is_exact(self):
        with patch('db_ingest.publication.publish',side_effect=self.transient()) as publish,patch('db_ingest.publication.find_receipt',return_value=None) as find:
            with self.assertRaises(IngestError):self.call()
        self.assertEqual(publish.call_count,3);self.assertEqual(find.call_count,2)

    def test_failed_receipt_resolution_does_not_replay(self):
        with patch('db_ingest.publication.publish',side_effect=self.transient()) as publish,patch('db_ingest.publication.find_receipt',side_effect=IngestError('DATABASE','simulated disconnected resolution',4)):
            with self.assertRaises(IngestError) as cm:self.call()
        self.assertEqual(publish.call_count,1);self.assertEqual(cm.exception.context['receipt_resolution']['code'],'DATABASE')


    def test_business_prepared_callback_binds_original_intent_and_is_idempotent(self):
        self.config['mapping']['existing_target_strategy']='mapped_business'
        metadata={'path':'unit-only-prepared.json','sha256':'a'*64,'bytes':12,'run_id':self.run_id,'intent_id':self.item['intent_id'],'values':{'count':4,'sha':'b'*64}}
        def simulated_publish(config,*args,**kwargs):
            self.assertEqual(config['_publication_run_id'],self.run_id)
            callback=config['_business_prepared_callback'];callback(metadata);callback(metadata)
            with self.assertRaises(IngestError):callback({**metadata,'intent_id':'wrong-intent'})
            return self.receipt
        with patch('db_ingest.publication.publish',side_effect=simulated_publish):self.call()
        self.assertEqual(self.journal.file(self.run_id,self.item['file_id'])['counts']['business_prepared'],metadata)
        self.assertEqual(sum(e['category']=='BUSINESS_PREPARED' for e in self.journal.events(self.run_id)),1)


    def test_optional_target_review_reaches_scoped_publish_and_remains_sealed(self):
        path=self.root/'review.json';snapshot={'object_id':None}
        data={'run_id':self.run_id,'intent_id':self.item['intent_id'],'snapshot':snapshot,'snapshot_hash':stable_hash(snapshot)}
        raw=json.dumps(data).encode();path.write_bytes(raw)
        pointer={'path':str(path),'run_id':self.run_id,'intent_id':self.item['intent_id'],'sha256':hashlib.sha256(raw).hexdigest(),'snapshot_hash':data['snapshot_hash']}
        counts=self.journal.file(self.run_id,self.item['file_id'])['counts'];counts['target_review']=pointer
        self.journal.update_file(self.run_id,self.item['file_id'],counts=counts)
        self.journal.event(self.run_id,None,'TARGET_REVIEW',{'subject_id':self.item['file_id'],**pointer})
        with patch('db_ingest.publication.publish',return_value=self.receipt) as publish:self.call()
        self.assertEqual(publish.call_args.args[0]['_target_review'],pointer)
        self.assertEqual(self.journal.file(self.run_id,self.item['file_id'])['counts']['target_review'],pointer)


if __name__=='__main__':unittest.main()
