"""Coordinator recovery-proof simulations; SQL proofs have separate live oracles."""
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from db_ingest.config import load_config,redact
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.models import SourceArtifact,PublicationReceipt
from db_ingest.pipeline import prepare,execute,reconcile,status,_known_receipts,_plan_from_dict,_resolve_interrupted
import json


class ResolutionCoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.root=Path(self.temp.name)
        source=self.root/'data.csv';source.write_bytes(b'id,text\n1,ok\n')
        self.config=load_config(environ={})
        self.config['source'].update(root=str(self.root),path=str(source),include=['**/*.csv'])
        self.config['preservation'].update(work_directory=str(self.root/'work'),minimum_free_disk_bytes=0)
        self.config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
        self.config['parallel']['enabled']=False
        report=prepare(self.config);self.assertEqual(report['exit_code'],0,report)
        self.run_id=report['run_id'];self.item=report['files'][0]
        p=_plan_from_dict(self.item['plan'])
        self.receipt=asdict(PublicationReceipt(self.item['intent_id'],self.run_id,p.database,p.schema,p.table,p.source_hash,p.interpretation_hash,p.mapping_hash,1,'unit-proof-digest','full','2026-10-29T00:00:00Z',{'control_mirror_state':'VERIFIED'}))

    def tearDown(self):self.temp.cleanup()

    def journal(self):return Journal(self.config['preservation']['work_directory'])

    def mark_unknown(self):
        journal=self.journal()
        try:
            journal.update_file(self.run_id,self.item['file_id'],state='COMMIT_OUTCOME_UNKNOWN')
            return [r.record_id for r in journal.records(self.run_id,self.item['file_id'])]
        finally:journal.close()

    def audit_context(self):
        return patch('db_ingest.adapters.mssql.control.mirror_run_audit',return_value={'state':'VERIFIED'})

    def test_proven_absence_replays_same_intent_without_resetting_ledger(self):
        original_ids=self.mark_unknown()
        proof={'version':1,'database_guid':'proof-guid','recovery_fork_guid':'proof-fork','current_intent_records':0}
        with patch('db_ingest.publication.resolve_publication',return_value={'state':'RESOLVED_ABSENT','proof':proof}) as resolve,patch('db_ingest.publication.publish',return_value=self.receipt) as publish,patch('db_ingest.pipeline._audit_backfill',side_effect=lambda j,c,p,r,*a,**k:r),self.audit_context():
            report=execute(self.config,self.run_id,'user','secret')
        self.assertEqual(report['exit_code'],0,report)
        resolve.assert_called_once();publish.assert_called_once()
        self.assertEqual(resolve.call_args.args[0]['_publication_run_id'],self.run_id)
        attempted=publish.call_args.args[0]
        self.assertEqual(attempted['_publication_resolution'],proof)
        self.assertEqual(report['files'][0]['counts']['publication_resolution'],proof)
        self.assertEqual(attempted['_publication_attempt']['number'],1)
        self.assertEqual(publish.call_args.args[3],self.item['intent_id'])
        journal=self.journal()
        try:
            self.assertEqual([r.record_id for r in journal.records(self.run_id,self.item['file_id'])],original_ids)
            self.assertEqual(journal.counts(self.run_id,self.item['file_id'])['committed'],1)
            events=journal.events(self.run_id)
            self.assertEqual(sum(e['category']=='COMMIT_OUTCOME_RESOLVED' for e in events),1)
        finally:journal.close()

    def test_existing_verified_receipt_never_replays(self):
        self.mark_unknown()
        with patch('db_ingest.publication.resolve_publication',return_value={'state':'COMMITTED','receipt':self.receipt}),patch('db_ingest.publication.publish') as publish,patch('db_ingest.pipeline._audit_backfill',side_effect=lambda j,c,p,r,*a,**k:r),self.audit_context():
            report=execute(self.config,self.run_id,'user','secret')
        self.assertEqual(report['exit_code'],0,report);publish.assert_not_called()

    def test_known_local_ack_missing_destination_never_replays(self):
        self.mark_unknown()
        journal=self.journal()
        try:journal.update_file(self.run_id,self.item['file_id'],receipt=self.receipt)
        finally:journal.close()
        with patch('db_ingest.publication.resolve_publication',side_effect=IngestError('RECEIPT_MISSING','simulated acknowledged receipt absent',5)),patch('db_ingest.publication.publish') as publish,self.audit_context():
            report=execute(self.config,self.run_id,'user','secret')
        self.assertEqual(report['exit_code'],5,report);publish.assert_not_called()

    def test_current_committed_disposition_without_local_receipt_blocks_before_sql(self):
        self.mark_unknown();journal=self.journal()
        try:journal.committed(self.run_id,self.item['file_id'])
        finally:journal.close()
        with patch('db_ingest.publication.resolve_publication') as resolve,patch('db_ingest.publication.publish') as publish,self.audit_context():
            report=execute(self.config,self.run_id,'user','secret')
        self.assertEqual(report['exit_code'],5,report);resolve.assert_not_called();publish.assert_not_called()

    def test_streaming_history_includes_prior_ack_but_not_other_server(self):
        journal=self.journal()
        try:
            for run_id,server in [('prior','localhost,1433'),('other','another-server')]:
                cfg=redact(self.config);cfg['destination']['server']=server
                journal.create_run(run_id,cfg)
                journal.add_file(run_id,SourceArtifact(**self.item['artifact']),run_id+'-intent')
                receipt={**self.receipt,'run_id':run_id,'intent_id':run_id+'-intent'}
                journal.update_file(run_id,self.item['file_id'],plan=self.item['plan'],receipt=receipt,state='RECONCILED')
            history=list(_known_receipts(journal,self.config,_plan_from_dict(self.item['plan'])))
            self.assertEqual([h['receipt']['run_id'] for h in history],['prior'])
        finally:journal.close()


    def complete_local_simulation(self):
        with patch('db_ingest.publication.publish',return_value=self.receipt),patch('db_ingest.pipeline._audit_backfill',side_effect=lambda j,c,p,r,*a,**k:r),self.audit_context():report=execute(self.config,self.run_id)
        self.assertEqual(report['exit_code'],0,report)
        return report

    def test_reconcile_denies_changed_ledger_even_when_archive_and_sql_receipt_match(self):
        self.complete_local_simulation();journal=self.journal()
        try:
            row=journal.db.execute("SELECT record_id,payload FROM records WHERE run_id=? AND disposition='committed'",(self.run_id,)).fetchone()
            payload=json.loads(row[1]);payload['fields'][1]['text']='altered'
            with journal.db:journal.db.execute('UPDATE records SET payload=? WHERE run_id=? AND record_id=?',(json.dumps(payload),self.run_id,row[0]))
        finally:journal.close()
        with patch('db_ingest.publication.find_receipt',return_value=self.receipt) as find,patch('db_ingest.reconciliation.verify_destination') as verify:
            with self.assertRaises(IngestError) as raised:reconcile(self.config,self.run_id)
        self.assertEqual(raised.exception.code,'LEDGER_MISMATCH');self.assertEqual(raised.exception.exit_code,5)
        find.assert_not_called();verify.assert_not_called()

    def test_reconcile_denies_missing_committed_mark_and_unknown_or_wrong_dispositions(self):
        self.complete_local_simulation()
        for disposition in ('pending','header','invented'):
            with self.subTest(disposition=disposition):
                journal=self.journal()
                try:
                    with journal.db:journal.db.execute('UPDATE records SET disposition=? WHERE run_id=? AND kind=?',(disposition,self.run_id,'data'))
                finally:journal.close()
                with patch('db_ingest.publication.find_receipt',return_value=self.receipt),patch('db_ingest.reconciliation.verify_destination') as verify:
                    with self.assertRaises(IngestError) as raised:reconcile(self.config,self.run_id)
                self.assertEqual(raised.exception.exit_code,5);verify.assert_not_called()
        journal=self.journal()
        try:
            with self.assertRaises(IngestError):journal.set_disposition(self.run_id,self.item['file_id'],'unimportant','invented')
        finally:journal.close()

    def test_status_uses_live_dispositions_without_overwriting_seals(self):
        completed=self.complete_local_simulation();approved=completed['files'][0]['counts']['ledger_sha256']
        journal=self.journal()
        try:
            counts=journal.file(self.run_id,self.item['file_id'])['counts'];counts.update(pending=1,committed=0)
            journal.update_file(self.run_id,self.item['file_id'],counts=counts)
        finally:journal.close()
        observed=status(self.config,self.run_id)['files'][0]['counts']
        self.assertEqual((observed['pending'],observed['committed']),(0,1));self.assertEqual(observed['ledger_sha256'],approved)


    def test_missing_approved_ledger_or_fullplan_seal_never_bypasses_integrity_checks(self):
        for key in ('ledger_sha256','plan_artifact','plan_artifact_sha256'):
            with self.subTest(key=key):
                journal=self.journal()
                try:
                    original=journal.file(self.run_id,self.item['file_id'])['counts'];counts=dict(original);counts.pop(key,None)
                    journal.update_file(self.run_id,self.item['file_id'],counts=counts)
                finally:journal.close()
                with patch('db_ingest.publication.publish') as publish,self.audit_context():report=execute(self.config,self.run_id)
                self.assertEqual(report['exit_code'],5,report);publish.assert_not_called()
                journal=self.journal()
                try:
                    journal.update_file(self.run_id,self.item['file_id'],counts=original)
                    for selected in journal.selections(self.run_id):journal.update_selection(self.run_id,selected['selection_id'],state='PROFILED',error=None)
                    journal.policy(self.run_id,{'planning_incident_floor':journal.db.execute('SELECT COALESCE(MAX(id),0) FROM incidents WHERE run_id=?',(self.run_id,)).fetchone()[0]})
                finally:journal.close()


    def test_empty_or_unmatched_inventory_cannot_reconcile_clean(self):
        for run_id,complete in [('empty-open',False),('empty-complete',True)]:
            journal=self.journal()
            try:
                journal.create_run(run_id,self.config)
                if complete:journal.complete_inventory(run_id)
            finally:journal.close()
            with patch('db_ingest.publication.find_receipt') as find:
                with self.assertRaises(IngestError) as raised:reconcile(self.config,run_id)
            self.assertEqual(raised.exception.code,'RECONCILE_INVENTORY_INCOMPLETE');self.assertEqual(raised.exception.exit_code,5);find.assert_not_called()
        journal=self.journal()
        try:self.assertEqual(journal.run('empty-complete')['state'],'RECONCILIATION_FAILED')
        finally:journal.close()


    def test_reconcile_full_values_does_not_hide_pending_required_audit(self):
        self.complete_local_simulation();journal=self.journal()
        try:journal.policy(self.run_id,{'audit_state':'pending'})
        finally:journal.close()
        with patch('db_ingest.publication.find_receipt',return_value=self.receipt),patch('db_ingest.reconciliation.verify_destination',return_value={'state':'VERIFIED','assurance':'full'}):report=reconcile(self.config,self.run_id)
        self.assertEqual(report['exit_code'],3,report);self.assertEqual(report['files'][0]['destination']['state'],'VERIFIED')


    def test_missing_business_prepared_pointer_repairs_only_from_original_event(self):
        self.config['mapping']['existing_target_strategy']='mapped_business'
        metadata={'path':str(self.root/'work'/'prepared.json'),'sha256':'a'*64,'bytes':27,'run_id':self.run_id,'intent_id':self.item['intent_id'],'values':{'count':4,'sha':'b'*64}}
        self.mark_unknown();journal=self.journal()
        try:
            journal.event(self.run_id,self.item['file_id'],'BUSINESS_PREPARED',metadata)
            with patch('db_ingest.publication.resolve_publication',return_value={'state':'RESOLVED_ABSENT','proof':{}}) as resolve:
                _resolve_interrupted(journal,self.config,_plan_from_dict(self.item['plan']),journal.file(self.run_id,self.item['file_id']))
                self.assertEqual(resolve.call_args.args[0]['_business_prepared'],metadata)
                # The prepared file intentionally does not exist: repair cannot
                # mint a replacement SHA from file/current SQL contents.
                self.assertFalse(Path(metadata['path']).exists())
                _resolve_interrupted(journal,self.config,_plan_from_dict(self.item['plan']),journal.file(self.run_id,self.item['file_id']))
            self.assertEqual(journal.file(self.run_id,self.item['file_id'])['counts']['business_prepared'],metadata)
            self.assertEqual(sum(e['category']=='BUSINESS_PREPARED_POINTER_REPAIRED' for e in journal.events(self.run_id)),1)
            with journal.db:
                counts=journal.file(self.run_id,self.item['file_id'])['counts'];counts.pop('business_prepared')
                journal.db.execute('UPDATE files SET counts=? WHERE run_id=? AND file_id=?',(json.dumps(counts),self.run_id,self.item['file_id']))
                journal.db.execute("DELETE FROM events WHERE run_id=? AND category='BUSINESS_PREPARED'",(self.run_id,))
            def require_independent_seal(config,*args,**kwargs):
                self.assertNotIn('_business_prepared',config)
                raise IngestError('RECOVERY_BUSINESS_BASELINE_UNSEALED','simulated SQL absence guard',5)
            with patch('db_ingest.publication.resolve_publication',side_effect=require_independent_seal):
                with self.assertRaises(IngestError) as raised:_resolve_interrupted(journal,self.config,_plan_from_dict(self.item['plan']),journal.file(self.run_id,self.item['file_id']))
            self.assertEqual(raised.exception.exit_code,5)
        finally:journal.close()


if __name__=='__main__':unittest.main()
