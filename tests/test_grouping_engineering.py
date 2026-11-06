"""Authored local merged-source planning/provenance oracles (no SQL assertions)."""
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from dataclasses import asdict
from db_ingest.models import PublicationReceipt
from db_ingest.pipeline import execute,reconcile
from db_ingest.grouping import group,update
from db_ingest.config import load_config
from db_ingest.journal import Journal
from db_ingest.pipeline import prepare
from db_ingest.grouping import records,members


class GroupPlanningTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.root=Path(self.temp.name)
        self.source=self.root/'source';self.source.mkdir()
        self.config=load_config(environ={})
        self.config['source'].update(root=str(self.source),path=str(self.source),include=['**/*.csv'])
        self.config['preservation'].update(work_directory=str(self.root/'work'),minimum_free_disk_bytes=0)
        self.config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
        self.config['parallel']['enabled']=False
        self.config['mapping'].update(table_strategy='per_directory',name_collision='stable_hash_suffix')

    def tearDown(self):self.temp.cleanup()

    def test_strict_duplicate_headers_keep_rows_and_global_ordinals(self):
        raw_a=b'id,id\n1,same\n1,same\n';raw_b=b'id,id\n2,last\n'
        (self.source/'A.csv').write_bytes(raw_a);(self.source/'B.csv').write_bytes(raw_b)
        report=prepare(self.config)
        self.assertEqual(report['exit_code'],0,report)
        self.assertEqual(len(report['files']),2)
        self.assertEqual(report['publishable_groups'],1)
        merged=report['groups'][0]
        self.assertEqual([c['original_name'] for c in merged['plan']['columns']],['id','id'])
        self.assertNotEqual(merged['plan']['columns'][0]['target_name'],merged['plan']['columns'][1]['target_name'])
        self.assertEqual(merged['counts']['pending'],3)
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            rows=list(records(journal,report['run_id'],merged['group_id']))
            original_ids={r.record_id for f in report['files'] for r in journal.records(report['run_id'],f['file_id']) if r.kind=='data'}
            self.assertEqual({r.record_id for r in rows},original_ids)
            self.assertEqual([r.ordinal for r in rows],[2,3,5])
            self.assertEqual([[v.text for v in r.fields] for r in rows],[['1','same'],['1','same'],['2','last']])
        finally:journal.close()
        manifest=[json.loads(line) for line in Path(merged['manifest']['manifest_path']).read_text().splitlines()]
        self.assertEqual([m['relative_path'] for m in manifest],['A.csv','B.csv'])
        self.assertEqual([m['records'] for m in manifest],[2,1])
        expected=hashlib.sha256(b'db_ingest:group-sources:v1\x00')
        for path,raw in [('A.csv',raw_a),('B.csv',raw_b)]:
            for value in (path.encode(),hashlib.sha256(raw).hexdigest().encode()):expected.update(len(value).to_bytes(8,'big'));expected.update(value)
            expected.update(len(raw).to_bytes(8,'big'))
        self.assertEqual(merged['plan']['source_hash'],expected.hexdigest())

    def test_union_reorders_by_unique_names_and_preserves_missing_vs_null(self):
        self.config['mapping']['directory_merge_schema']='union_by_name'
        self.config['parsing']['null_tokens']=['NULL']
        (self.source/'A.csv').write_bytes(b'id,note\n1,NULL\n')
        (self.source/'B.csv').write_bytes(b'extra,id\nx,2\n')
        report=prepare(self.config);self.assertEqual(report['exit_code'],0,report)
        merged=report['groups'][0]
        self.assertEqual([c['original_name'] for c in merged['plan']['columns']],['extra','id','note'])
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            rows=list(records(journal,report['run_id'],merged['group_id']))
            self.assertEqual([[f.state for f in r.fields] for r in rows],[['MISSING','VALUE','NULL'],['VALUE','VALUE','MISSING']])
            self.assertEqual([[f.text for f in r.fields] for r in rows],[[None,'1',None],['x','2',None]])
            mapping=list(members(journal,report['run_id'],merged['group_id']))
            self.assertEqual([m['mapping'] for m in mapping],[[1,2],[0,1]])
        finally:journal.close()
        self.assertTrue(all(c['stats']['missing_policy']=='null_with_provenance' for c in merged['plan']['columns']))

    def test_strict_schema_difference_and_ambiguous_union_stop(self):
        (self.source/'A.csv').write_bytes(b'id,name\n1,a\n')
        (self.source/'B.csv').write_bytes(b'name,id\nb,2\n')
        report=prepare(self.config)
        self.assertEqual(report['exit_code'],2,report)
        self.assertEqual(report['publishable_groups'],0)
        self.assertTrue(any(e['code']=='GROUP_SCHEMA_MISMATCH' for e in report['errors']))
        self.config['mapping']['directory_merge_schema']='union_by_name'
        (self.source/'A.csv').write_bytes(b'id,id\n1,a\n')
        report=prepare(self.config)
        self.assertEqual(report['exit_code'],2,report)
        self.assertTrue(any(e['code']=='GROUP_COLUMN_AMBIGUOUS' for e in report['errors']))

    def test_unresolved_member_blocks_whole_group(self):
        (self.source/'A.csv').write_bytes(b'id,name\n1,a\n')
        (self.source/'B.csv').write_bytes(b'id,name\n2,"unclosed\n')
        report=prepare(self.config)
        self.assertEqual(report['exit_code'],5,report)
        self.assertEqual(report['publishable_files'],0)
        self.assertEqual(report['publishable_groups'],0)
        self.assertFalse(report['global_publication_blocked'])
        self.assertEqual(report['files'][0]['counts']['committed'],0)


if __name__=='__main__':unittest.main()


class GroupPublicationLocalTests(GroupPlanningTests):
    """Coordinator/ACK crash safety only; transport is deliberately simulated."""
    def setUp(self):
        super().setUp()
        history=patch('db_ingest.adapters.mssql.contributors.mirror_destination_contributions',return_value={'state':'VERIFIED'})
        history.start();self.addCleanup(history.stop) #This class simulates SQL publication/metadata; independent live oracles exercise both.

    def _planned(self):
        self.config['audit']['mirror_receipts_after_commit']=False
        (self.source/'A.csv').write_bytes(b'id,name\n1,same\n1,same\n')
        (self.source/'B.csv').write_bytes(b'id,name\n2,last\n')
        report=prepare(self.config)
        self.assertEqual(report['exit_code'],0,report)
        return report

    def _fake_publish(self,config,plan,record_factory,intent_id,run_id,*args):
        rows=list(record_factory());self.assertEqual([r.ordinal for r in rows],[2,3,5])
        return PublicationReceipt(intent_id,run_id,plan.database,plan.schema,plan.table,plan.source_hash,plan.interpretation_hash,plan.mapping_hash,len(rows),'a'*64,'full','2026-10-29T00:00:00Z',{'source_group':config['_publication_group']})

    def test_atomic_group_receipt_real_source_counts_and_idempotent_resume(self):
        plan=self._planned()
        with patch('db_ingest.publication.publish',side_effect=self._fake_publish) as publish:
            report=execute(self.config,plan['run_id'])
        self.assertEqual(report['exit_code'],0,report)
        self.assertEqual(publish.call_count,1)
        self.assertEqual([f['counts']['committed'] for f in report['files']],[2,1])
        merged=report['groups'][0]
        self.assertEqual(merged['counts']['committed'],3)
        self.assertEqual(merged['receipt']['committed_records'],3)
        self.assertTrue(all(f['receipt']['kind']=='group_member' for f in report['files']))
        with patch('db_ingest.publication.resolve_publication',return_value={'state':'COMMITTED','receipt':merged['receipt']}),patch('db_ingest.publication.publish') as publish:
            resumed=execute(self.config,plan['run_id'])
        self.assertEqual(resumed['exit_code'],0,resumed);publish.assert_not_called()
        self.assertEqual([f['counts']['committed'] for f in resumed['files']],[2,1])

    def test_ack_before_member_propagation_recovers_without_republish(self):
        plan=self._planned();gid=plan['groups'][0]['group_id']
        journal=Journal(self.config['preservation']['work_directory'])
        receipt=asdict(self._fake_publish({**self.config,'_publication_group':plan['groups'][0]['manifest']},__import__('db_ingest.grouping',fromlist=['_plan'])._plan(plan['groups'][0]['plan']),lambda:records(journal,plan['run_id'],gid),plan['groups'][0]['intent_id'],plan['run_id']))
        update(journal,plan['run_id'],gid,state='COMMITTED',receipt=receipt);journal.close()
        with patch('db_ingest.publication.resolve_publication',return_value={'state':'COMMITTED','receipt':receipt}),patch('db_ingest.publication.publish') as publish:
            resumed=execute(self.config,plan['run_id'])
        self.assertEqual(resumed['exit_code'],0,resumed);publish.assert_not_called()
        self.assertEqual([f['counts']['committed'] for f in resumed['files']],[2,1])


    def test_union_selected_subset_retains_acknowledged_nullable_schema_and_missing(self):
        self.config['mapping']['directory_merge_schema']='union_by_name'
        self.config['audit']['mirror_receipts_after_commit']=False
        (self.source/'A.csv').write_bytes(b'id,note\n1,a\n')
        (self.source/'B.csv').write_bytes(b'extra,id\nlong-value,2\n')
        planned=prepare(self.config)
        def fake(config,plan,record_factory,intent_id,run_id,*args):
            rows=list(record_factory())
            return PublicationReceipt(intent_id,run_id,plan.database,plan.schema,plan.table,plan.source_hash,plan.interpretation_hash,plan.mapping_hash,len(rows),'a'*64,'full','2026-10-29T00:00:00Z',{'source_group':config['_publication_group']})
        with patch('db_ingest.publication.publish',side_effect=fake):published=execute(self.config,planned['run_id'])
        self.assertEqual(published['exit_code'],0,published)
        self.config['source']['include']=['A.csv']
        selected=prepare(self.config,new_intent=True)
        self.assertEqual(selected['exit_code'],0,selected)
        old=published['groups'][0]['plan']['columns'];new=selected['groups'][0]['plan']['columns']
        self.assertEqual([(c['target_name'],c['sql_type'],c['nullable']) for c in new],[(c['target_name'],c['sql_type'],c['nullable']) for c in old])
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            values=list(records(journal,selected['run_id'],selected['groups'][0]['group_id']))
            self.assertEqual(values[0].fields[0].state,'MISSING')
            self.assertTrue(new[0]['nullable']);self.assertEqual(new[0]['stats']['missing_policy'],'null_with_provenance')
        finally:journal.close()


    def test_local_ack_manifests_rebuild_deleted_contributor_cache_and_detect_tamper(self):
        from db_ingest.group_execution import _required_history
        from db_ingest.contributors import iter_manifest
        planned=self._planned()
        with patch('db_ingest.publication.publish',side_effect=self._fake_publish):published=execute(self.config,planned['run_id'])
        self.assertEqual(published['exit_code'],0,published)
        self.config['source']['include']=['A.csv']
        selected=prepare(self.config,new_intent=True);gid=selected['groups'][0]['group_id']
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            with journal.db:journal.db.execute('DELETE FROM group_contributors WHERE group_id=?',(gid,))
            required=_required_history(journal,group(journal,selected['run_id'],gid))
            metadata={'manifest_path':required['required_manifest_path'],'manifest_sha256':required['required_manifest_sha256'],'contributor_count':required['required_contributor_count']}
            self.assertEqual([row['relative_path'] for row in iter_manifest(metadata)],['A.csv','B.csv'])
            path=Path(published['groups'][0]['manifest']['manifest_path'])
            path.write_bytes(path.read_bytes().replace(b'B.csv',b'C.csv'))
            with self.assertRaises(__import__('db_ingest.errors',fromlist=['IngestError']).IngestError) as raised:_required_history(journal,group(journal,selected['run_id'],gid))
            self.assertEqual(raised.exception.exit_code,5)
        finally:journal.close()


    def test_all_rejected_group_materializes_source_failures_without_fake_receipt(self):
        self.config['audit']['mirror_receipts_after_commit']=False
        self.config['schema']['on_length_overflow']='quarantine'
        self.config['columns']=[{'source_ordinal':1,'target_type':'nvarchar(1)'}]
        (self.source/'A.csv').write_bytes(b'text\nlong\n')
        (self.source/'B.csv').write_bytes(b'text\nlonger\n')
        planned=prepare(self.config);self.assertEqual(planned['exit_code'],0,planned)
        names=[]
        def materialize(*args,**kwargs):names.append(kwargs['table_name']);return {'state':'VERIFIED','rejected_records':1}
        with patch('db_ingest.adapters.mssql.control.materialize_failures',side_effect=materialize),patch('db_ingest.publication.publish') as publish:
            first=execute(self.config,planned['run_id']);again=execute(self.config,planned['run_id'])
        self.assertEqual(first['exit_code'],3,first);self.assertEqual(again['exit_code'],3,again);publish.assert_not_called()
        self.assertIsNone(first['groups'][0]['receipt']);self.assertTrue(all(f['receipt'] is None for f in first['files']))
        self.assertEqual(len(names),4);self.assertEqual(len(set(names)),2)
        checked=reconcile(self.config,planned['run_id'])
        self.assertEqual(checked['exit_code'],3,checked);self.assertEqual(checked['groups'][0]['destination']['state'],'NO_PUBLICATION_ALL_RECORDS_REJECTED')
