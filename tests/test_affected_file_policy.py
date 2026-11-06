"""Affected-file coordinator simulations; real SQL oracles are separate."""
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from db_ingest.config import load_config
from db_ingest.models import PublicationReceipt
from db_ingest.pipeline import prepare,execute


class AffectedFilePolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.root=Path(self.temp.name)
        self.source=self.root/'source';self.source.mkdir()
        self.config=load_config(environ={})
        self.config['source'].update(root=str(self.source),path=str(self.source),include=['**/*.csv'])
        self.config['preservation'].update(work_directory=str(self.root/'work'),minimum_free_disk_bytes=0)
        self.config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
        self.config['parallel']['enabled']=False
        (self.source/'good.csv').write_bytes(b'id,text\n1,ok\n')

    def tearDown(self):self.temp.cleanup()

    def receipt(self,item,run_id):
        p=item['plan']
        return asdict(PublicationReceipt(item['intent_id'],run_id,p['database'],p['schema'],p['table'],p['source_hash'],p['interpretation_hash'],p['mapping_hash'],1,'simulated-verified-digest','full','2026-10-29T00:00:00Z',{'control_mirror_state':'VERIFIED'}))

    def test_unresolved_file_does_not_publish_but_good_file_can(self):
        bad=b'id,text\n1,known\n2,"unclosed\n'
        (self.source/'bad.csv').write_bytes(bad)
        prepared=prepare(self.config)
        self.assertEqual(prepared['exit_code'],5,prepared)
        self.assertFalse(prepared['global_publication_blocked'])
        self.assertEqual(prepared['publishable_files'],1)
        good=next(i for i in prepared['files'] if i['artifact']['relative_path']=='good.csv')
        with patch('db_ingest.publication.publish',return_value=self.receipt(good,prepared['run_id'])) as publish,patch('db_ingest.pipeline._audit_backfill',side_effect=lambda j,c,p,r,*a,**k:r),patch('db_ingest.adapters.mssql.control.mirror_run_audit',return_value={'state':'VERIFIED'}):
            result=execute(self.config,prepared['run_id'],'user','secret')
        self.assertEqual(result['exit_code'],5,result)
        self.assertEqual(result['state'],'RECONCILIATION_FAILED')
        publish.assert_called_once()
        self.assertEqual(publish.call_args.args[1].table,good['plan']['table'])
        by_name={i['artifact']['relative_path']:i for i in result['files']}
        self.assertEqual(by_name['good.csv']['counts']['committed'],1)
        self.assertEqual(by_name['bad.csv']['counts']['committed'],0)
        self.assertEqual(by_name['bad.csv']['counts']['unresolved'],1)
        self.assertEqual(Path(by_name['bad.csv']['artifact']['path']).read_bytes(),bad)

    def test_corrupt_other_ready_archive_globally_blocks_before_good_publication(self):
        (self.source/'other.csv').write_bytes(b'id,text\n2,also_ok\n')
        prepared=prepare(self.config);self.assertEqual(prepared['exit_code'],0,prepared)
        other=next(i for i in prepared['files'] if i['artifact']['relative_path']=='other.csv')
        Path(other['artifact']['path']).write_bytes(b'corrupt captured original')
        with patch('db_ingest.publication.publish') as publish,patch('db_ingest.adapters.mssql.control.mirror_run_audit',return_value={'state':'VERIFIED'}):
            result=execute(self.config,prepared['run_id'],'user','secret')
        self.assertEqual(result['exit_code'],5,result)
        self.assertTrue(result['global_publication_blocked'])
        publish.assert_not_called()

    def test_encoding_failure_is_affected_only(self):
        (self.source/'bad.csv').write_bytes(b'id,text\n1,\xff\n')
        result=prepare(self.config)
        self.assertEqual(result['exit_code'],2,result)
        self.assertFalse(result['global_publication_blocked'])
        self.assertEqual(result['publishable_files'],1)
        self.assertTrue(any(e['code']=='ENCODING_DECISION' for e in result['errors']))

    def test_capture_failure_is_global_and_never_publishes_good_prefix(self):
        from db_ingest.capture import capture_source
        from db_ingest.errors import IngestError
        (self.source/'bad.csv').write_bytes(b'id,text\n1,other\n')
        def capture(path,*args,**kwargs):
            if Path(path).name=='bad.csv':raise IngestError('CAPTURE_IO','simulated source capture outage',4)
            return capture_source(path,*args,**kwargs)
        with patch('db_ingest.capture.capture_source',side_effect=capture):
            result=prepare(self.config)
            self.assertEqual(result['exit_code'],4,result)
            self.assertTrue(result['global_publication_blocked'])
            self.assertEqual(result['publishable_files'],1)
            with patch('db_ingest.publication.publish') as publish,patch('db_ingest.adapters.mssql.control.mirror_run_audit',return_value={'state':'VERIFIED'}):
                resumed=execute(self.config,result['run_id'],'user','secret')
            self.assertEqual(resumed['exit_code'],4,resumed)
            publish.assert_not_called()


if __name__=='__main__':unittest.main()
