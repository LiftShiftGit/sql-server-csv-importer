"""Independent verifier policy tests with explicitly labeled fake SQL responses."""
from dataclasses import asdict
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from db_ingest.config import load_config
from db_ingest.fixturegen import create_test_files,verify_imported_fixtures
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.models import PublicationReceipt
from db_ingest import pipeline

class IndependentSqlFixtureGuardTests(unittest.TestCase):
    def test_production_destination_rejected_before_connect(self):
        with patch('db_ingest.adapters.mssql.connect.connect') as connect:
            with self.assertRaises(IngestError) as cm:
                verify_imported_fixtures('does-not-exist.json',{},'some-run','Production',None,None)
        self.assertEqual(cm.exception.code,'FIXTURE_DESTINATION_REQUIRED')
        connect.assert_not_called()
    def test_empty_manifest_never_verified(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/'manifest.json'
            path.write_text(json.dumps({'format':'db_ingest_fixture_manifest','version':1,'fixtures':[]}),encoding='utf-8')
            with patch('db_ingest.adapters.mssql.connect.connect') as connect:
                with self.assertRaises(IngestError) as cm:
                    verify_imported_fixtures(path,{},'some-run','IngestFixture_empty')
            self.assertEqual(cm.exception.code,'FIXTURE_MANIFEST_EMPTY')
            connect.assert_not_called()
    def test_missing_local_capture_rejected_before_connect(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);fixtures=root/'fixtures'
            create_test_files(fixtures,'csv')
            config=load_config(environ={});config['preservation']['work_directory']=str(root/'work')
            journal=Journal(root/'work');journal.create_run('run',config);journal.close()
            with patch('db_ingest.adapters.mssql.connect.connect') as connect:
                result=verify_imported_fixtures(fixtures/'manifest.json',config,'run','IngestFixture_test')
            self.assertEqual(result['errors'][0]['code'],'CAPTURE_MISSING')
            connect.assert_not_called()
    def test_authored_vertical_slice_independent_fake_sql_values(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);fixtures=root/'fixtures'
            manifest=create_test_files(fixtures,'vertical_slice')
            config=load_config(environ={})
            config['destination']['database']='IngestFixture_unit'
            config['source'].update(root=str(fixtures),path=str(fixtures),include=['**/*.csv'])
            config['parsing'].update(header='present',delimiter=',',encoding='utf-8',null_tokens=['NULL'])
            config['file_rules']=[{'glob':'vertical_slice.csv','columns':manifest['fixtures'][0]['sql_schema']}]
            config['preservation'].update(work_directory=str(root/'work'),minimum_free_disk_bytes=0)
            plan=pipeline.prepare(config)
            self.assertEqual(plan['exit_code'],0,plan)
            run_id=plan['run_id'];item=plan['files'][0];schema=item['plan']
            receipt=asdict(PublicationReceipt(item['intent_id'],run_id,schema['database'],schema['schema'],schema['table'],schema['source_hash'],schema['interpretation_hash'],schema['mapping_hash'],2,'fake-digest','full','2026-10-29T00:00:00Z',{'control_mirror_state':'mirrored','recovery_fork_guid':'fork-test'}))
            journal=Journal(root/'work')
            try:
                ids={r.ordinal:r.record_id for r in journal.records(run_id,item['file_id'],'pending')}
                journal.committed(run_id,item['file_id'])
                journal.update_file(run_id,item['file_id'],state='RECONCILED',receipt=receipt)
            finally:journal.close()
            # Authored typed expectation, not computed with importer conversion/parser.
            values=['001','\U0001f600 trailing  ','one,\r\ntwo',None,'','NULL',Decimal('123456789012345.1234'),'2024-02-29','2026-10-29T12:34:56.1234567','2026-10-29T12:34:56.1234567+05:30',b'\x00\x01\x02\xff']
            class Cursor:
                rows=[]
                def execute(self,sql,*params):
                    if sql=='SELECT DB_NAME()':self.rows=[('IngestFixture_unit',)]
                    elif sql.startswith('SELECT [payload]'):self.rows=[(json.dumps(receipt),123,'guid-test')]
                    elif sql.startswith('SELECT OBJECT_ID'):self.rows=[(123,'guid-test','fork-test')]
                    elif sql.startswith('SELECT c.name,t.name'):
                        self.rows=[('code','nvarchar',20,0,0,True),('text1','nvarchar',160,0,0,True),('text2','nvarchar',160,0,0,True),('null_value','nvarchar',40,0,0,True),('empty','nvarchar',20,0,0,True),('literal','nvarchar',20,0,0,True),('amount','decimal',9,19,4,True),('on_date','date',3,0,0,True),('occurred_at','datetime2',8,0,7,True),('offset_at','datetimeoffset',10,0,7,True),('attachment','varbinary',-1,0,0,True)]
                    elif sql.startswith('SELECT [__ingest_record]'):self.rows=[(ids[o],o,*values) for o in (2,3)]
                    else:raise AssertionError(sql)
                    return self
                def __iter__(self):return iter(self.rows)
                def fetchone(self):return self.rows.pop(0) if self.rows else None
            class Connection:
                def cursor(self):return Cursor()
                def rollback(self):pass
                def close(self):pass
            with patch('db_ingest.adapters.mssql.connect.connect',return_value=Connection()) as connect:
                result=verify_imported_fixtures(fixtures/'manifest.json',config,run_id,'IngestFixture_unit','test','test-secret')
            self.assertEqual(result['errors'],[],result)
            self.assertEqual(result['state'],'FIXTURE_VERIFIED')
            self.assertEqual(result['exit_code'],0)
            connect.assert_called_once_with(config,'test','test-secret',database='IngestFixture_unit')

if __name__=='__main__':unittest.main()
