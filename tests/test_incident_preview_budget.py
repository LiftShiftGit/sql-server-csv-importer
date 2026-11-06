"""Console incident previews must never materialize giant schema contexts."""
import json
from tempfile import TemporaryDirectory
from pathlib import Path
import unittest
from db_ingest.journal import Journal
from db_ingest.errors import IngestError

class IncidentPreviewBudgetTests(unittest.TestCase):
    def test_large_schema_context_and_messages_stay_complete_in_sqlite_not_console(self):
        with TemporaryDirectory() as folder:
            journal=Journal(Path(folder)/'work');journal.create_run('run',{'limits':{'max_queue_bytes':4096}})
            try:
                journal.incident('run','file',IngestError('SCHEMA_DRIFT','schema differs',2,{'schema_diff':{'inspection':'x'*16777216}}))
                journal.incident('run','file',IngestError('LARGE_MESSAGE','😀'*20000,4,{'small':'context'}))
                preview=journal.errors('run',1000)
                self.assertLessEqual(len(json.dumps(preview,ensure_ascii=True).encode('ascii')),4096)
                large=[item for item in preview if item['code']=='SCHEMA_DRIFT'][0]
                self.assertTrue(large['context_truncated']);self.assertNotIn('schema_diff',large['context'])
                self.assertTrue(large['context']['retained_in_journal'])
                self.assertEqual(journal.db.execute("SELECT json_extract(context,'$.schema_diff.inspection') IS NOT NULL,length(json_extract(context,'$.schema_diff.inspection')) FROM incidents WHERE code='SCHEMA_DRIFT'").fetchone()[1],16777216)
                message=[item for item in preview if item['code']=='LARGE_MESSAGE'][0];self.assertTrue(message['message_truncated'])
                self.assertEqual(journal.db.execute("SELECT length(message) FROM incidents WHERE code='LARGE_MESSAGE'").fetchone()[0],20000)
            finally:journal.close()
    def test_remaining_budget_and_small_full_context_are_respected(self):
        with TemporaryDirectory() as folder:
            journal=Journal(Path(folder)/'work');journal.create_run('run',{'limits':{'max_queue_bytes':4096}})
            try:
                for index in range(100):journal.incident('run','f',IngestError('TEST','message',2,{'index':index,'padding':'x'*500}))
                preview=journal.errors('run',1000)
                self.assertLessEqual(len(json.dumps(preview,ensure_ascii=True).encode('ascii')),4096)
                self.assertGreater(len(preview),0);self.assertLess(len(preview),100)
                self.assertEqual(preview[0]['context']['index'],99)
                self.assertTrue(preview[-1]['additional_incidents_retained'])
                self.assertEqual(journal.db.execute('SELECT COUNT(*) FROM incidents').fetchone()[0],100)
            finally:journal.close()
