"""Authored physical/logical/count/byte-coverage evidence, independent of parser output."""
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest import pipeline

class IndependentLocalEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name);self.source=self.root/'input';self.source.mkdir()
        self.config=load_config(environ={})
        self.config['source'].update(root=str(self.source),path=str(self.source),include=['**/*.csv'])
        self.config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
        self.config['preservation'].update(work_directory=str(self.root/'work'),minimum_free_disk_bytes=0)
    def tearDown(self):self.temp.cleanup()
    def evidence(self,raw):
        (self.source/'data.csv').write_bytes(raw)
        result=pipeline.prepare(self.config)
        journal=Journal(self.root/'work')
        try:
            item=next(journal.files(result['run_id']))
            return result,pipeline.local_evidence(journal,result['run_id'],item['file_id'],item['artifact'],item['decision'])
        finally:journal.close()
    def test_mixed_line_endings_multiline_unterminated_final_line(self):
        raw=b'a,b\r\n1,"x\r\ny"\r2,z\n3,last'
        result,counts=self.evidence(raw)
        self.assertEqual(result['exit_code'],0)
        self.assertEqual(counts['source_bytes'],len(raw))
        self.assertEqual(counts['physical_line_breaks'],4)
        self.assertEqual(counts['physical_lines'],5)
        self.assertEqual(counts['logical_records'],4)
        self.assertEqual(counts['data_records'],3)
        self.assertEqual(counts['header'],1)
        self.assertEqual(counts['pending'],3)
        self.assertEqual(counts['byte_coverage'],'complete contiguous spans plus BOM')
    def test_unclosed_span_never_fabricates_logical_or_data_total(self):
        result,counts=self.evidence(b'a,b\n1,ok\n2,"broken\n3,inside\n')
        self.assertEqual(result['exit_code'],5)
        self.assertIsNone(counts['logical_records'])
        self.assertIsNone(counts['data_records'])
        self.assertEqual(counts['known_data_records'],1)
        self.assertEqual(counts['physical_lines'],4)
        self.assertEqual(counts['physical_line_breaks'],4)
        self.assertEqual(counts['unresolved'],1)
        self.assertEqual(counts['pending'],1)
    def test_crlf_split_across_decode_blocks_counted_once(self):
        raw=b'a,b\n1,'+b'x'*65529+b'\r\n2,ok\n'
        self.assertEqual(raw[65535:65537],b'\r\n')
        _,counts=self.evidence(raw)
        self.assertEqual(counts['physical_line_breaks'],3)
        self.assertEqual(counts['physical_lines'],3)
        self.assertEqual(counts['logical_records'],3)
        self.assertEqual(counts['data_records'],2)
    def test_utf16_bom_excluded_from_physical_lines_and_covered(self):
        self.config['parsing']['encoding']='auto'
        raw='a,b\r\n1,\U0001f600\r\n'.encode('utf-16')
        _,counts=self.evidence(raw)
        self.assertEqual(counts['physical_line_breaks'],2)
        self.assertEqual(counts['physical_lines'],2)
        self.assertEqual(counts['logical_records'],2)
        self.assertEqual(counts['data_records'],1)
        self.assertEqual(counts['source_bytes'],len(raw))
    def test_corrupted_journal_span_is_reconciliation_failure(self):
        (self.source/'data.csv').write_bytes(b'a,b\n1,ok\n')
        result=pipeline.prepare(self.config)
        journal=Journal(self.root/'work')
        try:
            item=next(journal.files(result['run_id']))
            record=next(journal.records(result['run_id'],item['file_id'],'pending'))
            record.byte_start+=1
            journal.set_disposition(result['run_id'],item['file_id'],record.record_id,'pending',record)
            with self.assertRaises(IngestError) as cm:
                pipeline.local_evidence(journal,result['run_id'],item['file_id'],item['artifact'],item['decision'])
            self.assertEqual(cm.exception.code,'BYTE_COVERAGE_MISMATCH')
            self.assertEqual(cm.exception.exit_code,5)
        finally:journal.close()
if __name__=='__main__':unittest.main()
