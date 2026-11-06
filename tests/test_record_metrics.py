"""Retained-frame, parsed-value and SQL-acknowledged counts have distinct meaning."""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from db_ingest.config import load_config
from db_ingest.pipeline import prepare

class RecordMetricTests(unittest.TestCase):
    def run_data(self,raw):
        root=Path(self.temp.name);source=root/'source';source.mkdir(exist_ok=True);(source/'rows.csv').write_bytes(raw)
        config=load_config(environ={});config['parallel']['enabled']=False
        config['source'].update(root=str(source),path=str(source),include=['**/*.csv'])
        config['preservation'].update(work_directory=str(root/'work'),minimum_free_disk_bytes=0)
        config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
        config['columns']=[{'source_ordinal':1,'target_type':'int'},{'source_ordinal':2,'target_type':'nvarchar(20)'}]
        return prepare(config)
    def setUp(self):self.temp=TemporaryDirectory()
    def tearDown(self):self.temp.cleanup()
    def test_blank_ragged_invalid_value_and_error_events_are_not_conflated(self):
        report=self.run_data(b'id,note\n1,good\n\n2,too,many\nbad,value\n');self.assertEqual(report['exit_code'],0,report)
        counts=report['files'][0]['counts']
        self.assertEqual(counts['known_data_records'],4)
        self.assertEqual(counts['blank_records'],1);self.assertEqual(counts['parsed_data_records'],3)
        self.assertEqual(counts['header_records'],1);self.assertEqual(counts['record_error_events'],3)
        self.assertEqual(counts['conversion_validated_records'],1)
        self.assertIsNone(counts['staged_records']);self.assertIsNone(counts['validated_records'])
        self.assertIsNone(counts['sql_staged_records']);self.assertIsNone(counts['sql_validated_records'])
    def test_unresolved_tail_exposes_known_prefix_and_leaves_totals_unknown(self):
        report=self.run_data(b'id,note\n1,good\n\n2,too,many\nbad,value\n3,"unterminated\n');self.assertEqual(report['exit_code'],5,report)
        counts=report['files'][0]['counts']
        self.assertIsNone(counts['blank_records']);self.assertIsNone(counts['parsed_data_records'])
        self.assertEqual(counts['known_blank_records'],1);self.assertEqual(counts['known_parsed_data_records'],3)
        self.assertEqual(counts['record_error_events'],4);self.assertEqual(counts['conversion_validated_records'],1)
        self.assertIsNone(counts['staged_records']);self.assertIsNone(counts['validated_records'])
        self.assertIsNone(counts['sql_staged_records']);self.assertIsNone(counts['sql_validated_records'])
