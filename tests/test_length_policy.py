"""Lossless full-stream length policy oracles; no SQL transport assertions."""
from dataclasses import asdict
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from db_ingest.config import load_config
from db_ingest.pipeline import prepare
from db_ingest.journal import Journal
from db_ingest.adapters.mssql.types import convert_field
from db_ingest.models import ColumnProfile

class LengthPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.root=Path(self.temp.name);self.source=self.root/'source';self.source.mkdir()
        self.config=load_config(environ={});self.config['parallel']['enabled']=False
        self.config['source'].update(root=str(self.source),path=str(self.source),include=['**/*.csv'])
        self.config['preservation'].update(work_directory=str(self.root/'work'),minimum_free_disk_bytes=0)
        self.config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
        self.config['schema'].update(on_length_overflow='widen',text_length_headroom_percent=20)
    def tearDown(self):self.temp.cleanup()
    def run_source(self,raw,columns):
        (self.source/'input.csv').write_bytes(raw);self.config['columns']=columns
        report=prepare(self.config);journal=Journal(self.config['preservation']['work_directory'])
        try:
            file=journal.file(report['run_id'],report['files'][0]['file_id'])
            rows=list(journal.records(report['run_id'],file['file_id']))
            events=[json.loads(r[0]) for r in journal.db.execute("SELECT details FROM events WHERE run_id=? AND category='SCHEMA_LENGTH_POLICY' ORDER BY id",(report['run_id'],))]
        finally:journal.close()
        return report,file,[r for r in rows if r.kind=='data'],events
    def test_widen_counts_utf16_encoded_and_decoded_units_without_cell_changes(self):
        raw='unicode,encoded,binary\na,ab,YQ==\n😀😀,éé,YWJjZA==\n'.encode()
        report,file,rows,events=self.run_source(raw,[{'source_ordinal':1,'target_type':'nvarchar(2)'},{'source_ordinal':2,'target_type':'varchar(2)','text_encoding':'utf-8','collation':'Latin1_General_100_BIN2_UTF8'},{'source_ordinal':3,'target_type':'varbinary(2)','binary_encoding':'base64'}])
        self.assertEqual(report['exit_code'],0,report)
        columns=[ColumnProfile(**v) for v in file['plan']['columns']]
        self.assertEqual([v.sql_type for v in columns],['nvarchar(5)','varchar(5)','varbinary(5)'])
        self.assertEqual([[f.text for f in r.fields] for r in rows],[['a','ab','YQ=='],['😀😀','éé','YWJjZA==']])
        self.assertEqual([convert_field(f,c) for f,c in zip(rows[1].fields,columns)],['😀😀','éé',b'abcd'])
        self.assertEqual({e['unit'] for e in events},{'utf16_units','encoded_bytes','decoded_bytes'})
        self.assertEqual(file['counts']['rejected'],0);self.assertEqual(len(events),3)
        self.assertEqual(Path(file['artifact']['path']).read_bytes(),raw)
    def test_retype_binary_retains_lexical_and_invalid_later_base64_stays_rejected(self):
        self.config['schema']['on_length_overflow']='retype_text'
        raw=b'blob\nYQ==\nYWJjZA==\nnot-base64\n'
        report,file,rows,events=self.run_source(raw,[{'source_ordinal':1,'target_type':'varbinary(2)','binary_encoding':'base64'}])
        self.assertEqual(report['exit_code'],0,report)
        column=ColumnProfile(**file['plan']['columns'][0]);self.assertEqual(column.sql_type,'nvarchar(10)')
        self.assertEqual(column.stats['source_declared_type'],'varbinary(2)')
        self.assertEqual(column.stats['source_content_encoding'],'base64')
        self.assertEqual(column.stats['length_policy_representation'],'source_lexical')
        self.assertEqual([r.fields[0].text for r in rows],['YQ==','YWJjZA==','not-base64'])
        self.assertEqual(file['counts']['pending'],2);self.assertEqual(file['counts']['rejected'],1)
        self.assertEqual(len(events),1)
        self.assertEqual(convert_field(rows[1].fields[0],column),'YWJjZA==')
    def test_invalid_varchar_encoding_is_not_fixed_by_retype(self):
        self.config['schema']['on_length_overflow']='retype_text'
        report,file,rows,events=self.run_source('text\nabc\n😀\n'.encode(),[{'source_ordinal':1,'target_type':'varchar(1)','text_encoding':'cp1252','collation':'Latin1_General_100_BIN2'}])
        self.assertEqual(report['exit_code'],0,report)
        self.assertEqual(file['counts']['pending'],1);self.assertEqual(file['counts']['rejected'],1)
        self.assertEqual(len(events),1);self.assertEqual(file['plan']['columns'][0]['sql_type'],'nvarchar(4)')
    def test_explicit_right_projection_takes_priority_over_widen(self):
        self.config['schema']['allow_lossy_projection']=True
        report,file,rows,events=self.run_source(b'text\nabcdef\n',[{'source_ordinal':1,'target_type':'nvarchar(2)','truncation':'right'}])
        self.assertEqual(report['exit_code'],0,report)
        self.assertEqual(file['plan']['columns'][0]['sql_type'],'nvarchar(2)')
        self.assertEqual(rows[0].fields[0].text,'ab');self.assertEqual(events,[])
    def test_max_promotion_and_disabled_max_are_explicit(self):
        report,file,rows,events=self.run_source(('text\n'+'x'*4001+'\n').encode(),[{'source_ordinal':1,'target_type':'nvarchar(2)'}])
        self.assertEqual(report['exit_code'],0,report);self.assertEqual(file['plan']['columns'][0]['sql_type'],'nvarchar(max)')
        self.config['schema']['allow_max_types']=False
        report,file,rows,events=self.run_source(('text\n'+'x'*4001+'\n').encode(),[{'source_ordinal':1,'target_type':'nvarchar(2)'}])
        self.assertEqual(report['exit_code'],2,report)
        self.assertTrue(any(e['code']=='TEXT_REPRESENTATION_LIMIT' for e in report['errors']))
        self.assertEqual(Path(file['artifact']['path']).read_bytes(),('text\n'+'x'*4001+'\n').encode())

    def test_group_widen_unifies_same_declared_lengths_and_binary_fallback_keeps_all_lexical(self):
        self.config['mapping'].update(table_strategy='per_directory',name_collision='stable_hash_suffix')
        self.config['columns']=[{'source_ordinal':1,'target_type':'nvarchar(2)'}]
        (self.source/'A.csv').write_bytes(b'text\na\n')
        (self.source/'B.csv').write_bytes(b'text\nabcdef\n')
        report=prepare(self.config)
        self.assertEqual(report['exit_code'],0,report)
        self.assertEqual(report['groups'][0]['plan']['columns'][0]['sql_type'],'nvarchar(8)')
        self.config['schema']['on_length_overflow']='retype_text'
        self.config['columns']=[{'source_ordinal':1,'target_type':'varbinary(2)','binary_encoding':'base64'}]
        (self.source/'A.csv').write_bytes(b'blob\nYQ==\n')
        (self.source/'B.csv').write_bytes(b'blob\nYWJjZA==\n')
        report=prepare(self.config)
        self.assertEqual(report['exit_code'],0,report)
        self.assertEqual(report['groups'][0]['plan']['columns'][0]['sql_type'],'nvarchar(10)')
        from db_ingest.grouping import records
        journal=Journal(self.config['preservation']['work_directory'])
        try:self.assertEqual([r.fields[0].text for r in records(journal,report['run_id'],report['groups'][0]['group_id'])],['YQ==','YWJjZA=='])
        finally:journal.close()

    def test_empty_source_and_blank_header_cannot_authorize_replace(self):
        from unittest.mock import patch
        from db_ingest.pipeline import execute
        self.config['load'].update(mode='replace',allow_empty_replace=True)
        for raw in (b'',b'\n'):
            with self.subTest(raw=raw):
                report,file,rows,events=self.run_source(raw,[])
                self.assertNotEqual(report['exit_code'],0,report)
                with patch('db_ingest.publication.publish') as publish:
                    executed=execute(self.config,report['run_id'])
                self.assertNotEqual(executed['exit_code'],0,executed);publish.assert_not_called()
                self.assertEqual(Path(file['artifact']['path']).read_bytes(),raw)

    def test_header_only_replace_requires_explicit_allow_empty(self):
        from unittest.mock import patch
        from db_ingest.pipeline import execute
        self.config['load'].update(mode='replace',allow_empty_replace=False)
        report,file,rows,events=self.run_source(b'id\n',[])
        self.assertEqual(report['exit_code'],0,report)
        with patch('db_ingest.publication.publish') as publish:executed=execute(self.config,report['run_id'])
        self.assertEqual(executed['exit_code'],2,executed);publish.assert_not_called()
        self.assertTrue(any(e['code']=='NO_DATA' for e in executed['errors']))
