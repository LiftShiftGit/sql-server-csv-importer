"""Independent authored fixture expectations; no importer parser computes the oracle."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from decimal import Decimal
from db_ingest.errors import IngestError
from db_ingest.fixturegen import CATEGORIES, create_test_files, verify_test_files

class IndependentFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
    def tearDown(self):
        self.temp.cleanup()

    def test_all_categories_have_independent_byte_manifest_and_determinism(self):
        a = create_test_files(self.root / 'a', 'all', rows=3, size_bytes=4096)
        b = create_test_files(self.root / 'b', 'all', rows=3, size_bytes=4096)
        self.assertEqual(a, b)
        self.assertEqual({f['category'] for f in a['fixtures']}, set(CATEGORIES))
        for fixture in a['fixtures']:
            data = (self.root / 'a' / fixture['path']).read_bytes()
            self.assertEqual(fixture['sha256'], hashlib.sha256(data).hexdigest())
            self.assertEqual(fixture['size'], len(data))
        report = verify_test_files(self.root / 'a' / 'manifest.json')
        self.assertEqual(report['errors'], [])
        self.assertEqual(report['state'], 'SOURCE_ONLY_NOT_SQL_VERIFIED')
        self.assertEqual(report['exit_code'], 3)
        self.assertEqual(report['sql_integration'], 'NOT_EXECUTED')

    def test_malformed_oracle_keeps_unknown_logical_total(self):
        manifest = create_test_files(self.root, ['csv-bad-format'])
        unclosed = next(x for x in manifest['fixtures'] if x['path'] == 'unclosed.csv')
        self.assertIsNone(unclosed['logical_records'])
        self.assertEqual(unclosed['expected_rows'], 1)
        self.assertEqual((self.root / 'unclosed.csv').read_bytes()[15:], b'2,"unknown\nremainder')

    def test_nulls_expected_are_authored_not_reserialized(self):
        create_test_files(self.root, ['csv_nulls_defaults'])
        data = json.loads((self.root / 'nulls.csv.expected.jsonl').read_text())
        self.assertEqual([v['value'] for v in data['values']], ['', '', None, 'NULL'])
        self.assertEqual(data['ordinal'], 2)

    def test_missing_and_modified_archive_fail(self):
        create_test_files(self.root, 'csv')
        missing = verify_test_files(self.root / 'manifest.json', captures={})
        self.assertEqual(missing['errors'][0]['code'], 'CAPTURE_MISSING')
        retained = self.root / 'retained.bin'
        retained.write_bytes(b'altered')
        mismatch = verify_test_files(self.root / 'manifest.json', captures={'csv.csv': str(retained)})
        self.assertEqual(mismatch['exit_code'], 5)
        self.assertEqual(mismatch['errors'][0]['code'], 'SOURCE_BYTE_MISMATCH')

    def test_explicit_run_and_database_required_before_reader(self):
        create_test_files(self.root, 'csv')
        called = []
        def reader(*args):
            called.append(args)
            return []
        with self.assertRaises(IngestError) as cm:
            verify_test_files(self.root / 'manifest.json', destination_reader=reader)
        self.assertEqual(cm.exception.code, 'FIXTURE_DESTINATION_REQUIRED')
        self.assertEqual(called, [])

    def test_order_duplicate_multiplicity_and_trailing_spaces(self):
        create_test_files(self.root, 'csv_unicode')
        # Reader is a test double, explicitly not SQL integration evidence.
        def reader(fixture, run_id, db):
            if fixture['path'] == 'unicode.csv':
                return iter([{'ordinal': 2, 'values': ['001', '\U0001f600 e\u0301 \u4e2d\u6587  ']}, {'ordinal': 3, 'values': ['001', '\U0001f600 e\u0301 \u4e2d\u6587  ']}])
            return iter([{'ordinal': 2, 'values': [1, '\u03a9' if fixture['path']=='utf16.csv' else 'ok']}])
        checked = verify_test_files(self.root / 'manifest.json', destination_reader=reader, run_id='fixture-test-double', database='DisposableFixtureTestDouble')
        self.assertEqual(checked['errors'], [])
        self.assertEqual(checked['exit_code'], 3) # Even correct values do not certify receipts.
        def missing_duplicate(fixture, run_id, db):
            return iter([{'ordinal': 2, 'values': ['001', '\U0001f600 e\u0301 \u4e2d\u6587  ']}]) if fixture['path']=='unicode.csv' else reader(fixture,run_id,db)
        checked = verify_test_files(self.root / 'manifest.json', destination_reader=missing_duplicate, run_id='fixture-test-double', database='DisposableFixtureTestDouble')
        self.assertIn('DESTINATION_ROW_MISSING', [e['code'] for e in checked['errors']])

    def test_no_overwrite_and_unsupported_destination(self):
        create_test_files(self.root, 'csv')
        with self.assertRaises(IngestError) as cm:
            create_test_files(self.root, 'csv')
        self.assertEqual(cm.exception.code, 'FIXTURE_EXISTS')
        with self.assertRaises(IngestError) as cm:
            create_test_files(self.root / 'other', destination='postgresql')
        self.assertEqual(cm.exception.code, 'UNAVAILABLE_ADAPTER')

    def test_manifest_escape_rejected(self):
        manifest = create_test_files(self.root, 'csv')
        manifest['fixtures'][0]['path'] = '../escape.csv'
        (self.root / 'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
        with self.assertRaises(IngestError) as cm:
            verify_test_files(self.root / 'manifest.json')
        self.assertEqual(cm.exception.code, 'FIXTURE_PATH_ESCAPE')

if __name__ == '__main__':
    unittest.main()
