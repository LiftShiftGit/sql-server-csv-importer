import codecs
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from db_ingest.capture import capture_source, verify_capture
from db_ingest.config import load_config
from db_ingest.discovery import matches, discover
from db_ingest.errors import IngestError
from db_ingest.models import FieldValue, RecordEnvelope
from db_ingest.parsers.delimited import decide, parse
from db_ingest.profile import profile_records


class EngineeringTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        self.cfg = load_config(environ={})
        self.cfg['source'].update(root=str(self.source), path=str(self.source), include=['**/*'])
        self.cfg['preservation']['work_directory'] = str(self.root / 'work')
        self.cfg['parsing'].update(encoding='auto', delimiter=',', header='present')

    def tearDown(self):
        self.temp.cleanup()

    def read(self, raw):
        path = self.source / 'data.any'
        path.write_bytes(raw)
        artifact = capture_source(path, self.source, self.root / 'work')
        decision = decide(artifact, self.cfg)
        return artifact, decision, list(parse(artifact, decision, self.cfg))

    def test_capture_windows_metadata_stable_repeated(self):
        for _ in range(25):
            artifact, _, _ = self.read(b'a,b\n1,2\n')
            self.assertTrue(verify_capture(artifact)['verified'])

    def test_capture_modified_archive_detected(self):
        artifact, _, _ = self.read(b'a,b\n1,2\n')
        Path(artifact.path).write_bytes(b'modified')
        with self.assertRaises(IngestError) as cm:
            verify_capture(artifact)
        self.assertEqual(cm.exception.code, 'ARCHIVE_MISMATCH')

    def test_original_header_not_trimmed_or_nulled(self):
        self.cfg['parsing'].update(trim_whitespace=True, null_tokens=['NULL'])
        _, _, rows = self.read(b'NULL, spaced \nNULL, value \n')
        self.assertEqual([v.text for v in rows[0].fields], ['NULL', ' spaced '])
        self.assertEqual([v.text for v in rows[1].fields], [None, 'value'])

    def test_mixed_line_endings_exact_coverage(self):
        raw = b'a,b\r1,x\r\n2,y\n3,z'
        _, _, rows = self.read(raw)
        self.assertEqual([(r.byte_start, r.byte_end) for r in rows], [(0, 4), (4, 9), (9, 13), (13, 16)])
        self.assertEqual([(r.line_start, r.line_end) for r in rows], [(1, 1), (2, 2), (3, 3), (4, 4)])

    def test_escape_mode_and_quote_none(self):
        self.cfg['parsing'].update(quotechar='none', escapechar='\\')
        _, _, rows = self.read(b'a,b\n1,x\\,y\n')
        self.assertEqual([v.text for v in rows[-1].fields], ['1', 'x,y'])
        self.assertEqual([v.quoted for v in rows[-1].fields], [False, False])

    def test_double_quotes_preserved_and_null_quote(self):
        self.cfg['parsing'].update(null_tokens=['NULL'])
        _, _, rows = self.read(b'a,b\n"NULL","say ""hello"""\n')
        self.assertEqual([(v.text, v.quoted) for v in rows[-1].fields], [('NULL', True), ('say "hello"', True)])

    def test_header_absent_preserves_first_record(self):
        self.cfg['parsing']['header'] = 'absent'
        _, _, rows = self.read(b'1,alpha\n2,beta\n')
        self.assertEqual([r.kind for r in rows], ['data', 'data'])
        self.assertEqual([v.text for v in rows[0].fields], ['1', 'alpha'])

    def test_repeated_header_remains_data(self):
        _, _, rows = self.read(b'a,b\n1,x\na,b\n')
        self.assertEqual([r.kind for r in rows], ['header', 'data', 'data'])

    def test_blank_policy_does_not_fabricate_fields(self):
        _, _, rows = self.read(b'a,b\n\n1,x\n')
        self.assertEqual([r.kind for r in rows], ['header', 'rejected', 'data'])
        self.assertEqual(rows[1].errors[0]['code'], 'BLANK_RECORD')
        self.cfg['parsing']['blank_record_policy'] = 'explicitly_skip'
        _, _, rows = self.read(b'a,b\n\n1,x\n')
        self.assertEqual(rows[1].kind, 'excluded')
        self.assertEqual(rows[1].errors[0]['code'], 'BLANK_EXPLICITLY_EXCLUDED')
        self.assertEqual(rows[1].fields, [])
        self.cfg['parsing']['blank_record_policy'] = 'preserve'
        _, _, rows = self.read(b'a,b\n\n1,x\n')
        self.assertEqual(rows[1].kind, 'rejected')
        self.assertEqual(rows[1].errors[0]['code'], 'BLANK_PRESERVED_UNIMPORTED')

    def test_cp1252_requires_explicit_choice(self):
        self.cfg['parsing']['encoding'] = 'cp1252'
        _, _, rows = self.read(b'a,b\n1,caf\xe9\n')
        self.assertEqual(rows[-1].fields[-1].text, 'caf\xe9')

    def test_utf16be_utf32le_and_bom_coverage(self):
        for bom, encoding in [(codecs.BOM_UTF16_BE, 'utf-16-be'), (codecs.BOM_UTF32_LE, 'utf-32-le')]:
            with self.subTest(encoding=encoding):
                raw = bom + 'a,b\n1,\U0001f600\n'.encode(encoding)
                artifact, decision, rows = self.read(raw)
                self.assertEqual(decision.encoding, encoding)
                self.assertEqual(rows[0].byte_start, len(bom))
                self.assertEqual(rows[-1].byte_end, len(raw))
                self.assertEqual(rows[-1].fields[-1].text, '\U0001f600')

    def test_content_not_extension_binary_and_xml(self):
        for raw, code in [(b'PK\x03\x04data', 'UNSUPPORTED_FORMAT'), (b'<root><row/></root>', 'STRUCTURED_MAPPING_REQUIRED')]:
            with self.subTest(code=code), self.assertRaises(IngestError) as cm:
                self.read(raw)
            self.assertEqual(cm.exception.code, code)

    def test_delimiter_auto_requires_unique_candidate(self):
        self.cfg['parsing']['delimiter'] = 'auto'
        _, decision, rows = self.read(b'a|b\n1|x\n2|y\n')
        self.assertEqual(decision.delimiter, '|')
        self.assertEqual(len(rows), 3)
        with self.assertRaises(IngestError) as cm:
            self.read(b'a,b|c\n1,x|y\n')
        self.assertEqual(cm.exception.code, 'DIALECT_AMBIGUOUS')

    def test_glob_star_and_recursive_double_star(self):
        self.assertTrue(matches('root.csv', ['**/*.csv']))
        self.assertTrue(matches('A/child.xyz', ['A/**/*.xyz']))
        self.assertTrue(matches('A/B/child.xyz', ['A/**/*.xyz']))
        self.assertFalse(matches('A/child.csv', ['*.csv']))
        self.assertTrue(matches('A/work/file.csv', ['**/work/**']))

    def test_full_profile_late_type_change_and_unicode_units(self):
        _, _, rows = self.read('id,value\n1,short\n2,\U0001f600  \n3,later\n'.encode())
        profiles = profile_records(iter(rows), ['id', 'value'], self.cfg)
        self.assertEqual(profiles[0].sql_type, 'tinyint')
        self.assertEqual(profiles[1].stats['max_utf16_units'], 5)
        self.assertEqual(profiles[1].sql_type, 'nvarchar(6)')

    def test_oversized_record_physical_span_and_undecoded_tail(self):
        self.cfg['limits'].update(max_in_memory_record_bytes=64, max_in_memory_field_bytes=32)
        _, _, rows = self.read(b'a,b\n1,"' + b'x' * 128 + b'"\n2,ok\n')
        self.assertEqual([(r.line_start, r.line_end) for r in rows], [(1,1),(2,2),(3,3)])
        self.cfg['parsing']['sample_bytes'] = 4
        _, _, rows = self.read(b'a,b\n1,ok\n2,\xff\n')
        self.assertEqual(rows[-1].kind, 'unresolved')
        self.assertIsNone(rows[-1].line_end)

    def test_decimal_precision_requires_all_rows(self):
        _, _, rows = self.read(b'value,identifier\n1.20,001\n999.123,002\n')
        profiles = profile_records(iter(rows), ['value', 'identifier'], self.cfg)
        self.assertEqual(profiles[0].sql_type, 'decimal(6,3)')
        self.assertTrue(profiles[1].sql_type.startswith('nvarchar('))

    def test_exact_numeric_boundary_and_seven_digit_temporal_inference(self):
        _, _, rows = self.read(b'n,t\n123456789012345678901234567890123456789,2026-10-29T12:30:00.1234567+02:00\n')
        profiles = profile_records(iter(rows), ['n', 't'], self.cfg)
        self.assertEqual(profiles[0].sql_type, 'nvarchar(47)')
        self.assertEqual(profiles[1].sql_type, 'datetimeoffset(7)')
        from db_ingest.adapters.mssql.types import convert_field
        self.assertEqual(convert_field(rows[1].fields[1],profiles[1]), '2026-10-29T12:30:00.1234567+02:00')
        _, _, excess = self.read(b'n,t\n1,2026-10-29T12:30:00.12345678+02:00\n')
        excess_profiles = profile_records(iter(excess), ['n','t'], self.cfg)
        self.assertEqual(excess_profiles[1].sql_type, 'nvarchar(41)')

    def test_decimal_combined_range_falls_back_without_rounding(self):
        _, _, rows = self.read(b'n\n99999999999999999999999999999999999999\n0.1\n')
        profiles = profile_records(iter(rows), ['n'], self.cfg)
        self.assertTrue(profiles[0].sql_type.startswith('nvarchar('))

    def test_column_override_and_empty_value_stay_explicit(self):
        _, _, rows = self.read(b'n,n\n,""\n')
        self.cfg['columns'] = [{'source_ordinal': 2, 'target_type': 'varbinary(max)', 'nullable': False, 'target_name': 'binary_data'}]
        profiles = profile_records(iter(rows), ['n', 'n'], self.cfg)
        self.assertEqual(profiles[1].sql_type, 'varbinary(max)')
        self.assertFalse(profiles[1].nullable)
        self.assertEqual(profiles[1].target_name, 'binary_data')
        self.assertEqual(profiles[0].stats['empty_count'], 1)
        self.assertNotEqual(profiles[0].target_name, profiles[1].target_name)


if __name__ == '__main__':
    unittest.main()
