import codecs
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from db_ingest.capture import capture_source
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.parsers import decide, parse


class StructuredTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.cfg = load_config(environ={})
        self.cfg['source'].update(root=str(self.root), path=str(self.root), format='auto')
        self.cfg['parsing'].update(encoding='auto', header='present')
        self.cfg['xml'].update(enabled=True, record_selector='row', fields=[{'name':'id','path':'@id'},{'name':'value','path':'value'}])
        self.cfg['html'].update(enabled=True, table_selector='0', fields=['id','value'])

    def tearDown(self):
        self.temp.cleanup()

    def read(self, data):
        path = self.root/'data.any'
        path.write_bytes(data)
        artifact = capture_source(path, self.root, self.root/'work')
        decision = decide(artifact, self.cfg)
        return artifact, decision, list(parse(artifact, decision, self.cfg))

    def test_xml_attributes_scalar_and_metadata_header(self):
        raw = '<?xml version="1.0" encoding="UTF-8"?><data><row id="001"><value> café &amp; 😀 </value></row><row id="001"><value></value></row></data>'.encode()
        artifact, decision, records = self.read(raw)
        self.assertEqual(decision.evidence['adapter'], 'xml')
        self.assertEqual([r.kind for r in records], ['header','data','data'])
        self.assertEqual([r.ordinal for r in records], [0,1,2])
        self.assertEqual([v.text for v in records[1].fields], ['001',' café & 😀 '])
        self.assertEqual([v.state for v in records[2].fields], ['VALUE','EMPTY'])
        self.assertIsNone(records[1].byte_start)
        self.assertEqual(Path(artifact.path).read_bytes(), raw)

    def test_xml_namespaces(self):
        self.cfg['xml'].update(record_selector='{urn:test}row', fields=[{'name':'id','path':'@id'},{'name':'value','path':'{urn:test}value'}])
        _, _, records = self.read(b'<data xmlns="urn:test"><row id="1"><value>x</value></row></data>')
        self.assertEqual([v.text for v in records[1].fields], ['1','x'])

    def test_xml_missing_field_rejected_not_null(self):
        _, _, records = self.read(b'<data><row id="1"/></data>')
        self.assertEqual(records[1].kind, 'rejected')
        self.assertEqual(records[1].errors[0]['code'], 'XML_MISSING_FIELD')

    def test_xml_no_undeclared_flattening(self):
        for raw, code in [(b'<data><row id="1"><value><child>x</child></value></row></data>', 'XML_NESTED_SCALAR'), (b'<data><row id="1"><value>x</value><value>y</value></row></data>','XML_REPEATED_SCALAR')]:
            with self.subTest(code=code), self.assertRaises(IngestError) as cm:
                self.read(raw)
            self.assertEqual(cm.exception.code,code)

    def test_xml_dtd_forbidden_even_without_entity(self):
        with self.assertRaises(IngestError) as cm:
            self.read(b'<!DOCTYPE data><data><row id="1"><value>x</value></row></data>')
        self.assertEqual(cm.exception.code,'XML_DTD_FORBIDDEN')

    def test_xml_malformed_tail_blocks_file(self):
        with self.assertRaises(IngestError) as cm:
            self.read(b'<data><row id="1"><value>x</value></row><row')
        self.assertEqual(cm.exception.code,'MARKUP_UNCLOSED')

    def test_xml_disabled_by_default(self):
        self.cfg['xml']['enabled']=False
        with self.assertRaises(IngestError) as cm:
            self.read(b'<data><row id="1"><value>x</value></row></data>')
        self.assertEqual(cm.exception.code,'XML_DISABLED')

    def test_xml_depth_limit(self):
        self.cfg['xml']['max_depth']=3
        with self.assertRaises(IngestError) as cm:
            self.read(b'<data><a><b><c/></b></a><row id="1"><value>x</value></row></data>')
        self.assertEqual(cm.exception.code,'XML_DEPTH')

    def test_markup_token_limit_before_library_buffers(self):
        self.cfg['limits'].update(max_in_memory_record_bytes=64,max_in_memory_field_bytes=64)
        with self.assertRaises(IngestError) as cm:
            self.read(b'<data><row id="'+b'x'*128+b'"><value>x</value></row></data>')
        self.assertEqual(cm.exception.code,'MARKUP_TOKEN_LIMIT')

    def test_html_entities_text_whitespace_and_header(self):
        raw = '<!DOCTYPE html><html><body><table><tr><th>id</th><th>value</th></tr><tr><td>001</td><td> café &amp; 😀 </td></tr></table></body></html>'.encode()
        artifact, decision, records = self.read(raw)
        self.assertEqual(decision.evidence['adapter'], 'html')
        self.assertEqual([v.text for v in records[1].fields], ['001',' café & 😀 '])
        self.assertIsNone(records[1].byte_start)
        self.assertEqual(Path(artifact.path).read_bytes(),raw)

    def test_html_explicit_index_and_absent_header(self):
        self.cfg['html']['table_selector']='1'
        self.cfg['parsing']['header']='absent'
        self.cfg['source']['format']='html'
        _, _, records = self.read(b'<div><table><tr><td>ignore</td></tr></table><table><tr><td>1</td><td>x</td></tr></table></div>')
        self.assertEqual([v.text for v in records[1].fields], ['1','x'])

    def test_html_no_span_or_nested_table_expansion(self):
        for raw, code in [(b'<table><tr><td rowspan="2">id</td><td>value</td></tr></table>','HTML_SPAN_UNSUPPORTED'), (b'<table><tr><td><table></table></td></tr></table>','HTML_NESTED_TABLE')]:
            with self.subTest(code=code), self.assertRaises(IngestError) as cm:
                self.read(raw)
            self.assertEqual(cm.exception.code,code)

    def test_html_header_mismatch_and_unclosed(self):
        for raw, code in [(b'<table><tr><th>wrong</th><th>value</th></tr></table>','HTML_HEADER_MISMATCH'), (b'<table><tr><td>id</td><td>value</td></tr>','HTML_UNCLOSED_STRUCTURE')]:
            with self.subTest(code=code), self.assertRaises(IngestError) as cm:
                self.read(raw)
            self.assertEqual(cm.exception.code,code)

    def test_html_missing_cell_rejected_with_counts(self):
        _, _, records = self.read(b'<table><tr><th>id</th><th>value</th></tr><tr><td>1</td></tr></table>')
        self.assertEqual(records[1].kind,'rejected')
        self.assertEqual(records[1].errors[0]['code'],'ROW_WIDTH')

    def test_html_utf16_decoded_before_markup(self):
        raw = '<table><tr><th>id</th><th>value</th></tr><tr><td>1</td><td>😀</td></tr></table>'.encode('utf-16')
        _, decision, records = self.read(raw)
        self.assertEqual(decision.encoding,'utf-16-le')
        self.assertEqual(records[1].fields[1].text,'😀')

    def test_structured_record_identity_tracks_semantic_global_policy(self):
        raw=b'<data><row id="1"><value>NULL</value></row></data>'
        artifact,first,records=self.read(raw)
        self.assertEqual(first.evidence['record_identity_policy_version'],2)
        self.cfg['parsing']['null_tokens']=['NULL']
        second=decide(artifact,self.cfg)
        changed=list(parse(artifact,second,self.cfg))
        self.assertNotEqual(records[1].record_id,changed[1].record_id)
        self.assertEqual(changed[1].fields[1].state,'NULL')
        # Sampling limits are operational for already-selected structured input.
        self.cfg['parsing']['sample_records']=1
        third=decide(artifact,self.cfg)
        self.assertEqual(second,third)



if __name__=='__main__': unittest.main()
