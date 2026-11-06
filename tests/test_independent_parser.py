"""Cross-role parser oracle: all expected bytes/values/spans are hand-authored."""
from dataclasses import asdict
import hashlib
from pathlib import Path
import tempfile
import unittest
from db_ingest.capture import capture_source
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.parsers.delimited import decide, parse
from db_ingest.discovery import discover

class IndependentParserTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / 'input'
        self.source.mkdir()
        self.config = load_config(environ={})
        self.config['source'].update(root=str(self.source),path=str(self.source),include=['**/*'])
        self.config['preservation']['work_directory']=str(self.root/'work')
        self.config['parsing'].update(encoding='utf-8',delimiter=',',quotechar='"',header='present')
    def tearDown(self):
        self.temp.cleanup()
    def records(self,data,name='file.csv'):
        path=self.source/name
        path.write_bytes(data)
        artifact=capture_source(path,self.source,self.root/'work')
        return artifact,list(parse(artifact,decide(artifact,self.config),self.config))

    def test_exact_byte_spans_multiline_and_duplicate_rows(self):
        raw=b'a,b\r\n1,"two\r\nlines"\r\n1,"two\r\nlines"\n'
        artifact,records=self.records(raw)
        self.assertEqual([r.kind for r in records],['header','data','data'])
        self.assertEqual([[f.text for f in r.fields] for r in records],[['a','b'],['1','two\r\nlines'],['1','two\r\nlines']])
        self.assertEqual([(r.byte_start,r.byte_end) for r in records],[(0,5),(5,21),(21,len(raw))])
        self.assertEqual([(r.line_start,r.line_end) for r in records],[(1,1),(2,3),(4,5)])
        self.assertEqual(len({r.record_id for r in records}),3)
        self.assertEqual(Path(artifact.path).read_bytes(),raw)
        self.assertEqual(artifact.sha256,hashlib.sha256(raw).hexdigest())

    def test_null_quote_provenance_and_ordered_duplicate_header(self):
        self.config['parsing']['null_tokens']=['NULL']
        _,records=self.records(b'a,a,,d\n,"",NULL,"NULL"\n')
        self.assertEqual([f.text for f in records[0].fields],['a','a','','d'])
        self.assertEqual([(f.state,f.text,f.quoted) for f in records[1].fields],[('EMPTY','',False),('EMPTY','',True),('NULL',None,False),('VALUE','NULL',True)])

    def test_utf16_supplementary_characters_byte_spans(self):
        self.config['parsing']['encoding']='auto'
        raw='a,b\r\n1,\U0001f600\r\n'.encode('utf-16')
        artifact,records=self.records(raw)
        self.assertEqual([f.text for f in records[1].fields],['1','\U0001f600'])
        self.assertEqual(records[0].byte_start,2)
        self.assertEqual(records[-1].byte_end,len(raw))
        self.assertEqual(records[0].byte_end,records[1].byte_start)
        self.assertEqual(Path(artifact.path).read_bytes(),raw)

    def test_unclosed_tail_is_unresolved_no_resynchronization(self):
        artifact,records=self.records(b'a,b\n1,good\n2,"broken\n3,looks_valid\n')
        self.assertEqual([r.kind for r in records],['header','data','unresolved'])
        self.assertEqual(records[-1].byte_start,11)
        self.assertEqual(records[-1].byte_end,artifact.size)
        self.assertEqual(records[-1].errors[0]['code'],'FRAMING_UNRESOLVED')
        self.assertEqual(records[-1].fields,[])

    def test_oversize_field_quarantined_bounded_without_losing_following_row(self):
        self.config['limits'].update(max_in_memory_record_bytes=64,max_in_memory_field_bytes=32)
        artifact,records=self.records(b'a,b\n1,"'+b'x'*128+b'"\n2,ok\n')
        self.assertEqual([r.kind for r in records],['header','rejected','data'])
        self.assertEqual(records[1].errors[0]['code'],'RECORD_LIMIT')
        self.assertEqual(records[1].fields,[])
        self.assertEqual([f.text for f in records[-1].fields],['2','ok'])
        self.assertEqual(records[-1].byte_end,artifact.size)

    def test_oversize_unclosed_tail_remains_unknown(self):
        self.config['limits'].update(max_in_memory_record_bytes=64,max_in_memory_field_bytes=32)
        _,records=self.records(b'a,b\n1,"'+b'x'*128+b'\n2,ok\n')
        self.assertEqual([r.kind for r in records],['header','unresolved'])
        self.assertEqual(records[-1].errors[0]['code'],'FRAMING_UNRESOLVED')

    def test_wrong_width_does_not_pad_or_drop(self):
        _,records=self.records(b'a,b\n1\n2,x,extra\n')
        self.assertEqual([r.kind for r in records],['header','rejected','rejected'])
        self.assertEqual([f.text for f in records[-1].fields],['2','x','extra'])
        self.assertEqual([e['code'] for r in records[1:] for e in r.errors],['ROW_WIDTH','ROW_WIDTH'])

    def test_invalid_utf8_never_replacement_decoded(self):
        with self.assertRaises(IngestError) as cm:
            self.records(b'a,b\n1,\xff\n')
        self.assertEqual(cm.exception.code,'ENCODING_DECISION')
        artifacts=list((self.root/'work'/'captures').glob('*/source.bin'))
        self.assertEqual(len(artifacts),1)
        self.assertEqual(artifacts[0].read_bytes(),b'a,b\n1,\xff\n')

    def test_ambiguous_header_cannot_drop_first_string_row(self):
        self.config['parsing']['header']='auto'
        with self.assertRaises(IngestError) as cm:
            self.records(b'alpha,beta\ngamma,delta\n')
        self.assertEqual(cm.exception.code,'HEADER_AMBIGUOUS')

    def test_work_exclusion_and_root_glob(self):
        work=self.source/'nested'/'work'
        work.mkdir(parents=True)
        (work/'must_not_read.csv').write_text('x\n1\n')
        (self.source/'root.csv').write_text('x\n1\n')
        (self.source/'nested'/'child.xyz').write_text('x\n2\n')
        self.config['source']['include']=['**/*.csv','**/*.xyz']
        self.config['preservation']['work_directory']=str(work)
        self.assertEqual([p.relative_to(self.source).as_posix() for p in discover(self.config)],['nested/child.xyz','root.csv'])

if __name__=='__main__': unittest.main()
