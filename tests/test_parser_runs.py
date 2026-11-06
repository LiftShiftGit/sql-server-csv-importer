"""Scalar/reference parity across strict codecs, controls and forced decode seams."""
from dataclasses import asdict
import codecs
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import random
import unittest
from unittest.mock import patch
from db_ingest.models import SourceArtifact,DialectDecision
from db_ingest.config import load_config
import db_ingest.parsers.delimited as parser

class RunFramingTests(unittest.TestCase):
    def compare(self,raw,encoding,quote='"',escape='\\',doublequote=True,delimiter=',',chunk=65536,cap=90,bom=0):
        path=self.root/'raw';path.write_bytes(raw)
        artifact=SourceArtifact('fixed',str(path),str(path),'raw',hashlib.sha256(raw).hexdigest(),len(raw))
        decision=DialectDecision(encoding,delimiter,quote,escape,doublequote,'present',{'version':1,'bom_bytes':bom})
        config=load_config(environ={});config['limits'].update(max_in_memory_record_bytes=cap,max_in_memory_field_bytes=cap//2)
        config['parsing'].update(null_tokens=['NULL'],null_tokens_in_quoted_fields=False)
        original_chars=parser._chars
        def chunks(*args,**kwargs):return original_chars(*args,**{**kwargs,'chunk_bytes':chunk})
        with patch.object(parser,'_chars',chunks):actual=[asdict(r) for r in parser.parse(artifact,decision,config)]
        with patch.object(parser,'_chars',chunks),patch.object(parser,'_tokens',lambda text,decision:iter(text)):
            expected=[asdict(r) for r in parser.parse(artifact,decision,config)]
        self.assertEqual(actual,expected)
        return actual
    def setUp(self):self.temp=TemporaryDirectory();self.root=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def test_all_codecs_quote_escape_controls_and_decode_boundaries(self):
        text='a,b\r\n1,"ordinary text long enough to use runs"\r2,"two\r\nlines and ""quotes"""\n3,escaped\\,comma\r\n4,"NULL"\n5,NULL\n6,\\ordinary runs\n7,"unterminated tail'
        for encoding in ('utf-8','utf-16-le','utf-16-be','utf-32-le','utf-32-be','cp1252','iso8859-1','ascii'):
            for chunk in (1,2,3,7,64,65536):
                for quote,escape,doublequote in [('"','\\',True),('"',None,True),('"','\\',False),(None,'\\',False),(None,None,False)]:
                    with self.subTest(encoding=encoding,chunk=chunk,quote=quote,escape=escape,doublequote=doublequote):self.compare(text.encode(encoding),encoding,quote,escape,doublequote,chunk=chunk)
    def test_unicode_multibyte_boms_and_late_strict_decoding_failure(self):
        text='a,b\r\n1,"é中😀 ordinary words"\r\n2,"é中😀 escaped \\" text"\n3,last'
        for encoding,bom in [('utf-8',codecs.BOM_UTF8),('utf-16-le',codecs.BOM_UTF16_LE),('utf-16-be',codecs.BOM_UTF16_BE),('utf-32-le',codecs.BOM_UTF32_LE),('utf-32-be',codecs.BOM_UTF32_BE)]:
            for chunk in (1,3,5,17,65536):
                with self.subTest(encoding=encoding,chunk=chunk):self.compare(bom+text.encode(encoding),encoding,chunk=chunk,bom=len(bom),cap=256)
        self.compare(b'a,b\n1,'+b'x'*1000+b'\xfftail','utf-8',chunk=64)
    def test_random_control_grammar_scalar_parity(self):
        rng=random.Random(91827);alphabet=['ordinaryrun','é中😀','"',',','\\','\r','\n','','NULL','  ',"'"]
        for index in range(160):
            text='a,b\n'+''.join(rng.choice(alphabet) for _ in range(45));encoding=rng.choice(['utf-8','utf-16-le','utf-32-be'])
            with self.subTest(index=index):self.compare(text.encode(encoding),encoding,chunk=rng.choice([1,3,11,64]),cap=rng.choice([20,80,1000]))
    def test_bulk_oversized_quoted_span_followed_by_good_record(self):
        raw=b'a,b\r\n1,"'+b'x'*131093+b'"\r\n2,last\r\n';rows=self.compare(raw,'utf-8',cap=128,chunk=65536)
        self.assertEqual([r['kind'] for r in rows],['header','rejected','data'])
        self.assertEqual([(r['byte_start'],r['byte_end']) for r in rows],[(0,5),(5,len(raw)-8),(len(raw)-8,len(raw))])
        self.assertEqual(rows[-1]['fields'][1]['text'],'last')
    def test_unusual_unicode_and_regex_control_characters(self):
        for delimiter,quote,escape in [('|',"'",'!'),(']', '[','^'),('😀','中','é'),('\t','"',None),(';',None,'\\')]:
            text=delimiter.join(['a','b'])+'\r\n'+delimiter.join(['ordinary words','long ordinary words'])+'\n'
            if quote:text+=quote+'more words'+quote+delimiter+'last\r'
            for chunk in (1,7,65536):
                with self.subTest(delimiter=delimiter,quote=quote,escape=escape,chunk=chunk):self.compare(text.encode(),'utf-8',delimiter=delimiter,quote=quote,escape=escape,chunk=chunk,cap=256)
