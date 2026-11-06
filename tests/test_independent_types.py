"""SQL exactness unit checks; these are NOT live ODBC/SQL integration."""
import unittest
from decimal import Decimal
from db_ingest.adapters.mssql.types import normalize_value, canonical_value, parse_sql_type
from db_ingest.errors import IngestError

class IndependentTypeTests(unittest.TestCase):
    def reject(self,value,kind,**kwargs):
        with self.assertRaises(IngestError) as cm: normalize_value(value,kind,**kwargs)
        self.assertEqual(cm.exception.code,'VALUE_INVALID')
    def test_decimal_38_digits_and_no_rounding(self):
        lexical='9'*36+'.12'
        self.assertEqual(normalize_value(lexical,'decimal(38,2)'),Decimal(lexical))
        self.reject('9'*37+'.12','decimal(38,2)')
        self.reject('0.001','decimal(38,2)')
        self.assertEqual(normalize_value('+0001.2300','decimal(6,4)'),Decimal('1.2300'))
    def test_seven_fractional_digits_and_explicit_offsets(self):
        value='2024-02-29T12:34:56.1234567+05:30'
        self.assertEqual(normalize_value(value,'datetimeoffset(7)'),value)
        self.reject(value,'datetimeoffset(6)')
        self.reject('2024-02-29T12:34:56.12345678','datetime2(7)')
        self.reject('2023-02-29','date')
        self.reject('2024-01-01T00:00:00+14:01','datetimeoffset(7)')
    def test_storage_units_not_codepoints_and_no_space_trimming(self):
        self.assertEqual(normalize_value('\U0001f600','nvarchar(2)'),'\U0001f600')
        self.reject('\U0001f600','nvarchar(1)')
        self.assertEqual(normalize_value('a  ','nvarchar(3)'),'a  ')
        self.reject('a  ','nvarchar(2)')
    def test_guid_overlong_cannot_truncate(self):
        guid='01234567-89ab-cdef-0123-456789abcdef'
        self.assertEqual(normalize_value(guid,'uniqueidentifier'),guid)
        self.reject(guid+'junk','uniqueidentifier')
    def test_binary_is_explicit_strict_and_empty_survives(self):
        self.assertEqual(normalize_value('AAEC/w==','varbinary(4)',binary_encoding='base64'),b'\x00\x01\x02\xff')
        self.assertEqual(normalize_value('','varbinary(max)',binary_encoding='base64'),b'')
        self.reject('AAEC/w==','varbinary(3)',binary_encoding='base64')
        self.reject('AAEC/w==\n','varbinary(max)',binary_encoding='base64')
        self.reject('AAEC/w==','varbinary(max)')
    def test_canonical_type_and_state_framing(self):
        vectors=[canonical_value(None,'nvarchar(10)'),canonical_value('','nvarchar(10)'),canonical_value('a ','nvarchar(10)'),canonical_value('a','nvarchar(10)'),canonical_value(b'','varbinary(10)'),canonical_value(1,'int'),canonical_value('1','nvarchar(10)')]
        self.assertEqual(len(set(vectors)),len(vectors))
    def test_varchar_requires_explicit_reversible_byte_policy(self):
        self.assertEqual(str(parse_sql_type('varchar(max)')),'varchar(max)')
        with self.assertRaises(IngestError):normalize_value('abc','varchar(max)')
        self.assertEqual(normalize_value('é🙂  ','varchar(8)',text_encoding='utf-8'),'é🙂  ')
        self.reject('é🙂   ','varchar(8)',text_encoding='utf-8')
        self.assertEqual(normalize_value('é  ','varchar(3)',text_encoding='cp1252'),'é  ')
        self.reject('🙂','varchar(max)',text_encoding='cp1252')
    def test_type_injection_blocked(self):
        for kind in ['nvarchar(10)); DROP TABLE t;--','float','decimal(39,0)','time(8)']:
            with self.subTest(kind=kind),self.assertRaises(IngestError): parse_sql_type(kind)
if __name__=='__main__': unittest.main()
