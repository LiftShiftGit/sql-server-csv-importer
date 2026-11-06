"""Independent full type/state boundaries; no implementation-derived expected values."""
from decimal import Decimal
from pathlib import Path
import runpy
import tempfile
import unittest
from db_ingest.adapters.mssql.types import convert_field,normalize_value,parse_sql_type
from db_ingest.errors import IngestError
from db_ingest.models import ColumnProfile,FieldValue
from db_ingest.staging import typed_stage_parameters
ORACLE=runpy.run_path(str(Path(__file__).parent/'oracles/type_matrix.py'))

class IndependentBatchTypeTests(unittest.TestCase):
    def test_every_authored_type_and_precision_converts_exactly(self):
        columns=ORACLE['columns']()
        self.assertEqual(len(columns),38)
        for row in ORACLE['authored_rows']():
            for ordinal,(column,source,want) in enumerate(zip(columns,row['source'],row['expected'],strict=True),1):
                with self.subTest(row=row['ordinal'],column=column.name):
                    profile=ColumnProfile(ordinal,column.name,column.name,column.sql_type,True,{'binary_encoding':column.binary_encoding} if column.binary_encoding else {})
                    field=FieldValue('NULL' if source is None else 'EMPTY' if source=='' else 'VALUE',source,ordinal-1 in row['force_quote'])
                    actual=convert_field(field,profile)
                    if column.sql_type=='bit' and actual is not None:
                        # Conversion yields 0/1; actual ODBC bool readback is independently asserted live.
                        self.assertIs(type(actual),int);self.assertEqual(actual,int(want))
                    else:self.assertTrue(ORACLE['equivalent'](actual,want,column),(actual,want))
    def test_numeric_family_exact_lower_upper_and_outside(self):
        limits={'tinyint':(0,255),'smallint':(-32768,32767),'int':(-2147483648,2147483647),'bigint':(-9223372036854775808,9223372036854775807)}
        for kind,(low,high) in limits.items():
            for value in (low,high):self.assertEqual(normalize_value(str(value),kind),value)
            for value in (low-1,high+1):
                with self.subTest(kind=kind,value=value),self.assertRaises(IngestError) as cm:normalize_value(str(value),kind)
                self.assertEqual(cm.exception.code,'VALUE_INVALID')
        self.assertEqual(normalize_value('9'*38,'decimal(38,0)'),Decimal('9'*38))
        self.assertEqual(normalize_value('1e5','decimal(38,0)'),Decimal('100000'))
        for value in ('9'*39,'0.1','NaN','Infinity'):
            with self.subTest(value=value),self.assertRaises(IngestError):normalize_value(value,'decimal(38,0)')
        for value in ('2','-1','true','false','1.0'):
            with self.subTest(value=value),self.assertRaises(IngestError):normalize_value(value,'bit')
        self.assertEqual(normalize_value(True,'bit'),1)
        self.assertEqual(normalize_value(False,'bit'),0)
    def test_all_temporal_precisions_keep_fraction_offset_and_reject_rounding(self):
        for base in ('time','datetime2','datetimeoffset'):
            for precision in range(8):
                fragment='12:34:56'+('.'+'1234567'[:precision] if precision else '')
                value=fragment if base=='time' else '2024-02-29T'+fragment+('-04:00' if base=='datetimeoffset' else '')
                self.assertEqual(normalize_value(value,f'{base}({precision})'),value)
                bad_fraction='12:34:56.'+'1'*(precision+1)
                bad=bad_fraction if base=='time' else '2024-02-29T'+bad_fraction+('-04:00' if base=='datetimeoffset' else '')
                with self.subTest(base=base,precision=precision),self.assertRaises(IngestError):normalize_value(bad,f'{base}({precision})')
        for value,kind in [('2023-02-29','date'),('01/02/2024','date'),('24:00:00','time(0)'),('2024-01-01T00:00:00+14:01','datetimeoffset(0)'),('0001-01-01T00:00:00+14:00','datetimeoffset(0)'),('9999-12-31T23:59:59-14:00','datetimeoffset(0)')]:
            with self.subTest(value=value),self.assertRaises(IngestError):normalize_value(value,kind)
    def test_null_empty_missing_are_distinct_for_nullable_and_not_null(self):
        text=ColumnProfile(1,'text','text','nvarchar(32)',True)
        binary=ColumnProfile(2,'bytes','bytes','varbinary(32)',True,{'binary_encoding':'hex'})
        for column in (text,binary):
            self.assertIsNone(convert_field(FieldValue('NULL',None),column))
            self.assertEqual(convert_field(FieldValue('EMPTY',''),column),b'' if column is binary else '')
            with self.assertRaises(IngestError) as cm:convert_field(FieldValue('MISSING',None),column)
            self.assertEqual(cm.exception.code,'MISSING_FIELD')
            column.nullable=False
            with self.assertRaises(IngestError) as cm:convert_field(FieldValue('NULL',None),column)
            self.assertEqual(cm.exception.code,'NULLABILITY')
    def test_fixed_storage_units_max_binary_encodings_and_trailing_material(self):
        self.assertEqual(normalize_value('🙂  ','nvarchar(4)'),'🙂  ')
        with self.assertRaises(IngestError):normalize_value('🙂  ','nvarchar(3)')
        self.assertEqual(normalize_value('x'*8192,'nvarchar(max)'),'x'*8192)
        self.assertEqual(normalize_value('000102ff','varbinary(4)',binary_encoding='hex'),bytes([0,1,2,255]))
        self.assertEqual(normalize_value('AAEC/w==','varbinary(4)',binary_encoding='base64'),bytes([0,1,2,255]))
        for value,encoding in [('0','hex'),('zz','hex'),('AAEC/w==\n','base64'),('%%%','base64')]:
            with self.subTest(value=value),self.assertRaises(IngestError):normalize_value(value,'varbinary(max)',binary_encoding=encoding)
        with self.assertRaises(IngestError):normalize_value('AAEC/w==','varbinary(3)',binary_encoding='base64')
    def test_null_staging_tags_never_send_python_none(self):
        columns=[ColumnProfile(i+1,c.name,c.name,c.sql_type,True,{'binary_encoding':c.binary_encoding} if c.binary_encoding else {}) for i,c in enumerate(ORACLE['columns']())]
        values=[None]*len(columns)
        parameters=typed_stage_parameters(columns,values)
        self.assertEqual(len(parameters),2*len(columns))
        self.assertNotIn(None,parameters)
        self.assertEqual(parameters[::2],[1]*len(columns))
    def test_bcp_typed_spool_is_independently_reversible_and_delimiter_safe(self):
        import base64,hashlib,json
        from db_ingest.models import RecordEnvelope
        from db_ingest.transports.bcp import encode_typed_row
        columns=[ColumnProfile(i+1,c.name,c.name,c.sql_type,True,{'binary_encoding':c.binary_encoding} if c.binary_encoding else {}) for i,c in enumerate(ORACLE['columns']())]
        for row in ORACLE['authored_rows']():
            fields=[FieldValue('NULL' if value is None else 'EMPTY' if value=='' else 'VALUE',value,index in row['force_quote']) for index,value in enumerate(row['source'])]
            record=RecordEnvelope('authored-id-'+str(row['ordinal']),row['ordinal'],None,None,None,None,fields)
            expected_values=[int(value) if column.sql_type=='bit' and value is not None else value for column,value in zip(ORACLE['columns'](),row['expected'],strict=True)]
            wire=encode_typed_row(record,expected_values,columns)
            self.assertTrue(wire.endswith(b'\n'))
            self.assertNotIn(b'\n',wire[:-1]);self.assertNotIn(b'\r',wire)
            components=wire[:-1].split(b'\t');self.assertEqual(len(components),4)
            self.assertEqual(base64.b64decode(components[0],validate=True).decode('utf-16-le'),record.record_id)
            self.assertEqual(int(components[1]),row['ordinal'])
            self.assertEqual(components[3].decode('ascii'),hashlib.sha256(b'\t'.join(components[:3])).hexdigest())
            payload=json.loads(components[2]);self.assertEqual(payload['v'],3)
            self.assertEqual(len(payload['s']),len(columns))
            self.assertEqual(len(payload['f']),len(columns))
            for column,want,state,field in zip(ORACLE['columns'](),expected_values,payload['s'],payload['f'],strict=True):
                raw=base64.b64decode(field,validate=True)
                if want is None:self.assertEqual((state,raw),('N',b''));continue
                if column.sql_type.startswith('varbinary('):
                    self.assertEqual(raw,want)
                else:
                    expected_text=format(want,'f') if isinstance(want,Decimal) else str(want)
                    self.assertEqual(raw,expected_text.encode('utf-16-le'))
                self.assertEqual(state,'E' if raw==b'' else 'V')
    def test_fast_batch_guard_splits_thousands_of_tiny_rows_around_large_values(self):
        from db_ingest.transports.odbc import OdbcBatchWriter
        class Cursor:
            fast_executemany=False
            def __init__(self):self.groups=[];self.descriptors=[]
            def setinputsizes(self,sizes):self.descriptors.append(sizes)
            def executemany(self,sql,rows):self.groups.append(list(rows))
        for position in (0,500,1000):
            rows=[(str(i),b'x') for i in range(1001)]
            rows[position]=('X'*1048576,b'Y'*1048576)
            cursor=Cursor();limit=4*1048576
            writer=OdbcBatchWriter(cursor,'authored fake SQL',{'load':{'batch_rows':5000,'batch_bytes':limit,'fast_executemany':True}})
            for row in rows:writer.add(row)
            summary=writer.finish()
            self.assertEqual([row for group in cursor.groups for row in group],rows)
            self.assertGreater(summary['fast_array_batches'],0)
            self.assertEqual(summary['fallback_rows'],1)
            self.assertLessEqual(summary['max_native_array_bytes'],limit)
            self.assertGreater(len(cursor.groups),1)
            self.assertLessEqual(summary['max_bytes'],limit)
            self.assertLessEqual(summary['max_rows'],5000)
            for group in cursor.groups:
                # Independent lower-bound native stride: largest UTF16/binary payload
                # in each parameter column times parameter-array row count.
                stride=sum(max(len(row[index].encode('utf-16-le')) if isinstance(row[index],str) else len(row[index]) for row in group) for index in range(2))*len(group)
                self.assertLessEqual(stride,limit)
    def test_unimplemented_specialized_types_do_not_masquerade_as_supported(self):
        for kind in ['rowversion','timestamp','image','ntext','text','xml','sql_variant','geometry','geography','hierarchyid','vector(4)','json','float','real','money','datetime','smalldatetime','char(10)','nchar(10)','binary(10)','blob','clob','nvarchar(10));DROP TABLE x;--']:
            with self.subTest(kind=kind),self.assertRaises(IngestError):parse_sql_type(kind)
    def test_authored_positions_do_not_depend_on_implementation(self):
        for position,expected_index in [('first',0),('middle',8),('last',16)]:
            rows=list(ORACLE['authored_rows'](17,position))
            self.assertGreater(len(rows[expected_index]['expected'][-3]),8192)
            self.assertEqual(rows[1]['expected'][1:], [None]*37)
            self.assertEqual(rows[2]['expected'][-4:],[ '', '', b'', b''])
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'authored.csv';manifest=ORACLE['write_fixture'](p,negative=True)
            self.assertEqual(manifest['accepted_rows'],17)
            self.assertEqual(manifest['rejected_rows'],2)
            self.assertIn(b'"\\N"',p.read_bytes())
            self.assertIn(b'9'*39,p.read_bytes())
if __name__=='__main__':unittest.main()
