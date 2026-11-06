"""Authored data oracle. No db_ingest imports, parser, converter or digest calls."""
from dataclasses import dataclass
from decimal import Decimal
import base64
import csv
import hashlib
from pathlib import Path

@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    binary_encoding: str | None = None


def columns():
    result=[Column('row_id','int'),Column('flag','bit'),Column('tiny','tinyint'),Column('small','smallint'),Column('number','int'),Column('big','bigint'),Column('exact38','decimal(38,0)'),Column('fraction38','decimal(38,18)'),Column('day','date')]
    for base,prefix in [('time','clock'),('datetime2','local'),('datetimeoffset','offset')]:
        result.extend(Column(prefix+str(p),f'{base}({p})') for p in range(8))
    result.extend([Column('guid','uniqueidentifier'),Column('text_fixed','nvarchar(32)'),Column('text_max','nvarchar(max)'),Column('binary_fixed','varbinary(256)','base64'),Column('binary_max','varbinary(max)','hex')])
    return result


def normal_values(index):
    want=[index,bool(index%2),0 if index%2 else 255,-32768 if index%2 else 32767,-2147483648 if index%2 else 2147483647,-9223372036854775808 if index%2 else 9223372036854775807,Decimal('9'*38),Decimal('9'*20+'.'+'1'*18),'2024-02-29']
    for base in ('time','datetime2','datetimeoffset'):
        for precision in range(8):
            clock='12:34:56'+('.'+'1234567'[:precision] if precision else '')
            want.append(clock if base=='time' else '2024-02-29T'+clock+('+05:30' if base=='datetimeoffset' else ''))
    want.extend(['01234567-89ab-cdef-0123-456789abcdef','🙂 é  ',f'row {index}, "quoted"\r\nsecond line  ',bytes(range(256)),bytes(range(256))+index.to_bytes(4,'big')])
    return want


def authored_rows(count=17,outlier_position='middle'):
    if count<5:raise ValueError('Use at least five rows to keep NULL/empty/literal controls independent')
    candidates={'first':1,'middle':count//2+1,'last':count}
    outlier=candidates[outlier_position]
    for index in range(1,count+1):
        values=normal_values(index)
        quoted=set()
        if index==2:
            values=[index]+[None]*(len(values)-1)
        elif index==3:
            for name in ('text_fixed','text_max','binary_fixed','binary_max'):
                values[next(i for i,c in enumerate(columns()) if c.name==name)] = b'' if name.startswith('binary') else ''
        elif index==4:
            position=next(i for i,c in enumerate(columns()) if c.name=='text_fixed')
            values[position]='\\N';quoted.add(position)
        if index==outlier:
            values[-3]='L'*8192+'🙂  '
            values[-1]=bytes(range(256))*32
        source=[]
        for column,value in zip(columns(),values,strict=True):
            if value is None:source.append(None)
            elif isinstance(value,bool):source.append('1' if value else '0')
            elif isinstance(value,Decimal):source.append(format(value,'f'))
            elif isinstance(value,bytes):source.append(base64.b64encode(value).decode('ascii') if column.binary_encoding=='base64' else value.hex())
            else:source.append(str(value))
        yield {'ordinal':index+1,'source':source,'expected':values,'force_quote':quoted}


def encode_record(values,force_quote=()):
    fields=[]
    for index,value in enumerate(values):
        if value is None:fields.append(chr(92)+'N');continue
        value=str(value)
        if index in force_quote or any(c in value for c in ',"\r\n'):
            value='"'+value.replace('"','""')+'"'
        fields.append(value)
    return (','.join(fields)+'\r\n').encode('utf-8')


def write_fixture(path,count=17,outlier_position='middle',negative=False):
    path=Path(path)
    digest=hashlib.sha256();length=0
    with path.open('wb') as target:
        for data in [encode_record([c.name for c in columns()])]:
            target.write(data);digest.update(data);length+=len(data)
        for row in authored_rows(count,outlier_position):
            data=encode_record(row['source'],row['force_quote']);target.write(data);digest.update(data);length+=len(data)
        if negative:
            too_many_digits=normal_values(count+1)
            source=[str(v) for v in too_many_digits]
            # Derive lexical negatives from a valid independently authored row; never app conversion.
            source=list(next(authored_rows(count,'first'))['source'])
            source[0]=str(count+1);source[6]='9'*39;source[1]='2';source[next(i for i,c in enumerate(columns()) if c.name=='guid')]='01234567-89ab-cdef-0123-456789abcdefEXTRA'
            data=encode_record(source);target.write(data);digest.update(data);length+=len(data)
            source=list(next(authored_rows(count,'first'))['source']);source[0]=str(count+2)
            data=encode_record(source[:-1]);target.write(data);digest.update(data);length+=len(data)
    return {'source_bytes':length,'source_sha256':digest.hexdigest(),'accepted_rows':count,'rejected_rows':2 if negative else 0,'logical_records':count+1+(2 if negative else 0),'oracle':'authored constants/formulas independent of application'}


def equivalent(actual,expected,column):
    base=column.sql_type.split('(')[0]
    if expected is None:return actual is None
    if base=='bit':return isinstance(actual,bool) and actual is expected
    if base in ('tinyint','smallint','int','bigint'):return type(actual) is int and actual==expected
    if base=='decimal':return isinstance(actual,Decimal) and actual==expected
    if base=='varbinary':return isinstance(actual,(bytes,bytearray,memoryview)) and bytes(actual)==expected
    if base=='uniqueidentifier':return str(actual).lower()==expected
    return isinstance(actual,str) and actual==expected
