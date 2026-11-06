"""Create a scalable schema-free CSV with final-row type/length outliers.

Only stdlib. Does not import the application's parser, inference or converters.
The physical header deliberately lacks its fourth name; use the printed column_names.
"""
import argparse
import hashlib
import json
from pathlib import Path

NAMES=['code','mixed','amount','label']

def values(index,rows,padding):
    return [f'{index:09d}','LATE TEXT' if index==rows-1 else str(index%100),
            '9999.1234567890123456789' if index==rows-1 else '1.25',
            '\U0001f600'*2100 if index==rows-1 else 'x'*padding]

def generate(folder,rows=20001,padding=2048):
    if rows<2 or not 1<=padding<=4000:raise ValueError('rows >=2 and padding1..4000 required')
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    source=folder/'late-types.csv'
    digest=hashlib.sha256();size=0
    with source.open('xb') as output:
        def write(text):
            nonlocal size
            data=text.encode('utf-8');output.write(data);digest.update(data);size+=len(data)
        write('code,mixed,amount\r\n')
        for index in range(rows):write(','.join(values(index,rows,padding))+'\r\n')
    report={'source':source.name,'rows':rows,'padding':padding,'bytes':size,'sha256':digest.hexdigest(),
            'encoding':'utf-8','delimiter':',','header':'present','column_names':NAMES,
            'expected_sql_types':['nvarchar(11)','nvarchar(11)','decimal(23,19)','nvarchar(max)'],
            'oracle':'Authored formulas; complete ordered target rows must equal values(index,rows,padding). Original three-field header is retained.',
            'tail':values(rows-1,rows,padding)}
    with (folder/'independent-manifest.json').open('x',encoding='utf-8') as output:json.dump(report,output,ensure_ascii=True,indent=2)
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--rows',type=int,default=20001)
    parser.add_argument('--padding',type=int,default=2048)
    args=parser.parse_args();report=generate(args.output,args.rows,args.padding)
    print(json.dumps({k:v for k,v in report.items() if k!='tail'},indent=2))
