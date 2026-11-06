"""Independent authored >2GiB fully headerless CSV oracle; never imports importer code."""
import hashlib,json
from pathlib import Path
from decimal import Decimal

NAMES=['code','mixed','amount','note']
REJECT_INDEXES={4095,65535,100000}
TYPES=['nvarchar(11)','nvarchar(11)','decimal(23,19)','nvarchar(max)']

def cells(index,rows,padding):
    prefix=f'row-{index:09d}:'
    if index%4096==0:prefix+='comma, pipe| quote" CRLF\r\n'
    note=prefix+'x'*(padding-len(prefix))
    if index==rows-1:note='late unicode, "quoted"\r\n'+'\U0001f600'*20000
    return [f'{index:09d}','LATE TEXT' if index==rows-1 else str(index%100),
            '9999.1234567890123456789' if index==rows-1 else '1.25',note]

def expected(index,rows,padding):
    code,mixed,amount,note=cells(index,rows,padding)
    return [code,mixed,Decimal(amount),note]

def generate(folder,rows=131073,padding=16384):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    source=folder/'source.csv';manifest=folder/'independent-source.json'
    if source.exists() or manifest.exists():raise FileExistsError('Use a fresh source folder or reuse the completed independent manifest')
    digest=hashlib.sha256();size=0;physical_breaks=0
    with source.open('xb') as output:
        def write(text):
            nonlocal size,physical_breaks
            data=text.encode('utf-8');output.write(data);digest.update(data);size+=len(data)
            physical_breaks+=text.count('\r')+text.count('\n')-text.count('\r\n')
        for index in range(rows):
            values=cells(index,rows,padding)
            if index in REJECT_INDEXES:
                values=values[:3] if index!=100000 else values+['unexpected fifth field']
            encoded=[('"'+v.replace('"','""')+'"') if any(c in v for c in ',"\r\n') else v for v in values]
            write(','.join(encoded)+'\r\n')
    rejected=sorted(i for i in REJECT_INDEXES if i<rows)
    report={'source':str(source.resolve()),'source_bytes':size,'sha256':digest.hexdigest(),'rows':rows,'padding':padding,
        'rejected_indexes':rejected,'rejected_ordinals':[i+1 for i in rejected],'accepted_rows':rows-len(rejected),
        'raw_header':[],'column_names':NAMES,'expected_types':TYPES,
        'physical_lines':physical_breaks,'logical_records':rows,
        'oracle':'Authored formula and raw writer; independent of importer parser/types/digests. Wrong widths are expected rejects, never padded.'}
    manifest.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    return report
