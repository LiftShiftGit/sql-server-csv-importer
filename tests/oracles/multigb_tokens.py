"""Independent multi-token sibling of the ordinary >2GiB source oracle."""
from pathlib import Path
import runpy,hashlib,json
BASE=runpy.run_path(str(Path(__file__).with_name('multigb_csv.py')))
NAMES=BASE['NAMES'];TYPES=BASE['TYPES'];REJECT_INDEXES=BASE['REJECT_INDEXES']

def cells(index,rows,padding):
    result=BASE['cells'](index,rows,padding)
    if index%4096==0 and index!=rows-1:
        prefix=f'row-{index:09d}: literal||delimiter @@qualifier@@ escaped!!token <EOR> record token\r\n'
        result[3]=prefix+'x'*(padding-len(prefix))
    return result

def expected(index,rows,padding):
    from decimal import Decimal
    code,mixed,amount,note=cells(index,rows,padding)
    return [code,mixed,Decimal(amount),note]

def generate(folder,rows=131073,padding=16384):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    source=folder/'source.csv';manifest=folder/'independent-source.json'
    if source.exists() or manifest.exists():raise FileExistsError('Use a fresh token source folder')
    digest=hashlib.sha256();size=0;breaks=0
    with source.open('xb') as output:
        def write(text):
            nonlocal size,breaks
            raw=text.encode('utf-8');output.write(raw);digest.update(raw);size+=len(raw)
            breaks+=text.count('\r')+text.count('\n')-text.count('\r\n')
        def row(values):return '||'.join('@@'+v.replace('!!','!!!!').replace('@@','@@@@')+'@@' for v in values)+'<EOR>'
        write(row(['code','mixed','amount']))
        for index in range(rows):
            values=cells(index,rows,padding)
            if index in REJECT_INDEXES:values=values[:3] if index!=100000 else values+['unexpected fifth field']
            write(row(values))
    rejected=sorted(i for i in REJECT_INDEXES if i<rows)
    report={'source':str(source.resolve()),'source_bytes':size,'sha256':digest.hexdigest(),'rows':rows,'padding':padding,
        'rejected_indexes':rejected,'rejected_ordinals':[i+2 for i in rejected],'accepted_rows':rows-len(rejected),
        'raw_header':['code','mixed','amount'],'column_names':NAMES,'expected_types':TYPES,
        'physical_lines':breaks+1,'logical_records':rows+1,
        'parsing':{'delimiter':'||','quotechar':'@@','escapechar':'!!','doublequote':True,'record_terminator':'<EOR>','allow_multiline_fields':True},
        'oracle':'Authored multi-token writer/formula independent of importer. Ordinary notes contain literal qualifier, field and record tokens; last-row changes and rejected widths match sibling CSV oracle.'}
    manifest.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8');return report
