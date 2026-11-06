"""Generated categories exercise independently authored schemas and positive/negative outcomes."""
import json,os,runpy,subprocess,sys
from pathlib import Path
import pytest
from db_ingest.fixturegen import CATEGORIES,create_test_files
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_batch_types.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Explicit disposable SQL required')

@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('category',CATEGORIES)
def test_generated_category_full_authored_schema_values_outcomes_and_sql_absence(engine,category,rows=3):
 if engine not in API['ENGINES']:pytest.skip('Engine not selected')
 case=API['TypeCase'](engine=engine,batch_rows=7,batch_bytes=2097152)
 fixture_root=case.folder/'fixtures';manifest=create_test_files(fixture_root,[category],rows=rows)
 for fixture in manifest['fixtures']:
  # A subset manifest certifies exactly one selected source, preserving its authored
  # declarations/outcomes; no parser/inference/converter writes expected values.
  selected=fixture_root/(fixture['path']+'.manifest.json')
  selected.write_text(json.dumps({**manifest,'fixtures':[fixture]},ensure_ascii=True),encoding='utf-8')
  original=case.config.read_text(encoding='utf-8').split('[[file_rules]]')[0]
  # Keep only destination/engine/credentials/tool fields from the independently
  # selected disposable setup, supply every interpretation explicitly.
  import tomllib
  base=tomllib.loads(original)
  sections={k:v for k,v in base.items() if k in ['destination','load','credentials','tools']}
  sections.update(source={'root':str(fixture_root),'path':str(fixture_root/fixture['path']),'include':[fixture['path']],'format':'delimited'},parsing={'encoding':fixture['encoding'],'delimiter':fixture['delimiter'],'quotechar':fixture['quotechar'],'header':fixture['header']},preservation={'work_directory':str(case.folder/('work-'+fixture['path'].replace('.','_'))),'minimum_free_disk_bytes':1048576},parallel={'enabled':False},interaction={'non_interactive':True},schema={'on_length_overflow':'quarantine'})
  for key,value in fixture['overrides'].items():
   if key in ('xml','html'):sections[key]=value
   elif key=='format':sections['source']['format']=value
   else:sections['parsing'][key]=value
  text=''
  for section,values in sections.items():
   fields={k:v for k,v in values.items() if not isinstance(v,list) or not v or not isinstance(v[0],dict)}
   text+='['+section+']\n'+'\n'.join(k+'='+API['lit'](v) for k,v in fields.items())+'\n'
   for key,items in values.items():
    if isinstance(items,list) and items and isinstance(items[0],dict):
     for item in items:text+='[['+section+'.'+key+']]\n'+'\n'.join(k+'='+API['lit'](v) for k,v in item.items())+'\n'
  text+='[[file_rules]]\nglob='+API['lit'](fixture['path'])+'\n'
  assert fixture['sql_schema'] and fixture['expected_record_outcomes']
  for column in fixture['sql_schema']:text+='[[file_rules.columns]]\n'+'\n'.join(k+'='+API['lit'](v) for k,v in column.items())+'\n'
  case.config.write_text(text,encoding='utf-8')
  expected={'parsed':0,'rejected':3,'type_failure':3,'oversized':3,'unresolved':5,'encoding_failure':2,'unsupported_unsafe_xml':2,'unsupported_html_span':2,'unsupported_format':2,'header_only':2}[fixture['expected_outcome']]
  imported=case.cli('run','--new-intent','--no-parallel','--non-interactive',exit_code=expected)
  run_id=imported['run_id']
  result=case.cli('verify-test-files','--manifest',str(selected),'--run-id',run_id,'--database',case.d['database'],exit_code=0 if fixture['expected_outcome']=='parsed' else 3)
  assert result['sql_integration']=='EXECUTED_EXPLICIT_DISPOSABLE_DESTINATION' and result['errors']==[]
  assert result['unverified_outcomes']==[] and result['outcome_checks'][0]['outcome']==fixture['expected_outcome']
  if expected:assert result['expected_nonclean'] and result['state']=='FIXTURE_EXPECTED_FAILURES_VERIFIED'

@pytest.mark.parametrize('engine',['odbc','bcp'])
def test_generated_header_only_source_requires_explicit_no_data_and_absent_sql_receipt(engine):
 test_generated_category_full_authored_schema_values_outcomes_and_sql_absence(engine,'csv_large_file',rows=0)
