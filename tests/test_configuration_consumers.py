"""Consumer oracles for optional configuration controls (no SQL mocks as SQL proof)."""
import itertools
from pathlib import Path
import pytest
from db_ingest.config import load_config,effective_for_file
from db_ingest.capture import capture_source
from db_ingest.parsers.delimited import decide,parse
from db_ingest.profile import profile_records
from db_ingest.models import RecordEnvelope,FieldValue
from db_ingest.discovery import discover
from db_ingest.planning import map_destination
from db_ingest.errors import IngestError

@pytest.mark.parametrize('case,quoted,empty,trim',itertools.product([False,True],repeat=4))
def test_null_controls_interact_without_losing_quote_provenance(tmp_path,case,quoted,empty,trim):
 c=load_config(environ={});c['parsing'].update(header='present',delimiter=',',quotechar='"',encoding='utf-8',null_tokens=['NIL'],null_tokens_case_sensitive=case,null_tokens_in_quoted_fields=quoted,empty_string_is_null=empty,trim_whitespace=trim)
 raw=b'a,b,c,d,e\nNIL,nil,"NIL",,  x  \n';p=tmp_path/'x.csv';p.write_bytes(raw)
 a=capture_source(p,tmp_path,tmp_path/'work');rs=list(parse(a,decide(a,c),c));fs=rs[1].fields
 assert [(f.state,f.text) for f in fs]==[('NULL',None),('VALUE','nil') if case else ('NULL',None),('NULL',None) if quoted else ('VALUE','NIL'),('NULL',None) if empty else ('EMPTY',''),('VALUE','x' if trim else '  x  ')]
 assert fs[2].quoted and Path(a.path).read_bytes()==raw

@pytest.mark.parametrize('recursive,include,exclude,expected',[
 (False,['**/*.csv'],[],['a.csv']),
 (True,['**/*.csv'],[],['a.csv','nested/b.csv']),
 (True,['nested/*'],[],['nested/b.csv','nested/c.txt']),
 (True,['**/*'],['**/*.txt','nested/**'],['a.csv']),
])
def test_discovery_controls_select_exact_files_and_always_exclude_work(tmp_path,recursive,include,exclude,expected):
 for name in ['a.csv','nested/b.csv','nested/c.txt','work/hidden.csv']:
  p=tmp_path/name;p.parent.mkdir(exist_ok=True);p.write_text('x')
 c=load_config(overrides={'source.root':str(tmp_path),'source.path':str(tmp_path),'source.recursive':recursive,'source.include':include,'source.exclude':exclude,'preservation.work_directory':str(tmp_path/'work')},environ={})
 assert [p.relative_to(tmp_path).as_posix() for p in discover(c)]==expected

@pytest.mark.parametrize('headroom,nullable,expected',[(0,False,'nvarchar(3)'),(100,True,'nvarchar(6)'),(20,True,'nvarchar(4)')])
def test_inference_controls_change_real_profile(headroom,nullable,expected):
 c=load_config(overrides={'schema.text_length_headroom_percent':headroom,'schema.new_columns_nullable':nullable},environ={})
 r=RecordEnvelope('x',1,0,1,1,1,[FieldValue('VALUE','abc')])
 p=profile_records([r],['label'],c)[0]
 assert p.sql_type==expected and p.nullable is nullable

@pytest.mark.parametrize('allowed',[False,True])
def test_max_type_permission_controls_long_text(allowed):
 c=load_config(overrides={'schema.allow_max_types':allowed},environ={})
 r=RecordEnvelope('x',1,0,1,1,1,[FieldValue('VALUE','x'*4001)])
 if allowed:assert profile_records([r],['v'],c)[0].sql_type=='nvarchar(max)'
 else:
  with pytest.raises(IngestError,match='MAX'):profile_records([r],['v'],c)

@pytest.mark.parametrize('policy,expected',[('text','nvarchar(4)'),('numeric','tinyint')])
def test_leading_zero_policy_changes_inferred_type(policy,expected):
 c=load_config(overrides={'schema.leading_zero_policy':policy},environ={})
 r=RecordEnvelope('x',1,0,1,1,1,[FieldValue('VALUE','001')])
 assert profile_records([r],['v'],c)[0].sql_type==expected

@pytest.mark.parametrize('strategy',['single','per_top_level_directory','per_directory','per_file'])
def test_database_mapping_strategies_keep_stable_subtree_identity(strategy):
 c=load_config(overrides={'mapping.name_collision':'stable_hash_suffix','mapping.database_strategy':strategy,'mapping.database_prefix':'Import_','mapping.per_file_database_table':'fixed_data','destination.database':'Landing'},environ={})
 result=map_destination(c,'north/sub/a.csv')
 assert result['schema']=='dbo'
 if strategy=='single':assert result['database']=='Landing'
 else:assert result['database'].startswith('Import_north')
 if strategy=='per_file':assert result['table']=='fixed_data'
 c['source']['path']='arbitrary-subtree'
 assert map_destination(c,'north/sub/a.csv')==result

@pytest.mark.parametrize('policy',['root_database','error'])
def test_root_file_policy_has_observable_effect(policy):
 c=load_config(overrides={'mapping.name_collision':'stable_hash_suffix','mapping.database_strategy':'per_directory','mapping.root_files':policy,'mapping.root_database':'RootDB'},environ={})
 if policy=='error':
  with pytest.raises(IngestError) as e:map_destination(c,'a.csv')
  assert e.value.code=='ROOT_FILE_POLICY'
 else:assert map_destination(c,'a.csv')['database']=='RootDB'

RULE_VALUES={'format':'delimited','encoding':'cp1252','delimiter':'||','quotechar':'@@','escapechar':'!!','header':'absent','null_tokens':['nil'],'empty_string_is_null':True,'trim_whitespace':True,'doublequote':False,'nel_is_newline':True,'column_names':['x','y'],'record_terminator':'<END>','allow_multiline_fields':False}
@pytest.mark.parametrize('key,value',RULE_VALUES.items(),ids=RULE_VALUES)
def test_every_scalar_file_rule_applies_only_to_matching_path_and_respects_cli(key,value):
 c=load_config(environ={});c['file_rules']=[{'glob':'folder_a/*.csv',key:value}]
 section='source' if key=='format' else 'parsing'
 assert effective_for_file(c,'folder_a/a.csv')[section][key]==value
 assert effective_for_file(c,'other/a.csv')[section][key]==c[section][key]
 c['_provenance'][section+'.'+key]='cli'
 assert effective_for_file(c,'folder_a/a.csv')[section][key]==c[section][key]

@pytest.mark.parametrize('value,expected',[('quarantine','rejected'),('preserve','rejected'),('explicitly_skip','excluded')])
def test_blank_record_policy_observable(tmp_path,value,expected):
 c=load_config(overrides={'parsing.header':'present','parsing.delimiter':',','parsing.blank_record_policy':value},environ={})
 p=tmp_path/'a.csv';p.write_bytes(b'a\n\nend\n');a=capture_source(p,tmp_path,tmp_path/'work')
 rows=list(parse(a,decide(a,c),c))
 assert rows[1].kind==expected
 if value=='preserve':assert rows[1].errors[0]['code']=='BLANK_PRESERVED_UNIMPORTED'

@pytest.mark.parametrize('jitter,factor,expected_seconds',[(False,1,8),(True,.5,4)])
def test_retry_backoff_maximum_and_jitter_affect_actual_consumer(monkeypatch,jitter,factor,expected_seconds):
 from db_ingest.adapters.mssql import control
 import time,random
 clock=[0.0];random_calls=[];attempts=[]
 monkeypatch.setattr(time,'monotonic',lambda:clock[0])
 monkeypatch.setattr(time,'sleep',lambda seconds:clock.__setitem__(0,clock[0]+seconds))
 def choose_random(low,high):random_calls.append((low,high));return factor
 monkeypatch.setattr(random,'uniform',choose_random)
 @control._metadata_retry
 def operation(config):
  attempts.append(1)
  if len(attempts)<4:raise IngestError('DEADLOCK','authored retry',4,{'native_code':1205})
  return {}
 c=load_config(overrides={'retry.max_attempts':4,'retry.initial_backoff_seconds':2,'retry.maximum_backoff_seconds':3,'retry.jitter':jitter},environ={})
 assert operation(c)['metadata_attempts']==4
 assert clock[0]==pytest.approx(expected_seconds)
 assert random_calls==([(0.5,1.0)]*3 if jitter else [])

@pytest.mark.parametrize('detect',[False,True])
def test_tool_version_detection_switch_controls_native_process_calls(tmp_path,monkeypatch,detect):
 from types import SimpleNamespace
 from db_ingest.adapters.mssql import connect
 calls=[]
 monkeypatch.setattr(connect.importlib,'import_module',lambda name:SimpleNamespace(version='test',drivers=lambda:['ODBC Driver 18 for SQL Server']))
 monkeypatch.setattr(connect,'tool_info',lambda path,args,*rest:calls.append((path,args)) or {'available':False})
 c=load_config(overrides={'source.root':str(tmp_path),'source.path':str(tmp_path),'preservation.work_directory':str(tmp_path),'tools.detect_versions':detect,'tools.bcp_path':'chosen-bcp','tools.sqlcmd_path':'chosen-sqlcmd'},environ={})
 report=connect.doctor(c)
 assert report['exit_code']==0
 assert calls==([('chosen-bcp',['-v']),('chosen-sqlcmd',['-?'])] if detect else [])
