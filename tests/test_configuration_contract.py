"""Public field/type contract plus independently specified choices and boundaries.

The frozen inventory catches additions/removals; consumer expectations below and
in the parser/SQL suites are separate from that default snapshot.
"""
import copy,json
from pathlib import Path
import pytest
from db_ingest.config import load_config,DEFAULTS,effective_for_file
from db_ingest.errors import IngestError

FIELDS=json.loads(Path(__file__).with_name('configuration_fields.json').read_text())
FIXED={
 'load.fallback_engine':'odbc',
 'preservation.hash':'sha256','preservation.source_change_policy':'abort',
 'preservation.retain_originals':'indefinitely','preservation.retain_journal':'indefinitely',
 'preservation.retain_failure_artifacts':'indefinitely','preservation.cleanup_verified_transport_spools':False,
 'parsing.ambiguity_policy':'pause','parsing.ragged_row_policy':'quarantine',
 'parsing.repeated_header_policy':'preserve_as_data','parsing.repair_policy':'none',
 'schema.on_type_conflict':'retype_text','schema.on_append_schema_mismatch':'pause',
 'schema.numeric_policy':'exact','schema.date_policy':'iso_unambiguous',
 'schema.timezone_policy':'preserve','schema.fractional_second_policy':'preserve_exact',
 'schema.text_fallback':'nvarchar','schema.inference':'full',
 'verification.unresolved_policy':'block_affected_publication',
 'interaction.on_required_decision':'pause','limits.oversized_record_policy':'preserve_and_quarantine',
 'audit.event_timestamps':'utc','audit.failure_table_pattern':'{table}_fail_{utc_timestamp}_{run_short}',
 'audit.json_log':True,'audit.console_log_level':'INFO','audit.show_raw_errors':False,
 'audit.log_source_values':False,'credentials.password_source':'prompt',
 'tools.capture_stdout_stderr':True,'tools.sqlcmd_variant':'odbc',
 'load.replace_requires_complete_input':True,'load.retain_previous_generation':True,
 'parallel.strategy':'files','parallel.serialize_same_target_publication':True,
 'preservation.capture_original_bytes':True,'preservation.flush_durable_checkpoints':True,
 'parsing.strict_decoding':True,'audit.local_failure_fallback':True,
 'retry.require_commit_resolution':True,'schema.infer_defaults':False,
 'credentials.redact_secrets':True,'xml.allow_entities':False,'xml.allow_network':False,
}
CHOICES={
 'mapping.name_collision':['stable_hash_suffix','filename_error','path_error'],
 'destination.type':['mssql'],'destination.authentication':['sql','integrated'],
 'source.format':['auto','delimited','xml','html'],
 'mapping.database_strategy':['single','per_top_level_directory','per_directory','per_file'],
 'mapping.table_strategy':['per_file','per_directory'],
 'mapping.existing_target_strategy':['owned_landing','mapped_business'],
 'mapping.identity_policy':['generate','preserve'],
 'mapping.directory_merge_schema':['strict','union_by_name'],
 'mapping.root_files':['root_database','error'],'parsing.header':['auto','present','absent'],
 'load.mode':['append','replace','create_only'],'load.engine':['auto','bcp','odbc'],
 'load.preferred_engine':['bcp','odbc'],'load.partial_publish':['new_or_append','none'],
 'load.replace_scope':['complete_group','selected_subset'],
 'schema.leading_zero_policy':['text','numeric'],'verification.level':['full','counts','sample'],
 'parsing.blank_record_policy':['quarantine','preserve','explicitly_skip'],
 'schema.on_length_overflow':['ask','quarantine','abort','widen','retype_text'],
}

def at(c,path):
 s,k=path.split('.');return c[s][k]

def lit(v):return json.dumps(v,ensure_ascii=True)


def test_inventory_has_every_global_field():
 c=load_config(environ={})
 actual={s+'.'+k for s,d in c.items() if isinstance(d,dict) and not s.startswith('_') for k in d}
 assert actual==set(FIELDS)
 assert len(FIELDS)==153 # plus separately tested config_version

@pytest.mark.parametrize('path,value',FIELDS.items(),ids=FIELDS)
def test_every_default_roundtrips_through_toml(tmp_path,path,value):
 s,k=path.split('.');p=tmp_path/'config.toml';p.write_text('['+s+']\n'+k+'='+lit(value),encoding='utf-8')
 assert at(load_config(p,environ={}),path)==value

@pytest.mark.parametrize('path,value',FIELDS.items(),ids=FIELDS)
@pytest.mark.parametrize('entry',['toml','override'])
def test_every_field_rejects_wrong_type(tmp_path,path,value,entry):
 bad={'wrong':'table'} if isinstance(value,list) else []
 s,k=path.split('.')
 with pytest.raises(IngestError):
  if entry=='override':load_config(overrides={path:bad},environ={})
  else:
   p=tmp_path/'bad.toml';p.write_text('['+s+']\n'+k+'='+ ('{wrong="table"}' if isinstance(bad,dict) else '[]'))
   load_config(p,environ={})

@pytest.mark.parametrize('path,required',FIXED.items(),ids=FIXED)
def test_every_fixed_capability_refuses_alternate(path,required):
 with pytest.raises(IngestError):load_config(overrides={path:not required if isinstance(required,bool) else 'unsupported'},environ={})

@pytest.mark.parametrize('path,value',[(p,v) for p,vs in CHOICES.items() for v in vs])
def test_all_documented_enum_alternatives(path,value):
 overrides={path:value}
 if path in ('mapping.database_strategy','mapping.table_strategy','mapping.existing_target_strategy'):overrides['mapping.name_collision']='stable_hash_suffix'
 assert at(load_config(overrides=overrides,environ={}),path)==value

@pytest.mark.parametrize('path',CHOICES)
def test_enum_typos_fail(path):
 with pytest.raises(IngestError):load_config(overrides={path:'typo'},environ={})

INTS=[p for p,v in FIELDS.items() if type(v) is int]
@pytest.mark.parametrize('path',INTS)
@pytest.mark.parametrize('bad',[True,False,'7',1.5])
def test_integer_options_cannot_accept_bool_string_float(path,bad):
 with pytest.raises(IngestError):load_config(overrides={path:bad},environ={})

@pytest.mark.parametrize('path',INTS)
def test_integer_negative_boundaries(path):
 with pytest.raises(IngestError):load_config(overrides={path:-1},environ={})

@pytest.mark.parametrize('path',INTS)
def test_integer_zero_boundaries(path):
 if path in ('schema.text_length_headroom_percent','preservation.minimum_free_disk_bytes'):
  assert at(load_config(overrides={path:0},environ={}),path)==0
 else:
  with pytest.raises(IngestError):load_config(overrides={path:0},environ={})

@pytest.mark.parametrize('path',[p for p,v in FIELDS.items() if type(v) is bool and p not in FIXED])
@pytest.mark.parametrize('value,expected',[('yes',True),('no',False),('1',True),('0',False)])
def test_boolean_alternatives_normalize_for_every_mutable_switch(path,value,expected):
 assert at(load_config(overrides={path:value},environ={}),path) is expected

@pytest.mark.parametrize('version',[True,False,1.0,'1',2])
def test_version_is_strict_integer(tmp_path,version):
 p=tmp_path/'c.toml';p.write_text('config_version='+lit(version))
 with pytest.raises(IngestError):load_config(p,environ={})

@pytest.mark.parametrize('path',['typo.option','load.typo','load','load.engine.extra'])
def test_unknown_overrides_fail_as_config_errors(path):
 with pytest.raises(IngestError):load_config(overrides={path:1},environ={})

@pytest.mark.parametrize('value',['bad',{},[1],True])
def test_rule_container_errors_are_config_errors(tmp_path,value):
 c=load_config(environ={});c['file_rules']=value
 with pytest.raises(IngestError):effective_for_file(c,'x.csv')

@pytest.mark.parametrize('value',['bad',{},[1],True])
def test_column_container_errors_are_config_errors(value):
 c=load_config(environ={});c['file_rules']=[{'glob':'x.csv','columns':value}]
 with pytest.raises(IngestError):effective_for_file(c,'x.csv')

def test_backoff_bounds_are_consistent():
 with pytest.raises(IngestError):load_config(overrides={'retry.initial_backoff_seconds':16,'retry.maximum_backoff_seconds':15},environ={})

@pytest.mark.parametrize('key,suffix,value,expected',[
 ('server','SERVER','local,1444','local,1444'),('database','DATABASE','other','other'),
 ('driver','DRIVER','test-driver','test-driver'),('control_database','CONTROL_DATABASE','control','control'),
 ('connect_timeout_seconds','CONNECT_TIMEOUT','47',47),('encrypt','ENCRYPT','false','no'),
 ('trust_server_certificate','TRUST_SERVER_CERTIFICATE','1','yes'),
 ('username','USERNAME','test-user','test-user'),('password','PASSWORD','private-test','private-test')])
def test_all_environment_fields_and_cli_precedence(key,suffix,value,expected):
 assert load_config(environ={'DB_'+suffix:value})['destination'][key]==expected
 override=19 if key=='connect_timeout_seconds' else 'no' if key in ('encrypt','trust_server_certificate') else 'cli'
 assert load_config(environ={'DB_'+suffix:value},overrides={'destination.'+key:override})['destination'][key]==override

@pytest.mark.parametrize('key',['source_name','target_name','target_type','binary_encoding','attachment_root','collation','text_encoding'])
@pytest.mark.parametrize('value',[False,3,[],{}])
def test_column_string_options_reject_wrong_types(key,value):
 from db_ingest.column_policy import validate_spec
 with pytest.raises(IngestError):validate_spec({key:value})

@pytest.mark.parametrize('value',['','x'*129,'\U0001f600'*65,'bad\x00name'])
def test_target_name_rejects_invalid_identifiers_before_sql(value):
 from db_ingest.column_policy import validate_spec
 with pytest.raises(IngestError):validate_spec({'target_name':value})

def test_target_name_utf16_boundary_and_empty_source_header_are_legal():
 from db_ingest.column_policy import validate_spec
 validate_spec({'source_name':'','source_ordinal':1,'target_name':'\U0001f600'*64})

COLUMN_EXAMPLES={
 'source_name':{'source_name':'value'},'source_ordinal':{'source_ordinal':1},
 'target_name':{'target_name':'renamed'},'target_type':{'target_type':'int'},
 'nullable':{'nullable':False},
 'binary_encoding':{'target_type':'varbinary(max)','binary_encoding':'hex'},
 'temporal_format':{'target_type':'date','temporal_format':'iso8601'},
 'timezone_policy':{'target_type':'datetimeoffset(7)','timezone_policy':'preserve'},
 'null_tokens':{'null_tokens':['nil']},
 'length':{'target_type':'nvarchar','length':4},
 'precision':{'target_type':'decimal','precision':5,'scale':2},
 'scale':{'target_type':'decimal(5,3)','scale':2},
 'date_format':{'target_type':'date','date_format':'%Y%m%d'},
 'fractional_second_policy':{'target_type':'time(7)','fractional_second_policy':'preserve_exact'},
 'truncation':{'target_type':'nvarchar(2)','truncation':'right'},
 'rounding':{'target_type':'decimal(3,1)','rounding':'half_even'},
 'attachment_root':{'target_type':'varbinary(max)','binary_encoding':'attachment_path','attachment_root':'D:\\attachments'},
 'collation':{'target_type':'varchar(5)','collation':'Latin1_General_100_BIN2','text_encoding':'cp1252'},
 'text_encoding':{'target_type':'varchar(5)','collation':'Latin1_General_100_BIN2_UTF8','text_encoding':'utf-8'},
}

def test_rule_and_column_inventory_cannot_silently_expand():
 from db_ingest.config import RULE_KEYS,COLUMN_KEYS
 assert COLUMN_KEYS==set(COLUMN_EXAMPLES)
 assert RULE_KEYS==set(RULE_VALUES_FOR_INVENTORY)|{'glob','columns'}

RULE_VALUES_FOR_INVENTORY={'format','encoding','delimiter','quotechar','escapechar','header','null_tokens','empty_string_is_null','trim_whitespace','doublequote','nel_is_newline','column_names','record_terminator','allow_multiline_fields'}

@pytest.mark.parametrize('key,example',COLUMN_EXAMPLES.items(),ids=COLUMN_EXAMPLES)
def test_every_column_option_resolves_and_profiles(key,example):
 from db_ingest.config import resolve_columns
 from db_ingest.models import FieldValue,RecordEnvelope
 from db_ingest.profile import profile_records
 c=load_config(overrides={'schema.allow_lossy_projection':True},environ={})
 spec={'source_ordinal':1,**example}
 c['file_rules']=[{'glob':'a.csv','columns':[spec]}]
 resolved=effective_for_file(c,'a.csv');columns=resolve_columns(resolved,['value']);resolved['columns']=columns
 assert columns[0][key]==example[key]
 r=RecordEnvelope('x',1,0,1,1,1,[FieldValue('VALUE','1')])
 p=profile_records([r],['value'],resolved)[0]
 if key=='target_name':assert p.target_name=='renamed'
 if key=='nullable':assert p.nullable is False
 if key=='length':assert p.sql_type=='nvarchar(4)'
 if key in ('precision','scale'):assert p.sql_type=='decimal(5,2)'

@pytest.mark.parametrize('key',sorted(RULE_VALUES_FOR_INVENTORY))
def test_every_rule_scalar_rejects_wrong_type(key):
 c=load_config(environ={});c['file_rules']=[{'glob':'x.csv',key:{'bad':'table'}}]
 with pytest.raises(IngestError):effective_for_file(c,'x.csv')
