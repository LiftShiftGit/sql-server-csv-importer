"""Maintain the public test inventory from frozen test contracts, not runtime validators."""
import json,runpy
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
API=runpy.run_path(str(ROOT/'tests/test_configuration_contract.py'))
F=API['FIELDS'];fixed=API['FIXED'];choices=API['CHOICES']
consumer={
 'destination':['test_sql_engineering.py','test_sql_permissions.py','test_sql_filesystem_doctor.py'],
 'source':['test_configuration_consumers.py','test_parser_engineering.py'],
 'mapping':['test_folder_presets.py','integration/test_live_folder_presets.py','test_filename_naming.py','integration/test_live_filename_naming.py','test_configuration_consumers.py','test_grouping_engineering.py','test_target_review.py'],
 'parsing':['test_configuration_consumers.py','test_independent_parser.py','test_parser_tokens.py','test_missing_headers_and_inference.py','test_legacy_corpus.py'],
 'schema':['test_configuration_consumers.py','test_independent_types.py','test_column_controls.py','test_schema_diff.py','test_projections.py'],
 'load':['test_independent_batch_types.py','test_parallel_engineering.py','test_affected_file_policy.py','test_lead.py'],
 'tools':['test_configuration_consumers.py','test_sql_process_output.py','test_sql_engineering.py'],
 'credentials':['test_independent_cli.py','test_sql_engineering.py'],
 'preservation':['test_capture_faults_cli.py','test_independent_local_evidence.py','test_sql_filesystem_doctor.py'],
 'audit':['test_reporting.py','test_source_audit.py','test_diagnostic_envelope.py'],
 'verification':['test_independent_pipeline.py','test_independent_local_evidence.py'],
 'parallel':['test_parallel_engineering.py','test_windows_sampler_allocations.py'],
 'limits':['test_parser_engineering.py','test_parser_tokens.py','test_parallel_engineering.py','test_sql_process_output.py'],
 'retry':['test_configuration_consumers.py','test_control_retry.py','test_pipeline_retry.py'],
 'interaction':['test_independent_cli.py','test_operator_decisions.py'],
 'xml':['test_parser_structured.py'],'html':['test_parser_structured.py'],
}
rows=[]
for path,default in F.items():
 kind='fixed' if path in fixed else 'enum' if path in choices else 'integer' if type(default) is int else 'boolean' if type(default) is bool else 'string/list'
 rows.append({'field':path,'default':default,'contract_class':kind,'default_roundtrip':'test_every_default_roundtrips_through_toml','invalid_types':['test_every_field_rejects_wrong_type[toml]','test_every_field_rejects_wrong_type[override]'],'value_checks':('test_every_fixed_capability_refuses_alternate' if kind=='fixed' else 'test_all_documented_enum_alternatives + test_enum_typos_fail' if kind=='enum' else 'test_integer_options_cannot_accept_bool_string_float + test_integer_negative_boundaries + test_integer_zero_boundaries' if kind=='integer' else 'test_boolean_alternatives_normalize_for_every_mutable_switch' if kind=='boolean' else 'format/consumer-specific; free strings/lists have no finite exhaustive value set'),'related_consumer_suites':['tests/'+x for x in consumer[path.split('.')[0]]]})
result={'scope':'Every global field has a default/type contract test; fixed values, enums, integer lower boundaries and mutable boolean aliases have explicit tests. Related suites are navigation, not a per-value behavioral certificate. No all-combinations claim.','global_fields_including_version':len(rows)+1,'config_version_test':'test_version_is_strict_integer','fields':rows,'rule_keys':sorted(API['RULE_VALUES_FOR_INVENTORY']|{'glob','columns'}),'column_keys':sorted(API['COLUMN_EXAMPLES']),'rule_column_tests':['test_rule_and_column_inventory_cannot_silently_expand','test_every_column_option_resolves_and_profiles','test_every_rule_scalar_rejects_wrong_type'],'integration_instructions':'testing.md','deployment_script':'../scripts/check_deployment.py'}
(ROOT/'docs/configuration-test-matrix.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'global_fields':len(rows)+1,'rule_keys':len(result['rule_keys']),'column_keys':len(result['column_keys'])}))
