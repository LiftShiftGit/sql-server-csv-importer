"""Diagnostic enrichment preserves original errors and avoids invented observations."""
from copy import deepcopy
from db_ingest.errors import complete_record_errors
from db_ingest.models import ColumnProfile,SchemaPlan,RecordEnvelope,FieldValue
from db_ingest.staging import validate_record

def test_conversion_envelope_has_exact_expected_observed_and_stable_time():
    column=ColumnProfile(1,'original','mapped','nvarchar(2)',False)
    row=RecordEnvelope('record',2,10,20,2,2,[FieldValue('VALUE','😀a')])
    plan=SchemaPlan('db','dbo','table',[column],'a','b','c')
    errors=validate_record(plan,row)
    assert len(errors)==1 and row.errors==[]
    error=errors[0]
    assert error['stage']=='conversion_validation' and error['category']=='length'
    assert error['original_name']=='original' and error['target_name']=='mapped'
    assert error['expected']=={'sql_type':'nvarchar(2)','nullable':False}
    assert error['observed']['characters']==2 and error['observed']['utf16_units']==3
    assert error['retryable'] is False and error['provider'] is None
    assert error['provider_diagnostic_completeness']=='not_applicable_local_validation'
    row.errors=errors;before=deepcopy(errors)
    complete_record_errors(row,columns=[column])
    assert row.errors==before

def test_parser_unknowns_are_explicit_and_existing_provider_facts_preserved():
    error={'code':'UNCLOSED_QUOTE','message':'unterminated quoted tail','provider':'authored-test','sqlstate':'HY000','native_code':123,'raw_diagnostic':'exact provider text','provider_diagnostic_completeness':'unknown'}
    row=RecordEnvelope('r',1,0,12,1,None,[],errors=[error])
    complete_record_errors(row,stage='framing')
    assert error['category']=='framing/quoting' and error['stage']=='framing'
    assert error['expected'] is None and error['observed'] is None and error['original_name'] is None
    assert error['raw_diagnostic']=='exact provider text' and error['provider_diagnostic_completeness']=='unknown'
    assert error['recorded_at_utc'].endswith('+00:00')

def test_width_has_exact_counts_and_invalid_unicode_does_not_mask_error():
    column=ColumnProfile(1,'x','x','int')
    plan=SchemaPlan('db','dbo','x',[column],'a','b','c')
    row=RecordEnvelope('r',1,0,1,1,1,[])
    error=validate_record(plan,row)[0]
    assert error['expected']=={'field_count':1} and error['observed']=={'field_count':0}
    row.fields=[FieldValue('VALUE','\ud800')];row.errors=[{'code':'ENCODING','message':'invalid scalar','column_ordinal':1}]
    complete_record_errors(row)
    assert row.errors[0]['observed']['utf16_units'] is None
