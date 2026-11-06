"""Stable diagnostics; unknown observations remain explicit rather than invented."""
from datetime import datetime,timezone

class IngestError(Exception):
    def __init__(self, code, message, exit_code=2, context=None):
        super().__init__(message)
        self.code, self.exit_code, self.context = code, exit_code, context or {}


def _category(code,message):
    text=(str(code)+' '+str(message)).lower()
    if any(x in text for x in ('length','units exceed','bytes exceed')):return 'length'
    if any(x in text for x in ('encoding','decode','unicode')):return 'encoding'
    if any(x in text for x in ('precision','round','decimal','fractional')):return 'type/range/precision'
    if any(x in text for x in ('nullab','required column')):return 'nullability/default'
    if any(x in text for x in ('width','field count','fields, observed')):return 'row-width'
    if any(x in text for x in ('quote','framing','unterminated')):return 'framing/quoting'
    if any(x in text for x in ('header','dialect','delimiter')):return 'header/dialect ambiguity'
    if any(x in text for x in ('type','range','binary','base64','hex','guid','date','time','integer')):return 'type/range/precision'
    if any(x in text for x in ('field_limit','record_limit','budget','oversized')):return 'resource/representation limit'
    return 'source interpretation'


def complete_record_errors(record,stage='source_interpretation',columns=(),disposition=None):
    """Enrich new retained errors once; never rewrite existing sealed readback."""
    profiles={c.ordinal:c for c in columns}
    for error in record.errors:
        ordinal=error.get('column_ordinal');column=profiles.get(ordinal)
        field=record.fields[ordinal-1] if isinstance(ordinal,int) and not isinstance(ordinal,bool) and 0<ordinal<=len(record.fields) else None
        error.setdefault('category',_category(error.get('code','UNKNOWN'),error.get('message','')))
        error.setdefault('stage',stage)
        error.setdefault('record_id',record.record_id)
        error.setdefault('column_ordinal',ordinal)
        error.setdefault('original_name',column.original_name if column else error.get('column_name'))
        error.setdefault('target_name',column.target_name if column else None)
        error.setdefault('expected',{'sql_type':column.sql_type,'nullable':column.nullable} if column else None)
        # Original fields are already retained in their ordered payload and archive.
        # Diagnostics record measurements rather than duplicating large source values.
        units=None
        if field is not None and field.text is not None:
            try:units=len(field.text.encode('utf-16-le'))//2
            except UnicodeError:pass # Invalid scalar representation stays explicitly unmeasurable.
        observed=None if field is None else {'state':field.state,'characters':len(field.text) if field.text is not None else None,'utf16_units':units,'original_retained_in':'ordered record fields and source archive'}
        error.setdefault('observed',observed)
        error.setdefault('remediation','Review the retained original and declared '+('column type/representation policy; correct through linked reimport or explicitly quarantine.' if column else 'source format/policy; correct through linked reimport.'))
        error.setdefault('policy',{'sql_type':column.sql_type,**{key:column.stats.get(key) for key in ('binary_encoding','text_encoding','temporal_format','timezone_policy','null_tokens')}} if column else None)
        error.setdefault('policy_reference','retained run interpretation/configuration and source provenance')
        current=disposition or record.kind
        record_exit={'rejected':3,'unresolved':5,'pending':2}.get(current)
        if error.get('exit_code') is None:error['exit_code']=record_exit
        error.setdefault('exit_code_basis','current record outcome; run aggregate is reported separately' if record_exit is not None else 'unknown until a current disposition is assigned; run aggregate is reported separately')
        if record_exit is not None:error['exit_code_basis']='current record outcome; run aggregate is reported separately'
        error.setdefault('validator','db_ingest local validation')
        error.setdefault('tool',None)
        error.setdefault('retryable',False)
        error.setdefault('recorded_at_utc',datetime.now(timezone.utc).isoformat())
        error.setdefault('provider',None)
        error.setdefault('sqlstate',None)
        error.setdefault('native_code',None)
        error.setdefault('raw_diagnostic',None)
        error.setdefault('provider_diagnostic_completeness','not_applicable_local_validation')
    return record
