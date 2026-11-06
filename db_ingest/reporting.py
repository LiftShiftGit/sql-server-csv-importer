"""Bounded operator summaries; unknown measurements remain explicit NULL values."""
import json
from .config import hashes


def file_summary(journal,config,item):
    counts={**(item.get('counts') or {}),**journal.counts(item['run_id'],item['file_id'])}
    plan=item.get('plan') or {};receipt=item.get('receipt') or {};details=receipt.get('details') or {}
    decision=item.get('decision') or {};artifact=item['artifact']
    schema_diff=details.get('schema_diff') or details.get('evolution') or {}
    if not isinstance(schema_diff,dict):schema_diff={}
    actions=schema_diff.get('actions') or []
    schema_changes={'actions_preview':actions[:20],'actions_truncated':len(actions)>20,'full_evidence':'destination receipt / SchemaVersions; local journal'}
    phases=[]
    rows=journal.db.execute("SELECT json_extract(details,'$.phase'),SUM(json_extract(details,'$.elapsed_seconds')),COUNT(*),SUM(CASE WHEN json_extract(details,'$.succeeded')=1 THEN 1 ELSE 0 END) FROM events WHERE run_id=? AND file_id=? AND category='PHASE_TIMING' GROUP BY json_extract(details,'$.phase') ORDER BY 1",(item['run_id'],item['file_id']))
    for phase,seconds,attempts,succeeded in rows:
        if len(phases)>=32:break
        phases.append({'phase':phase,'elapsed_seconds_all_attempts':seconds,'attempts':attempts,'succeeded_attempts':succeeded})
    keys=('source_bytes','physical_line_breaks','physical_lines','logical_records','header_records','blank_records','parsed_data_records','known_data_records','known_parsed_data_records','known_blank_records','conversion_validated_records','sql_staged_records','sql_validated_records','record_error_events','pending','committed','rejected','excluded','header','unresolved')
    measured={key:counts.get(key) for key in keys};measured['source_bytes']=artifact['size']
    prior=details.get('previous_generation_proof',{}).get('all_previous_values',{}).get('count')
    if prior is None:prior=details.get('business_previous_generation',{}).get('all_previous_values',{}).get('count')
    if prior is None:prior=schema_diff.get('old_projection_before',{}).get('count')
    run=journal.run(item['run_id']);policy=json.loads(run.get('execution_policy') or '{}')
    summary={'version':1,'run_id':item['run_id'],'file_id':item['file_id'],'intent_id':item['intent_id'],'state':item['state'],'destination':{key:plan.get(key) for key in ('database','schema','table')},'source':{key:artifact[key] for key in ('relative_path','original_path','sha256','size','path')},'format':{key:decision.get(key) for key in ('encoding','delimiter','quotechar','escapechar','header')},'configuration_hashes':hashes(config),'interpretation_hash':plan.get('interpretation_hash'),'mapping_hash':plan.get('mapping_hash'),'schema_changes':schema_changes,'measures':measured,'sql_measure_basis':counts.get('sql_stage_metric_basis','unknown until acknowledged SQL evidence'),'target_before_records':prior,'target_after_records':None,'target_totals_basis':'total target counts only when independently recorded; source publication count is measures.committed','failure_table':item.get('failures'),'source_only_profile':not bool(receipt),'publication_assurance':receipt.get('assurance'),'group_publication_intent_id':receipt.get('publication_intent_id'),'phase_timings':phases,'resources':{'sampled_run_process_tree_peak_bytes':policy.get('sampled_peak_process_tree_rss_bytes'),'basis':'run-level sampled observation; no per-file peak is inferred'},'next_commands':['status --run-id '+item['run_id'],'show-errors --run-id '+item['run_id'],'reconcile --run-id '+item['run_id']]}
    # Preserve complete originals in their own artifacts; never truncate those to
    # fit an operational SQL/console summary.
    text=json.dumps(summary,ensure_ascii=True,separators=(',',':'))
    if len(text)>1048576:
        summary['schema_changes']={'preview_omitted':True,'full_evidence':'destination receipt / SchemaVersions; local journal'}
        text=json.dumps(summary,ensure_ascii=True,separators=(',',':'))
    if len(text)>1048576:
        from .errors import IngestError
        raise IngestError('REPORT_BUDGET','Per-file operational summary exceeds1MiB; complete evidence remains in journal/artifacts',4)
    return summary

def console_summary(report):
    """Readable terminal view; --json and redirected output retain structured data."""
    def safe(value):
        text=str(value)
        return ''.join(ch if ord(ch)>=32 and ord(ch)!=127 else '\\x'+format(ord(ch),'02x') for ch in text)
    def number(value):return '?' if value is None else str(value)
    lines=[safe(report.get('state','VERIFIED' if report.get('exit_code')==0 else 'NEEDS ATTENTION'))+' (exit '+str(report.get('exit_code',0))+')','Run: '+safe(report['run_id'])]
    files=report.get('files',[])
    for index,item in enumerate(files[:100],1):
        artifact=item.get('artifact') or {};plan=item.get('plan') or {};counts=item.get('counts') or {};receipt=item.get('receipt') or {}
        label=artifact.get('relative_path') or item.get('file_id') or 'source '+str(index)
        target='.'.join(str(plan[k]) for k in ('database','schema','table') if plan.get(k))
        lines.append(safe(label)+((' -> '+safe(target)) if target else ''))
        lines.append('  committed '+number(counts.get('committed'))+'; rejected '+number(counts.get('rejected'))+'; pending '+number(counts.get('pending'))+'; unresolved '+number(counts.get('unresolved')))
        lines.append('  SQL staged '+number(counts.get('sql_staged_records'))+'; validated '+number(counts.get('sql_validated_records'))+'; assurance '+safe(receipt.get('assurance',item.get('assurance','not acknowledged'))))
        if artifact.get('path'):lines.append('  source archive: '+safe(artifact['path']))
        failure=item.get('failures') or {}
        if failure:lines.append('  failure table: '+safe(failure.get('table') or failure.get('name','not materialized'))+' ('+safe(failure.get('state','unknown'))+')')
        review=counts.get('target_review')
        if review:lines.append('  target review: '+safe(review['path']))
    if len(files)>100 or report.get('files_report_truncated'):lines.append('Additional files are retained in the journal/SQL audit; this is a bounded console preview.')
    for error in report.get('errors',[])[:10]:lines.append(safe(error.get('code','ERROR'))+': '+safe(error.get('message','')))
    for command in report.get('next_commands',[])[:10]:lines.append('Next: '+safe(command))
    lines.append('Use --json for the structured report. ? means unknown or not observed.')
    return '\n'.join(lines)


def export_mapping(config,run_id,output):
    """Stream every selected source from the saved plan, without contacting SQL."""
    import csv
    from .journal import Journal
    journal=Journal(config['preservation']['work_directory'])
    count=0
    try:
        saved=json.loads(journal.run(run_id)['config'])
        with open(output,'x',encoding='utf-8-sig',newline='') as stream:
            writer=csv.writer(stream)
            writer.writerow(['source_relative_path','database','schema','table','state','load_mode'])
            for selected in journal.selections(run_id,selected_only=False):
                if selected['status'] not in ('selected','unreadable'):continue
                item=journal.file(run_id,selected['file_id']) if selected.get('file_id') else {}
                plan=item.get('plan') or {}
                writer.writerow([selected['relative_path'],plan.get('database',''),plan.get('schema',''),plan.get('table',''),item.get('state',selected['status']),saved['load']['mode']])
                count+=1
        return {'run_id':run_id,'files':count,'output':str(output),'exit_code':0,'scope':'All selected sources; blank targets mean no completed file plan. Does not certify SQL target availability or group readiness.'}
    finally:journal.close()
