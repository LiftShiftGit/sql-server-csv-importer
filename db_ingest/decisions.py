"""Bounded terminal decisions before any publication; choices create linked runs."""
import copy
import json
import sys


def resolve_length_decision(report, config, args, overrides):
    """Offer only explicit lossless/quarantine choices on a real operator terminal."""
    if (report.get('exit_code') != 2 or config['interaction']['non_interactive']
            or getattr(args, 'json', False) or not sys.stdin.isatty()):
        return report, config
    from .journal import Journal
    from . import pipeline
    journal = Journal(config['preservation']['work_directory'])
    try:
        run_id = report['run_id']
        # Never reinterpret a run with already acknowledged publications.
        if journal.db.execute('SELECT 1 FROM files WHERE run_id=? AND receipt IS NOT NULL LIMIT 1', (run_id,)).fetchone():
            return report, config
        errors = journal.errors(run_id, limit=1000)
        decision_errors = [e for e in errors if e['code'] == 'LENGTH_DECISION_REQUIRED']
        # Other interpretation/integrity problems need their own explicit correction.
        if not decision_errors or any(e['code'] != 'LENGTH_DECISION_REQUIRED' for e in errors):
            return report, config
        file_count = journal.db.execute('SELECT COUNT(*) FROM files WHERE run_id=?', (run_id,)).fetchone()[0]
        print('Length overflow: a new linked interpretation will reuse all '+str(file_count)+' captured sources. No publication has started.', file=sys.stderr)
        for error in decision_errors[:10]:
            item = journal.file(run_id, error['file_id'])
            columns = {c['ordinal']:c for c in item['plan']['columns']}
            evidence = []
            for detail in error['context'].get('columns', [])[:10]:
                column = columns.get(detail.get('column_ordinal'), {})
                row = journal.db.execute('SELECT payload FROM records WHERE run_id=? AND file_id=? AND record_id=?', (run_id,error['file_id'],detail.get('record_id'))).fetchone()
                record = json.loads(row[0]) if row else {}
                ordinal = detail.get('column_ordinal', 0)
                fields = record.get('fields', [])
                value = fields[ordinal-1].get('text') if 0<ordinal<=len(fields) else None
                current = column.get('sql_type', '')
                evidence.append({**detail, 'current_type':current, 'widen_proposal':current.split('(')[0]+'(max) or a sufficient fixed length after full reprofile', 'retype_proposal':'nvarchar(max) or a sufficient fixed length after full reprofile', 'value_preview':value[:80] if value is not None else None, 'preview_truncated':value is not None and len(value)>80})
            print(json.dumps({'source':item['artifact']['relative_path'], 'destination':error['context'].get('destination'), 'columns':evidence}, ensure_ascii=True), file=sys.stderr)
        if len(decision_errors)>10:
            print('Additional affected files: '+str(len(decision_errors)-10)+'. See show-errors for details.', file=sys.stderr)
        print('Apply to length overflows in this linked run: [w] widen exact text/binary storage; [t] preserve lexical values as Unicode text; [q] quarantine overflowing records; [s] stop. Existing-target ALTER still requires configured approval.', file=sys.stderr)
        choices = {'w':'widen', 't':'retype_text', 'q':'quarantine'}
        # A typo must not become an implicit approval or an endless automated prompt.
        for _ in range(3):
            print('Choice [w/t/q/s]: ', end='', file=sys.stderr, flush=True)
            answer = sys.stdin.readline().strip().lower()
            if answer in ('', 's', 'stop'):
                return report, config
            if answer in choices:
                break
            print('Enter w, t, q or s.', file=sys.stderr)
        else:
            return report, config
        policy = choices[answer]
        changed = copy.deepcopy(config)
        changed['schema']['on_length_overflow'] = policy
        changed.setdefault('_provenance', {})['schema.on_length_overflow'] = 'interactive explicit choice'
        journal.event(run_id, None, 'INTERPRETATION_DECISION', {'policy':'schema.on_length_overflow', 'choice':policy, 'scope':'all captured sources in linked interpretation', 'source_run_id':run_id})
        # Consume the durable inventory rather than a possibly truncated console report.
        artifacts = (item['artifact'] for item in journal.files(run_id))
        linked = pipeline.prepare(changed, linked_run=run_id, new_intent=True, artifacts=artifacts)
        linked['decision'] = {'linked_run':run_id, 'on_length_overflow':policy, 'resume_option':'--on-length-overflow '+policy}
        journal.event(linked['run_id'], None, 'INTERPRETATION_DECISION_APPLIED', linked['decision'])
        return linked, changed
    finally:
        journal.close()

def resolve_schema_decision(report,config,args,username=None,password=None):
    """Review supported owned-target schema policies after a rolled-back attempt."""
    if (report.get('exit_code')!=2 or config['interaction']['non_interactive']
            or getattr(args,'json',False) or not sys.stdin.isatty()
            or config['mapping']['existing_target_strategy']!='owned_landing'
            or config['mapping']['table_strategy']!='per_file'):
        return report
    from .journal import Journal
    from . import pipeline
    from .target_review import difference
    from .config import effective_for_file,resolve_columns,COLUMN_KEYS
    from .errors import IngestError
    import glob
    journal=Journal(config['preservation']['work_directory'])
    try:
        run_id=report['run_id'];errors=journal.errors(run_id,limit=1000)
        eligible=[e for e in errors if e['code']=='SCHEMA_DRIFT' and e['context'].get('schema_diff',{}).get('blocked')==['schema policy disabled']]
        if not eligible or any(e not in eligible for e in errors):return report
        affected={e['file_id']:e for e in eligible if e['file_id']}
        if not affected:return report
        pending=journal.db.execute('SELECT COUNT(*),COALESCE(SUM(length(plan)),0) FROM files WHERE run_id=? AND receipt IS NULL',(run_id,)).fetchone()
        if pending[0]!=len(affected) or len(affected)>10 or pending[1]>min(8388608,config['limits']['max_queue_bytes']):return report
        print('Schema policy decision after a rolled-back publication attempt. Staging may already have occurred. Only the '+str(len(affected))+' affected unpublished sources will enter a new linked interpretation; acknowledged files stay in the original run.',file=sys.stderr)
        candidates=[]
        for fid,error in affected.items():
            item=journal.file(run_id,fid)
            if item.get('receipt') is not None:return report
            from .pipeline import _plan_from_dict
            plan=_plan_from_dict(item['plan']);metadata=error['context']['schema_diff']['inspection']
            changes=difference(plan,{'metadata':metadata})['changes']
            candidates.append((item,metadata))
            if len(candidates)<=10:print(json.dumps({'source':item['artifact']['relative_path'],'destination':{k:item['plan'][k] for k in ('database','schema','table')},'changes':changes[:10],'changes_truncated':len(changes)>10},ensure_ascii=True),file=sys.stderr)
        print('[w] authorize compatible widening; [t] retype conflicting supplied columns to Unicode lexical text; [q] validate against current target types and quarantine unrepresentable records; [s] stop. Index/constraint/dependency and exact-old-value checks still apply. No existing target is changed by this menu.',file=sys.stderr)
        for _ in range(3):
            print('Choice [w/t/q/s]: ',end='',file=sys.stderr,flush=True)
            answer=sys.stdin.readline().strip().lower()
            if answer in ('','s','stop'):return report
            if answer in ('w','t','q'):break
            print('Enter w, t, q or s.',file=sys.stderr)
        else:return report
        changed=copy.deepcopy(config);policies=changed['schema']
        if answer=='w':policies['allow_widen_on_append']=True
        if answer=='t':policies.update(allow_retype_on_append=True,allow_widen_on_append=True)
        if answer in ('t','q'):
            for item,metadata in candidates:
                columns=item['plan']['columns'];headers=[c['original_name'] for c in columns]
                specs={s['source_ordinal']:{k:v for k,v in s.items() if k in COLUMN_KEYS} for s in resolve_columns(effective_for_file(config,item['artifact']['relative_path']),headers)}
                actual={r[0]:r for r in metadata['columns'] if not r[0].startswith('__ingest_')}
                for column in columns:
                    row=actual.get(column['target_name'])
                    if row is None:
                        if answer=='q':raise IngestError('SCHEMA_QUARANTINE_UNSUPPORTED','Current target has no column for '+column['target_name']+'; quarantine would require discarding a source column. Stop and choose an explicit mapping instead',2,{'run_id':run_id})
                        continue
                    base,length,precision,scale=row[2:6]
                    if base in ('nvarchar','varchar','varbinary'):current=base+'('+str('max' if length==-1 else length//2 if base=='nvarchar' else length)+')'
                    elif base in ('decimal','numeric'):current='decimal('+str(precision)+','+str(scale)+')'
                    elif base in ('time','datetime2','datetimeoffset'):current=base+'('+str(scale)+')'
                    else:current=base
                    if answer=='t' and current==column['sql_type']:continue
                    spec={**specs.get(column['ordinal'],{}),'source_ordinal':column['ordinal'],'target_name':column['target_name'],'target_type':'nvarchar(max)' if answer=='t' else current,'nullable':bool(row[6])}
                    for key in ('length','precision','scale','truncation','rounding'):
                        spec.pop(key,None)
                    if answer=='t' or base not in ('date','time','datetime2','datetimeoffset'):
                        for key in ('date_format','temporal_format','timezone_policy','fractional_second_policy'):spec.pop(key,None)
                    if answer=='t' or base!='varbinary':
                        for key in ('binary_encoding','attachment_root'):spec.pop(key,None)
                    elif 'binary_encoding' not in spec:raise IngestError('SCHEMA_QUARANTINE_UNSUPPORTED','Existing binary target requires an explicit verified source binary_encoding; no decoder is inferred',2,{'run_id':run_id})
                    if answer=='t' or base!='varchar':
                        for key in ('collation','text_encoding'):spec.pop(key,None)
                    elif base=='varchar':
                        collation=row[10]
                        pairs={'Latin1_General_100_BIN2_UTF8':'utf-8','Latin1_General_100_BIN2':'cp1252'}
                        if collation not in pairs:raise IngestError('SCHEMA_QUARANTINE_UNSUPPORTED','Existing VARCHAR collation lacks a verified encoder policy',2,{'run_id':run_id,'collation':collation})
                        spec.update(collation=collation,text_encoding=pairs[collation])
                    specs[column['ordinal']]=spec
                changed.setdefault('file_rules',[]).append({'glob':glob.escape(item['artifact']['relative_path']),'columns':list(specs.values())})
            if answer=='q':policies['on_length_overflow']='quarantine'
        decision={'source_run_id':run_id,'choice':{'w':'compatible widening','t':'Unicode lexical retype','q':'current-target validation and quarantine'}[answer],'affected_unpublished_files':len(candidates),'scope':'explicit linked interpretation; publication still enforces constraints and complete value verification'}
        journal.event(run_id,None,'SCHEMA_INTERPRETATION_DECISION',decision)
        linked=pipeline.prepare(changed,linked_run=run_id,new_intent=True,artifacts=(item['artifact'] for item,_ in candidates))
        journal.event(linked['run_id'],None,'SCHEMA_INTERPRETATION_DECISION_APPLIED',decision)
        if linked['exit_code'] and (linked.get('global_publication_blocked',True) or not linked.get('publishable_files',0)):return {**linked,'decision':decision}
        result=pipeline.execute(changed,linked['run_id'],username,password)
        return {**result,'decision':{**decision,'configuration':'The redacted effective choices are retained in the linked run; persist them in TOML before later resume.'}}
    finally:journal.close()
