"""Read-only supplemental attestation for a completed run_multigb_acceptance case.

Does not repeat SQL cell readback or import application code. Prints bounded metadata;
credentials/source values are never printed. It reads retained chunk files to verify hashes.
"""
import argparse,hashlib,json,runpy,sqlite3,sys,tomllib
from pathlib import Path
import pyodbc

MAX_JSON=32*1024*1024
ORACLE=runpy.run_path(str(Path(__file__).resolve().parents[1]/'tests/oracles/multigb_csv.py'))

def require(condition,code):
    if not condition:raise ValueError(code)

def read_json(path,limit=MAX_JSON):
    with Path(path).open('rb') as stream:raw=stream.read(limit+1)
    require(len(raw)<=limit,'JSON_BUDGET')
    return json.loads(raw)

def digest_file(path):
    digest=hashlib.sha256();size=0
    with Path(path).open('rb') as stream:
        while block:=stream.read(1048576):digest.update(block);size+=len(block)
    return size,digest.hexdigest()

def quote(value):
    require(isinstance(value,str) and value and '\x00' not in value and len(value.encode('utf-16-le'))//2<=128,'IDENTIFIER_INVALID')
    return '['+value.replace(']',']]')+']'

def brace(value):return '{'+str(value).replace('}','}}')+'}'

def chunk_metadata(batches):
    if 'chunk_ledger' in batches:return batches
    candidates=[v for v in batches.values() if isinstance(v,dict) and 'chunk_ledger' in v]
    require(len(candidates)==1,'CHUNK_LEDGER_MISSING_OR_AMBIGUOUS')
    return candidates[0]

def attest_chunks(folder,batches,accepted,rows_limit,bytes_limit,expected_ordinals=None,rejected_ordinals=()):
    metadata=chunk_metadata(batches);ledger=metadata['chunk_ledger'];path=Path(ledger['path']).resolve()
    transport=(folder/'work/transport').resolve();require(path.is_relative_to(transport),'LEDGER_PATH_OUTSIDE_CASE')
    digest=hashlib.sha256();events=chunks=total_rows=total_bytes=0;pending=None;neighbors={}
    expected_iter=iter(expected_ordinals) if expected_ordinals is not None else None
    wanted_neighbors={o+delta for o in rejected_ordinals for delta in [-1,1]}
    with path.open('rb') as stream:
        while raw:=stream.readline(1048577):
            require(len(raw)<=1048576 and raw.endswith(b'\n'),'LEDGER_LINE_BUDGET_OR_FRAMING')
            digest.update(raw);events+=1;entry=json.loads(raw)
            if pending is None:
                require(entry['state']=='SPOOLED' and entry['index']==chunks+1,'LEDGER_CHUNK_START')
                pending=entry;continue
            require(entry['state']=='TYPED_STAGE_DECODED','LEDGER_CHUNK_NOT_VERIFIED')
            require(all(entry[k]==pending[k] for k in ['index','path','rows','bytes','sha256']),'LEDGER_IMMUTABLE_FIELDS_CHANGED')
            require(0<entry['rows']<=rows_limit and 0<entry['bytes']<=bytes_limit,'WIRE_CHUNK_LIMIT')
            chunk=Path(entry['path']).resolve();require(chunk.is_relative_to(transport),'CHUNK_PATH_OUTSIDE_CASE')
            chunk_digest=hashlib.sha256();observed_bytes=observed_rows=0
            with chunk.open('rb') as data:
                while line:=data.readline(min(bytes_limit+1,1048577)):
                    require(len(line)<=1048576 and line.endswith(b'\n'),'CHUNK_LINE_BUDGET_OR_FRAMING')
                    chunk_digest.update(line);observed_bytes+=len(line);observed_rows+=1
                    if expected_iter is not None:
                        parts=line[:-1].split(b'\t',3);require(len(parts)==4,'CHUNK_CANONICAL_FIELD_COUNT')
                        ordinal=int(parts[1]);require(ordinal==next(expected_iter,None),'CHUNK_SOURCE_ORDINAL_SEQUENCE')
                        if ordinal in wanted_neighbors:neighbors[str(ordinal)]=entry['index']
            require((observed_bytes,chunk_digest.hexdigest())==(entry['bytes'],entry['sha256']),'CHUNK_BYTES_OR_HASH_MISMATCH')
            require(observed_rows==entry['rows'],'CHUNK_ACTUAL_ROW_COUNT')
            chunks+=1;total_rows+=entry['rows'];total_bytes+=entry['bytes'];pending=None
    require(pending is None and events==ledger['events']==chunks*2,'LEDGER_EVENT_COUNT')
    require(chunks==ledger['chunk_count'] and digest.hexdigest()==ledger['sha256'],'LEDGER_COMPLETE_HASH_OR_COUNT')
    require(total_rows==accepted,'LEDGER_ACCEPTED_ROW_TOTAL')
    if expected_iter is not None:require(next(expected_iter,None) is None,'CHUNK_MISSING_SOURCE_ORDINALS')
    require(len(metadata.get('chunks',[]))<=20,'CHUNK_PREVIEW_COUNT')
    return {'chunk_count':chunks,'events':events,'rows':total_rows,'bytes_hashed':total_bytes,'ledger_sha256':digest.hexdigest(),'chunk_content_hashes_verified':True,'observed_spool_rows_verified':True,'ordered_source_ordinals_verified':expected_iter is not None,'reject_neighbor_wire_chunk_ids':neighbors}

def authored_storage_bytes(manifest):
    """Independent SQL payload sizes for this authored four-column, non-NULL fixture."""
    rows=manifest['rows'];padding=manifest['padding'];bad=set(manifest['rejected_indexes']);accepted=manifest['accepted_rows']
    require(2<=rows<=1000000000 and accepted==rows-len(bad) and accepted>=2,'PAYLOAD_FORMULA_ROW_DOMAIN')
    require(all(0<=i<rows for i in bad) and rows-1 not in bad,'PAYLOAD_FORMULA_LAST_ROW_REJECTED')
    require(manifest['column_names']==ORACLE['NAMES'] and manifest['expected_types']==ORACLE['TYPES'],'PAYLOAD_FORMULA_SCHEMA')
    longest_prefix=len('row-')+max(9,len(str(rows-1)))+1+len('comma, pipe| quote" CRLF\r\n')
    require(padding>=longest_prefix,'PAYLOAD_FORMULA_PADDING')
    last_bytes=len(ORACLE['cells'](rows-1,rows,padding)[3].encode('utf-16-le'))
    note_bytes=(accepted-1)*padding*2+last_bytes
    mixed_bytes=sum(18 if i==rows-1 else len(str(i%100))*2 for i in range(rows) if i not in bad)
    text_columns=[18*accepted,mixed_bytes,note_bytes]
    return {'text_column_indexes':[0,1,3],'ordered_text_column_bytes':text_columns,'total_text_business_payload_bytes':sum(text_columns),'note_bytes':note_bytes,'last_note_bytes':last_bytes,'decimal_storage_expected':None,'formula_scope':'Only the three NVARCHAR source columns; decimal representation size, ingestion metadata, indexes, row overhead and SQL log allocation excluded.'}


def attest(folder,manifest,credentials):
    folder=folder.resolve();report=read_json(folder/'stdout.json');execution=read_json(folder/'execution.json');completed=read_json(folder/'verified.json')
    config=tomllib.loads((folder/'ingest.toml').read_text(encoding='utf-8'));d=config['destination'];engine=config['load']['engine']
    require(engine in ['odbc','bcp'],'EXPLICIT_ENGINE_REQUIRED')
    require(str(d['server']).startswith('127.0.0.1,') and d['database'].startswith('DbIngestTest_') and d['control_database'].startswith('DbIngestTest_'),'DISPOSABLE_LOOPBACK_REQUIRED')
    require(execution['exit_code']==3 and not execution.get('supervisor_failure') and report['exit_code']==3,'CASE_NOT_COMPLETED_PARTIAL')
    require(completed['verification']['state']=='ALL_ACCEPTED_SQL_CELLS_TYPES_LINEAGE_AND_EXACT_ARCHIVE_VERIFIED','PRIOR_FULL_VALUE_CERTIFICATE_REQUIRED')
    require(len(report['files'])==1,'ONE_SOURCE_REQUIRED');item=report['files'][0];plan=item['plan'];local=item['receipt'];accepted=manifest['accepted_rows'];run_id=report['run_id'];intent=item['intent_id']
    require(plan['database']==d['database'] and completed['verification']['database']==d['database'],'DESTINATION_MAPPING_MISMATCH')
    require(completed['verification']['run_id']==run_id and completed['verification']['intent_id']==intent and completed['verification']['verified_rows']==accepted,'VALUE_CERTIFICATE_IDENTITY')
    require(plan['source_hash']==manifest['sha256']==item['artifact']['sha256'] and item['artifact']['size']==manifest['source_bytes'],'GOLDEN_SOURCE_BINDING')
    require([c['original_name'] for c in plan['columns']]==manifest['column_names'] and [c['sql_type'] for c in plan['columns']]==manifest['expected_types'],'DECLARED_ORACLE_SCHEMA')
    require(item['counts']['committed']==accepted and item['counts']['rejected']==len(manifest['rejected_ordinals']) and not any(item['counts'][k] for k in ['pending','unresolved','excluded']),'LOCAL_DISPOSITIONS')
    db=sqlite3.connect('file:'+str(folder/'work/journal.sqlite3')+'?mode=ro',uri=True)
    try:
        rows=db.execute('SELECT artifact,plan,receipt FROM files WHERE run_id=? AND file_id=?',(run_id,item['file_id'])).fetchall();require(len(rows)==1,'LOCAL_FILE_BINDING')
        artifact,stored_plan,stored_receipt=[json.loads(v) for v in rows[0]]
        require(artifact['sha256']==manifest['sha256'] and stored_plan==plan,'LOCAL_PLAN_SOURCE_BINDING')
        header=db.execute("SELECT payload FROM records WHERE run_id=? AND file_id=? AND disposition='header'",(run_id,item['file_id'])).fetchall();require(len(header)==1,'RAW_HEADER_COUNT')
        require([f['text'] for f in json.loads(header[0][0])['fields']]==manifest['raw_header'],'RAW_THREE_FIELD_HEADER')
        rejects=db.execute("SELECT ordinal,payload FROM records WHERE run_id=? AND file_id=? AND disposition='rejected' ORDER BY ordinal",(run_id,item['file_id'])).fetchall()
        require([r[0] for r in rejects]==manifest['rejected_ordinals'],'REJECT_ORDINALS')
        widths=[]
        for ordinal,payload in rejects:
            record=json.loads(payload);width=5 if ordinal-2==100000 else 3
            require(record['ordinal']==ordinal and len(record['fields'])==width,'REJECT_RAW_FIELD_COUNT')
            require({error['code'] for error in record['errors']}=={'ROW_WIDTH'},'REJECT_REASON_NOT_PURE_WIDTH')
            widths.append(width)
    finally:db.close()
    target=quote(plan['schema'])+'.'+quote(plan['table']);audit=quote(config.get('audit',{}).get('control_schema','audit'))
    props={'DRIVER':d.get('driver','ODBC Driver 18 for SQL Server'),'SERVER':d['server'],'DATABASE':d['database'],'Encrypt':d.get('encrypt','yes'),'TrustServerCertificate':d.get('trust_server_certificate','no'),'UID':credentials['username'],'PWD':credentials['password']}
    conn=pyodbc.connect(';'.join(k+'='+brace(v) for k,v in props.items()),autocommit=False,timeout=30);conn.timeout=1800
    try:
        cur=conn.cursor();cur.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
        require(cur.execute('SELECT DB_NAME()').fetchval()==d['database'],'ACTUAL_DATABASE_MISMATCH')
        rows=cur.execute('SELECT TOP(2) CASE WHEN DATALENGTH(payload)<=? THEN payload ELSE NULL END,target,target_object_id,CONVERT(nvarchar(36),database_guid) FROM '+audit+'.[__db_ingest_receipts] WHERE intent_id=?',1048576,intent).fetchall()
        require(len(rows)==1 and rows[0][0] is not None,'DESTINATION_RECEIPT_MISSING_OR_OVERSIZED');sql=json.loads(rows[0][0])
        identity=cur.execute('SELECT OBJECT_ID(?),CONVERT(nvarchar(36),database_guid),CONVERT(nvarchar(36),recovery_fork_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID()',target).fetchone()
        require(identity is not None and rows[0][1]==target and rows[0][2]==identity[0]==sql['details']['target_object_id'],'TARGET_OBJECT_BINDING')
        require(str(rows[0][3]).lower()==str(identity[1]).lower()==str(sql['details']['database_guid']).lower(),'DATABASE_GUID_BINDING')
        require(str(identity[2]).lower()==str(sql['details']['recovery_fork_guid']).lower(),'RECOVERY_FORK_BINDING')
        expected={'run_id':run_id,'intent_id':intent,'database':d['database'],'schema':plan['schema'],'table':plan['table'],'source_hash':manifest['sha256'],'interpretation_hash':plan['interpretation_hash'],'mapping_hash':plan['mapping_hash'],'committed_records':accepted,'assurance':'full'}
        for key,value in expected.items():require(sql.get(key)==local.get(key)==stored_receipt.get(key)==value,'RECEIPT_IDENTITY_'+key.upper())
        require(sql['value_digest']==local['value_digest']==stored_receipt['value_digest'] and len(sql['value_digest'])==64,'VALUE_DIGEST_BINDING')
        require(sql['details']['schema_hash']==local['details']['schema_hash']==stored_receipt['details']['schema_hash'],'SCHEMA_HASH_BINDING')
        transport=sql['details']['transport'];require(transport['engine']==transport['requested']==engine,'ACTUAL_TRANSPORT_ENGINE')
        batches=sql['details']['staging_batches'];rows_limit=config['load']['batch_rows'];bytes_limit=config['load']['batch_bytes']
        require(batches['limit_rows']==rows_limit and batches['limit_bytes']==bytes_limit,'BATCH_REQUESTED_LIMITS')
        require(0<batches['max_rows']<=rows_limit and 0<batches['max_bytes']<=bytes_limit and batches['count']>0,'BATCH_OBSERVED_LIMITS')
        require(batches.get('max_native_array_bytes',0)<=bytes_limit,'NATIVE_ARRAY_BOUND')
        require(all(batches[k]==local['details']['staging_batches'][k] for k in ['limit_rows','limit_bytes','max_rows','max_bytes','count']),'LOCAL_SQL_BATCH_BINDING')
        # This narrow aggregate detects duplicate old intents in this fresh fixture target;
        # it does not repeat the multi-GB business-cell transfer.
        aggregate=cur.execute('SELECT COUNT_BIG(*),COUNT(DISTINCT [__ingest_intent]),COUNT(DISTINCT [__ingest_record]) FROM '+target).fetchone()
        require(tuple(aggregate)==(accepted,1,accepted),'FRESH_TARGET_DUPLICATE_INTENTS_OR_ROWS')
        payload_expected=authored_storage_bytes(manifest)
        decimal=cur.execute('SELECT t.name,c.max_length,c.precision,c.scale FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE c.object_id=OBJECT_ID(?) AND c.name=?',target,plan['columns'][2]['target_name']).fetchone()
        require(decimal is not None and tuple(decimal)==('decimal',13,23,19),'PAYLOAD_DECIMAL_STORAGE_POLICY')
        # Cast BEFORE summation: SUM(int DATALENGTH) can overflow signed32 bits.
        expressions=','.join('COALESCE(SUM(CONVERT(bigint,DATALENGTH('+quote(c['target_name'])+'))),CONVERT(bigint,0))' for c in plan['columns'])
        payload_row=cur.execute('SELECT '+expressions+' FROM '+target+' WHERE [__ingest_intent]=?',intent).fetchone()
        payload_actual=[int(value) for value in payload_row]
        actual_text=[payload_actual[i] for i in payload_expected['text_column_indexes']]
        require(actual_text==payload_expected['ordered_text_column_bytes'],'SQL_BIGINT_TEXT_BUSINESS_PAYLOAD_MISMATCH')
        payload_total=sum(actual_text)
        if manifest['source_bytes']>2*1024**3:require(payload_actual[3]>2*1024**3,'SQL_NOTE_PAYLOAD_NOT_ABOVE_2GIB')
        literal_lengths=cur.execute("SELECT DATALENGTH(CONVERT(decimal(23,19),'1.25')),DATALENGTH(CONVERT(decimal(23,19),'9999.1234567890123456789'))").fetchone()
        payload_proof={**payload_expected,'actual_ordered_text_column_bytes':actual_text,'actual_total_text_business_payload_bytes':payload_total,'decimal_datalength_sum_observed_not_oracle':payload_actual[2],'decimal_metadata_max_length':13,'decimal_literal_datalengths_observed':list(literal_lengths),'note_above_2GiB':payload_actual[3]>2*1024**3,'note_above_4GiB':payload_actual[3]>2**32,'text_business_payload_above_4GiB':payload_total>2**32,'query_scope':'BIGINT SUM(DATALENGTH) metadata for exact text payload lower bound; decimal observed separately. No business values transferred or full SQL cell re-read.'}
    finally:conn.rollback();conn.close()
    if engine=='bcp' or config['load'].get('fast_executemany',False):
        rejected=set(manifest['rejected_ordinals'])
        expected_ordinals=(ordinal for ordinal in range(2,manifest['rows']+2) if ordinal not in rejected)
        ledger=attest_chunks(folder,batches,accepted,rows_limit,bytes_limit,expected_ordinals,manifest['rejected_ordinals'])
    else:ledger={'state':'NOT_APPLICABLE_SCALAR_ODBC','rows':accepted,'basis':'full receipt plus prior independent SQL cells/lineage proof; no canonical chunk ledger expected'}
    return {'state':'COMPLETED_CASE_RECEIPT_TRANSPORT_REJECTIONS_AND_CHUNKS_ATTESTED','exit_code':0,'run_id':run_id,'intent_id':intent,'database':d['database'],'requested_engine':engine,'actual_engine':transport['engine'],'accepted_rows':accepted,'rejected_ordinals':manifest['rejected_ordinals'],'retained_rejected_field_widths':widths,'source_bytes':manifest['source_bytes'],'source_exceeds_2GiB':manifest['source_bytes']>2*1024**3,'source_sha256':manifest['sha256'],'full_assurance_receipt':True,'value_digest':sql['value_digest'],'batch_limits':{'rows':rows_limit,'bytes':bytes_limit},'observed_batches':{k:batches.get(k) for k in ['count','max_rows','max_bytes','fast_array_batches','fallback_batches','fallback_rows','max_native_array_bytes']},'chunk_ledger':ledger,'SQL_payload_bytes':payload_proof,'full_cell_readback_scope':'Prior completed verified.json is independently authored all-cell+lineage proof; this supplemental command does not repeat that SQL value transfer.','secret_or_source_values_printed':False}

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--folder',type=Path,required=True);parser.add_argument('--source-manifest',type=Path,required=True);parser.add_argument('--credentials-json',type=Path,required=True);args=parser.parse_args();credentials={}
    try:
        credentials=read_json(args.credentials_json,65536)
        result=attest(args.folder,read_json(args.source_manifest),credentials);print(json.dumps(result,indent=2));return 0
    except Exception as error:
        text=str(error)
        for secret in [credentials.get('password'),credentials.get('username')]:
            if secret:
                for form in [secret,json.dumps(secret)[1:-1],repr(secret)[1:-1]]:text=text.replace(form,'<redacted>')
        print(json.dumps({'state':'ATTESTATION_FAILED','exit_code':5,'exception':type(error).__name__,'message':text[:500]}));return 5

if __name__=='__main__':raise SystemExit(main())
