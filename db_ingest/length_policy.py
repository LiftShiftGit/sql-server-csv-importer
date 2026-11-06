"""Explicit lossless length changes over fully verified lexical representations."""
import hashlib
import json
import math
import os
from .errors import IngestError
from .adapters.mssql.types import parse_sql_type,SqlType,normalize_value
from .resources import checkpoint


def initialize(journal):
    with journal.lease('length-policy-bootstrap',timeout=30):
        journal.db.execute('''CREATE TABLE IF NOT EXISTS length_policy_events(run_id TEXT NOT NULL,file_id TEXT NOT NULL,record_id TEXT NOT NULL,column_ordinal INTEGER NOT NULL,details TEXT NOT NULL,sha256 TEXT NOT NULL,PRIMARY KEY(run_id,file_id,record_id,column_ordinal))''')
        journal.db.commit()


class LengthPolicyContext:
    def __init__(self,journal,run_id,artifact,columns,config):
        self.journal,self.run_id=journal,run_id
        self.file_id=artifact.capture_id
        self.artifact={'path':artifact.path,'sha256':artifact.sha256,'capture_id':artifact.capture_id}
        self.settings=config['schema'];self.policy=self.settings['on_length_overflow']
        self.columns=[]
        if self.policy not in ('widen','retype_text'):return
        self.headroom=float(self.settings.get('text_length_headroom_percent',20))
        if not math.isfinite(self.headroom) or not 0<=self.headroom<=10000:raise IngestError('LENGTH_HEADROOM_RANGE','Length headroom must be a finite percentage from0 through10000')
        for column in columns:
            spec=parse_sql_type(column.sql_type)
            if spec.base not in ('nvarchar','varchar','varbinary') or spec.length=='max' or column.stats.get('truncation')=='right':continue
            self.columns.append({'column':column,'declared':spec,'lexical_max':0})
        if self.columns:initialize(journal)

    def _length(self,base,measured):
        limit=4000 if base=='nvarchar' else 8000
        wanted=max(1,measured,math.ceil(measured*(1+self.headroom/100)))
        if wanted<=limit:return wanted
        if self.settings.get('allow_max_types',True):return 'max'
        if measured<=limit:return limit
        raise IngestError('LENGTH_POLICY_LIMIT','Observed exact value requires '+base+'(max), but schema.allow_max_types=false',2,{'measured_units':measured,'fixed_type_limit':limit,'policy':self.policy})

    def _event(self,record,column,before,after,measured,unit):
        from .journal import utcnow
        details={'policy':self.policy,'record_id':record.record_id,'column_ordinal':column.ordinal,'source_declared_type':column.stats['source_declared_type'],'before_type':before,'after_type':after,'measured_units':measured,'unit':unit,'headroom_percent':self.headroom,'representation':column.stats['length_policy_representation'],'source_artifact':self.artifact,'source_byte_start':record.byte_start,'source_byte_end':record.byte_end}
        payload=json.dumps(details,ensure_ascii=True,sort_keys=True,separators=(',',':'));digest=hashlib.sha256(payload.encode()).hexdigest()
        old=self.journal.db.execute('SELECT sha256 FROM length_policy_events WHERE run_id=? AND file_id=? AND record_id=? AND column_ordinal=?',(self.run_id,self.file_id,record.record_id,column.ordinal)).fetchone()
        if old:
            if old[0]!=digest:raise IngestError('LENGTH_POLICY_EVIDENCE_MISMATCH','Saved length policy evidence differs from deterministic reprofile',5)
            return
        with self.journal.db:
            self.journal.db.execute('INSERT INTO length_policy_events VALUES(?,?,?,?,?,?)',(self.run_id,self.file_id,record.record_id,column.ordinal,payload,digest))
            self.journal.db.execute('INSERT INTO events(run_id,file_id,category,pid,created,details) VALUES(?,?,?,?,?,?)',(self.run_id,self.file_id,'SCHEMA_LENGTH_POLICY',os.getpid(),utcnow(),payload))

    def process(self,record,skip_columns=()):
        errors=[];skipped=set(skip_columns)
        for entry in self.columns:
            checkpoint();column,declared=entry['column'],entry['declared']
            if column.ordinal in skipped or column.ordinal>len(record.fields):continue
            field=record.fields[column.ordinal-1]
            if field.state not in ('VALUE','EMPTY'):continue
            text=field.text if field.text is not None else ''
            # Always check the ORIGINAL representation, even after retype_text.
            # Invalid encoded bytes/base64/hex can never become accepted plain text.
            try:value=normalize_value(text,SqlType(declared.base,length='max'),column.stats.get('binary_encoding'),text_encoding=column.stats.get('text_encoding'))
            except IngestError as error:
                errors.append({'code':error.code,'message':str(error),'column_ordinal':column.ordinal,'record_id':record.record_id});continue
            lexical_units=len(text.encode('utf-16-le'))//2
            entry['lexical_max']=max(entry['lexical_max'],lexical_units)
            if declared.base=='varbinary':measured=len(value);unit='decoded_bytes'
            elif declared.base=='varchar':
                from .adapters.mssql.text import encoding_name
                measured=len(text.encode(encoding_name(column.stats.get('text_encoding')),errors='strict'));unit='encoded_bytes'
            else:measured=lexical_units;unit='utf16_units'
            current=parse_sql_type(column.sql_type)
            if self.policy=='widen':
                if current.length=='max' or measured<=current.length:continue
                after=str(SqlType(current.base,length=self._length(current.base,measured)))
            else:
                if current.base==declared.base and measured<=declared.length:continue
                desired=self._length('nvarchar',entry['lexical_max'])
                if current.base=='nvarchar' and (current.length=='max' or desired!='max' and current.length>=desired):continue
                after=str(SqlType('nvarchar',length=desired))
            before=column.sql_type
            column.stats.setdefault('source_declared_type',str(declared))
            column.stats['length_policy']=self.policy
            column.stats['length_policy_representation']=('captured_attachment_content_base64' if column.stats.get('source_binary_encoding')=='attachment_path' else 'source_lexical') if self.policy=='retype_text' else 'declared_exact_value'
            column.stats['length_policy_changes']=column.stats.get('length_policy_changes',0)+1
            if declared.base=='varbinary':column.stats.setdefault('source_content_encoding',column.stats.get('binary_encoding'))
            column.sql_type=after
            self._event(record,column,before,after,measured,unit)
        return errors
