"""Transactional source-path claims for opt-in filename-only landing tables."""
import os
from pathlib import Path
from db_ingest.config import stable_hash
from db_ingest.errors import IngestError
from .ddl import qualified


def table(config):
    return qualified(config['audit']['control_schema'],'__db_ingest_name_claims')


def bootstrap(cur,config):
    target=table(config)
    if cur.execute('SELECT OBJECT_ID(?)',target).fetchone()[0] is None:
        if not config['destination']['create_tables']:
            raise IngestError('NAME_CLAIM_CREATION_DISABLED','Filename-only naming requires the destination name-claim table; allow initial provisioning')
        cur.execute('CREATE TABLE '+target+' (target nvarchar(260) NOT NULL PRIMARY KEY,source_identity char(64) NOT NULL,source_relative_path nvarchar(max) NOT NULL,source_root nvarchar(max) NOT NULL)')


def claim(cur,config,target,exists):
    source=config.get('_filename_source_path')
    if not isinstance(source,str) or not source:
        raise IngestError('TABLE_NAME_SOURCE_REQUIRED','Filename-only publication requires its retained source-relative path',5)
    root=os.path.normcase(str(Path(config["source"]["root"]).resolve()))
    identity=stable_hash([root,source])
    # SQL compares the target using its own database collation. Key-range locking
    # serializes competing claims, including aliases from independent processes.
    claims=table(config)
    old=cur.execute('SELECT source_identity,source_relative_path,source_root FROM '+claims+' WITH (UPDLOCK,HOLDLOCK) WHERE target=?',target).fetchone()
    if old is not None:
        if old[0]!=stable_hash([old[2],old[1]]):raise IngestError('TABLE_NAME_CLAIM_CORRUPT','Stored filename ownership evidence is inconsistent',5)
        if old[0]!=identity:
            raise IngestError('TABLE_NAME_COLLISION','This filename-only target is already claimed by another source path; target unchanged',2,{'target':target,'claimed_source':old[1],'requested_source':source,'claimed_root':old[2],'requested_root':root})
    elif exists:
        raise IngestError('TABLE_NAME_CLAIM_MISSING','Existing target has no filename ownership claim; refusing automatic adoption or replacement',5,{'target':target})
    else:
        cur.execute('INSERT INTO '+claims+' (target,source_identity,source_relative_path,source_root) VALUES (?,?,?,?)',target,identity,source,root)
