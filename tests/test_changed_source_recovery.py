
"""A retained prefix cannot prove that a restarted source is the same export."""
import hashlib,json,os,sqlite3,subprocess,sys,uuid
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]

@pytest.fixture
def case(tmp_path):
    source=tmp_path/'orders.csv';raw=b'id,payload\n001,'+b'x'*1200000+b'\n';source.write_bytes(raw)
    work=tmp_path/'work';cfg=tmp_path/'ingest.toml';cfg.write_text('[source]\nroot='+json.dumps(str(tmp_path))+'\npath='+json.dumps(str(source))+'\n[parsing]\nheader="present"\ndelimiter=","\nencoding="utf-8"\n[preservation]\nwork_directory='+json.dumps(str(work))+'\n[parallel]\nenabled=false\n[destination]\ndatabase="DbIngestTest_ChangedSource"\ncontrol_database="DbIngestTest_ChangedSource_Control"\n',encoding='utf-8')
    env={k:v for k,v in os.environ.items() if not k.startswith('DB_')}
    def cli(*args,fault=None):
        return subprocess.run([sys.executable,'-m','db_ingest','--config',str(cfg),*args,'--json'],cwd=ROOT,env={**env,**({'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':fault} if fault else {})},text=True,capture_output=True,timeout=60)
    return source,raw,work,cli

def selection(work):
    with sqlite3.connect(work/'journal.sqlite3') as c:
        c.row_factory=sqlite3.Row
        return dict(c.execute('SELECT * FROM selections').fetchone())

@pytest.mark.parametrize('change',['prefix','unread_tail_same_size_mtime','replace','append','truncate'])
def test_changed_export_cannot_be_recaptured_under_original_intent(case,change):
    source,raw,work,cli=case
    assert cli('plan',fault='CAPTURE_FIRST_BLOCK_DURABLE').returncode==86
    old=selection(work);partial=work/'captures'/old['file_id']/'source.partial';digest=hashlib.sha256(partial.read_bytes()).hexdigest();stat=source.stat()
    data=bytearray(raw)
    if change=='prefix':data[100]=ord('y')
    elif change=='unread_tail_same_size_mtime':data[1100000]=ord('y')
    elif change=='replace':source.unlink();data[100]=ord('z')
    elif change=='append':data.extend(b'002,new\n')
    elif change=='truncate':data=data[:100]
    source.write_bytes(data);os.utime(source,ns=(stat.st_atime_ns,stat.st_mtime_ns))
    for _ in range(2):
        p=cli('resume','--run-id',old['run_id'],'--plan-only')
        assert p.returncode==5,(p.stdout,p.stderr)
        assert 'CAPTURE_RETRY_UNVERIFIED' in p.stdout
        assert selection(work)['intent_id']==old['intent_id'] and selection(work)['file_id']==old['file_id']
        assert hashlib.sha256(partial.read_bytes()).hexdigest()==digest
    with sqlite3.connect(work/'journal.sqlite3') as c:assert c.execute('SELECT COUNT(*) FROM files').fetchone()[0]==0

def test_legacy_empty_retry_does_not_hide_earlier_incomplete_bytes(case):
    source,raw,work,cli=case;assert cli('plan',fault='CAPTURE_FIRST_BLOCK_DURABLE').returncode==86;old=selection(work)
    from db_ingest.journal import Journal
    j=Journal(work);empty=uuid.uuid4().hex
    j.update_selection(old['run_id'],old['selection_id'],file_id=empty,state='CAPTURING')
    j.event(old['run_id'],empty,'CAPTURE_ATTEMPT_STARTED',{'selection_id':old['selection_id'],'capture_id':empty})
    j.close()
    p=cli('resume','--run-id',old['run_id'],'--plan-only');assert p.returncode==5 and 'CAPTURE_RETRY_UNVERIFIED' in p.stdout

def test_complete_snapshot_ignores_later_source_replacement(case):
    source,raw,work,cli=case
    assert cli('plan',fault='CAPTURE_FINALIZED_BEFORE_JOURNAL').returncode==86;old=selection(work)
    source.write_bytes(b'id,payload\n002,replacement\n')
    p=cli('resume','--run-id',old['run_id'],'--plan-only');assert p.returncode==0,(p.stdout,p.stderr)
    item=json.loads(p.stdout)['files'][0];assert item['intent_id']==old['intent_id'];assert Path(item['artifact']['path']).read_bytes()==raw
