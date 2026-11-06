"""Actual CLI crash/restart and archive checks. No SQL calls are mocked.

These are local durability tests, not SQL transaction/crash integration evidence.
"""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]


class LocalRecoveryCliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root/'source'; self.source.mkdir()
        self.work = self.root/'work'
        self.config = self.root/'ingest.toml'
        self.config.write_text('[source]\nroot='+json.dumps(str(self.source))+'\npath='+json.dumps(str(self.source))+'\ninclude=["**/*.csv"]\n[parsing]\nheader="present"\ndelimiter=","\nencoding="utf-8"\n[preservation]\nwork_directory='+json.dumps(str(self.work))+'\nminimum_free_disk_bytes=0\n[destination]\nserver="127.0.0.1,1"\nconnect_timeout_seconds=1\n[parallel]\nenabled=false\n', encoding='utf-8')
        self.env = {k:v for k,v in os.environ.items() if not k.startswith('DB_')}

    def tearDown(self):
        self.temp.cleanup()

    def command(self, *args):
        return [sys.executable,'-m','db_ingest','--config',str(self.config),*args]

    def invoke(self, *args, expected=0):
        process = subprocess.run(self.command(*args), cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=30)
        self.assertEqual(process.returncode,expected,(process.stdout,process.stderr))
        return json.loads(process.stdout or process.stderr)

    def test_unresolved_plan_restart_cannot_publish_known_prefix(self):
        raw=b'id,value\n1,ok\n2,"unterminated\n3,looks_like_row\n'
        (self.source/'data.csv').write_bytes(raw)
        planned=self.invoke('plan','--json',expected=5)
        run_id=planned['run_id']
        self.assertEqual(planned['files'][0]['state'],'FAILED')
        self.assertIsNotNone(planned['files'][0]['plan'])
        self.assertEqual(planned['files'][0]['counts']['unresolved'],1)
        resumed=self.invoke('resume','--run-id',run_id,'--username','local-test','--password','unused-local-test','--json',expected=5)
        self.assertEqual(resumed['files'][0]['counts']['committed'],0)
        errors=self.invoke('show-errors','--run-id',run_id,'--json')
        self.assertEqual(errors['incidents'][0]['code'],'UNRESOLVED_FRAMING')
        self.assertEqual(Path(planned['files'][0]['artifact']['path']).read_bytes(),raw)

    def test_corrupt_archive_blocks_resume_before_sql(self):
        (self.source/'data.csv').write_bytes(b'id,value\n1,ok\n')
        planned=self.invoke('plan','--json')
        Path(planned['files'][0]['artifact']['path']).write_bytes(b'not-the-source')
        resumed=self.invoke('resume','--run-id',planned['run_id'],'--username','local-test','--password','unused-local-test','--json',expected=5)
        self.assertEqual(resumed['files'][0]['counts']['committed'],0)
        errors=self.invoke('show-errors','--run-id',planned['run_id'],'--json')
        self.assertEqual(errors['incidents'][0]['code'],'ARCHIVE_MISMATCH')

    def test_deleted_ledger_span_blocks_resume_before_sql(self):
        (self.source/'data.csv').write_bytes(b'id,value\n1,ok\n2,also_ok\n')
        planned=self.invoke('plan','--json')
        with closing(sqlite3.connect(self.work/'journal.sqlite3')) as db:
            db.execute("DELETE FROM records WHERE ordinal=2")
            db.commit()
        resumed=self.invoke('resume','--run-id',planned['run_id'],'--username','local-test','--password','unused-local-test','--json',expected=5)
        self.assertEqual(resumed['files'][0]['counts']['committed'],0)
        errors=self.invoke('show-errors','--run-id',planned['run_id'],'--json')
        self.assertEqual(errors['incidents'][0]['code'],'BYTE_COVERAGE_MISMATCH')

    def test_crlf_chunk_boundary_physical_and_logical_counts(self):
        raw=b'a,b\r\n1,"'+b'x'*65527+b'\r\nend"\n'
        self.assertEqual(raw[65535:65537],b'\r\n')
        (self.source/'data.csv').write_bytes(raw)
        planned=self.invoke('plan','--json')
        counts=planned['files'][0]['counts']
        self.assertEqual(counts['logical_records'],2)
        self.assertEqual(counts['data_records'],1)
        self.assertEqual(counts['physical_line_breaks'],3)
        self.assertEqual(counts['physical_lines'],3)
        self.assertEqual(counts['byte_coverage'],'complete contiguous spans plus BOM')

    def test_real_process_termination_retains_capture_and_no_partial_plan(self):
        source=self.source/'data.csv'
        digest=hashlib.sha256()
        with source.open('wb') as output:
            block=b'id,value\n';output.write(block);digest.update(block)
            for i in range(50000):
                block=f'{i},payload\n'.encode();output.write(block);digest.update(block)
        process=subprocess.Popen(self.command('plan','--json'),cwd=ROOT,env=self.env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        run_id=None;deadline=time.monotonic()+25
        try:
            while time.monotonic()<deadline and process.poll() is None:
                if (self.work/'journal.sqlite3').exists():
                    try:
                        with closing(sqlite3.connect(self.work/'journal.sqlite3',timeout=0.1)) as db:
                            run=db.execute('SELECT run_id FROM runs LIMIT 1').fetchone()
                            count=db.execute('SELECT COUNT(*) FROM records').fetchone()[0]
                            if run and count>=100:
                                owner=db.execute("SELECT pid FROM events WHERE category='WORKER_STARTED' ORDER BY id DESC LIMIT 1").fetchone()
                                if owner:
                                    import signal
                                    run_id=run[0];os.kill(owner[0],signal.SIGTERM);break
                    except sqlite3.Error:
                        pass
                time.sleep(0.02)
            stdout,stderr=process.communicate(timeout=10)
            self.assertIsNotNone(run_id,('did not reach durable parsing checkpoint',stdout,stderr))
            status=self.invoke('status','--run-id',run_id,'--json',expected=4)
            self.assertIsNone(status['files'][0]['plan'])
            captured=Path(status['files'][0]['artifact']['path'])
            self.assertEqual(hashlib.sha256(captured.read_bytes()).hexdigest(),digest.hexdigest())
            old_capture=status['files'][0]['file_id']
            old_intent=status['files'][0]['intent_id']
            source.unlink()  # A completed source capture is the recovery input.
            resumed=self.invoke('resume','--run-id',run_id,'--plan-only','--json')
            self.assertEqual(resumed['files'][0]['file_id'],old_capture)
            self.assertEqual(resumed['files'][0]['intent_id'],old_intent)
            self.assertEqual(resumed['files'][0]['counts']['pending'],50000)
            self.assertEqual(resumed['files'][0]['counts'].get('committed',0),0)
        finally:
            if process.poll() is None:
                process.kill();process.communicate(timeout=10)


if __name__=='__main__':unittest.main()
