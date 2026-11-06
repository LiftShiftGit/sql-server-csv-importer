"""Actual hard process exit at durable capture boundaries; no SQL mocks/calls."""
import hashlib,json,os,sqlite3,subprocess,sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
ROOT=Path(__file__).resolve().parents[1]

class CaptureFaultCliTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.root=Path(self.temp.name);self.source=self.root/'source';self.source.mkdir();self.work=self.root/'work'
        self.raw=b'id,note\r\n1,'+b'x'*1200000+b'\r\n';self.input=self.source/'input.csv';self.input.write_bytes(self.raw)
        self.config=self.root/'config.toml'
        self.config.write_text('[source]\nroot='+json.dumps(str(self.source))+'\npath='+json.dumps(str(self.source))+'\ninclude=["**/*.csv"]\n[parsing]\nheader="present"\ndelimiter=","\nencoding="utf-8"\n[preservation]\nwork_directory='+json.dumps(str(self.work))+'\nminimum_free_disk_bytes=0\n[destination]\nserver="127.0.0.1,1"\ndatabase="DbIngestTest_CaptureOnly"\ncontrol_database="DbIngestTest_CaptureOnlyControl"\n[parallel]\nenabled=false\n[load]\nengine="odbc"\n',encoding='utf-8')
        self.env={k:v for k,v in os.environ.items() if not k.startswith('DB_')}
    def tearDown(self):self.temp.cleanup()
    def invoke(self,*args,env=None):return subprocess.run([sys.executable,'-m','db_ingest','--config',str(self.config),*args,'--json'],cwd=ROOT,env=env or self.env,text=True,capture_output=True,timeout=45)
    def selection(self):
        with closing(sqlite3.connect(self.work/'journal.sqlite3')) as connection:
            connection.row_factory=sqlite3.Row
            row=dict(connection.execute('SELECT * FROM selections').fetchone())
            events=[json.loads(r[0]) for r in connection.execute("SELECT details FROM events WHERE category='CAPTURE_ATTEMPT_STARTED' ORDER BY id")]
            return row,events
    def _fault(self,point):
        process=self.invoke('plan',env={**self.env,'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':point})
        self.assertEqual(process.returncode,86,(process.stdout,process.stderr))
        selected,events=self.selection();self.assertEqual(selected['state'],'CAPTURING');self.assertIsNone(selected['artifact'])
        self.assertEqual(len(events),1);self.assertEqual(events[0]['intent_id'],selected['intent_id']);self.assertEqual(events[0]['capture_id'],selected['file_id'])
        self.assertEqual(self.input.read_bytes(),self.raw)
        return selected
    def test_f01_incomplete_capture_blocks_automatic_recapture_even_if_source_unchanged(self):
        selected=self._fault('CAPTURE_FIRST_BLOCK_DURABLE');partial=self.work/'captures'/selected['file_id']/'source.partial'
        expected=hashlib.sha256(partial.read_bytes()).hexdigest()
        process=self.invoke('resume','--run-id',selected['run_id'],'--plan-only')
        self.assertEqual(process.returncode,5,(process.stdout,process.stderr))
        self.assertIn('CAPTURE_RETRY_UNVERIFIED',process.stdout)
        after,events=self.selection();self.assertEqual(after['file_id'],selected['file_id']);self.assertEqual(after['intent_id'],selected['intent_id'])
        self.assertEqual(len(events),1);self.assertEqual(hashlib.sha256(partial.read_bytes()).hexdigest(),expected)
    def test_f02_final_orphan_capture_is_reused_with_original_capture_and_intent(self):
        selected=self._fault('CAPTURE_FINALIZED_BEFORE_JOURNAL');folder=self.work/'captures'/selected['file_id']
        metadata=json.loads((folder/'artifact.json').read_text(encoding='utf-8'));self.assertEqual(metadata['sha256'],hashlib.sha256(self.raw).hexdigest())
        self.assertFalse((folder/'source.partial').exists());self.assertEqual((folder/'source.bin').read_bytes(),self.raw)
        process=self.invoke('resume','--run-id',selected['run_id'],'--plan-only');self.assertEqual(process.returncode,0,(process.stdout,process.stderr))
        file=json.loads(process.stdout)['files'][0]
        self.assertEqual(file['intent_id'],selected['intent_id']);self.assertEqual(file['file_id'],selected['file_id'])
        self.assertEqual(Path(file['artifact']['path']).read_bytes(),self.raw)
        after,events=self.selection();self.assertEqual(len(events),1)
        with closing(sqlite3.connect(self.work/'journal.sqlite3')) as connection:self.assertEqual(connection.execute("SELECT COUNT(*) FROM events WHERE category='CAPTURE_ORPHAN_RECOVERED'").fetchone()[0],1)
    def test_fault_cannot_activate_on_production_named_destinations(self):
        self.config.write_text(self.config.read_text(encoding='utf-8').replace('DbIngestTest_CaptureOnly','Production'),encoding='utf-8')
        process=self.invoke('plan',env={**self.env,'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':'CAPTURE_FIRST_BLOCK_DURABLE'})
        self.assertEqual(process.returncode,2,(process.stdout,process.stderr))
        self.assertIn('TEST_FAULT_DESTINATION',process.stdout+process.stderr)
        self.assertFalse((self.work/'captures').exists());self.assertEqual(self.input.read_bytes(),self.raw)
