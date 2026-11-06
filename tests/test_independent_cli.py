"""Actual CLI process smoke tests against local files; no SQL endpoint is selected."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
class IndependentCliTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.source=self.root/'source';self.source.mkdir()
        (self.source/'data.csv').write_bytes(b'id,text\r\n1,"two\r\nlines"\r\n2\r\n')
        self.config=self.root/'ingest.toml'
        self.config.write_text('[source]\nroot = '+json.dumps(str(self.source))+'\npath = '+json.dumps(str(self.source))+'\ninclude = ["**/*.csv"]\n[parsing]\nheader = "present"\ndelimiter = ","\nencoding = "utf-8"\n[preservation]\nwork_directory = '+json.dumps(str(self.root/'work'))+'\nminimum_free_disk_bytes = 0\n[parallel]\nenabled = false\n',encoding='utf-8')
    def tearDown(self):self.temp.cleanup()
    def invoke(self,*args,expected=0,config=True):
        command=[sys.executable,'-m','db_ingest']
        if config:command+=['--config',str(self.config)]
        result=subprocess.run(command+list(args),cwd=ROOT,capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,expected,(result.stdout,result.stderr))
        return json.loads(result.stdout or result.stderr)
    def test_real_plan_status_inspect_export_lifecycle(self):
        plan=self.invoke('plan','--json','--output',str(self.root/'plan.json'))
        run_id=plan['run_id']
        self.assertEqual(plan['files'][0]['counts']['header'],1)
        self.assertEqual(plan['files'][0]['counts']['pending'],1)
        self.assertEqual(plan['files'][0]['counts']['rejected'],1)
        self.assertEqual(json.loads((self.root/'plan.json').read_text())['run_id'],run_id)
        status=self.invoke('status','--run-id',run_id,'--json')
        self.assertEqual(status['files'][0]['state'],'PROFILED')
        export=self.invoke('export-failures','--run-id',run_id,'--output',str(self.root/'rejects.jsonl'))
        self.assertEqual(export['records'],1)
        row=json.loads((self.root/'rejects.jsonl').read_text())
        self.assertEqual(row['record']['errors'][0]['code'],'ROW_WIDTH')
        self.assertEqual(row['record']['fields'][0]['text'],'2')
        artifact=Path(plan['files'][0]['artifact']['path'])
        self.assertEqual(artifact.read_bytes(),(self.source/'data.csv').read_bytes())
        inspect=self.invoke('inspect','--json')
        self.assertIs(inspect['provisional'],True)
        self.assertEqual(inspect['files'][0]['records'][-1]['kind'],'rejected')
    def test_init_config_refuses_overwrite(self):
        path=self.root/'generated.toml'
        result=self.invoke('init-config','--output',str(path),config=False)
        self.assertEqual(Path(result['path']),path)
        result=self.invoke('init-config','--output',str(path),expected=2,config=False)
        self.assertEqual(result['code'],'CONFIG_EXISTS')
    def test_real_fixture_command_and_source_only_outcome(self):
        output=self.root/'fixtures'
        manifest=self.invoke('create-test-files','--output',str(output),'--type','csv-multiline,csv-nulls-defaults','--seed','123',config=False)
        self.assertEqual(len(manifest['fixtures']),2)
        checked=self.invoke('verify-test-files','--manifest',str(output/'manifest.json'),expected=3,config=False)
        self.assertEqual(checked['state'],'SOURCE_ONLY_NOT_SQL_VERIFIED')
        blocked=self.invoke('verify-test-files','--manifest',str(output/'manifest.json'),'--run-id','not-a-real-run','--database','DisposableFixtureTest',expected=2,config=False)
        self.assertEqual(blocked['code'],'FIXTURE_DESTINATION_REQUIRED')
if __name__=='__main__':unittest.main()
