"""Actual isolated Windows RSS sampler must not allocate permanent ctypes types."""
import json,os,subprocess,sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
ROOT=Path(__file__).resolve().parents[1]

@unittest.skipUnless(os.name=='nt','Windows API cache regression requires Windows')
class WindowsSamplerAllocationTests(unittest.TestCase):
    def test_repeated_snapshots_keep_pointer_cache_and_native_handles_bounded(self):
        with TemporaryDirectory() as folder:
            output=Path(folder)/'plateau.json'
            completed=subprocess.run([sys.executable,str(ROOT/'scripts/resource_monitor_plateau.py'),'--samples','100','--output',str(output)],cwd=ROOT,capture_output=True,text=True,timeout=30)
            self.assertEqual(completed.returncode,0,(completed.stdout,completed.stderr))
            result=json.loads(output.read_text(encoding='utf-8'))
            self.assertEqual(result['samples'],100)
            self.assertEqual(result['pointer_cache_growth'],0)
            self.assertLessEqual(result['rss_growth_bytes'],8388608)
            self.assertLessEqual(result['handles_after']-result['handles_before'],4)
            self.assertTrue(result['passed'])
