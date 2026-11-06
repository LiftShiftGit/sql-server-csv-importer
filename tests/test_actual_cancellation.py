"""Actual two-process cooperative cancellation; deliberately no SQL assertions."""
import json,os,time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest
from db_ingest.config import load_config
from db_ingest.journal import Journal
from db_ingest.scheduling import bounded_map
from db_ingest.resources import checkpoint,process_identity


def _execute_one(argument):
    journal=Journal(argument['config']['preservation']['work_directory'])
    journal.event(argument['run_id'],None,'WORKER_STARTED',{'phase':'publish'})
    try:
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:checkpoint();time.sleep(.01)
        return {'exit_code':0}
    finally:
        journal.event(argument['run_id'],None,'WORKER_FINISHED',{'phase':'publish'});journal.close()


class ActualCancellationTests(unittest.TestCase):
    def test_two_started_workers_drain_before_injected_interrupt_returns(self):
        with TemporaryDirectory() as folder:
            config=load_config(environ={});config['preservation']['work_directory']=str(Path(folder)/'work')
            config['destination'].update(database='DbIngestTest_CancelOnly',control_database='DbIngestTest_CancelOnlyControl')
            config['parallel'].update(enabled=True,max_workers=2);config['load']['engine']='odbc'
            config['limits'].update(total_process_tree_memory_bytes=1024*1024*1024,worker_memory_bytes=128*1024*1024)
            journal=Journal(config['preservation']['work_directory']);journal.create_run('cancel-run',config);journal.close()
            arguments=[{'config':config,'run_id':'cancel-run','file_id':str(i)} for i in range(2)]
            with patch.dict(os.environ,{'DB_INGEST_TEST_MODE':'1','DB_INGEST_TEST_FAULT':'CANCEL_MULTIPLE_WORKERS'}):
                with self.assertRaises(KeyboardInterrupt):list(bounded_map(config,_execute_one,iter(arguments),{}))
            journal=Journal(config['preservation']['work_directory'])
            try:
                events=journal.events('cancel-run');request=[json.loads(e['details']) for e in events if e['category']=='TEST_CANCELLATION_REQUESTED']
                self.assertEqual(len(request),1);pids=set(request[0]['worker_pids']);self.assertEqual(len(pids),2)
                self.assertEqual(request[0]['native_pids'],[])
                self.assertEqual(set(request[0]['creation_identities']),{str(pid) for pid in pids})
                self.assertIn('injected KeyboardInterrupt',request[0]['signal'])
                self.assertEqual({e['pid'] for e in events if e['category']=='WORKER_FINISHED'},pids)
                self.assertTrue(all(process_identity(pid) is None for pid in pids))
            finally:journal.close()
