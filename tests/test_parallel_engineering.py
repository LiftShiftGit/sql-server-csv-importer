"""Local actual-process scheduling/resource tests; not SQL integration mocks."""
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import unittest
from tempfile import TemporaryDirectory
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.pipeline import prepare, recover_plan
from db_ingest.resources import process_tree_memory, resource_scope, checkpoint
from db_ingest.scheduling import admission, bounded_map

ROOT=Path(__file__).resolve().parents[1]


def allocated_worker(argument):
    buffer=bytearray(argument)
    for index in range(0,len(buffer),4096):buffer[index]=1
    checkpoint(force=True)
    return len(buffer)


class ParallelEngineeringTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.root=Path(self.temp.name)
        self.source=self.root/'source';self.source.mkdir()
        self.config=load_config(environ={})
        self.config['source'].update(root=str(self.source),path=str(self.source),include=['**/*.csv'])
        self.config['preservation'].update(work_directory=str(self.root/'work'),minimum_free_disk_bytes=0)
        self.config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
        self.config['parallel'].update(enabled=True,max_workers=2)
        self.config['limits'].update(total_process_tree_memory_bytes=1024*1024*1024,worker_memory_bytes=128*1024*1024)

    def tearDown(self):self.temp.cleanup()

    def files(self,count=4,rows=150):
        expected={}
        for i in range(count):
            raw=('id,text\n'+''.join(f'{j},file{i}-value{j}\n' for j in range(rows))).encode()
            (self.source/f'file{i}.csv').write_bytes(raw)
            expected[f'file{i}.csv']=hashlib.sha256(raw).hexdigest()
        return expected

    def test_two_actual_workers_separate_pids_and_full_ledgers(self):
        expected=self.files()
        report=prepare(self.config)
        self.assertEqual(report['exit_code'],0,report)
        policy=report['execution_policy']
        self.assertEqual(policy['effective_workers'],2)
        self.assertEqual(policy['requested_workers'],2)
        self.assertLessEqual(policy['peak_pending_tasks'],2)
        self.assertLessEqual(policy['peak_queued_argument_bytes'],self.config['limits']['max_queue_bytes'])
        self.assertGreater(policy['sampled_peak_process_tree_rss_bytes'],0)
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            events=journal.events(report['run_id'])
            pids={e['pid'] for e in events if e['category']=='WORKER_STARTED'}
            self.assertEqual(len(pids),2)
            self.assertNotIn(os.getpid(),pids)
            self.assertEqual(journal.run(report['run_id'])['inventory_state'],'COMPLETE')
            self.assertEqual(len(list(journal.selections(report['run_id']))),4)
        finally:journal.close()
        for file in report['files']:
            self.assertEqual(file['counts']['pending'],150)
            self.assertEqual(file['counts']['header'],1)
            self.assertEqual(file['counts']['byte_coverage'],'complete contiguous spans plus BOM')
            self.assertEqual(file['artifact']['sha256'],expected[file['artifact']['relative_path']])

    def test_completed_capture_same_intent_reprofiles_without_source(self):
        self.files(count=1,rows=10)
        report=prepare(self.config)
        item=report['files'][0]
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            journal.reset_unpublished_profile(report['run_id'],item['file_id'])
            selected=next(journal.selections(report['run_id']))
            journal.update_selection(report['run_id'],selected['selection_id'],state='CAPTURED')
        finally:journal.close()
        (self.source/'file0.csv').unlink()
        recovered=recover_plan(self.config,report['run_id'])
        self.assertEqual(recovered['exit_code'],0,recovered)
        again=recovered['files'][0]
        self.assertEqual(again['file_id'],item['file_id'])
        self.assertEqual(again['intent_id'],item['intent_id'])
        self.assertEqual(again['artifact']['path'],item['artifact']['path'])
        self.assertEqual(again['counts']['pending'],10)

    def test_admission_reduces_requested_workers(self):
        current=process_tree_memory().total_rss_bytes
        worker=128*1024*1024
        self.config['parallel']['max_workers']=8
        self.config['limits'].update(worker_memory_bytes=worker,total_process_tree_memory_bytes=current+2*worker+32*1024*1024)
        policy=admission(self.config)
        self.assertEqual(policy['workers'],2)
        self.assertEqual(policy['requested_workers'],8)

    def test_unmeetable_budget_blocks_admission(self):
        self.config['limits']['total_process_tree_memory_bytes']=1
        with self.assertRaises(IngestError) as cm:admission(self.config)
        self.assertEqual(cm.exception.code,'RESOURCE_ADMISSION')

    def test_worker_rss_breach_stops_admission_and_is_observed(self):
        outcomes=[];metrics={}
        self.config['parallel']['max_workers']=1
        with self.assertRaises(IngestError) as cm:
            for argument,outcome in bounded_map(self.config,allocated_worker,[128*1024*1024]*3,metrics):
                outcomes.append(outcome)
        self.assertEqual(cm.exception.code,'RESOURCE_BUDGET')
        self.assertEqual(len(outcomes),1)
        self.assertEqual(outcomes[0]['error']['code'],'RESOURCE_BUDGET')
        self.assertGreater(cm.exception.context['observed_rss_bytes'],128*1024*1024)

    def test_real_child_rss_and_os_released_selection_lease(self):
        script="from db_ingest.journal import Journal;import sys,os; j=Journal(sys.argv[1]); lease=j.lease('probe');lease.__enter__();buf=bytearray(32*1024*1024);buf[::4096]=b'x'*(len(buf)//4096);print('READY',os.getpid(),flush=True);sys.stdin.readline();lease.__exit__(None,None,None);j.close()"
        process=subprocess.Popen([sys.executable,'-c',script,str(self.root/'lease-work')],cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        journal=Journal(self.root/'lease-work')
        try:
            ready=process.stdout.readline().split()
            self.assertEqual(ready[0],'READY')
            owner_pid=int(ready[1])
            measured=process_tree_memory()
            self.assertIn(process.pid,measured.processes)
            # Windows venv python.exe is a small redirector parent; the actual
            # interpreter holds the allocation in its child process.
            child_tree=process_tree_memory(process.pid)
            self.assertGreater(child_tree.total_rss_bytes,32*1024*1024)
            self.assertTrue(set(child_tree.processes)<=set(measured.processes))
            with self.assertRaises(IngestError) as cm:
                with journal.lease('probe'):pass
            self.assertEqual(cm.exception.code,'RUN_BUSY')
            import signal
            os.kill(owner_pid,signal.SIGTERM);process.communicate(timeout=10)
            with journal.lease('probe'):pass
        finally:
            if process.poll() is None:process.kill();process.communicate(timeout=10)
            journal.close()

    @unittest.skipUnless(os.name=='nt','Windows Job Object lifecycle evidence')
    def test_forced_worker_exit_terminates_descendant_process_tree(self):
        from db_ingest.resources import process_identity
        import signal
        script="from db_ingest.config import load_config;from db_ingest.resources import configure;import subprocess,sys,os,json;configure(load_config(environ={}),None,int(sys.argv[1]));child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);print(json.dumps([os.getpid(),child.pid]),flush=True);sys.stdin.readline()"
        process=subprocess.Popen([sys.executable,'-c',script,str(os.getpid())],cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        child_ids={}
        try:
            line=process.stdout.readline()
            self.assertTrue(line, "worker did not emit lifecycle identity")
            owner,child=json.loads(line)
            child_tree=process_tree_memory(child)
            child_ids={pid:process_identity(pid) for pid in child_tree.processes}
            self.assertTrue(child_ids)
            os.kill(owner,signal.SIGTERM)
            process.communicate(timeout=10)
            deadline=time.monotonic()+3
            while time.monotonic()<deadline and any(process_identity(pid)==identity for pid,identity in child_ids.items() if identity is not None):
                time.sleep(0.02)
            self.assertTrue(all(process_identity(pid)!=identity for pid,identity in child_ids.items() if identity is not None))
        finally:
            if process.poll() is None:process.kill();process.communicate(timeout=10)
            for pid,identity in child_ids.items():
                if identity is not None and process_identity(pid)==identity:
                    try:os.kill(pid,signal.SIGTERM)
                    except OSError:pass

    def test_full_plan_artifact_survives_byte_bounded_report(self):
        self.config['parallel']['enabled']=False
        header=','.join(f'field_{i}' for i in range(30))
        values=','.join('1' for _ in range(30))
        (self.source/'wide.csv').write_text(header+'\n'+values+'\n',encoding='utf-8')
        report=prepare(self.config);self.assertEqual(report['exit_code'],0,report)
        item=report['files'][0]
        artifact=Path(item['counts']['plan_artifact'])
        self.assertEqual(hashlib.sha256(artifact.read_bytes()).hexdigest(),item['counts']['plan_artifact_sha256'])
        self.assertEqual(len(json.loads(artifact.read_text())['plan']['columns']),30)
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            small=journal.report_files(report['run_id'],max_bytes=4096)
            self.assertEqual(len(small),1)
            self.assertTrue(small[0].get('plan_omitted_from_report') or small[0].get('details_omitted_from_report'))
            self.assertLessEqual(len(json.dumps(small[0],ensure_ascii=True)),4096)
            self.assertEqual(len(journal.file(report['run_id'],item['file_id'])['plan']['columns']),30)
        finally:journal.close()

    def test_stale_parent_pid_cannot_attribute_older_unrelated_process(self):
        from db_ingest.resources import _tree_members
        # PID10 was recycled: oldprocess30 retained parentPID10, but was born
        # before the current parent10. Its largechild31 must also be excluded.
        parents={10:1,20:10,21:20,30:10,31:30}
        created={10:1000,20:1100,21:1200,30:500,31:600}
        members,identities,unknown=_tree_members(10,parents,created.get)
        self.assertEqual(members,{10,20,21})
        self.assertEqual(set(identities),members)
        self.assertEqual(unknown,set())

    def test_serial_worker_scope_still_enforces_worker_reservation(self):
        self.config['parallel']['enabled']=False
        self.config['limits']['worker_memory_bytes']=1
        with self.assertRaises(IngestError) as cm:
            with resource_scope(self.config,worker=True):pass
        self.assertEqual(cm.exception.code,'RESOURCE_BUDGET')
        self.assertEqual(cm.exception.context['budget_bytes'],1)

    def test_interrupted_inventory_retains_assigned_intent_and_missing_selection(self):
        from db_ingest.config import redact
        self.files(count=2,rows=3)
        journal=Journal(self.config['preservation']['work_directory'])
        run_id='interrupted-inventory-test'
        try:
            journal.create_run(run_id,redact(self.config))
            journal.begin_inventory(run_id)
            selection=journal.add_selection(run_id,self.source/'file0.csv','file0.csv','selected','original partial inventory')
            journal.flush()
            before=journal.selection(run_id,selection)
        finally:journal.close()
        (self.source/'file0.csv').unlink()
        recovered=recover_plan(self.config,run_id)
        self.assertEqual(recovered['exit_code'],4,recovered)
        journal=Journal(self.config['preservation']['work_directory'])
        try:
            after=journal.selection(run_id,selection)
            self.assertEqual(after['intent_id'],before['intent_id'])
            self.assertEqual(after['status'],'selected')
            self.assertEqual(after['inventory_seen'],0)
            self.assertIn('not observed',after['reason'])
            self.assertEqual(len(list(journal.selections(run_id))),2)
        finally:journal.close()
        self.assertEqual(len(recovered['files']),1)
        self.assertEqual(recovered['files'][0]['counts']['pending'],3)

    def test_eight_cold_coordinators_share_bootstrap_without_sqlite_busy(self):
        directory=self.root/'cold-journal';gate=self.root/'start-gate'
        script="from db_ingest.journal import Journal;from pathlib import Path;import sys,time,json,os;gate=Path(sys.argv[2]);deadline=time.monotonic()+10;exec('while not gate.exists() and time.monotonic()<deadline: time.sleep(0.005)');j=Journal(sys.argv[1]);j.create_run(sys.argv[3],{});print(json.dumps({'pid':os.getpid(),'version':j.db.execute('SELECT MAX(version) FROM versions').fetchone()[0],'mode':j.db.execute('PRAGMA journal_mode').fetchone()[0]}),flush=True);j.close()"
        processes=[subprocess.Popen([sys.executable,'-c',script,str(directory),str(gate),f'cold-{i}'],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for i in range(8)]
        gate.write_text('start')
        try:
            results=[]
            for process in processes:
                output,error=process.communicate(timeout=35)
                self.assertEqual(process.returncode,0,(output,error))
                results.append(json.loads(output))
            self.assertEqual(len({r['pid'] for r in results}),8)
            self.assertTrue(all(r['version']==Journal.SCHEMA_VERSION and r['mode']=='wal' for r in results))
            journal=Journal(directory)
            try:self.assertEqual(journal.db.execute('SELECT COUNT(*) FROM runs').fetchone()[0],8)
            finally:journal.close()
        finally:
            import signal
            from db_ingest.resources import process_identity
            for process in processes:
                if process.poll() is None:
                    try:tree=process_tree_memory(process.pid)
                    except IngestError:tree=None
                    if tree:
                        for pid,identity in tree.creation_identities.items():
                            if process_identity(pid)==identity:
                                try:os.kill(pid,signal.SIGTERM)
                                except OSError:pass
                    process.communicate(timeout=10)

    def test_pending_allocation_estimate_respects_global_gate(self):
        self.config['parallel']['enabled']=False
        with resource_scope(self.config):
            with self.assertRaises(IngestError) as cm:checkpoint(estimated_bytes=2*1024*1024*1024)
        self.assertEqual(cm.exception.code,'RESOURCE_BUDGET')


if __name__=='__main__':unittest.main()


class BoundedJournalTransactionTests(unittest.TestCase):
    def test_framing_never_holds_a_sqlite_write_transaction(self):
        from unittest.mock import patch
        import db_ingest.pipeline as pipeline
        with TemporaryDirectory() as folder:
            root=Path(folder);source=root/'source';source.mkdir()
            raw=b'number,text\n'+b'1,small\n'*103+b'2,'+b'x'*1100000+b'\n3,last\n'
            (source/'rows.csv').write_bytes(raw)
            config=load_config(environ={});config['parallel']['enabled']=False
            config['source'].update(root=str(source),path=str(source),include=['**/*.csv'])
            config['preservation'].update(work_directory=str(root/'work'),minimum_free_disk_bytes=0)
            config['parsing'].update(header='present',delimiter=',',encoding='utf-8')
            instances=[];batches=[];original_init=Journal.__init__;original_write=Journal.save_prepared_records
            def tracking_init(instance,*args,**kwargs):original_init(instance,*args,**kwargs);instances.append(instance)
            def tracking_write(instance,rows):
                sizes=[len(row[-1]) for row in rows]
                self.assertLessEqual(len(rows),100)
                self.assertTrue(sum(sizes)<=1048576 or len(rows)==1)
                batches.append((len(rows),sum(sizes)));original_write(instance,rows)
                self.assertFalse(instance.db.in_transaction)
            modules=list(pipeline._modules());original_parse=modules[3];framed=[]
            def checked_parse(*args):
                iterator=iter(original_parse(*args))
                while True:
                    for instance in instances:
                        try:self.assertFalse(instance.db.in_transaction,'parser.next() must not hold SQLite WAL write lock')
                        except __import__('sqlite3').ProgrammingError:pass
                    try:record=next(iterator)
                    except StopIteration:return
                    framed.append(record.record_id);yield record
            modules[3]=checked_parse
            with patch.object(Journal,'__init__',tracking_init),patch.object(Journal,'save_prepared_records',tracking_write),patch.object(pipeline,'_modules',return_value=tuple(modules)):
                report=prepare(config)
            self.assertEqual(report['exit_code'],0,report)
            self.assertEqual(len(framed),106);self.assertEqual(report['files'][0]['counts']['pending'],105)
            self.assertTrue(any(count==1 and size>1048576 for count,size in batches))
            self.assertTrue(any(count==100 for count,size in batches))
