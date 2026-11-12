"""UNEXECUTED adversarial test text. No activity until explicit unittest run.

Signal double models swallowed receiver exceptions; an additional genuine Celery
signal-dispatch check remains required in the disposable Source qualification.
"""
import importlib.util,json,os,tempfile,threading,types,unittest
from pathlib import Path
from unittest import mock
from bounded_process import Collected
from resource_window import Window,proposed_checks
HERE=Path(__file__).resolve().parent

class Signal:
 def connect(self,receiver):return receiver
 def send(self,receiver):
  try:return [(receiver,receiver())]
  except Exception as error:return [(receiver,error)]

class Adversarial(unittest.TestCase):
 def test_swallowed_signal_error_still_leaves_capacity_witness(self):
  with tempfile.TemporaryDirectory()as directory:
   base=Path(directory);(base/'worker-observer-budget.json').write_text(json.dumps({'records':1024,'bytes':0}))
   signals=types.ModuleType('celery.signals')
   for n in ['task_prerun','task_postrun','after_task_publish','worker_ready']:setattr(signals,n,Signal())
   celery=types.ModuleType('dispatcharr.celery');celery.app=object()
   with mock.patch.dict('sys.modules',{'celery.signals':signals,'dispatcharr.celery':celery}),mock.patch.dict(os.environ,JOB_DISPOSABLE_FIXTURE='playback-fixture-only'),mock.patch('pathlib.Path.mkdir'):
    spec=importlib.util.spec_from_file_location('bounded_task_control',HERE/'concurrent_task_observer.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
   original_open=os.open
   def owned_open(path,*args):return original_open(str(base/Path(path).name),*args)
   with mock.patch.object(module,'ROOT',base),mock.patch.object(module.os,'open',side_effect=owned_open):
    observed=Signal().send(lambda:module.append('prerun',{'task':'fixture'}))
   self.assertIsInstance(observed[0][1],RuntimeError)
   self.assertTrue(json.loads((base/'worker-observer-capacity.json').read_text())['capacity_exceeded'])
   self.assertFalse((base/'workflow-prerun.jsonl').exists())

 def test_sampler_failure_invalidates_foreground_check(self):
  window=object.__new__(Window);window.lock=threading.Lock();window.errors=[];window.stop=threading.Event();window.phase='idle';window.epoch=1
  observer=mock.Mock();observer.sample.side_effect=RuntimeError('fixture sampler failure');window.observer=observer
  window._resources()
  self.assertEqual(window.errors[0]['loop'],'resource')
  with self.assertRaises(RuntimeError):window.check()

 def test_abort_acknowledges_owned_stop_before_cli_termination(self):
  actions=[];collection=object.__new__(Collected);collection.lock=threading.Lock();collection.stop_lock=threading.Lock();collection.invalid=None;collection.stopped=False
  collection.stop_owned=lambda:actions.append('own-containers-stopped')
  collection.process=mock.Mock();collection.process.poll.return_value=None;collection.process.terminate.side_effect=lambda:actions.append('host-cli-terminated')
  collection.abort('ControllerAbort')
  self.assertEqual(actions,['own-containers-stopped','host-cli-terminated']);self.assertEqual(collection.invalid,'ControllerAbort')

 def test_unacknowledged_owned_stop_never_becomes_settled(self):
  collection=object.__new__(Collected);collection.lock=threading.Lock();collection.stop_lock=threading.Lock();collection.invalid=None;collection.stopped=False;collection.physically_settled=False;collection.process=mock.Mock()
  collection.stop_owned=mock.Mock(side_effect=RuntimeError('stop refused'))
  with self.assertRaises(RuntimeError):collection.abort()
  self.assertFalse(collection.physically_settled);self.assertFalse(collection.stopped);collection.process.terminate.assert_not_called()

 def test_insufficient_four_role_idle_census_fails_without_padding(self):
  policy=json.loads((HERE/'policy.json').read_text());window=types.SimpleNamespace(samples=types.SimpleNamespace(rows=[]),censuses=types.SimpleNamespace(rows=[]),workload_after=None)
  for phase in ['idle-before-first-playback','idle-after-import-playback']:
   for _ in range(4):
    from coverage import merge_census
    data=json.loads((HERE/'captured-kernel-denial.json').read_text())['complete'];service=merge_census(data['inventory_start'],data['per_uid'],data['inventory_end'])
    window.censuses.rows.append({'phase':phase,'phase_stable':True,'complete':True,'census':{s:service for s in ['baseline','source','origin','worker']}})
  checks=proposed_checks(window,policy);counts=[c for c in checks if c['id'].endswith('/complete-census-count')]
  self.assertEqual([c['observed']for c in counts],[4,4]);self.assertTrue(all(not c['passed']for c in counts))

 def test_unvalidated_execution_path_blocked(self):
  source=(HERE/'run.py').read_text();self.assertIn('RESOURCE_EXECUTION_BLOCKED=True',source);self.assertIn("raise RuntimeError('UnvalidatedBoundedCollectorExecutionBlocked')",source)

if __name__=='__main__':unittest.main()
