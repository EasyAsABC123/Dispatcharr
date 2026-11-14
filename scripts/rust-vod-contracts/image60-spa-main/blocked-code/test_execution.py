"""Measured cleanup coverage negatives, actual no-app process stop, immutable binding."""
import copy,json,os,sys,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
import test_plan
import execution_validation as E
import candidate_guard as C
from bounded_process import Collected
import run as R
class Completion(unittest.TestCase):
 def setUp(self):
  p=test_plan.Plan();p.setUp();self.v=copy.deepcopy(p.v);self.v.update(cleanup_steps=[{'step':k,'settled':True}for k in E.STEPS],owned_backends_after_stop=0,fresh_project_cleanup={'container':[],'network':[],'volume':[]},fresh_exact_CID_cleanup=[{'role':k,'CID':str(i)*64,'absent':True}for i,k in enumerate(['candidate','origin','baseline','data-init'],1)],capture_phase_elapsed_seconds=1.,cleanup_elapsed_seconds=1.,host_elapsed_seconds=3.,cleanup_within90=True,host_within600=True,commands=[{'PID':i+2,'starttime':'123','physically_settled':True,'pipe_threads_settled':True,'collection_invalid':None,'began_epoch':1.,'completed_epoch':2.,'actual_exit':0,'allowed_exit_codes':[0],'argv':['test'],'stdout_sha256':'a'*64,'stderr_sha256':'b'*64}for i in range(15)],original_host_identity_absence=[{'starttime':'123','current_starttime':None}for i in range(18)])

  from test_bootstrap_context import fixture
  context,audit=fixture();self.v['migration_builtin_context']=context
  for key in ['default_before','default_after']:self.v[key]['database']='dispatcharr';self.v[key]['tables'].update(copy.deepcopy(audit['tables']))
 def bad(self,fn):
  v=copy.deepcopy(self.v);fn(v)
  with self.assertRaises((ValueError,KeyError,TypeError)):E.validate(v)
 def test_complete_synthetic(self):self.assertTrue(E.validate(self.v)['full_measured_cleanup_qualified'])
 def test_missing_cleanup(self):self.bad(lambda v:v['cleanup_steps'].pop())
 def test_failed_cleanup(self):self.bad(lambda v:v['cleanup_steps'][0].update(settled=False))
 def test_backend_present(self):self.bad(lambda v:v.update(owned_backends_after_stop=1))
 def test_missing_CID(self):self.bad(lambda v:v['fresh_exact_CID_cleanup'].pop())
 def test_over_budget(self):self.bad(lambda v:v.update(cleanup_elapsed_seconds=91))
 def test_unsettled_pipe(self):self.bad(lambda v:v['commands'][0].update(pipe_threads_settled=False))
 def test_missing_stream_hash(self):self.bad(lambda v:v['commands'][0].pop('stdout_sha256'))
 def test_business_exit1(self):self.bad(lambda v:v['commands'][0].update(actual_exit=1,allowed_exit_codes=[0,1]))
 def test_original_PID_still_present(self):self.bad(lambda v:v['original_host_identity_absence'][0].update(current_starttime='123'))
 def test_readiness_only_exit1(self):
  self.v['commands'][0].update(actual_exit=1,allowed_exit_codes=[0,1],argv=['python','/spa-tools/native_control.py','ready']);E.validate(self.v)
class Binding(unittest.TestCase):
 def test_actual_candidate_metadata_archive_credit(self):self.assertEqual(C.validate()['app_inputs'],1353)
 def test_wrong_candidate_refused_before_archive(self):
  b=json.loads((R.HERE/'candidate-binding.json').read_text());b['image_id']='wrong'
  with self.assertRaisesRegex(ValueError,'ExactImage60'):C.validate(b)
 def test_real_origin_SIGTERM_wait_and_handler_close(self):
  with tempfile.TemporaryDirectory()as folder:
   root=Path(folder);program="from origin import Trap,serve;from pathlib import Path;serve(Trap('a'*32,Path("+repr(folder)+"),host='127.0.0.1',port=0))"
   env=dict(os.environ);env['PYTHONPATH']=str(R.HERE);job=Collected([sys.executable,'-c',program],env,65536,lambda:None,stdout_path=root/'stdout',stderr_path=root/'stderr')
   try:
    until=time.monotonic()+3
    while not(root/'origin-ready.json').exists():
     if job.process.poll()is not None or time.monotonic()>until:raise AssertionError('ActualOriginReady')
     time.sleep(.01)
    job.process.terminate();R.settled_output(job,4,(0,),root/'stdout');v=json.loads((root/'origin-terminal.json').read_text());self.assertTrue(v['shutdown_acknowledged']and v['socket_listener_closed']and v['handler_threads_joined']);self.assertTrue(job.physically_settled)
   finally:
    if job.process.poll()is None:job.process.kill();job.wait(3)
if __name__=='__main__':unittest.main()
