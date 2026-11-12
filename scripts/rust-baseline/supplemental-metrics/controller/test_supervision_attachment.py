"""Scoped extraction of reviewed supervision controls; no Source/Docker fixture."""
import concurrent.futures,json,os,sys,tempfile,threading,time,types,unittest
from pathlib import Path
from unittest import mock
from supervision import PayloadBudget,Capacity,Deadline,WorkDeadline,Registry,terminal,atomic_json
from bounded_process import Collected,CollectionFailure
LIMITS={'maximum_commands':4096,'cleanup_reserved_commands':32,'maximum_live_commands':16,'cleanup_reserved_live_commands':4,'final_join_seconds':30,'cleanup_wall_quality_ceiling_seconds':420}

class SupervisionControls(unittest.TestCase):
 def test_fixture_deadline_is_arrival_anchored_and_distinct(self):
   now=[10];d=WorkDeadline(5,lambda:now[0]);self.assertEqual(d.remaining(100),5);now[0]=15
   with self.assertRaisesRegex(Deadline,'FixtureWorkDeadline'):d.check()
 def test_global_payload_reserve_is_shared_and_precedes_write(self):
   b=PayloadBudget(100,20);b.charge(80)
   with self.assertRaises(Capacity):b.charge(1)
   b.charge(20,True)
   with self.assertRaises(Capacity):b.charge(1,True)
 def test_all_cleanup_steps_attempted_despite_independent_errors(self):
   with tempfile.TemporaryDirectory()as d:
    report={'passed':True};order=[]
    def step(name):
     def run():order.append(name);raise RuntimeError(name)
     return run
    terminal(report,d,[(n,step(n))for n in ['audit','logs','down','containers','networks','volumes']]);value=json.loads((Path(d)/'integration.json').read_text());self.assertEqual(order,['audit','logs','down','containers','networks','volumes']);self.assertEqual(len(value['terminal_actions']),6);self.assertFalse(value['passed']);self.assertTrue((Path(d)/'cleanup-checkpoint.json').exists())
 def test_primary_write_failure_has_independent_fallback(self):
   with tempfile.TemporaryDirectory()as d:
    def writer(p,v):
     if p.name=='integration.json':raise OSError('disk control')
     atomic_json(p,v)
    r=terminal({'passed':True},d,[],writer);self.assertEqual(r['terminal_persistence'],'terminal-fallback.json');self.assertFalse(json.loads((Path(d)/'terminal-fallback.json').read_text())['passed'])

class OwnedStopControls(unittest.TestCase):
 def call(self,replies,project='dispatcharr-m0-concurrent-0123456789'):
   from owned_stop import stop_project
   self.calls=[]
   def run(argv,**kwargs):self.calls.append(argv);return types.SimpleNamespace(stdout=replies.pop(0))
   return stop_project(['docker','compose','-p',project],{},project,15,run=run)
 def test_fixed_project_PIDzero_ack_after_both_inventories(self):
   cid=b'a'*64;project='dispatcharr-m0-concurrent-0123456789';r=self.call([cid+b'\n',b'',cid+b'\n',(project+' false 0\n').encode()]);self.assertTrue(r['all_stopped_PID_zero']);self.assertEqual(r['container_count'],1);self.assertEqual(len(self.calls),4)
 def test_running_or_foreign_label_never_acknowledged(self):
   cid=b'a'*64
   for state in [b'foreign false 0',b'dispatcharr-m0-concurrent-0123456789 true 123']:
    with self.assertRaisesRegex(RuntimeError,'Unacknowledged'):self.call([cid,b'',cid,state])
 def test_bad_or_excessive_IDs_refuse_before_kill(self):
   for ids in [b'not-a-container',b'\n'.join(bytes([c])*64 for c in b'abcde')]:
    with self.assertRaises(ValueError):self.call([ids])
    self.assertEqual(len(self.calls),1)
 def test_expired_stop_never_issues_command(self):
   from owned_stop import stop_project
   values=iter([0,16]);runner=mock.Mock()
   with self.assertRaisesRegex(TimeoutError,'OwnedStopDeadline'):stop_project([],{},'dispatcharr-m0-concurrent-0123456789',15,run=runner,clock=lambda:next(values))
   runner.assert_not_called()

class LocalProcessControls(unittest.TestCase):
 def registry(self,d,budget=None,limits=None,stop=None):return Registry(Collected,dict(os.environ),d,stop or(lambda:None),limits or LIMITS,budget or PayloadBudget(1000000))
 def test_actual_local_stdout_stderr_and_pipes_settle(self):
   with tempfile.TemporaryDirectory()as d:
    r=self.registry(d);value=r.run([sys.executable,'-c','import sys;print("out");print("err",file=sys.stderr)'],timeout=2);self.assertEqual(value,b'out\n');self.assertFalse(r.active);self.assertTrue(r.retired[0]['operation_successful']);self.assertEqual((Path(d)/'command-1.stderr').read_bytes(),b'err\n')
 def test_actual_local_nonzero_exit_retains_stdout_and_error(self):
   with tempfile.TemporaryDirectory()as d:
    r=self.registry(d)
    with self.assertRaisesRegex(CollectionFailure,'CommandExit7'):r.run([sys.executable,'-c','import sys;print("retained");sys.exit(7)'],timeout=2)
    self.assertFalse(r.active);self.assertEqual(r.retired[0]['returncode'],7);self.assertTrue(r.retired[0]['drained']);self.assertFalse(r.retired[0]['operation_successful'])
 def test_actual_early_child_exit_settles_blocked_stdin_writer(self):
   with tempfile.TemporaryDirectory()as d:
    r=self.registry(d)
    with self.assertRaisesRegex(CollectionFailure,'CommandExit7'):r.run([sys.executable,'-c','import sys;sys.exit(7)'],body=b'x'*100000,timeout=2)
    self.assertFalse(r.active);self.assertEqual(r.retired[0]['pipes_alive'],0);self.assertTrue(r.retired[0]['drained'])
 def test_actual_local_timeout_orders_stop_before_terminate(self):
   with tempfile.TemporaryDirectory()as d:
    events=[];r=self.registry(d,stop=lambda:events.append('owned-stop-ack'));item=r.launch([sys.executable,'-c','import time;time.sleep(10)']);original=item.process.terminate
    def terminate():events.append('terminate');original()
    item.process.terminate=terminate
    with self.assertRaisesRegex(CollectionFailure,'CollectionDeadline'):item.wait(.03)
    self.assertEqual(events,['owned-stop-ack','terminate']);self.assertTrue(r.retire(item)['drained']);self.assertFalse(r.active)
 def test_actual_global_output_cap_is_not_just_per_command_cap(self):
   with tempfile.TemporaryDirectory()as d:
    b=PayloadBudget(128);r=self.registry(d,budget=b)
    with self.assertRaisesRegex(CollectionFailure,'HostPayloadCapacity'):r.run([sys.executable,'-c','import sys;sys.stdout.write("x"*10000);sys.stdout.flush()'],timeout=2)
    self.assertLessEqual(b.used,128);self.assertFalse(r.active);self.assertTrue(r.retired[0]['drained'])
 def test_actual_concurrent_command_indices_and_paths_unique(self):
   with tempfile.TemporaryDirectory()as d:
    r=self.registry(d)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8)as pool:
     values=list(pool.map(lambda n:r.run([sys.executable,'-c','import sys;print(sys.argv[1]);print("diag",file=sys.stderr)',str(n)],timeout=3),range(12)))
    self.assertEqual(set(values),{(str(n)+'\n').encode()for n in range(12)});self.assertEqual(r.index,12);self.assertEqual(len(list(Path(d).glob('command-*.stderr'))),12);self.assertFalse(r.active)
 def test_actual_live_command_and_count_capacity_before_spawn(self):
   with tempfile.TemporaryDirectory()as d:
    limits={**LIMITS,'maximum_live_commands':1,'maximum_commands':1};r=self.registry(d,limits=limits);item=r.launch([sys.executable,'-c','import time;time.sleep(.05)'])
    with self.assertRaises(Capacity):r.launch([sys.executable,'-c','raise RuntimeError("never")'])
    item.wait(2);r.retire(item)
    with self.assertRaises(Capacity):r.launch([sys.executable,'-c','raise RuntimeError("never")'])
    self.assertEqual(r.index,1)
 def test_actual_unacknowledged_cleanup_retains_failure_without_termination(self):
   with tempfile.TemporaryDirectory()as d:
    def refused():raise RuntimeError('stop not acknowledged')
    r=self.registry(d,stop=refused);item=r.launch([sys.executable,'-c','import time;time.sleep(.15)'])
    with mock.patch.object(item.process,'terminate',wraps=item.process.terminate)as terminate:
     value=r.drain(.5);terminate.assert_not_called()
    self.assertFalse(value['drained']);self.assertTrue(value['host_clients_drained']);self.assertFalse(value['owned_stop_acknowledged']);self.assertEqual(value['owned_stop_error'],'RuntimeError');self.assertFalse(r.active)
 def test_command_input_and_argv_are_bounded_before_spawn(self):
   with tempfile.TemporaryDirectory()as d:
    r=self.registry(d,budget=PayloadBudget(10))
    with mock.patch('subprocess.Popen',side_effect=AssertionError('must not spawn')):
     with self.assertRaises(Capacity):r.launch([sys.executable,'-c','pass'],body=b'x'*11)
     with self.assertRaises(Capacity):r.launch([sys.executable,'x'*65537])
    self.assertEqual(r.index,0)
 def test_fixed_format_stop_controls_share_global_index_and_budget(self):
   import subprocess
   with tempfile.TemporaryDirectory()as d:
    b=PayloadBudget(1000);r=self.registry(d,budget=b)
    r.run([sys.executable,'-c','print("normal")'],timeout=2)
    result=r.stop_control([sys.executable,'-c','print("fixed")'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,stdin=subprocess.DEVNULL,timeout=2,check=True)
    self.assertEqual(result.stdout,b'fixed\n');self.assertEqual(r.index,2);self.assertEqual(b.used,13);self.assertTrue(r.retired[1]['fixed_format_stop_control']);self.assertFalse(r.active)
 def test_failed_owned_stop_is_not_implicitly_retried(self):
   with tempfile.TemporaryDirectory()as d:
    stop=mock.Mock(side_effect=RuntimeError('refused'));r=self.registry(d,stop=stop)
    for _ in range(2):
     with self.assertRaises(RuntimeError):r.stop()
    self.assertEqual(stop.call_count,1);self.assertFalse(r.stop_ack)
 def test_actual_terminal_drain_closes_child_and_both_pipes(self):
   with tempfile.TemporaryDirectory()as d:
    r=self.registry(d);item=r.launch([sys.executable,'-c','import time;time.sleep(10)']);value=r.drain(2);self.assertTrue(value['drained']);self.assertTrue(value['owned_stop_acknowledged']);self.assertFalse(r.active);self.assertFalse(item.settlement()['operation_successful'])

if __name__=='__main__':unittest.main()
