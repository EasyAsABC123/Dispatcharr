"""Offline virtual observation + explicitly local subprocess controls only.
No Docker, Source, /proc census or resource sampler is run by this module.
"""
import concurrent.futures,copy,json,os,sys,tempfile,threading,time,types,unittest
from pathlib import Path
from unittest import mock
from supervision import PayloadBudget,Capacity,Deadline,WorkDeadline,Registry,terminal,atomic_json
from bounded_process import Collected,CollectionFailure
from resource_window import Window,Journal,SERVICES
from interval_coverage import evaluate,intervals
from finalization import finish
import metadata
HERE=Path(__file__).resolve().parent
POLICY=json.loads((HERE/'policy.json').read_text())
LIMITS=POLICY['resource_draft']['supervision']

def complete():
 from coverage import merge_census
 value=json.loads((HERE/'captured-kernel-denial.json').read_text())['complete']
 return merge_census(value['inventory_start'],value['per_uid'],value['inventory_end'])

def virtual():
 phases=[dict(phase='idle-before-first-playback',epoch=1,begin=0,end=20),dict(phase='concurrent-import-playback',epoch=2,begin=20,end=140),dict(phase='idle-after-import-playback',epoch=3,begin=140,end=160)]
 samples=[];census=[]
 for p in phases:
  for t in range(p['begin'],p['end']):
   samples.append(dict(phase=p['phase'],phase_epoch=p['epoch'],phase_stable=True,cpu_interval_stable=True,cpu_intervals={s:[t,t+1]for s in SERVICES}))
  for t in range(p['begin'],p['end'],4):
   census.append(dict(phase=p['phase'],phase_epoch=p['epoch'],phase_stable=True,complete=True,monotonic=t,completed_monotonic=t+.1,census={s:complete()for s in SERVICES}))
 return samples,census,phases

class IntervalControls(unittest.TestCase):
 def test_complete_virtual_cadence_all_roles_and_phases(self):
  checks=evaluate(*virtual(),POLICY);self.assertEqual(len(checks),15);self.assertTrue(all(x['passed']for x in checks))
 def test_CPU_tail_missing_fails_even_with_sixty_samples(self):
  s,c,p=virtual();s=[r for r in s if r['phase_epoch']!=2 or r['cpu_intervals']['source'][0]<100]
  checks=evaluate(s,c,p,POLICY);self.assertTrue(all(not x['passed']for x in checks if '/CPU-'in x['id']and 'concurrent'in x['id']))
 def test_internal_gap_and_edges_not_padded(self):
  self.assertFalse(intervals([0,20],[(0,1),(10,20)],2.5,2.5)['passed']);self.assertFalse(intervals([0,20],[(5,20)],2.5,2.5)['passed'])
 def test_denied_census_never_fills_minimum_or_edges(self):
  s,c,p=virtual()
  for r in c:
   if r['phase_epoch']==1:r['census']['worker']['per_uid'][0]['coverage_complete']=False
  checks=evaluate(s,c,p,POLICY);self.assertFalse(next(x['passed']for x in checks if x['id']=='idle-before-first-playback/complete-census-intervals'))
 def test_wrong_epoch_and_incomplete_CPU_service_fail(self):
  s,c,p=virtual()
  for r in s:
   if r['phase_epoch']==1:r['phase_epoch']=0
   if r['phase_epoch']==2:del r['cpu_intervals']['worker']
  self.assertFalse(all(x['passed']for x in evaluate(s,c,p,POLICY)))
 def test_duplicate_required_phase_cannot_replace_missing(self):
  s,c,p=virtual();p[2]=p[0];self.assertFalse(all(x['passed']for x in evaluate(s,c,p,POLICY)))
 def test_duplicate_overlapping_intervals_cannot_pad_sample_minimum(self):
  self.assertFalse(intervals([0,20],[(0,20)]*60,2.5,2.5)['passed'])
 def test_nonfinite_clock_and_phase_instability_rejected(self):
  self.assertFalse(intervals([0,float('inf')],[(0,1)],1,1)['passed'])
  s,c,p=virtual()
  for r in s:r['phase_stable']=False
  self.assertFalse(all(x['passed']for x in evaluate(s,c,p,POLICY)))
 def test_existing_Source_budgets_and_minimums_unchanged(self):
  old=json.loads((HERE/'previous-policy.json').read_text());new=copy.deepcopy(POLICY)
  for key in ['budgets_inherited_without_tuning','minimum_workload_cpu_samples','minimum_workload_resource_samples','minimum_complete_stable_idle_censuses']:self.assertEqual(old['resource_draft'][key],new['resource_draft'][key])
  self.assertEqual(old['caps'],new['caps']);self.assertEqual(old['measure_seconds'],new['measure_seconds']);self.assertEqual(old['warmup_seconds'],new['warmup_seconds'])

class SupervisionControls(unittest.TestCase):
 def test_failed_role_retains_all_four_sibling_results(self):
  with tempfile.TemporaryDirectory()as d:
   w=Window(None,{s:{'id':s}for s in SERVICES},None,POLICY,Path(d)/'window',None)
   def one(item):
    if item['id']=='worker':raise ValueError('bad JSON')
    return dict(complete=True,role=item['id'],retained='sentinel')
   w._service_census=one
   with concurrent.futures.ThreadPoolExecutor(max_workers=4)as pool:r=w.collect_cycle(pool)
   self.assertEqual(set(r['census']),SERVICES);self.assertEqual(sum(v.get('retained')=='sentinel'for v in r['census'].values()),3);self.assertEqual(r['census']['worker']['error_type'],'ValueError');self.assertFalse(r['complete']);self.assertEqual(len(w.censuses.rows),1)
 def test_fixture_deadline_is_arrival_anchored_and_distinct(self):
  now=[10];d=WorkDeadline(5,lambda:now[0]);self.assertEqual(d.remaining(100),5);now[0]=15
  with self.assertRaisesRegex(Deadline,'FixtureWorkDeadline'):d.check()
 def test_global_payload_reserve_is_shared_and_precedes_write(self):
  b=PayloadBudget(100,20);b.charge(80)
  with self.assertRaises(Capacity):b.charge(1)
  b.charge(20,True)
  with self.assertRaises(Capacity):b.charge(1,True)
 def test_journal_total_budget_fails_before_disk_mutation(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'rows';b=PayloadBudget(15);j=Journal(p,10,100,100,b);j.append({'x':1});before=p.read_bytes()
   with self.assertRaises(Capacity):j.append({'long':'value'})
   self.assertEqual(p.read_bytes(),before)
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
 def test_finalize_never_skips_down_or_independent_inventory(self):
  with tempfile.TemporaryDirectory()as d:
   registry=mock.Mock();registry.lock=threading.Lock();registry.active={};registry.drain.return_value={'drained':True};events=[]
   def fail():events.append('logs');raise RuntimeError('logs')
   def inventory(kind):events.append(kind);return 'owned-remains'if kind=='containers'else''
   r=finish({'passed':True},d,None,registry,lambda:False,fail,lambda:events.append('down'),inventory,LIMITS)
   self.assertEqual(events,['logs','down','containers','networks','volumes']);self.assertFalse(r['passed']);self.assertFalse(r['terminal_actions']['containers']['ok']);self.assertTrue(r['terminal_actions']['volumes']['ok'])
 def test_final_join_occurs_after_exact_owned_stop(self):
  with tempfile.TemporaryDirectory()as d:
   w=Window(None,{s:{'id':s}for s in SERVICES},None,POLICY,Path(d)/'window',None);events=[]
   class Thread:
    name='held-census'
    def __init__(self):self.alive=True
    def join(self,seconds):events.append('join')
    def is_alive(self):return self.alive
   t=Thread();w.threads=[t];registry=mock.Mock();registry.lock=threading.Lock();item=mock.Mock();item.abort.side_effect=lambda reason:(events.append('abort-cli'),setattr(t,'alive',False));registry.active={1:item};registry.stop.side_effect=lambda:events.append('stop-PID0');w.registry=registry
   r=w.close();self.assertEqual(events,['join','stop-PID0','abort-cli','join']);self.assertTrue(r['observer_threads_drained']);self.assertTrue(r['escalated']);self.assertIs(w.close(),r)
 def test_unacknowledged_stop_retains_undrained_status(self):
  with tempfile.TemporaryDirectory()as d:
   w=Window(None,{s:{'id':s}for s in SERVICES},None,POLICY,Path(d)/'window',None);t=mock.Mock();t.name='unsettled';t.is_alive.return_value=True;w.threads=[t];w.registry=mock.Mock();w.registry.stop.side_effect=TimeoutError('not stopped');r=w.close();self.assertFalse(r['observer_threads_drained']);self.assertEqual(r['owned_stop_error'],'TimeoutError');self.assertEqual(r['undrained_threads'],['unsettled']);w.registry.active.assert_not_called()
 def test_metadata_virtual_commands_only_whitelisted_fields(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);source=root/'source';source.mkdir();(source/'file').write_text('owned');controller=root/'controller';controller.mkdir();(controller/'policy.json').write_text('{}');calls=[]
   selected={s:dict(id=str(i)*64,limits={'memory_bytes':5,'nano_cpus':6})for i,s in enumerate(sorted(SERVICES),1)}
   def command(argv,timeout):
    calls.append(argv)
    if argv[:2]==['docker','inspect']:return json.dumps([{'Id':argv[2],'Image':'image','Config':{'Image':'ref','Env':['SECRET=must-not-save']},'State':{'StartedAt':'clock'},'HostConfig':dict(Memory=5,NanoCpus=6,PidsLimit=0,MemorySwap=10)}]).encode()
    if argv[:2]==['docker','exec']:return b'[["Django","1"]]'
    return b'virtual diagnostic'
   original=Path.read_bytes
   def files(p):return b'virtual proc text'if str(p).startswith('/proc/')else original(p)
   with mock.patch.object(Path,'read_bytes',files),mock.patch.object(metadata.platform,'platform',return_value='virtual platform'):
    value=metadata.capture(command,selected,POLICY,source,controller,root,PayloadBudget(1000000))
   self.assertEqual(len(calls),8);self.assertNotIn('must-not-save',json.dumps(value));self.assertEqual(len(value['services']),4)

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
 def test_both_resource_entry_points_block_before_any_subprocess(self):
  import importlib.util
  spec=importlib.util.spec_from_file_location('blocked_resource6',HERE/'run.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  with mock.patch('subprocess.Popen',side_effect=AssertionError('must not run')):
   with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):module.main()
   with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):module.execute(None,None,None,1)

if __name__=='__main__':unittest.main()
