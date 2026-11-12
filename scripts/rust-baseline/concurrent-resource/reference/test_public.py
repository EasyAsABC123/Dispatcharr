"""Public numeric corruption/reconstruction controls; no Source/Docker/network."""
import copy,json,shutil,tempfile,unittest
from pathlib import Path
from unittest import mock
import verify,reconstruct
HERE=Path(__file__).resolve().parent
class Public(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.parent=Path(self.tmp.name)/'public';self.directory=self.parent/'reference';shutil.copytree(HERE,self.directory,ignore=shutil.ignore_patterns('__pycache__'));shutil.copytree(HERE.parent/'controller',self.parent/'controller',ignore=shutil.ignore_patterns('__pycache__'))
 def mutate(self,fn):
  p=self.directory/'observations.json';v=json.loads(p.read_text());fn(v);p.write_text(json.dumps(v))
 def check(self):return verify.verify(self.directory)
 def test_public_only_valid_reference_still_negative(self):
  r=self.check();self.assertTrue(r['reference_observation_valid_all3']);self.assertFalse(r['Source_budget_acceptance']);self.assertEqual((r['resource_rows'],r['census_rows'],r['eligible_census_rows']),(774,342,335));self.assertFalse((self.parent/'artifacts').exists())
 def test_missing_repetition_not_a_complete_characterization(self):
  self.mutate(lambda v:v['repetitions'].pop())
  with self.assertRaisesRegex(ValueError,'ExactlyThree'):self.check()
 def test_duplicate_repetition_not_three_independent_runs(self):
  self.mutate(lambda v:v['repetitions'].__setitem__(2,copy.deepcopy(v['repetitions'][0])))
  with self.assertRaisesRegex(ValueError,'ExactlyThree'):self.check()
 def test_CPU_counter_corruption_rejects(self):
  self.mutate(lambda v:v['repetitions'][0]['resources'][10]['containers'][0].update(cpu_usage_ns=0))
  with self.assertRaisesRegex(ValueError,'CPU'):self.check()
 def test_memory_sum_corruption_rejects(self):
  self.mutate(lambda v:v['repetitions'][0]['resources'][10].update(memory_bytes=1))
  with self.assertRaisesRegex(ValueError,'Sums'):self.check()
 def test_denied_census_cannot_be_counted_complete(self):
  def change(v):
   row=next(r for r in v['repetitions'][0]['censuses']if r['phase']=='idle-before-first-playback'and r['phase_stable']);row['census']['worker']['per_uid'][0]['coverage_complete']=False
  self.mutate(change)
  with self.assertRaises(ValueError):self.check()
 def test_zero_process_growth_not_relabelled_source_pass(self):
  self.mutate(lambda v:v['repetitions'][0]['recorded_checks'].__setitem__(0,{'id':'fake','passed':True}))
  with self.assertRaisesRegex(ValueError,'BudgetChecks'):self.check()
 def test_counter_reset_and_OOM_remain_fatal(self):
  def change(v):v['repetitions'][0]['workload_cgroup']['after']['services']['worker']['memory_events']['oom']+=1
  self.mutate(change)
  with self.assertRaisesRegex(ValueError,'OOM'):self.check()
 def test_observer_errors_not_source_budget_only(self):
  self.mutate(lambda v:v['repetitions'][0]['observer_errors'].append({'error_type':'PermissionError'}))
  with self.assertRaisesRegex(ValueError,'ObserverValidity'):self.check()
 def test_cue_or_summary_tamper_rejects(self):
  p=self.directory/'historical-cue-1.json';p.write_bytes(p.read_bytes()+b'\n')
  with self.assertRaisesRegex(ValueError,'CueHash'):self.check()
 def test_reconstruct_exact36_fresh_only_never_execute(self):
  output=Path(self.tmp.name)/'activated'
  with mock.patch('subprocess.Popen',side_effect=AssertionError('must never execute')):r=reconstruct.reconstruct(output,self.directory)
  self.assertEqual(r['reconstructed_files'],36);self.assertTrue(r['public_entries_still_blocked'])
  with self.assertRaisesRegex(ValueError,'FreshExternal'):reconstruct.reconstruct(output,self.directory)
 def test_reconstruct_rejects_wrong_base_or_template(self):
  p=self.directory/'activated-run.py.in';p.write_bytes(p.read_bytes()+b'\n')
  with self.assertRaisesRegex(ValueError,'ActivationBinding'):reconstruct.reconstruct(Path(self.tmp.name)/'activated',self.directory)
 def test_deep_mode_no_private_fallback(self):
  with self.assertRaises(FileNotFoundError):verify.verify(self.directory,Path(self.tmp.name))
if __name__=='__main__':unittest.main()
