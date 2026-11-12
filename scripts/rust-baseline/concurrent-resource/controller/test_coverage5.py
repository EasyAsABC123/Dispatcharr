"""Offline denial/coverage controls. Real-denial data is retained kernel evidence.
No Docker, proc read, sampler, network or Source workload is executed here.
"""
import copy,importlib.util,json,tempfile,threading,time,unittest
from pathlib import Path
from unittest import mock
import coverage
from resource_window import Window,proposed_checks
HERE=Path(__file__).resolve().parent
DATA=json.loads((HERE/'captured-kernel-denial.json').read_text())
POLICY=json.loads((HERE/'policy.json').read_text())
def merged(name):
 x=copy.deepcopy(DATA[name]);return coverage.merge_census(x['inventory_start'],x['per_uid'],x['inventory_end'])
def row(service,stable=True):return {'phase':'idle-before-first-playback','phase_stable':stable,'complete':True,'census':{s:copy.deepcopy(service)for s in coverage.SERVICES}}
class Coverage5(unittest.TestCase):
 def test_zero_denial_retained_kernel_positive(self):
  value=merged('complete');self.assertTrue(coverage.complete_service(value));self.assertTrue(coverage.eligible_census_row(row(value)))
 def test_real_nondumpable_kernel_denial_stays_incomplete(self):
  value=merged('denied');self.assertGreater(value['permission_denied_processes']+value['permission_denied_descriptors'],0);self.assertFalse(value['complete']);self.assertFalse(coverage.eligible_census_row(row(value)))
 def test_coverage_false_without_count_cannot_be_complete(self):
  data=copy.deepcopy(DATA['complete']);data['per_uid'][0]['coverage_complete']=False
  self.assertFalse(coverage.merge_census(data['inventory_start'],data['per_uid'],data['inventory_end'])['complete'])
 def test_stable_PID_guard_rejects_start_end_change(self):
  data=copy.deepcopy(DATA['complete']);data['inventory_end']['process_ids'].append(999999)
  self.assertFalse(coverage.merge_census(data['inventory_start'],data['per_uid'],data['inventory_end'])['complete'])
 def test_duplicate_processes_are_fatal(self):
  data=copy.deepcopy(DATA['complete']);data['per_uid'].append(copy.deepcopy(data['per_uid'][0]))
  with self.assertRaises(ValueError):coverage.merge_census(data['inventory_start'],data['per_uid'],data['inventory_end'])
 def test_error_or_incomplete_role_cannot_count(self):
  value=merged('complete');value['error_type']='JSONDecodeError';self.assertFalse(coverage.complete_service(value))
  r=row(merged('complete'));r['census'].pop('worker');self.assertFalse(coverage.eligible_census_row(r));self.assertFalse(coverage.eligible_census_row(row(merged('complete'),False)))
 def test_four_good_plus_many_denied_never_pad_five(self):
  from types import SimpleNamespace
  rows=[]
  for phase in ['idle-before-first-playback','idle-after-import-playback']:
   for name,count in [('complete',4),('denied',20)]:
    for _ in range(count):r=row(merged(name));r['phase']=phase;rows.append(r)
  window=SimpleNamespace(samples=SimpleNamespace(rows=[]),censuses=SimpleNamespace(rows=rows),workload_after=None)
  checks=proposed_checks(window,POLICY);counts=[c for c in checks if c['id'].endswith('/complete-census-count')];self.assertEqual([c['observed']for c in counts],[4,4]);self.assertTrue(all(not c['passed']for c in counts))
 def window(self,folder,reply):
  selected={s:{'id':s,'limits':POLICY['caps'][s]}for s in coverage.SERVICES};w=Window(mock.Mock(),selected,reply,POLICY,Path(folder)/'out',coverage);w.started=time.monotonic();return w
 def test_real_denial_command_replay_continues_without_probe(self):
  data=DATA['denied'];responses=[data['inventory_start'],*data['per_uid'],data['inventory_end']]
  with tempfile.TemporaryDirectory()as d:
   w=self.window(d,lambda *a,**k:json.dumps(responses.pop(0)).encode());v=w._service_census({'id':'control'});self.assertFalse(v['complete']);self.assertFalse(w.errors);w.check()
 def test_unrelated_parse_error_fatal_not_just_incomplete(self):
  with tempfile.TemporaryDirectory()as d:
   w=self.window(d,lambda *a,**k:b'not-json')
   with self.assertRaises(json.JSONDecodeError):w._service_census({'id':'control'})
   self.assertTrue(w.errors)
   with self.assertRaises(RuntimeError):w.check()
   self.assertTrue(list(w.output.glob('census-failed-*.json')))
 def test_main_and_direct_execute_still_hardblocked(self):
  spec=importlib.util.spec_from_file_location('resource5_blocked',HERE/'run.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  with mock.patch.object(module.subprocess,'check_output')as command:
   with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):module.main()
   with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):module.execute(None,None,None,1)
   command.assert_not_called()
if __name__=='__main__':unittest.main()
