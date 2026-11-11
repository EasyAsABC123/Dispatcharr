"""Retained actual evidence with isolated corruptions; no Docker or metrics."""
import copy,json,unittest
from pathlib import Path
import publish
ROOT=Path('artifacts/rust/m1-core-experiment-2')
# Explicit input allows public tests to use any retained exact experiment.
import os
ROOT=Path(os.environ.get('M1_RETAINED_EXPERIMENT',ROOT))
@unittest.skipUnless(ROOT.is_dir(),'Set M1_RETAINED_EXPERIMENT to exact retained evidence for actual publication corruption controls')
class PublicationTests(unittest.TestCase):
 def check_bad(self,name,mutate):
  def load(path):
   data=publish.read(path)
   if path.name==name:mutate(data)
   return data
  with self.assertRaises((ValueError,KeyError,TypeError)):publish.validate(ROOT,load)
 def test_actual_pass(self):self.assertEqual(publish.validate(ROOT)['sample_count'],1620)
 def test_missing_phase(self):self.check_bad('experiment.json',lambda v:v['phases'].pop())
 def test_seal_change(self):self.check_bad('frozen-plan.json',lambda v:v.update(selected_common_period_ms=500))
 def test_source_rate(self):self.check_bad('source-qualification.json',lambda v:v[0].update(pacing_fraction=.1))
 def test_cleanup(self):self.check_bad('functional.json',lambda v:v.update(cleanup='retained'))
 def test_byte_corruption(self):self.check_bad('measure-100-3-native.json',lambda v:v['readers'][0].update(bytes=1))
 def test_lifecycle_oom(self):self.check_bad('measure-1-1-source-setup-lifecycle.json',lambda v:v['services']['core']['memory_events'].update(oom=1))
 def test_reset(self):self.check_bad('measure-1-1-source-resources.json',lambda v:v['counters']['after']['services']['core']['cpu'].update(usage_usec=0))
 def test_working_set(self):self.check_bad('measure-1-1-source-resources.json',lambda v:v['samples'][0]['containers'][0].update(memory_bytes=1))
 def test_cadence_missing(self):self.check_bad('measure-1-1-source-resources.json',lambda v:v.update(samples=v['samples'][:2]))
 def test_late_sample(self):self.check_bad('measure-1-1-source-resources.json',lambda v:v['samples'][0].update(sample_completed_epoch=v['window_end_epoch']+1))
 def test_exit(self):self.check_bad('measure-1-1-source.json.exit.json',lambda v:v.update(worker_exit_code=9))
 def test_producer_pacing(self):self.check_bad('measure-1-1-source.json',lambda v:v.update(producer_late=300))
 def test_native_budget(self):self.check_bad('measure-1-1-native-normalized.json',lambda v:v['metrics'].update(cpu_percent_mean=999999))
 def test_final_counter_oom(self):self.check_bad('measure-1-1-source-resources.json',lambda v:v['counters']['after']['services']['core']['memory_events'].update(oom_kill=1))
if __name__=='__main__':unittest.main()
