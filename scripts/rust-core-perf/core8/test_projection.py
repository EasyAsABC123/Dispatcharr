"""Public numeric controls require only the committed sanitized projection.
No ignored raw archive, compiler, Docker, network or actual workload is needed.
"""
import copy
import importlib.util
import json
from pathlib import Path
import unittest
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('_core8_public_numeric_tested',HERE/'verify_projection.py')
v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
@unittest.skipUnless((HERE/'results/projection.json').is_file(),'Sanitized dataset must be bundled for public acceptance')
class PublicNumericControls(unittest.TestCase):
    def setUp(self):self.data=json.loads((HERE/'results/projection.json').read_text())
    def bad(self,change):
        change(self.data)
        with self.assertRaises((ValueError,KeyError,TypeError)):v.validate(self.data)
    def test_public_complete_arithmetic(self):self.assertEqual(v.validate(self.data)['samples'],1620)
    def test_production(self):self.bad(lambda x:x.update(production_source_map_sha256='0'*64))
    def test_ELF(self):self.bad(lambda x:x.update(native_binary_sha256='0'*64))
    def test_source_seal(self):self.bad(lambda x:x['source_plan'].update(selected_common_period_ms=1))
    def test_missing_release(self):self.bad(lambda x:x['source_plan']['fixture_binding'].pop('native_release_record_sha256'))
    def test_cleanup(self):self.bad(lambda x:x.update(cleanup='retained'))
    def test_db(self):self.bad(lambda x:x.update(default_database_unchanged=False))
    def test_missing_phase(self):self.bad(lambda x:x['phases'].pop())
    def test_reordered_phase(self):self.bad(lambda x:x['phases'].reverse())
    def test_cpu_delta(self):self.bad(lambda x:x['phases'][0]['counters']['services']['core'].update(cpu_percent_mean=0.1))
    def test_cpu_reset(self):self.bad(lambda x:x['phases'][0]['counters']['after']['services']['core']['cpu'].update(usage_usec=0))
    def test_oom(self):self.bad(lambda x:x['phases'][0]['counters']['after']['services']['core']['memory_events'].update(oom=1))
    def test_footprint_formula(self):self.bad(lambda x:x['phases'][0]['samples'][0]['container_memory'][0].update(memory_usage_bytes=1))
    def test_window(self):self.bad(lambda x:x['phases'][0]['samples'][0].update(included_in_window=False))
    def test_minimum_samples(self):self.bad(lambda x:x['phases'][0].update(samples=x['phases'][0]['samples'][:2]))
    def test_final_bytes(self):self.bad(lambda x:x['phases'][0]['readers'][0].update(bytes=1))
    def test_drain(self):self.bad(lambda x:x['phases'][0]['peer'].update(drain_release_seen=False))
    def test_count(self):self.bad(lambda x:x.update(sample_count=0))
    def test_pair_budget(self):self.bad(lambda x:x['pairs'][0].update(cpu_ratio=0.1))
    def test_median(self):self.bad(lambda x:x['cohort_medians']['1'].update(memory_ratio=0.1))
    def test_parent_review(self):self.bad(lambda x:x['parent_review'].update(passed=False))
    def test_final_lifecycle(self):self.bad(lambda x:x['global_lifecycle'].pop('final-lifecycle.json'))
if __name__=='__main__':unittest.main()
