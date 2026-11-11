"""Negative metric/fixture controls; no processes/samplers start on import."""
import copy
import json
from pathlib import Path
import unittest
from m1_imports import load
_policy=load('policy')
validate_plan,validate_work,select_common_period,number=(_policy.validate_plan,_policy.validate_work,_policy.select_common_period,_policy.number)


class Controls(unittest.TestCase):
    def setUp(self):
        self.plan = json.loads((Path(__file__).resolve().parent.parent / 'protocol.json').read_text())

    def work(self, viewers=10):
        return dict(passed=True, backend='native', viewers=viewers, period_ms=250, produced=44,
                    drain_release_seen=True,
                    chunk_bytes=255868, final_full_bytes_per_viewer=44*255868, producer_late=0,
                    readers=[dict(delivered=44, measured=20 if i%10==0 else 40,
                                  bytes=44*255868, historical_reads=2 if i%10==0 else 0)
                             for i in range(viewers)])

    def rows(self):
        return [dict(backend='source', viewers=v, period_ms=p, passed=True, errors=0,
                     pacing_fraction=1, core_cpu_percent=100, core_memory_fraction=.5,
                     core_throttled_period_fraction=0) for v in [1,10,100] for p in [250,500,1000]]

    def test_fixed_bounds(self):
        validate_plan(self.plan)
        for key in ['retention_seconds', 'native_authoritative_history_bytes', 'native_local_cache_bytes', 'chunk_bytes', 'input_frame_bytes', 'minimum_samples']:
            changed = copy.deepcopy(self.plan)
            changed[key] -= 1
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_plan(changed)

    def test_valid_full_byte_and_history_work(self):
        self.assertEqual(validate_work(self.work(), 'native', 10, 250, 1, 10, True)['chunks'], 44)

    def test_wrong_bytes_or_drain_rejected(self):
        for key in ['delivered', 'bytes', 'measured']:
            value = self.work()
            value['readers'][0][key] = 0
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_work(value, 'native', 10, 250, 1, 10, True)

    def test_history_not_exercised_rejected(self):
        value = self.work()
        value['readers'][0]['historical_reads'] = 0
        with self.assertRaises(ValueError):
            validate_work(value, 'native', 10, 250, 1, 10, True)

    def test_corrupted_geometry_or_actor_rejected(self):
        for key, replacement in [('chunk_bytes', 8192), ('backend','source'), ('viewers',100), ('produced',43)]:
            value = self.work()
            value[key] = replacement
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_work(value, 'native', 10, 250, 1, 10, True)

    def test_all_source_cohorts_required(self):
        with self.assertRaises(ValueError):
            select_common_period(self.plan, self.rows()[:-1])

    def test_common_period_not_per_cohort_tuning(self):
        rows = self.rows()
        rows[6]['pacing_fraction'] = .5
        self.assertEqual(select_common_period(self.plan, rows), 500)

    def test_native_metrics_for_pacing_rejected(self):
        rows = self.rows()
        rows[0]['native_metrics'] = {}
        with self.assertRaises(ValueError):
            select_common_period(self.plan, rows)

    def test_numeric_missing_reset_nonfinite_rejected(self):
        for value in [None, -1, True, float('nan'), float('inf')]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                number(value)

    def test_missing_resource_metric_rejected(self):
        rows = self.rows()
        rows[0].pop('core_cpu_percent')
        with self.assertRaises(ValueError):
            select_common_period(self.plan, rows)


if __name__ == '__main__':
    unittest.main()
