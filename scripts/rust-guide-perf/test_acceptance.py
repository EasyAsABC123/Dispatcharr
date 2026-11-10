import copy
import unittest
import acceptance
import plan


class Acceptance(unittest.TestCase):
    def phase(self):
        return dict(cpu_percent_mean=100, peak_working_memory_bytes=100000,
                    ttfb_p95_ms=10, completion_p95_ms=100, throughput_per_second=10,
                    pacing_fraction=1, samples=60, errors=0, sample_errors=0,
                    late_completed=0, oom_kills=0, counter_resets=0,
                    correct_responses=True, cache_state_verified=True,
                    fixture_unchanged=True, limits_verified=True)

    def test_each_predeclared_failure_is_retained(self):
        source = self.phase()
        for key, value in [('errors', 1), ('sample_errors', 1),
                           ('oom_kills', 1), ('counter_resets', 1), ('samples', 49),
                           ('pacing_fraction', .94), ('correct_responses', False),
                           ('cache_state_verified', False), ('fixture_unchanged', False),
                           ('limits_verified', False), ('cpu_percent_mean', 101),
                           ('peak_working_memory_bytes', 100001), ('completion_p95_ms', 111),
                           ('ttfb_p95_ms', 56), ('throughput_per_second', 9.4)]:
            candidate = dict(source, **{key: value})
            with self.subTest(key=key):
                self.assertFalse(acceptance.compare(source, candidate)['passed'])

    def test_floor_zero_and_numeric_types(self):
        value = self.phase()
        self.assertTrue(acceptance.compare(value, dict(value, ttfb_p95_ms=55))['passed'])
        self.assertTrue(acceptance.compare(value, dict(value, late_completed=1))['passed'])
        self.assertFalse(acceptance.compare(dict(value, cpu_percent_mean=0), value)['passed'])
        for change in [{'errors': False}, {'samples': 60.0}, {'cpu_percent_mean': float('nan')}]:
            with self.assertRaises(ValueError):
                acceptance.phase(dict(value, **change))

    def test_all_fifteen_pairs_required_without_failed_pair_replacement(self):
        rows = [dict(workload=work['name'], trial=trial,
                     order=plan.POLICY['orders'][trial - 1], source=self.phase(), candidate=self.phase())
                for work in plan.POLICY['workloads'] for trial in range(1, 4)]
        self.assertTrue(acceptance.matrix(rows)['passed'])
        for invalid in [rows[:-1], rows + [rows[0]]]:
            with self.assertRaises(ValueError):
                acceptance.matrix(invalid)
        failed = copy.deepcopy(rows)
        failed[4]['candidate']['cpu_percent_mean'] = 101
        self.assertFalse(acceptance.matrix(failed)['passed'])
        failed[4]['order'].reverse()
        with self.assertRaises(ValueError):
            acceptance.matrix(failed)


if __name__ == '__main__':
    unittest.main()
