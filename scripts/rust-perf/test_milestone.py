import copy
import unittest

import milestone


def budget():
    return dict(format_version=1, scope='docker-http-resource-harness', frozen=True,
                aggregation='every-pair', baseline_revision='old', candidate_revision='new',
                hardware_id='host', docker_version='29', minimum_repetitions=3,
                baseline_services=['web', 'redis'], candidate_services=['rust', 'redis'],
                scenarios=[dict(id='same-10', scenario='same-channel', viewers=10,
                                distinct_channels=1, fixture_id='fixture-copy', warmup_seconds=15,
                                measurement_seconds=60, timeout_seconds=3, slow_delay_seconds=0,
                                sample_interval_seconds=1, maximum_errors=0, minimum_bytes_per_second=95,
                                maximum_first_byte_p95_ms=100, minimum_first_byte_samples=10,
                                maximum_candidate_to_baseline_cpu_ratio=.8,
                                maximum_candidate_to_baseline_memory_ratio=.8,
                                minimum_candidate_to_baseline_throughput_ratio=.95)])


def evidence():
    pairs = []
    for trial in range(3):
        pair = {}
        for role in ('baseline', 'candidate'):
            report = dict(format_version=1, revision='old' if role == 'baseline' else 'new',
                          timestamp_utc=f'trial-{trial}-{role}',
                          services=['web', 'redis'] if role == 'baseline' else ['rust', 'redis'],
                          environment=dict(host='host', docker='29'),
                          workload=dict(scenario='same-channel', viewers=10, url_count=1,
                                        fixture_id='fixture-copy', warmup=15, duration=60,
                                        timeout=3, slow_delay=0, sample_interval=1),
                          measurement=dict(summary=dict(bytes_per_second=100, first_byte_samples=10,
                                                        first_byte_ms_p95=25, errors=0,
                                                        empty_responses=0, viewers_without_data=0,
                                                        cpu_percent_mean=100 if role == 'baseline' else 70,
                                                        memory_bytes_peak=1000 if role == 'baseline' else 700),
                                           resource_samples=[{}, {}, {}], sample_errors=[]))
            pair[role] = report
        pairs.append(pair)
    return {'scenarios': [{'id': 'same-10', 'pairs': pairs}]}


class MilestoneTests(unittest.TestCase):
    def test_all_three_pairs_pass_declared_resource_gates(self):
        result = milestone.evaluate(budget(), evidence())
        self.assertTrue(result['passed'], result)
        self.assertEqual(len(result['pairs']), 3)
        self.assertFalse(result['application_performance_verified'])

    def test_unfrozen_missing_null_or_unsupported_budget_fails(self):
        for key, value in [('frozen', False), ('hardware_id', None),
                           ('scope', 'full-service-playback')]:
            manifest = budget()
            manifest[key] = value
            self.assertFalse(milestone.evaluate(manifest, evidence())['passed'])
        manifest = budget()
        del manifest['candidate_revision']
        self.assertFalse(milestone.evaluate(manifest, evidence())['passed'])
        manifest = budget()
        manifest['scenarios'][0]['cleanup_deadline_seconds'] = 10
        self.assertFalse(milestone.evaluate(manifest, evidence())['passed'])

    def test_missing_scenarios_or_insufficient_pairs_fail(self):
        self.assertFalse(milestone.evaluate(budget(), {'scenarios': []})['passed'])
        reports = evidence()
        reports['scenarios'][0]['pairs'].pop()
        self.assertFalse(milestone.evaluate(budget(), reports)['passed'])

    def test_any_error_or_frozen_workload_mismatch_fails(self):
        for mutate in (lambda row: row['measurement']['summary'].update(errors=1),
                       lambda row: row['workload'].update(scenario='many-channel'),
                       lambda row: row['measurement']['summary'].update(bytes_per_second=10),
                       lambda row: row['measurement']['summary'].update(first_byte_ms_p95=200),
                       lambda row: row.update(services=['rust'])):
            reports = evidence()
            mutate(reports['scenarios'][0]['pairs'][1]['candidate'])
            self.assertFalse(milestone.evaluate(budget(), reports)['passed'])

    def test_duplicate_report_cannot_count_as_another_repetition(self):
        reports = evidence()
        reports['scenarios'][0]['pairs'][1] = copy.deepcopy(reports['scenarios'][0]['pairs'][0])
        self.assertFalse(milestone.evaluate(budget(), reports)['passed'])


if __name__ == '__main__':
    unittest.main()
