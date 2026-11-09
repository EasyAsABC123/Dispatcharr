import copy
import math
import unittest
from unittest.mock import MagicMock, patch
import api_load


class ApiLoadTests(unittest.TestCase):
    def response(self, payload):
        response = MagicMock(status=200)
        response.__enter__.return_value = response
        response.read.side_effect = [payload[:1], payload[1:]]
        return response

    def test_null_reference_is_checked_and_boolean_number_types_do_not_collapse(self):
        for payload, expected in [(b'1', None), (b'{"v":1}', {'v': True})]:
            with self.subTest(payload=payload), patch('urllib.request.urlopen', return_value=self.response(payload)):
                with self.assertRaises(ValueError):
                    api_load.request('http://fixture', '/data/', 'fixture-only', 1, expected)
        with patch('urllib.request.urlopen', return_value=self.response(b'null')) as opened:
            value, _, _ = api_load.request('http://fixture', '/data/', 'fixture-only', 1, None)
            self.assertIsNone(value)
            self.assertEqual(opened.call_args.args[0].get_header('Authorization'), 'Bearer fixture-only')

    def test_response_read_is_capped_before_json_decode(self):
        with patch('urllib.request.urlopen', return_value=self.response(b'x'*(api_load.MAX_RESPONSE_BYTES+1))) as opened:
            with self.assertRaises(ValueError):
                api_load.request('http://fixture', '/data/', 'fixture-only', 1)
            self.assertEqual(opened.return_value.read.call_args_list[-1].args, (api_load.MAX_RESPONSE_BYTES,))

    def fixture(self):
        return {'workload': {'users': 10, 'period': .25, 'paths': ['/a', '/b', '/c']},
                'environment': {'host': 'fixture'},
                'measurement': {'summary': {'completed': 2400, 'errors': 0,
                    'users_without_success': 0, 'requests_per_second': 40,
                    'ttfb_ms_p95': 10, 'roundtrip_ms_p95': 10,
                    'cpu_percent_mean': 10, 'memory_bytes_peak': 100,
                    'per_path': {'/a': 800, '/b': 800, '/c': 800}},
                    'resource_samples': [{}, {}, {}], 'sample_errors': []}}

    def test_error_latency_throughput_and_resource_regressions_cannot_pass(self):
        old = self.fixture()
        self.assertTrue(api_load.compare(old, copy.deepcopy(old))['passed'])
        for metric, value in [('errors', 1), ('users_without_success', 1),
                              ('requests_per_second', 37), ('cpu_percent_mean', 11),
                              ('memory_bytes_peak', 101), ('ttfb_ms_p95', 12),
                              ('roundtrip_ms_p95', 12), ('cpu_percent_mean', math.nan)]:
            with self.subTest(metric=metric, value=value):
                new = copy.deepcopy(old)
                new['measurement']['summary'][metric] = value
                self.assertFalse(api_load.compare(old, new)['passed'])

    def test_missing_endpoints_and_observer_errors_cannot_pass(self):
        old = self.fixture()
        for mutation in ['missing-path', 'zero-path', 'bad-count', 'samples', 'observer']:
            new = copy.deepcopy(old)
            measurement = new['measurement']
            if mutation == 'missing-path':
                del measurement['summary']['per_path']['/b']
            elif mutation == 'zero-path':
                measurement['summary']['per_path'] = {'/a': 2400, '/b': 0, '/c': 0}
            elif mutation == 'bad-count':
                measurement['summary']['completed'] = 2401
            elif mutation == 'samples':
                measurement['resource_samples'] = [{}, {}]
            else:
                measurement['sample_errors'] = ['observer-error']
            with self.subTest(mutation=mutation):
                self.assertFalse(api_load.compare(old, new)['passed'])

    def test_underdriven_source_and_workload_or_host_changes_cannot_pass(self):
        old = self.fixture()
        old['measurement']['summary']['requests_per_second'] = 30
        self.assertFalse(api_load.compare(old, copy.deepcopy(old))['passed'])
        old = self.fixture()
        for section, field, value in [('workload', 'users', 11), ('environment', 'host', 'other')]:
            new = copy.deepcopy(old)
            new[section][field] = value
            self.assertFalse(api_load.compare(old, new)['passed'])

    def test_workload_limits_reject_nan_or_unbounded_worker_counts(self):
        for users, duration in [(0, 1), (101, 1), (1, math.nan), (1, 301)]:
            with self.assertRaises(ValueError):
                api_load.phase('http://fixture', ['/a'], 'fixture', {}, users=users,
                               duration=duration, period=.25, timeout=1)


if __name__ == '__main__':
    unittest.main()
