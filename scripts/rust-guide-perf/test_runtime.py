import copy
import io
import unittest
from unittest.mock import patch
import cache
import observer
import orchestrator
import plan


class Cache(unittest.TestCase):
    def test_bounded_resp_and_exact_four_key_reset(self):
        self.assertEqual(cache.reply(io.BytesIO(b'*3\r\n:4\r\n$2\r\nok\r\n$-1\r\n')), [4, b'ok', None])
        for raw in [b'$1025\r\n', b'*17\r\n', b'$2\r\na\r\n', b'-permission denied\r\n']:
            with self.assertRaises(ValueError):
                cache.reply(io.BytesIO(raw))
        with self.assertRaises(ValueError):
            cache.Cache(6379)
        client = cache.Cache(62000)
        with patch.object(client, 'command', side_effect=[b'7', 4]) as command:
            self.assertEqual(client.clear()['owned_xml_keys_removed'], 4)
        args = command.call_args.args
        self.assertEqual(args, ('DEL', *(cache.BASE + '7:' + suffix for suffix in ['chunks', 'ready', 'status', 'lock'])))
        with patch.object(client, 'command', return_value=b'not-an-integer'):
            with self.assertRaises(ValueError):
                client.state()

    def test_cache_witness_preserves_member_sizes_types_ttls_and_empty_state(self):
        client = cache.Cache(62000)
        values = [3, 100, b'1', b'ready', 0, 299, 10, 60,
                  [b'list', b'string', b'string', b'none'], [299, 299, 299, -2]]
        with patch.object(client, 'command', side_effect=[b'0', values]):
            state = client.state()
        self.assertEqual(state['chunks'], 3)
        self.assertEqual(state['bytes'], 100)
        self.assertEqual(state['smallest_chunk_bytes'], 10)
        self.assertEqual(state['largest_chunk_bytes'], 60)
        self.assertEqual(state['key_types']['chunks'], 'list')
        self.assertEqual(state['key_ttl_seconds']['lock'], -2)
        self.assertTrue(orchestrator.cache_ready(state, {'bytes': 100}))
        with patch.object(client, 'command', side_effect=[b'0', values[:6]]):
            with self.assertRaises(ValueError):
                client.state()


class Observer(unittest.TestCase):
    def test_complete_stack_counters_and_working_memory(self):
        selected = {service: {'id': chr(97+i)*64, 'limits': {'memory_bytes': 100, 'nano_cpus': 10**9}}
                    for i, service in enumerate(['baseline', 'origin', 'candidate'])}
        limits = dict(cpu_quota=100000, cpu_period=100000, memory_max=100,
                      memory_events=dict(oom=0, oom_kill=0),
                      cpu=dict(usage_usec=100, nr_periods=10, nr_throttled=0, throttled_usec=0),
                      sampled_monotonic=1)
        def read(identifier, expected):
            return copy.deepcopy(limits)
        with patch('observer.load_perf') as perf, patch('observer.cgroup.read', side_effect=read), patch('observer.docker_stats.Stats') as direct:
            perf.return_value.resources.return_value = {'memory_bytes': 60, 'cpu_percent': 1,
                                                       'containers': [{'id': v['id'][:12], 'memory_bytes': 20} for v in selected.values()]}
            direct.return_value.sample.return_value = perf.return_value.resources.return_value
            seen = observer.Observer(selected, '.')
            before = seen.counters()
            limits['sampled_monotonic'] = 3
            limits['cpu']['usage_usec'] += 2000000
            result = seen.finish(before)
            self.assertEqual(result['cpu_percent_mean'], 300)
            self.assertEqual(set(result['services']), set(selected))
            self.assertEqual(seen.sample()['memory_bytes'], 60)


class Normalizer(unittest.TestCase):
    def test_cold_wave_completion_and_sample_errors_survive_normalization(self):
        work = next(v for v in plan.POLICY['workloads'] if v['name'] == 'xmltv-cold-10')
        measurement = {'waves': [{'consumers': [dict(passed=True, ttfb_ms=2, completion_ms=3)]}],
                       'completed': 1, 'errors': 1, 'late_completed': 1, 'expected_waves': 3,
                       'resource_samples': [{'memory_bytes': 100}], 'sample_errors': ['TimeoutError']}
        value = orchestrator.normalized(measurement, {'cpu_percent_mean': 10}, work, 20, 60,
                                       {'cache_state_verified': False, 'fixture_unchanged': True})
        self.assertEqual(value['pacing_fraction'], 1/30)
        self.assertEqual(value['errors'], 1)
        self.assertEqual(value['late_completed'], 1)
        self.assertEqual(value['sample_errors'], 1)
        self.assertFalse(value['cache_state_verified'])
        self.assertFalse(value['correct_responses'])
        self.assertTrue(orchestrator.cache_ready(dict(ready='1', status='ready', locked=False,
                                                     chunks=3, bytes=20, ready_ttl_seconds=299), {'bytes': 20}))


if __name__ == '__main__':
    unittest.main()
