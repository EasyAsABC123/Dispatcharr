import copy
import json
import unittest
from pathlib import Path
import publish


class Publish(unittest.TestCase):
    def test_all_retained_samples_and_counters_survive_without_unselected_secrets(self):
        artifact = Path(__file__).resolve().parents[2]/'artifacts/rust/m5-m7-guide-experiment23-recovery-1/matrix/xmltv-hot-1-trial-1-source.json'
        if not artifact.exists():
            self.skipTest('Historical ignored raw artifact absent; public projection control covers durable evidence')
        value = json.loads(artifact.read_text())
        value['token'] = 'PRIVATE_TOKEN'
        value['measurement']['body'] = {'uuid': 'PRIVATE_UUID'}
        value['measurement']['error_text'] = 'PRIVATE_EXCEPTION'
        value['counters']['before']['services']['source']['cpu']['PRIVATE_COUNTER_KEY'] = 1
        result = publish.project(value)
        raw = json.dumps(result)
        for secret in ['PRIVATE_TOKEN','PRIVATE_UUID','PRIVATE_EXCEPTION','PRIVATE_COUNTER_KEY']:
            self.assertNotIn(secret, raw)
        self.assertEqual(len(result['resource_samples']), 60)
        self.assertEqual(result['normalized'], value['normalized'])
        self.assertEqual(set(result['resource_samples'][0]['services']), {'baseline','origin','source'})
        self.assertEqual(result['counter_snapshots']['before']['services']['baseline']['sampled_relative_seconds'], 0)
        self.assertFalse(result['individual_current_hot_request_timings_available'])
        self.assertFalse(result['cache_numeric_witness_available'])
        value['measurement']['resource_samples'][0]['containers'][0]['id'] = 'unknown'
        with self.assertRaises(ValueError):
            publish.project(value)

    def test_bounded_traces_and_cache_witness_drop_unselected_text(self):
        names = ['baseline', 'origin', 'source']
        snapshot = {'observer_seconds': 0, 'services': {
            name: {'sampled_monotonic': 100, 'cpu': {'usage_usec': 1}, 'memory_events': {}}
            for name in names}}
        trace = dict(user_index=0, attempt_index=1, path_index=0,
                     started_seconds=0, ttfb_ms=1, completion_ms=2,
                     verified_complete_seconds=.002, in_window=True,
                     disposition='success', error_type=None, url='PRIVATE_URL',
                     authorization='PRIVATE_TOKEN', body='PRIVATE_BODY')
        state = dict(chunks=29, bytes=100, locked=False, ready_ttl_seconds=300,
                     smallest_chunk_bytes=1, largest_chunk_bytes=99, generation='0',
                     ready='1', status='ready', private_key='PRIVATE_KEY',
                     key_types={'chunks':'list','ready':'string','status':'string','lock':'none'},
                     key_ttl_seconds={'chunks':300,'ready':300,'status':300,'lock':-2})
        value = dict(backend='source', workload='xmltv-hot-1', normalized={},
                     counters={'selected_services': {name:{'id':'PRIVATE_'+name} for name in names},
                               'before':snapshot, 'after':copy.deepcopy(snapshot), 'services':{}},
                     measurement={'resource_samples':[], 'summary':{'completed':1},
                                  'request_traces':[trace]},
                     cache_witness={'before':state,'after':copy.deepcopy(state)})
        result = publish.project(value)
        self.assertTrue(result['individual_current_hot_request_timings_available'])
        self.assertTrue(result['cache_numeric_witness_available'])
        self.assertEqual(result['request_traces'][0]['completion_ms'], 2)
        self.assertEqual(result['cache_witness']['after']['key_ttl_seconds']['lock'], -2)
        self.assertNotIn('PRIVATE_', json.dumps(result))
        bad = copy.deepcopy(value)
        bad['measurement']['request_traces'][0]['error_type'] = 'ValueError: PRIVATE_EXCEPTION'
        with self.assertRaises(ValueError): publish.project(bad)
        bad = copy.deepcopy(value)
        bad['measurement']['request_traces'] = [trace] * 10001
        with self.assertRaises(ValueError): publish.project(bad)
        bad = copy.deepcopy(value)
        bad['cache_witness']['after']['generation'] = 'PRIVATE_GENERATION'
        with self.assertRaises(ValueError): publish.project(bad)

    def test_durable_projection_is_complete_and_never_claims_whole_acceptance(self):
        path = Path(__file__).with_name('evidence-guide-experiment23-recovery-1.json')
        value = json.loads(path.read_text())
        self.assertEqual(len(value['phases']), 38)
        self.assertFalse(value['performance_verified'])
        measured = [v for v in value['phases'] if v['stage'] == 'matrix']
        self.assertEqual(len(measured), 20)
        self.assertTrue(all(len(v['resource_samples']) == 60 for v in measured))
        self.assertTrue(all(set(row['services']) == {'baseline','origin', v['backend']}
                            for v in measured for row in v['resource_samples']))
        self.assertTrue(all(not v['individual_current_hot_request_timings_available'] for v in measured))


if __name__ == '__main__':
    unittest.main()
