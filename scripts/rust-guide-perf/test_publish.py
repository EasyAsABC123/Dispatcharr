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
