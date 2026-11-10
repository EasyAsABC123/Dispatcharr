import copy
import datetime
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import orchestrator
import plan


class Engine(unittest.TestCase):
    def test_complete_qualification_seal_then_all_fifteen_pairs_and_stops(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        seed = {'clock': now.isoformat(), 'channel_uuids': ['fixture-private']}
        manifest = dict(passed=True, image_id='sha256:fixture', source_tree_sha256=plan.TREE,
                        source_archive_sha256=plan.ARCHIVE)
        reference = {'current': {'kind': 'json', 'value': []},
                     'xml': {'kind': 'xml', 'bytes': 12, 'sha256': 'fixture'}}
        lifecycle = []
        selected = {'baseline': {'id': 'a'*64}, 'origin': {'id': 'b'*64}, 'source': {'id': 'c'*64}}

        class Cache:
            ready = True
            def clear(self):
                self.ready = False
            def state(self):
                return dict(ready='1' if self.ready else '', status='ready' if self.ready else '',
                            bytes=12 if self.ready else 0, chunks=2 if self.ready else 0,
                            generation='0', locked=False, ready_ttl_seconds=299)
        cache = Cache()

        def helper(op):
            self.assertEqual(op, 'phase')
            return dict(token='fixture-private', channels=500, stored_programmes=2250,
                        first=now.isoformat(), last=(now+datetime.timedelta(days=1)).isoformat(),
                        current_until=(now+datetime.timedelta(hours=2)).isoformat(),
                        fixture_sha256='fixture', schema_sha256='schema')
        def start(backend):
            lifecycle.append(('start', backend))
            return 'http://127.0.0.1:1'
        def stop():
            lifecycle.append(('stop', None))
        def request(*args, **kwargs):
            cache.ready = True
            return reference['xml'], 1, 2
        sample = {'cpu_percent': 100, 'memory_bytes': 300,
                  'containers': [{'id': value['id'][:12], 'memory_bytes': 100} for value in selected.values()]}
        def paced(*args, **kwargs):
            duration = kwargs['duration']
            count = round(duration*kwargs['users']/kwargs['period'])
            return {'summary': dict(completed=count, errors=0, late_completed=0,
                                    ttfb_ms_p95=1, roundtrip_ms_p95=2),
                    'resource_samples': [copy.deepcopy(sample) for _ in range(60)], 'sample_errors': []}
        def cold(*args, **kwargs):
            kwargs['before_wave']()
            cache.ready = True
            count = kwargs['users']
            return dict(waves=[{'consumers': [dict(passed=True, ttfb_ms=1, completion_ms=2) for _ in range(count)]}],
                        completed=count, errors=0, late_completed=0, expected_waves=1,
                        resource_samples=[copy.deepcopy(sample) for _ in range(60)], sample_errors=[])

        class Observer:
            def __init__(self, services, tools):
                pass
            def counters(self):
                return {}
            def sample(self):
                return copy.deepcopy(sample)
            def finish(self, before):
                return dict(cpu_percent_mean=100, selected_services=selected,
                            services={'source': {'cpu_percent_mean': 100, 'throttled_period_fraction': 0}})
        with tempfile.TemporaryDirectory() as folder, patch('orchestrator.observer.Observer', Observer), \
             patch('orchestrator.load.request', side_effect=request), \
             patch('orchestrator.load.paced', side_effect=paced), \
             patch('orchestrator.load.cold_waves', side_effect=cold), \
             patch('orchestrator.qualification.authorize', return_value='qualification-cue'), \
             patch('orchestrator.plan.authorize_quiet', return_value='matrix-cue'):
            root = Path(folder)
            q = root/'qualification'; q.mkdir()
            m = root/'matrix'; m.mkdir()
            shared = dict(manifest=manifest, tools='.', seed=seed, reference=reference,
                          current={}, xml={}, helper=helper, cache=cache, start=start, stop=stop,
                          select_services=lambda backend: selected, quiet_path=root/'cue')
            qualified = orchestrator.run(action='qualify', output=q, **shared)
            self.assertTrue(qualified['passed'])
            rates = json.loads((q/'source-qualification.json').read_text())
            self.assertTrue(all(row['backend'] == 'source' for rows in rates.values() for row in rows))
            frozen = json.loads((q/'frozen-plan.json').read_text())
            self.assertIn('fixture_binding', frozen)
            measured = orchestrator.run(action='matrix', output=m, plan_path=q/'frozen-plan.json',
                                        qualification_path=q/'source-qualification.json', **shared)
            self.assertTrue(measured['passed'])
            self.assertEqual(len(measured['acceptance']['pairs']), 15)
            self.assertEqual(len(measured['trials']), 30)
            saved = json.loads((m/measured['trials'][0]['file']).read_text())
            self.assertEqual(saved['cache_witness']['requested_state'], plan.POLICY['workloads'][0]['cache_state'])
            self.assertIn('fixture_sha256', saved['fixture_witness'])
            self.assertNotIn('token', saved['fixture_witness'])
            xml_file = next(row['file'] for row in measured['trials'] if row['file'].startswith('xmltv-cold-1'))
            witness = json.loads((m/xml_file).read_text())['cache_witness']
            self.assertEqual(witness['before']['bytes'], 12)
            self.assertEqual(witness['after']['generation'], witness['before']['generation'])
            self.assertEqual(sum(v[0] == 'start' for v in lifecycle),
                             sum(v[0] == 'stop' for v in lifecycle))
            # A new disposable DB must not reuse an old qualification seal.
            changed = dict(shared, seed=dict(seed, channel_uuids=['other']))
            with self.assertRaises(ValueError):
                orchestrator.run(action='matrix', output=m, plan_path=q/'frozen-plan.json',
                                 qualification_path=q/'source-qualification.json', **changed)

    def test_backend_is_stopped_when_reference_or_sampler_fails(self):
        class FailedObserver:
            def __init__(self, selected, tools):
                pass
        seed = {'clock': datetime.datetime.now(datetime.timezone.utc).isoformat()}
        with tempfile.TemporaryDirectory() as folder, patch('orchestrator.observer.Observer', FailedObserver), \
             patch('orchestrator.load.request', side_effect=ValueError('fixture response differs')):
            stops = []
            now = datetime.datetime.now(datetime.timezone.utc)
            def helper(op):
                return dict(token='fixture', channels=500, stored_programmes=2250,
                            first='first', last='last', current_until=(now+datetime.timedelta(hours=2)).isoformat(),
                            fixture_sha256='fixture', schema_sha256='schema')
            with self.assertRaises(ValueError):
                orchestrator.run(action='observer-smoke', output=folder, manifest={}, tools='.', seed=seed,
                                 reference={'current': {}, 'xml': {}}, current={}, xml={}, helper=helper, cache=None,
                                 start=lambda backend: 'http://127.0.0.1:1', stop=lambda: stops.append(True),
                                 select_services=lambda backend: {})
            self.assertEqual(stops, [True])
            self.assertFalse(json.loads((Path(folder)/'guide-run.json').read_text())['passed'])


if __name__ == '__main__':
    unittest.main()
