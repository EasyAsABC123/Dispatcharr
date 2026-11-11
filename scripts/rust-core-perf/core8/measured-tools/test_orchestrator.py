"""Mock-only orchestration controls; no Docker/resource sampler is invoked."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from m1_imports import load
orchestrator=load('orchestrator')


class Runtime:
    def __init__(self, output):
        self.output = output
        self.calls = []
        self.late_backend = None

    def start(self, backend, viewers, period, plan, label):
        state = dict(backend=backend, viewers=viewers, period=period, plan=plan, label=label, selected={})
        self.calls.append(state)
        return state

    def inspected(self, service):
        return {'id':'b'*64,'limits':{}}

    def finish(self, state, before_remove=None):
        if before_remove is not None: before_remove()
        chunks = 75000 // state['period']
        late = chunks//10 if state['backend']==self.late_backend and state['label'].startswith('measure-') else 0
        return dict(passed=True, backend=state['backend'], viewers=state['viewers'], period_ms=state['period'],
                    drain_release_seen=True,
                    produced=chunks, chunk_bytes=255868, final_full_bytes_per_viewer=chunks*255868, producer_late=late,
                    readers=[dict(delivered=chunks, bytes=chunks*255868, measured=60000//state['period']//(2 if state['viewers']>1 and i%10==0 else 1), historical_reads=1)
                             for i in range(state['viewers'])])


class Observer:
    def smoke(self):
        return {'passed': True, 'mock_only': True}


class SequenceControls(unittest.TestCase):
    def invoke(self, fail=False, absent=False, late_backend=None, oom_at=None):
        plan = json.loads((Path(__file__).resolve().parent.parent/'protocol.json').read_text())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = Runtime(root)
            runtime.late_backend=late_backend
            (root/'native-core-peer').write_bytes(b'fixture')
            cue = root/'measurement.json'
            if not absent:
                cue.write_text('{}')

            def metrics(state, _plan):
                native = state['backend']=='native'
                return dict(cpu_percent_mean=(110 if fail else 50) if native else 100,
                            peak_working_memory_bytes=50 if native else 100,
                            core_cpu_percent=80, core_memory_fraction=.5, core_throttled_period_fraction=0)

            life_calls=0
            def life(*args):
                nonlocal life_calls
                life_calls+=1
                if life_calls==oom_at:raise ValueError('absolute lifecycle OOM')
            with patch.object(orchestrator, 'authorize'), patch.object(orchestrator, 'lifecycle',side_effect=life), patch.object(orchestrator, 'observer', return_value=Observer()), \
                    patch.object(orchestrator, 'window', side_effect=lambda observer,state,plan,output:state), \
                    patch.object(orchestrator, 'summarize', side_effect=metrics), \
                    patch('builtins.print'):
                if absent:
                    with patch.object(orchestrator.time, 'monotonic', side_effect=[0,1201]):
                        with self.assertRaises(TimeoutError):
                            orchestrator.experiment(runtime,plan,root,__import__('hashlib').sha256(b'fixture').hexdigest(),root/'source.json',cue,{'tools_sha256':{}})
                elif fail or late_backend or oom_at:
                    with self.assertRaises(ValueError):
                        orchestrator.experiment(runtime,plan,root,__import__('hashlib').sha256(b'fixture').hexdigest(),root/'source.json',cue,{'tools_sha256':{}})
                else:
                    result = orchestrator.experiment(runtime,plan,root,__import__('hashlib').sha256(b'fixture').hexdigest(),root/'source.json',cue,{'tools_sha256':{}})
                    self.assertTrue(result['passed'])
                    self.assertEqual(len(result['pairs']),9)
            return runtime.calls, json.loads((root/'experiment.json').read_text())

    def test_all_nine_source_qualifications_precede_any_native_measurement(self):
        calls, result = self.invoke()
        self.assertEqual(len(calls),27)
        self.assertTrue(all(row['backend']=='source' for row in calls[:9]))
        self.assertEqual({(row['viewers'],row['period'])for row in calls[:9]}, {(v,p)for v in [1,10,100]for p in [250,500,1000]})
        self.assertEqual(len(result['qualification']),9)

    def test_absent_measurement_cue_never_starts_native(self):
        calls, result = self.invoke(absent=True)
        self.assertEqual(len(calls),9)
        self.assertFalse(result['performance_verified'])

    def test_first_failed_pair_stops_before_next_setup(self):
        calls, result = self.invoke(fail=True)
        self.assertEqual(len(calls),11)
        self.assertEqual(len(result['pairs']),1)
        self.assertFalse(result['pairs'][0]['passed'])
        self.assertFalse(result['performance_verified'])

    def test_source_and_native_measured_producer_lag_each_reject(self):
        for backend,count in [('source',10),('native',11)]:
            calls,result=self.invoke(late_backend=backend)
            self.assertEqual(len(calls),count)
            self.assertFalse(result['performance_verified'])
            self.assertIn('pacing',result['failure_reason'])

    def test_setup_and_between_phase_oom_never_reach_native(self):
        for point in [1,2,4]:
            calls,result=self.invoke(oom_at=point)
            self.assertTrue(all(v['backend']=='source'for v in calls))
            self.assertFalse(result['performance_verified'])
            self.assertIn('OOM',result['failure_reason'])


if __name__ == '__main__':
    unittest.main()
