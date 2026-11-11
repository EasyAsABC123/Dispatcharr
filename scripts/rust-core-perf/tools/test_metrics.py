"""Synthetic metric rejection controls, never a resource experiment."""
import copy
import json
from pathlib import Path
import unittest
import tempfile
import types
from unittest.mock import patch
from m1_imports import load
metrics=load('metrics')
summarize,assert_no_oom,lifecycle,window=metrics.summarize,metrics.assert_no_oom,metrics.lifecycle,metrics.window


class MetricControls(unittest.TestCase):
    def plan(self):
        return json.loads((Path(__file__).resolve().parent.parent/'protocol.json').read_text())

    def raw(self):
        return {'samples': [dict(included_in_window=True, memory_bytes=300,
                                containers=[dict(id='core', memory_bytes=100),dict(id='infra',memory_bytes=200)]) for _ in range(50)],
                'counters': {'services': {'core':dict(cpu_usage_usec=1000,cpu_percent_mean=50,throttled_period_fraction=0),
                                          'infrastructure':dict(cpu_usage_usec=1000,cpu_percent_mean=20,throttled_period_fraction=0)},
                             'selected_services': {'core':{'id':'core'},'infrastructure':{'id':'infra'}}}}

    def test_complete_services_and_samples(self):
        value = summarize(self.raw(),self.plan())
        self.assertEqual(value['cpu_percent_mean'],70)
        self.assertEqual(value['peak_working_memory_bytes'],300)

    def test_fewer_than_fifty_samples_rejected(self):
        value = self.raw()
        value['samples'].pop()
        with self.assertRaises(ValueError): summarize(value,self.plan())

    def test_zero_or_missing_cpu_never_fabricated(self):
        for replacement in [0,None,-1,float('nan')]:
            value=self.raw()
            value['counters']['services']['core']['cpu_usage_usec']=replacement
            with self.subTest(value=replacement),self.assertRaises(ValueError): summarize(value,self.plan())

    def test_missing_infrastructure_or_memory_rejected(self):
        for mutate in [lambda v:v['counters']['services'].pop('infrastructure'),
                       lambda v:v['samples'][0]['containers'].pop(),
                       lambda v:v['samples'][0]['containers'][0].update(memory_bytes=0),
                       lambda v:v['samples'][0].update(memory_bytes=1)]:
            value=self.raw();mutate(value)
            with self.assertRaises(ValueError):summarize(value,self.plan())

    def test_absolute_setup_and_earlier_phase_oom_rejected_without_delta(self):
        for key in ['oom','oom_kill']:
            counters={'services':{'core':{'memory_events':{'oom':0,'oom_kill':0}}}}
            counters['services']['core']['memory_events'][key]=1
            with self.subTest(key=key),self.assertRaises(ValueError):assert_no_oom(counters)

    def test_child_oom_preserved_while_container_still_running(self):
        with tempfile.TemporaryDirectory()as directory:
            target=Path(directory)/'life.json'
            group=types.SimpleNamespace(read=lambda *args:{'memory_events':{'oom':1,'oom_kill':1}})
            with patch.object(metrics.subprocess,'check_output',return_value=json.dumps([{'State':{'Running':True,'OOMKilled':False}}])),patch.object(metrics.importlib,'import_module',return_value=group):
                with self.assertRaises(ValueError):lifecycle({'core':{'id':'a'*64,'limits':{}}},directory,target)
            retained=json.loads(target.read_text())
            self.assertFalse(retained['docker_state']['core']['OOMKilled'])
            self.assertEqual(retained['services']['core']['memory_events']['oom_kill'],1)
            self.assertFalse(retained['zero_oom_verified'])

    def test_failure_before_first_counters_retains_raw_and_releases_barrier(self):
        class Failed:
            def counters(self):raise RuntimeError('owned counter failure')
        with tempfile.TemporaryDirectory()as directory:
            target=Path(directory)/'raw.json';release=Path(directory)/'release'
            with patch.object(metrics.time,'time',return_value=0),patch.object(metrics.time,'sleep'):
                with self.assertRaises(RuntimeError):window(Failed(),{'start_ms':0,'drain_release':str(release)},dict(warmup_seconds=1,measure_seconds=1),target)
            self.assertTrue(release.exists())
            self.assertEqual(json.loads(target.read_text())['error_type'],'RuntimeError')

    def test_drain_not_released_until_finish_counters(self):
        with tempfile.TemporaryDirectory()as directory:
            target=Path(directory)/'raw.json';release=Path(directory)/'release'
            class Checked:
                def counters(self):return {'services':{'core':{'memory_events':{'oom':0,'oom_kill':0}}}}
                def finish(self,before):
                    if release.exists():raise AssertionError('drain released before final counters')
                    return {'after':self.counters()}
            with patch.object(metrics.time,'time',side_effect=[0,0,2]),patch.object(metrics.time,'sleep'):
                window(Checked(),{'start_ms':0,'drain_release':str(release)},dict(warmup_seconds=1,measure_seconds=1),target)
            self.assertTrue(release.exists())


if __name__=='__main__':unittest.main()
