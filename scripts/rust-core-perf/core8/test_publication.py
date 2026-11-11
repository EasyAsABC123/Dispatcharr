"""Raw-evidence controls. Explicit retained run path required.
No in-flight/default private experiment is read at import or when unset.
"""
import copy
import importlib.util
import json
import os
from pathlib import Path
import unittest
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('_core8_publication_tested',HERE/'publish.py')
publish=importlib.util.module_from_spec(spec);spec.loader.exec_module(publish)
RETAINED=os.environ.get('CORE8_RETAINED_COMPLETE_RUN')
REVIEW=os.environ.get('CORE8_PARENT_REVIEW')
@unittest.skipUnless(RETAINED and REVIEW,'After quiet: explicit complete run and parent review required')
class ActualRetainedControls(unittest.TestCase):
    def setUp(self):
        self.root=Path(RETAINED);self.review=publish.read(Path(REVIEW))
    def bad(self,name,mutate):
        def loader(path):
            value=publish.read(path)
            if path.name==name:mutate(value)
            return value
        with self.assertRaises((ValueError,KeyError,TypeError)):
            publish.validate(self.root,self.review,loader)
    def test_all_complete_raw_recomputed(self):
        result=publish.validate(self.root,self.review)
        self.assertEqual(len(result['pairs']),9);self.assertEqual(len(result['phases']),27)
    def test_missing_phase(self):self.bad('experiment.json',lambda x:x['phases'].pop())
    def test_changed_source_selected_period(self):self.bad('frozen-plan.json',lambda x:x.update(selected_common_period_ms=x['selected_common_period_ms']+1))
    def test_omitted_release_binding(self):self.bad('frozen-plan.json',lambda x:x['fixture_binding'].pop('native_release_record_sha256'))
    def test_wrong_production_source(self):self.bad('functional.json',lambda x:x.update(production_source_map_canonical_sha256='0'*64))
    def test_wrong_benchmark_source(self):self.bad('functional.json',lambda x:x.update(native_source_map_canonical_sha256='0'*64))
    def test_missing_functional_proof(self):self.bad('functional.json',lambda x:x.pop('functional_proof_sha256'))
    def test_wrong_original_full_map(self):self.bad('functional.json',lambda x:x.update(original_full_source_map_sha256='0'*64))
    def test_cleanup(self):self.bad('functional.json',lambda x:x.update(cleanup='retained'))
    def test_database_mutation(self):self.bad('functional.json',lambda x:x.update(default_database_unchanged=False))
    def test_final_bytes(self):self.bad('measure-100-3-native.json',lambda x:x['readers'][0].update(bytes=1))
    def test_setup_oom(self):self.bad('measure-1-1-source-setup-lifecycle.json',lambda x:x['services']['core']['memory_events'].update(oom=1))
    def test_between_phases_oom(self):self.bad('measure-1-1-source-between-phase-lifecycle.json',lambda x:x['services']['infrastructure']['memory_events'].update(oom_kill=1))
    def test_drain_oom(self):self.bad('measure-1-1-source-drain-lifecycle.json',lambda x:x['services']['core']['memory_events'].update(oom=1))
    def test_final_oom(self):self.bad('final-lifecycle.json',lambda x:x['services']['infrastructure']['memory_events'].update(oom=1))
    def test_counter_reset(self):self.bad('measure-1-1-source-resources.json',lambda x:x['counters']['after']['services']['core']['cpu'].update(usage_usec=0))
    def test_working_set_formula(self):self.bad('measure-1-1-source-resources.json',lambda x:x['samples'][0]['containers'][0].update(memory_bytes=1))
    def test_sample_cadence(self):self.bad('measure-1-1-source-resources.json',lambda x:x.update(samples=x['samples'][:2]))
    def test_out_of_window_sample(self):self.bad('measure-1-1-source-resources.json',lambda x:x['samples'][0].update(sample_completed_epoch=x['window_end_epoch']+1))
    def test_worker_exit(self):self.bad('measure-1-1-source.json.exit.json',lambda x:x.update(worker_exit_code=9))
    def test_pacing(self):self.bad('measure-1-1-source.json',lambda x:x.update(producer_late=300))
    def test_missing_drain_barrier(self):self.bad('measure-1-1-native.json',lambda x:x.update(drain_release_seen=False))
    def test_source_qualification(self):self.bad('source-qualification.json',lambda x:x[0].update(pacing_fraction=.1))
    def test_changed_normalized_cpu(self):self.bad('measure-1-1-native-normalized.json',lambda x:x['metrics'].update(cpu_percent_mean=999999))
    def test_unreviewed_run(self):
        review=copy.deepcopy(self.review);review['experiment_report_sha256']='0'*64
        with self.assertRaises(ValueError):publish.validate(self.root,review)

# Postquiet additions: local data corruptions only, never edit retained inputs.
from unittest.mock import patch
import sys
import types
import tempfile
@unittest.skipUnless(RETAINED and REVIEW,'Explicit complete run and parent review required')
class ClosureAndProjectionControls(unittest.TestCase):
    def setUp(self):self.root=Path(RETAINED);self.review=publish.read(Path(REVIEW))
    def changed_sha(self,predicate):
        original=publish.sha
        with patch.object(publish,'sha',side_effect=lambda path:'0'*64 if predicate(Path(path))else original(path)):
            with self.assertRaises(ValueError):publish.validate(self.root,self.review)
    def test_wrong_actual_ELF(self):self.changed_sha(lambda p:p.name=='native-core-peer')
    def test_modified_measured_math(self):self.changed_sha(lambda p:'measured-tools'in p.parts and p.name=='policy.py')
    def test_modified_support(self):self.changed_sha(lambda p:'support'in p.parts and p.name=='cgroup.py')
    def test_missing_support_entry(self):
        original=publish.read
        def incomplete(path):
            value=original(path)
            if Path(path).name=='support-manifest.json':value.pop(next(iter(value)))
            return value
        with patch.object(publish,'read',side_effect=incomplete):
            with self.assertRaises(ValueError):publish.validate(self.root,self.review)
    def test_extra_resource_window(self):
        original=Path.glob
        def extra(path,pattern,**kwargs):
            items=list(original(path,pattern,**kwargs))
            return iter(items+[path/'foreign-resources.json']) if path==self.root and pattern=='*-resources.json'else iter(items)
        with patch.object(Path,'glob',extra):
            with self.assertRaises(ValueError):publish.validate(self.root,self.review)
    def test_ambient_generic_modules_cannot_supply_math(self):
        with patch.dict(sys.modules,{name:types.ModuleType(name)for name in ['schema','orchestrator','run','cgroup']}):
            policy,metrics,pairs,cgroup=publish.checked_math()
            self.assertEqual(policy.digest(publish.read(HERE/'protocol.json')),publish.POLICY)
            self.assertTrue(callable(metrics.summarize));self.assertTrue(callable(pairs.pair));self.assertTrue(callable(cgroup.delta))
    def test_profile_false_not_integer_zero(self):
        ready=publish.read(HERE/'ready-manifest.json');release=publish.read(HERE/'release-record.json')
        release['profile']['debug_assertions']=0
        with self.assertRaises(ValueError):publish.identity(ready,release)
    def test_review_true_not_integer_one(self):
        review=copy.deepcopy(self.review);review['passed']=1
        with self.assertRaises(ValueError):publish.validate(self.root,review)
    def plot_bad(self,mutate):
        spec=importlib.util.spec_from_file_location('_core8_plot_tested',HERE/'plots.py')
        plots=importlib.util.module_from_spec(spec);spec.loader.exec_module(plots)
        projection=publish.validate(self.root,self.review);mutate(projection)
        with tempfile.TemporaryDirectory()as directory:
            source=Path(directory)/'projection.json';source.write_text(json.dumps(projection))
            with self.assertRaises(ValueError):plots.render(source,Path(directory)/'figures')
    def test_plot_wrong_source(self):self.plot_bad(lambda x:x.update(native_source_map_sha256='0'*64))
    def test_plot_missing_pair(self):self.plot_bad(lambda x:x['pairs'].pop())
    def test_plot_median_corruption(self):self.plot_bad(lambda x:x['cohort_medians']['1'].update(cpu_ratio=0.01))
    def test_plot_class_corruption(self):self.plot_bad(lambda x:x['pairs'][0]['measured_class_byte_ratios'].update(fast=0.1))

if __name__=='__main__':unittest.main()
