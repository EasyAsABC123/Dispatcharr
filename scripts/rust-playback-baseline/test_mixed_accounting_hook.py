import importlib.util
from pathlib import Path
import sys
import threading
import unittest
from unittest import mock
from mixed_accounting import Hook

class HookControls(unittest.TestCase):
    def bare(self):
        hook=object.__new__(Hook);hook.lock=threading.RLock();hook.overhead=[];hook.transitions=[]
        hook.active_state_done=False;hook.active_state_allowed=True;hook.bindings={};hook.ledger=mock.Mock(active_phase='active',epoch=1)
        return hook
    def test_pinned_Source_channels_app_label_table(self):
        text=Path(__file__).with_name('mixed_accounting.py').read_text()
        self.assertIn('JOIN dispatcharr_channels_stream s',text)
        self.assertNotIn('JOIN channels_stream s',text)
    def test_collector_file_readonly_mount_not_private_directory(self):
        text=Path(__file__).with_name('run.py').read_text()
        self.assertIn('collector.chmod(0o444)',text)
        self.assertIn("':/m0-census.py:ro'",text)
        self.assertNotIn('/verification/m0_proc_snapshot.py',Path(__file__).with_name('mixed_accounting.py').read_text())
    def test_Source_unlimited_fixture_does_not_increment_profile_counter(self):
        from mixed_accounting import unlimited_fixture_counter_matches
        self.assertTrue(unlimited_fixture_counter_matches({'provider_capacity':{'kind':'unlimited'},'profile':{'max_streams':0},'provider_slots':0}))
        self.assertFalse(unlimited_fixture_counter_matches({'provider_capacity':{'kind':'unlimited'},'profile':{'max_streams':0},'provider_slots':1}))
        self.assertFalse(unlimited_fixture_counter_matches({'provider_capacity':{'kind':'finite'},'profile':{'max_streams':10},'provider_slots':1}))
    def test_fault_does_not_sample_active_state_before_body_readiness(self):
        hook=self.bare();hook.active_state_allowed=False;hook.state=mock.Mock()
        hook.observe(('active',1),{},1,2)
        hook.state.assert_not_called()
        hook.allow_active_state();hook.state.return_value={'census':{'a':{'complete':True}}}
        hook.observe(('active',1),{},2,3);hook.state.assert_called_once()
    def test_second_fault_resource_sampler_refused(self):
        hook=self.bare();hook.resource_thread=object()
        with self.assertRaises(ValueError):hook.start_resources(mock.Mock())
    def test_fault_context_does_not_populate_empty_capture_directory(self):
        text=Path(__file__).with_name('run.py').read_text()
        self.assertIn("context = output/(directory_name+'.accounting-context.json')",text)
        self.assertNotIn("context = probe_output/'accounting-context.json'",text)
    def test_many_channel_source_bound_refuses_before_SQL(self):
        hook=self.bare();hook.context={'seed':{'stream_pk':1},'selected_channels':[{'stream_pk':1}]*11}
        hook.inspection=mock.Mock(return_value={});hook.command=mock.Mock()
        with self.assertRaises(ValueError):hook.state('before')
        hook.command.assert_not_called()
    def test_many_driver_keeps_one_existing_sampler(self):
        import ast
        p=Path(__file__).resolve().parents[1]/'rust-perf/perf.py';tree=ast.parse(p.read_text())
        threads=[n for n in ast.walk(tree)if isinstance(n,ast.Call)and isinstance(n.func,ast.Attribute)and isinstance(n.func.value,ast.Name)and n.func.value.id=='threading'and n.func.attr=='Thread']
        self.assertEqual(len(threads),1)
    def test_many_mode_refuses_wrong_qualified_schedule_before_Docker(self):
        spec=importlib.util.spec_from_file_location('private_many_runner',Path(__file__).with_name('run.py'));runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        with mock.patch.object(sys,'argv',['run','--output','/not-created','--many-accounting']),mock.patch.object(runner.shutil,'which')as executable:
            with self.assertRaises(SystemExit):runner.main()
            executable.assert_not_called()
    def test_zero_upstream_accepted_only_bounded_injected_failover_gap(self):
        from mixed_accounting import upstream_observation_matches as matches
        events=[{'name':'fault-ack','monotonic':2},{'name':'first-recovery','monotonic':3}]
        state={'origin_active':0,'origin_observed_monotonic':2.5}
        self.assertTrue(matches('failover',state,1,events))
        self.assertFalse(matches('stall',state,1,events))
        self.assertFalse(matches('mixed',state,1,events))
        for moment in [1,4]:self.assertFalse(matches('failover',dict(state,origin_observed_monotonic=moment),1,events))
        self.assertFalse(matches('failover',{'origin_active':0},1,events))
        self.assertFalse(matches('failover',state,1,[]))
    def test_unclosed_body_refused_before_state_probe(self):
        hook=self.bare();hook.state=mock.Mock()
        with self.assertRaises(RuntimeError):hook.after_readers([{'physical_socket_closed':False}])
        hook.state.assert_not_called()
    def test_cross_phase_sample_retained_as_transition_without_callback(self):
        hook=self.bare();hook.state=mock.Mock()
        hook.observe(('before',0),{'raw':1},1,2)
        hook.state.assert_not_called();hook.ledger.observation.assert_not_called()
        self.assertEqual(hook.transitions[0]['observer_record'],{'raw':1})
    def test_terminal_sample_not_mislabeled_active(self):
        hook=self.bare();hook.ledger.active_phase=None;hook.state=mock.Mock()
        hook.observe((None,1),{'raw':1},1,2)
        hook.state.assert_not_called();hook.ledger.observation.assert_not_called()
    def test_active_state_callback_runs_once(self):
        hook=self.bare();hook.state=mock.Mock(return_value={'census':{'a':{'complete':True}}})
        hook.observe(('active',1),{},1,2);hook.observe(('active',1),{},2,3)
        self.assertEqual(hook.state.call_count,1)
    def test_state_or_parse_failure_not_swallowed(self):
        hook=self.bare();hook.state=mock.Mock(side_effect=ValueError('bad JSON'))
        with self.assertRaises(ValueError):hook.observe(('active',1),{},1,2)
    def test_observer_only_one_original_sampler_thread(self):
        import ast
        tree=ast.parse(Path(__file__).with_name('mixed_probe.py').read_text())
        threads=[n for n in ast.walk(tree)if isinstance(n,ast.Call)and isinstance(n.func,ast.Attribute)and isinstance(n.func.value,ast.Name)and n.func.value.id=='threading'and n.func.attr=='Thread']
        self.assertEqual(len(threads),1)
    def test_new_mode_refuses_nonfunctional_or_native_before_docker(self):
        spec=importlib.util.spec_from_file_location('private_mixed_runner',Path(__file__).with_name('run.py'));runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        for extra in [[],['--functional-only','--functional-probes','mixed','--candidate-overlay','/no-native']]:
            with self.subTest(extra=extra),mock.patch.object(sys,'argv',['run','--output','/not-created','--mixed-accounting',*extra]),mock.patch.object(runner.shutil,'which')as executable:
                with self.assertRaises(SystemExit):runner.main()
                executable.assert_not_called()
if __name__=='__main__':unittest.main()
