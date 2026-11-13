"""Finite publication corruption controls; no Docker, app or private fallback."""
import copy
import unittest
from unittest import mock
import verify

class PublicControls(unittest.TestCase):
    def setUp(self):
        self.data=verify.load_data()
        self.authority=verify.read('authority.json')
    def rejected(self, mutate):
        data=copy.deepcopy(self.data)
        mutate(data)
        with self.assertRaises((ValueError,KeyError,TypeError)):
            verify.validate(data,self.authority)
    def test_positive_has_scoped_source100_and_native7_only(self):
        result=verify.validate(self.data,self.authority)
        self.assertEqual(result['Source_literal_observations'],100)
        self.assertEqual(result['Native_qualified_prefix'],7)
        self.assertFalse(result['Native_overall_fixture_pass'])
    def test_scope_and_counter_expansion_rejected(self):
        for field in ['worker_graph_activation','Main_acceptance','performance_acceptance','full_M3_M4_M7_acceptance','process_delivery_qualified','Native_overall_fixture_pass']:
            with self.subTest(field=field): self.rejected(lambda d:d['scope'].__setitem__(field,True))
        for field in ['Source_observations','Native_qualified_test_prefix','Factory_actual_SQL_calls','TCP38_status200','Source_argv24_ValueError']:
            with self.subTest(field=field): self.rejected(lambda d:d['scope'].__setitem__(field,0))
    def test_source_argv_coverage_and_literal_results_rejected(self):
        self.rejected(lambda d:d['argv']['cases'].pop())
        self.rejected(lambda d:d['argv']['cases'].reverse())
        self.rejected(lambda d:d['argv']['cases'][0]['observed']['argv'].__setitem__(0,'changed-program'))
        self.rejected(lambda d:d['argv']['cases'][0].__setitem__('SQL_journal_invalid','overflow'))
    def test_both_xc_epochs_reject_coverage_empty_sql_and_missing_sequences(self):
        for label in ['factory','tcp']:
            with self.subTest(label=label):
                self.rejected(lambda d:d[label].__setitem__('cases',[]))
                self.rejected(lambda d:d[label]['cases'].pop())
                def empty(d):
                    for row in d[label]['cases']:
                        row['before']['tables']={};row['after']['tables']={}
                self.rejected(empty)
                def sequences(d):
                    for row in d[label]['cases']:
                        row['before']['sequences']={};row['after']['sequences']={}
                self.rejected(sequences)
    def test_body_status_and_sensitive_header_corruptions_rejected(self):
        for label in ['factory','tcp']:
            with self.subTest(label=label):
                self.rejected(lambda d:d[label]['cases'][0].__setitem__('body_sha256','0'*64))
                self.rejected(lambda d:d[label]['cases'][0].__setitem__('status',503))
                self.rejected(lambda d:d[label]['cases'][0]['headers'].append(['Set-Cookie','synthetic-test-cookie']))
    def test_source_clock_and_physical_close_corruptions_rejected(self):
        self.rejected(lambda d:d['tcp'].__setitem__('phase_seconds',0))
        self.rejected(lambda d:d['tcp']['cases'][0].__setitem__('began_seconds',-1))
        self.rejected(lambda d:d['tcp']['cases'][0]['absolute_observation'].__setitem__('deadline_seconds',-1))
        for field,value in [('fd_after',7),('original_inode_absent',False),('timer_joined',False)]:
            self.rejected(lambda d:d['tcp']['cases'][0]['physical_close'].__setitem__(field,value))
    def test_source_failures_and_all_three_cleanup_categories_retained(self):
        self.rejected(lambda d:d['source_attempts']['attempts'][0].__setitem__('original_passed',True))
        self.rejected(lambda d:d['source_attempts']['attempts'][0].__setitem__('cleanup_errors',None))
        self.rejected(lambda d:d['source_attempts']['attempts'][1].__setitem__('scope_boundary','zero TCP requests'))
        self.rejected(lambda d:d['source_attempts']['attempts'][2]['independent_cleanup'].pop('volumes'))
    def test_native_coverage_transcript_and_identity_rejected(self):
        self.rejected(lambda d:d['native'].__setitem__('gates',[]))
        self.rejected(lambda d:d['native']['gates'].pop())
        self.rejected(lambda d:d['native']['gates'][0].__setitem__('actual_passed_tests',0))
        self.rejected(lambda d:d['native']['gates'][0].__setitem__('stdout_sha256','0'*64))
        self.rejected(lambda d:d['native']['gates'][0]['owned_process'].__setitem__('terminal_identity_sha256','0'*64))
        self.rejected(lambda d:d['native']['gates'][0]['command'].__setitem__('pipe_reader_threads_settled',False))
    def test_native_budgets_clocks_and_sql_schema_rejected(self):
        self.rejected(lambda d:d['native']['expected_gates'][0].__setitem__('command_seconds',46))
        self.rejected(lambda d:d['native']['gates'][0].__setitem__('started_seconds',-1))
        self.rejected(lambda d:d['native'].__setitem__('phase_seconds',0))
        def sequences(d):
            d['native']['default_before']['sequences']={};d['native']['default_after']['sequences']={}
        self.rejected(sequences)
    def test_origin_failure_cannot_turn_into_global_success(self):
        self.rejected(lambda d:d['native'].__setitem__('original_fixture_passed',True))
        self.rejected(lambda d:d['native']['terminal_origin_audit'].__setitem__('qualified',True))
        self.rejected(lambda d:d['native']['terminal_origin_audit'].__setitem__('origin_exit_or_log_captured',True))
        self.rejected(lambda d:d['native_attempts']['attempts'][0].__setitem__('archive_sha256','0'*64))
        self.rejected(lambda d:d['native_attempts'].__setitem__('overall_fixture_pass',True))
    def test_authorization_and_full_nonce_fields_are_not_public_data(self):
        for name in ['nonce','boot_id','environment','xc_password','password','jwt','cookie']:
            with self.subTest(name=name):
                self.rejected(lambda d:d['tcp']['cases'][0].__setitem__(name,'synthetic-private-value'))
    def test_in_range_clock_rewrites_and_runtime_metadata_are_bound(self):
        self.rejected(lambda d:d['native']['gates'][0].__setitem__('Rust_reported_seconds',0.01))
        self.rejected(lambda d:d['native']['gates'][0].__setitem__('started_seconds',0.001))
        self.rejected(lambda d:d['factory']['metadata'].__setitem__('Django','wrong'))
    def test_actual_transcript_content_is_checked_not_only_its_hash(self):
        data=copy.deepcopy(self.data);row=data['native']['gates'][0]
        original=verify.Path.read_text
        def text(path,*args,**kwargs):
            if path==verify.HERE/row['stdout_path']:
                return 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 20 filtered out; finished in 0.00s\n'
            return original(path,*args,**kwargs)
        with mock.patch.object(verify.Path,'read_text',text):
            with self.assertRaises(ValueError):verify.validate(data,self.authority)

if __name__=='__main__':unittest.main(verbosity=2)
