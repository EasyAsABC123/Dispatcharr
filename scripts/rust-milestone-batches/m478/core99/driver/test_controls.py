"""Offline guards for false acceptance and diagnostic privacy; no applications."""
import copy,json,unittest
from gpg_trace import validate,read_events
TARGET='dispatcharr::plugin_gpg'
HOME='dispatcharr-gpg-00000000-0000-4000-8000-000000000000'
def fixture():
    source={'requests':[{'index':1}], 'GPG_observer_terminal':{'value':{'direct_GPG_processes':[{'case':1,'argv':['gpg','--import'],'actual_wait_result':[13,0]}]}}}
    native={'capture_complete':True,'actual_request_count':1,'completed_capture_count':1,'requests':[{'began_utc':'1970-01-01T00:00:01+00:00','completed_utc':'1970-01-01T00:00:02+00:00'}]}
    spawn={'event':'spawn','pid':13,'start_generation':'Some("7")','pid_namespace':'Some("pid:[4]")','role':'import','home_id':'Some("'+HOME+'")','stdin_bytes':4}
    joined={'event':'joined','pid':13,'start_generation':'Some("7")','role':'import','home_id':'Some("'+HOME+'")','stdin_complete':True,'stdout_eof':True,'stderr_eof':True,'stdout_bytes':'Some(0)','stderr_bytes':'Some(23)','wait_observed':True,'wait_exit_code':'Some(0)','wait_signal':'None'}
    dropped={'event':'home_drop','home_id':'Some("'+HOME+'")','remove_ok':True,'home_absent':True,'error_kind':'None'}
    rows=[{'target':TARGET,'epoch_seconds':clock,'fields':row} for clock,row in zip([1.2,1.3,1.4],[spawn,joined,dropped])]
    return source,native,rows

def raw(rows):return ('\n'.join('2026-01-01T00:00:00Z '+json.dumps(row) for row in rows)+'\n').encode()
class Controls(unittest.TestCase):
    def test_HTTP_close_dependency_is_present(self):
        from reader import descriptor_absent
        self.assertTrue(callable(descriptor_absent))
    def test_missing_transitive_dependency_refuses_before_fixture(self):
        import tempfile
        from pathlib import Path
        from run import fixture_imports
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);(path/'fixture.py').write_text('from missing_owned_helper import close\n')
            with self.assertRaises(ValueError):fixture_imports(path)
    def test_typed_wait_and_namespace_metadata(self):
        s,n,r=fixture();result=validate(s,n,raw(r));self.assertEqual(result['actual_children'],1);self.assertEqual(result['observed_waits'][0]['exit'],0)
    def test_child_without_wait_cannot_pass(self):
        s,n,r=fixture()
        with self.assertRaises(ValueError):validate(s,n,raw([r[0],r[2]]))
    def test_wrong_actual_wait_cannot_pass(self):
        s,n,r=fixture();r[1]['fields']['wait_exit_code']='Some(2)'
        with self.assertRaises(ValueError):validate(s,n,raw(r))
    def test_failed_pipe_does_not_count_as_EOF(self):
        s,n,r=fixture();r[1]['fields']['stderr_eof']=False
        with self.assertRaises(ValueError):validate(s,n,raw(r))
    def test_home_removal_is_required(self):
        s,n,r=fixture();r[2]['fields']['home_absent']=False
        with self.assertRaises(ValueError):validate(s,n,raw(r))
    def test_partial_cohort_does_not_accept_prefix(self):
        s,n,r=fixture();n['capture_complete']=False
        with self.assertRaises(ValueError):validate(s,n,raw(r))
    def test_unknown_plaintext_field_is_refused(self):
        s,n,r=fixture();r[0]['fields']['signature']='unretained-fixture-value'
        with self.assertRaises(ValueError):read_events(raw(r))
    def test_observer_error_cannot_be_silently_ignored(self):
        with self.assertRaises(ValueError):read_events(raw([{'target':TARGET,'trace_error':'event_capacity'}]))
    def test_current_missing_GPG_receipt_never_looks_successful(self):
        s,n,r=fixture()
        with self.assertRaises(ValueError):validate(s,n,b'')
if __name__=='__main__':unittest.main()
