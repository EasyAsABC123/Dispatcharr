"""Real retained witness + process/error classification corruptions; no app."""
import copy,json,unittest
from pathlib import Path
from known_failure_gate import review
ROOT=Path(__file__).resolve().parent;RUN=ROOT.parents[1]/'clocked-recordings-native-physical-3-1';HOST=json.loads((RUN/'integration.json').read_text());RAW=json.loads((RUN/'native-clocked-recording-private.json').read_text())
class Controls(unittest.TestCase):
 def test_actual_retained_body_only(self):self.assertTrue(review(HOST,RAW)['body_effects_passed'])
 def reject(self,host_change=None,raw_change=None):
  host=copy.deepcopy(HOST);raw=copy.deepcopy(RAW)
  if host_change:host_change(host)
  if raw_change:raw_change(raw)
  with self.assertRaises((ValueError,KeyError,TypeError)):review(host,raw)
 def test_host0_forbidden(self):self.reject(lambda h:h.update(actual_host_exit=0))
 def test_guest0_forbidden(self):self.reject(lambda h:h.update(actual_controller_exit=0))
 def test_bool_status_forbidden(self):self.reject(lambda h:h.update(actual_host_exit=True))
 def test_unsettled_forbidden(self):self.reject(lambda h:h.update(actual_controller_settled=False))
 def test_unknown_error_forbidden(self):self.reject(raw_change=lambda r:r.update(error='unknown'))
 def test_wrong_error_type_forbidden(self):self.reject(raw_change=lambda r:r.update(error_type='RuntimeError'))
 def test_changed_passed_forbidden(self):self.reject(raw_change=lambda r:r.update(passed=True))
 def test_peer_failure_forbidden(self):self.reject(raw_change=lambda r:r['physical']['peer_collection'].update(actual_exit=1))
 def test_incomplete_capture_forbidden(self):self.reject(raw_change=lambda r:r['physical'].update(capture_complete=False))
 def test_cleanup_unknown_forbidden(self):self.reject(raw_change=lambda r:r['physical']['cleanup_errors'].append('unknown'))
if __name__=='__main__':unittest.main()
