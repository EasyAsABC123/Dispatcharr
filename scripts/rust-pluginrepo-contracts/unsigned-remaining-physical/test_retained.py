"""Corrupt retained positive captures, never application replay."""
import json,tempfile,unittest
from pathlib import Path
import verify as V
class Retained(unittest.TestCase):
 def reject(self,case,name,change):
  with tempfile.TemporaryDirectory()as tmp:
   out=Path(tmp)/'evidence';V.materialize('case'+str(case),out);p=out/name;r=json.loads(p.read_bytes());change(r);p.write_text(json.dumps(r))
   with self.assertRaises((ValueError,KeyError,FileNotFoundError)):V.validate_remaining(out,case)
 def test_all_actual_baselines_and_original_negative(self):self.assertTrue(V.validate()['passed_retained4_case_qualification'])
 def test_original_native_socket_still_present(self):self.reject(1,'unsigned-http-event-1.json',lambda r:r.__setitem__('after_link',r['original_socket']['socket_link']))
 def test_owner_retired_early(self):self.reject(1,'unsigned-http-event-1.json',lambda r:r.__setitem__('all_original_weak_at_event',[False]))
 def test_fake_observer_zero(self):self.reject(1,'unsigned-remaining-physical-private.json',lambda r:r['observer_drain'].__setitem__('pool_size',False))
 def test_missing_Ready(self):self.reject(2,'unsigned-remaining-physical-private.json',lambda r:r['actual_held']['held'].__setitem__('sync_seen',False))
 def test_other_warm_backend(self):self.reject(2,'unsigned-remaining-physical-private.json',lambda r:r['actual_held']['held'].__setitem__('backend_pid',999999))
 def test_missing_sustained_record(self):self.reject(2,'SQL-sustained-owner-samples.json',lambda r:r.__setitem__('samples',[]))
 def test_no_actual_cancel(self):self.reject(3,'unsigned-remaining-physical-private.json',lambda r:r['actual_cancel'].__setitem__('pg_cancel_backend_result',False))
 def test_error_wrong_response_CT(self):self.reject(3,'unsigned-remaining-physical-private.json',lambda r:r['actual_response_after_Ready'].__setitem__('headers_value_hex',[['content-type',b'text/plain'.hex()]]))
 def test_clone_does_not_retain_original(self):self.reject(4,'unsigned-remaining-physical-private.json',lambda r:r['clone_kept_after_original_drop'].__setitem__('slots',1))
 def test_first_frame_hash(self):self.reject(4,'actual-response-frame-1.json',lambda r:r.__setitem__('frame_sha256','0'*64))
 def test_fifth_has_SQL_effect(self):self.reject(4,'unsigned-remaining-physical-private.json',lambda r:r['fifth_SQL_after']['tables']['accounts_user'][0].__setitem__('username','unmeasured'))
 def test_sixth_does_not_reuse_one_slot(self):self.reject(4,'unsigned-remaining-physical-private.json',lambda r:r['sixth_reuse'].__setitem__('slots',1))
if __name__=='__main__':unittest.main()
