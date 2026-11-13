"""Finite corruptions of selected public evidence; no application dispatch."""
import copy,unittest
import verify as V
class Public(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.data=V.observations();cls.life=V.load('lifecycle.json')
 def reject(self,change):
  d=copy.deepcopy(self.data);l=copy.deepcopy(self.life);change(d,l)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.validate(d,l)
 def test_actual_trace(self):self.assertEqual(V.validate(self.data,self.life)['public_response_SQL_and_selected_trace_checks'],32)
 def test_missing_request(self):self.reject(lambda d,l:d['requests'].pop())
 def test_wrong_role(self):self.reject(lambda d,l:d['requests'][0].update(role='other'))
 def test_wrong_method(self):self.reject(lambda d,l:d['requests'][0].update(method='DELETE'))
 def test_wrong_status(self):self.reject(lambda d,l:d['requests'][0].update(status=599))
 def test_wrong_body(self):self.reject(lambda d,l:d['requests'][0].update(response_body_hex=''))
 def test_missing_content_type(self):self.reject(lambda d,l:d['requests'][0].update(headers={}))
 def test_missing_SQL_table(self):self.reject(lambda d,l:d['requests'][0]['before']['public_manifest']['tables'].pop('accounts_user'))
 def test_missing_SQL_sequence(self):self.reject(lambda d,l:d['requests'][0]['before']['public_manifest']['sequences'].pop('accounts_user_id_seq'))
 def test_missing_cache_evidence(self):self.reject(lambda d,l:d['requests'][0]['before']['raw_Redis'].clear())
 def test_socket_not_settled(self):self.reject(lambda d,l:d['requests'][0]['socket_settlement'].update(timer_joined=False))
 def test_bad_clock(self):self.reject(lambda d,l:d['requests'][0].update(began_utc='1970-01-01T00:00:00+00:00'))
 def test_raw_recorder_false_cannot_upgrade(self):self.reject(lambda d,l:d.update(passed=True))
 def test_host_success_cannot_relabel_failure(self):self.reject(lambda d,l:l.update(actual_controller_exit=1,actual_controller_passed=False))
 def test_measured_default_cannot_relabel_unmeasured(self):self.reject(lambda d,l:l.update(default_after_audit='unmeasured'))
 def test_settled_collector_cannot_relabel_pending(self):self.reject(lambda d,l:l['host_children'][0].update(physically_settled=False))
 def test_Main_cannot_upgrade(self):self.reject(lambda d,l:l.update(Main_activated=True))
class Physical(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.data=V.load('physical.json')
 def reject(self,change):
  import validate_physical as P
  value=copy.deepcopy(self.data);change(value)
  with self.assertRaises((ValueError,KeyError,TypeError)):P.validate(value,V.HERE/'physical')
 def test_all_six(self):self.assertEqual(V.physical()['qualified_physical_cases'],6)
 def test_missing_case(self):self.reject(lambda v:v['cases'].pop())
 def test_empty_cases(self):self.reject(lambda v:v.update(cases=[]))
 def test_wrong_DB0_history(self):self.reject(lambda v:v['cache_binding'].update(Django_default_database=0))
 def test_missing_sequence(self):self.reject(lambda v:v['before_SQL']['full62']['sequences'].pop('accounts_user_id_seq'))
 def test_old_ELF(self):self.reject(lambda v:v['cases'][0]['live_identity'].update(exe_sha256='0'*64))
 def test_wrong_parameter_identity(self):self.reject(lambda v:v['cases'][0]['held']['parameter_hex'][0].update(sha256='0'*64))
 def test_parameter_evidence_is_not_raw_wire(self):self.reject(lambda v:v['cases'][0]['held']['parameter_hex'].__setitem__(0,'00'))
 def test_wrong_cap(self):self.reject(lambda v:v['cases'][0]['held']['parameter_hex'].__setitem__(1,'7fffffff'))
 def test_old_backend(self):self.reject(lambda v:v['cases'][0]['held'].update(backend_pid=0))
 def test_stale_execute(self):self.reject(lambda v:v['cases'][0]['held'].update(execute_generation=1))
 def test_wrong_error(self):self.reject(lambda v:v['cases'][1]['held'].update(errors=[]))
 def test_returned_owner_early(self):self.reject(lambda v:v['cases'][1]['state_held'].update(active=0))
 def test_premature_shutdown(self):self.reject(lambda v:v['cases'][1].update(drain_held={'shutdown_drained':True}))
 def test_bad_request_deadline(self):self.reject(lambda v:v['cases'][1]['result'].update(elapsed_seconds=8))
 def test_close_before_join(self):self.reject(lambda v:v['cases'][0]['caller_close_details'].update(reader_joined=False))
 def test_missing_held_error_response(self):self.reject(lambda v:v['cases'][3]['held_error_responses'].pop())
 def test_fifth_auth_penalty(self):self.reject(lambda v:v['cases'][3].update(history_after_fifth=[1]))
 def test_no_capacity_response_owner(self):self.reject(lambda v:v['cases'][4]['state_held'].update(active=0))
 def test_no_capacity_restore(self):self.reject(lambda v:v['cases'][5].update(fixture_only_capacity_restore=False))
 def test_unsettled_cleanup(self):self.reject(lambda v:v['cases'][5]['cleanup_steps'][0].update(settled=False))
 def test_consistent_case_tamper_rejected_by_authority(self):
  value=copy.deepcopy(self.data);value['cases'][5]['fixture_only_capacity_setup']['before_sha256']='0'*64
  with self.assertRaises(ValueError):V.physical(value)
class Reconstruction(unittest.TestCase):
 def tar(self,rows):
  import io,tarfile
  out=io.BytesIO()
  with tarfile.open(fileobj=out,mode='w')as archive:
   for name,data,kind in rows:
    item=tarfile.TarInfo(name);item.type=kind;item.size=len(data);archive.addfile(item,io.BytesIO(data))
  return out.getvalue()
 def test_actual_manifest854plus5(self):
  import reconstruct as R
  self.assertEqual(len(R.manifest()['full_source_sha256']),859)
 def test_finite_archive_positive(self):
  import reconstruct as R,hashlib,tarfile
  self.assertEqual(R.validate_archive(self.tar([('rust/a',b'ok',tarfile.REGTYPE)]),{'rust/a':hashlib.sha256(b'ok').hexdigest()}),{'rust/a':b'ok'})
 def test_missing_blob(self):
  import reconstruct as R
  with self.assertRaises(ValueError):R.validate_archive(self.tar([]),{'rust/a':'0'*64})
 def test_wrong_blob(self):
  import reconstruct as R,tarfile
  with self.assertRaises(ValueError):R.validate_archive(self.tar([('rust/a',b'bad',tarfile.REGTYPE)]),{'rust/a':'0'*64})
 def test_unsafe_path(self):
  import reconstruct as R,tarfile
  with self.assertRaises(ValueError):R.validate_archive(self.tar([('../a',b'bad',tarfile.REGTYPE)]),{'rust/a':'0'*64})
 def test_symlink(self):
  import reconstruct as R,tarfile
  with self.assertRaises(ValueError):R.validate_archive(self.tar([('rust/a',b'',tarfile.SYMTYPE)]),{'rust/a':'0'*64})
 def test_duplicate_blob(self):
  import reconstruct as R,hashlib,tarfile
  with self.assertRaises(ValueError):R.validate_archive(self.tar([('rust/a',b'ok',tarfile.REGTYPE)]*2),{'rust/a':hashlib.sha256(b'ok').hexdigest()})
class Admission(unittest.TestCase):
 def changed(self,file,mutate):
  from unittest.mock import patch
  original=V.load;value=copy.deepcopy(original(file));mutate(value)
  with patch.object(V,'load',side_effect=lambda name:value if name==file else original(name)):
   with self.assertRaises((ValueError,KeyError,TypeError)):V.admission()
 def test_current_admission(self):self.assertTrue(V.admission()['once_scope'])
 def test_different_nonce_evidence(self):self.changed('admission.json',lambda v:v['claim'].update(nonce_sha256='0'*64))
 def test_changed_scope(self):self.changed('admission.json',lambda v:v['cue'].update(physical_case_count=0))
 def test_future_cue(self):self.changed('admission.json',lambda v:v['cue'].update(issued_epoch=99999999999))
 def test_Main_activation(self):self.changed('admission.json',lambda v:v['cue'].update(Main_activation=True))
 def test_agent_receipt_not_Root(self):self.changed('agent-independent-review.json',lambda v:v.update(reviewer='Root'))
 def test_missing_cleanup(self):self.changed('agent-independent-review.json',lambda v:v['fresh_cleanup_queries'].clear())
 def test_old_DB0_cannot_claim_new_DB1(self):self.changed('agent-independent-review.json',lambda v:v['physical_DB1_scalar_history'].update(direct_and_Django_and_peer_database=0))
class Privacy(unittest.TestCase):
 def test_fake_header_is_evidence_not_plain_or_hex(self):
  import privacy_projection as P,hashlib,json
  secret=b'example-owned-test-key';value,rules=P.project({'raw_headers_private':{'X-API-Key':secret.hex()},'database':'example-owned-database'});raw=json.dumps(value).encode();self.assertNotIn(secret,raw);self.assertNotIn(secret.hex().encode(),raw);self.assertEqual(value['raw_headers_private']['X-API-Key']['sha256'],hashlib.sha256(secret).hexdigest())
if __name__=='__main__':unittest.main()


