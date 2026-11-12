"""Portable bounded corruption controls. No app, Docker, network or private raw requirement."""
import copy,json,shutil,tempfile,unittest
from pathlib import Path
import validate as v
from reconstruct import reconstruct
from verify_execution import verify
HERE=Path(__file__).resolve().parent
class Controls(unittest.TestCase):
 def setUp(self):self.data=json.loads((HERE/'retained.json').read_text());self.cleanup=json.loads((HERE/'Root-independent-cleanup-review.json').read_text());self.coverage=json.loads((HERE/'coverage-authority.json').read_text())
 def check(self):return v.validate(self.data,self.cleanup,self.coverage)
 def reject(self,mutator):
  self.setUp();mutator(self.data,self.cleanup)
  with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):self.check()
 def test_actual_threecases_derive_without_application_replay(self):
  r=self.check();self.assertTrue(r['passed']);self.assertEqual(r['response_header_maps_retained'],4);self.assertFalse(r['full_M4_acceptance'])
 def test_missing_duplicate_reordered_cases(self):
  for mutation in [lambda d,c:d['summary'].update(cases=[]),lambda d,c:d['summary']['cases'].pop(),lambda d,c:d['summary']['cases'].reverse()]:self.reject(mutation)
 def test_private_redaction_map_coverage(self):self.reject(lambda d,c:d.update(private_seed_or_process_field_redactions=[]))
 def test_wrong_actual_exit(self):self.reject(lambda d,c:d.update(actual_exit_code=1))
 def test_false_recorded_phase_and_pair_flags(self):
  for key in ['passed','observation_valid','parity_passed']:
   self.reject(lambda d,c,k=key:d['summary'].update({k:False}));self.reject(lambda d,c,k=key:d['summary']['cases'][0].update({k:False}))
 def test_each_backend_missing_phase(self):
  for mode in v.MODES:self.reject(lambda d,c,m=mode:d['summary']['cases'][0].pop(m))
 def test_each_backend_SQL_and_follower_mandatory(self):
  for mode in v.MODES:
   for key in ['actual_SQL_Bearer_user_verified','early_follower_valid']:self.reject(lambda d,c,m=mode,k=key:d['summary']['cases'][0][m].update({k:False}))
 def test_follower_bytes_hash_and_stop(self):
  for value in [8191,True]:self.reject(lambda d,c,x=value:d['summary']['cases'][0]['source']['follower'].update(prefix_bytes=x))
  self.reject(lambda d,c:d['summary']['cases'][0]['source']['follower']['state'].update(stop=None))
 def test_all_eight_checkpoint_coverage_each_backend(self):
  for mode in v.MODES:
   self.reject(lambda d,c,m=mode:d['summary']['cases'][0][m].update(checkpoints=[]));self.reject(lambda d,c,m=mode:d['summary']['cases'][0][m]['checkpoints'].pop())
 def test_snapshot_mutation_and_cadence(self):
  self.reject(lambda d,c:d['summary']['cases'][0]['source']['checkpoints'][1]['state'].update(began_monotonic=0))
 def test_physical_trace_missing_wrongseen_or_wrongclock(self):
  for mut in [lambda p:p.update(rows=[]),lambda p:p.update(seen=0),lambda p:p['rows'][1].update(monotonic=0)]:self.reject(lambda d,c,m=mut:m(d['summary']['cases'][0]['Source_proxy_terminal']))
 def test_physical_hold_max_and_control_budget(self):
  for value in [4.36,3.9,float('nan')]:self.reject(lambda d,c,x=value:d['summary']['cases'][0]['source']['hold_observation'].update(actual_seconds=x))
  self.reject(lambda d,c:d['summary']['cases'][0]['source']['hold_observation'].update(control_seconds=3.2))
 def test_target_status_and_semantic_headers(self):
  self.reject(lambda d,c:d['summary']['cases'][0]['native']['target_outcome'].update(status=503));self.reject(lambda d,c:d['summary']['cases'][0]['native']['target_outcome'].update(headers={}))
 def test_header_omission_not_allowed(self):self.reject(lambda d,c:d.update(omitted_fields=[{'path':'headers'}]))
 def test_immediate_outcome_state_before_header_rejected(self):self.reject(lambda d,c:d['summary']['cases'][0]['native']['target_outcome']['state_at_outcome'].update(began_monotonic=0))
 def test_clientloss_no_status_or_fakeclose(self):
  for mode in v.MODES:
   self.reject(lambda d,c,m=mode:d['summary']['cases'][2][m]['target_outcome'].update(status=503));self.reject(lambda d,c,m=mode:d['summary']['cases'][2][m]['target_client_loss'].update(fd_after=3));self.reject(lambda d,c,m=mode:d['summary']['cases'][2][m]['target_client_loss'].update(reader_joined=False))
 def test_clientloss_server_cancel_never_inferred(self):self.reject(lambda d,c:d['summary']['cases'][2]['native']['target_client_loss'].update(server_cancellation_inferred=True))
 def test_close_roles_mandatory(self):self.reject(lambda d,c:d['summary']['cases'][0]['source'].update(client_closes=[]))
 def test_Source_signal_rows_roles_clock_missing(self):
  self.reject(lambda d,c:d['Source_signals'].update(success=[]));self.reject(lambda d,c:d['Source_signals']['busy'][0].update(role='foreign'))
 def test_Source_status_journal_correlation(self):self.reject(lambda d,c:d['summary']['cases'][0]['Source_signal_terminal_status'].update(rows=0))
 def test_owned_PID_records_mandatory_and_bound(self):
  self.reject(lambda d,c:d['owned_process_record_projection'].update(success=[]));self.reject(lambda d,c:d['owned_process_record_projection']['success'][0].update(pid=200))
 def test_default_audits_sequences_missing_even_both_equal(self):
  for k in ['tables','sequences']:
   self.reject(lambda d,c,key=k:[d['summary']['cases'][0][field].update({key:{}})for field in ['default_database_before','default_database_after']])
 def test_default_or_clone_drift_rejected(self):
  self.reject(lambda d,c:d['summary']['cases'][0]['default_database_after'].update(dataset_sha256='0'*64));self.reject(lambda d,c:d['summary']['cases'][0]['native_before'].update(database='dispatcharr'))
 def test_other_owner_token_and_fence_or_counter_corruption(self):
  self.reject(lambda d,c:d['summary']['cases'][1]['native']['other_private_after']['ledger'].update(fence='0'));self.reject(lambda d,c:d['summary']['cases'][1]['native'].update(other_global_after_close=0))
 def test_candidate_and_cleanup_corruption(self):
  self.reject(lambda d,c:d['summary']['cases'][0].update(actual_Main_image_id='old51'));self.reject(lambda d,c:c['projects'][d['summary']['cases'][0]['project']].update(containers='live'))
 def test_public_execution_hashes_and_claim_bindings(self):self.assertFalse(verify()['fresh_authorization_granted'])
 def test_both_exact25_reconstruction_modes(self):
  with tempfile.TemporaryDirectory()as temp:
   for mode in ['blocked','executed-historical']:
    out=Path(temp)/mode;r=reconstruct(out,mode);self.assertEqual(r['restored_files'],25);self.assertFalse(r['application_executed'])
 def test_reconstruction_existing_output_and_corruption(self):
  with tempfile.TemporaryDirectory()as temp:
   root=Path(temp)
   with self.assertRaises(ValueError):reconstruct(root)
   src=root/'source';shutil.copytree(HERE,src);(src/'blocked-inputs/tools/phase.py').write_text('corrupt')
   with self.assertRaises(ValueError):reconstruct(root/'out',source=src)
   self.assertFalse((root/'out').exists())
 def test_reconstruction_rejects_symlink(self):
  with tempfile.TemporaryDirectory()as temp:
   root=Path(temp);src=root/'source';shutil.copytree(HERE,src);f=src/'blocked-inputs/tools/phase.py';target=root/'target';f.rename(target);f.symlink_to(target)
   with self.assertRaises(ValueError):reconstruct(root/'out',source=src)
if __name__=='__main__':unittest.main(verbosity=2)
