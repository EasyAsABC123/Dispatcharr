"""Public finite corruption controls; no Source/app/Docker/build or private fallback."""
import copy,json,tempfile,shutil,unittest
from pathlib import Path
from unittest import mock
import verify as V
import reconstruct as R
class Public(unittest.TestCase):
 def setUp(self):self.values=[V.read(x)for x in ['retained.json','schema.json','source-tcp38-authority.json','Root-review.json','privacy.json','build-proof.json']]+[{}]
 def rejected(self,fn):
  values=copy.deepcopy(self.values);fn(*values)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.validate(*values)
 def test_positive_exact_public_payload_verification(self):self.assertEqual(V.verify()['HTTP_cases'],12)
 def test_empty_case_coverage_refused(self):self.rejected(lambda d,*_:d.update(cases=[]))
 def test_missing_case_refused(self):self.rejected(lambda d,*_:d['cases'].pop())
 def test_reordered_case_refused(self):self.rejected(lambda d,*_:d['cases'].reverse())
 def test_duplicate_case_refused(self):self.rejected(lambda d,*_:d['cases'][1].update(name=d['cases'][0]['name']))
 def test_role_mapping_refused(self):self.rejected(lambda d,*_:d['cases'][0].update(role='other'))
 def test_changed_native_status_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual'].update(status=404))
 def test_source_body_cannot_be_replaced_consistently_on_both_sides(self):
  def change(d,*_):
   for side in ['Source_reference','Native_actual']:d['cases'][0][side].update(body_hex='5b5d',body_sha256=V.sha(b'[]'))
  self.rejected(change)
 def test_body_hash_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual'].update(body_sha256='0'*64))
 def test_missing_content_type_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual']['headers'].update(content_type=[]))
 def test_wrong_content_type_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual']['headers'].update(content_type=['text/html']))
 def test_request_deadline_widening_refused(self):
  def change(d,*_):
   row=d['cases'][0]['Native_actual'];row['absolute_observation']['deadline']=row['began_monotonic']+9
  self.rejected(change)
 def test_outside_phase_clock_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual'].update(began_monotonic=d['HTTP_phase']['started']-1))
 def test_fd_not_closed_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual']['physical_close'].update(fd_after=3))
 def test_timer_not_joined_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual']['absolute_observation'].update(timer_joined=False))
 def test_inode_coverage_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual']['physical_close'].update(original_inode_absent=False))
 def test_empty_sql_tables_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual']['before'].update(tables={}))
 def test_missing_sql_sequences_refused(self):self.rejected(lambda d,*_:d['owned_before'].update(sequences={}))
 def test_sql_namespace_mismatch_refused(self):self.rejected(lambda d,*_:d['cases'][0]['Native_actual']['before'].update(database_identifier_sha256='0'*64))
 def test_changed_default_sequence_refused(self):
  def change(d,*_):next(iter(d['default_after']['sequences'].values()))['last_value']+=1
  self.rejected(change)
 def test_provider_contact_refused(self):self.rejected(lambda d,*_:d['origin'].update(terminal_rows=[{'unexpected':True}]))
 def test_no_native_gates_refused(self):self.rejected(lambda d,*_:d.update(gates=[]))
 def test_missing_gate_refused(self):self.rejected(lambda d,*_:d['gates'].pop())
 def test_wrong_native_filter_refused(self):self.rejected(lambda d,*_:d['gates'][0].update(filter='wrong'))
 def test_no_test_actually_passed_refused(self):self.rejected(lambda d,*_:d['gates'][0].update(actual_passed_tests=0))
 def test_wrong_filtered_count_refused(self):self.rejected(lambda d,*_:d['gates'][0].update(actual_filtered_tests=59))
 def test_stdout_binding_refused(self):self.rejected(lambda d,*_:d['gates'][0].update(stdout_sha256='0'*64))
 def test_wrong_physical_terminal_refused(self):self.rejected(lambda d,*_:d['gates'][0]['terminal'].update(physical_process_stop_observed=False))
 def test_extended_native_phase_refused(self):self.rejected(lambda d,*_:d['native_phase'].update(completed=d['native_phase']['started']+181))
 def test_missing_command_coverage_refused(self):self.rejected(lambda d,*_:d.update(commands=[]))
 def test_unsettled_command_refused(self):self.rejected(lambda d,*_:d['commands'][0].update(physically_settled=False))
 def test_collector_overflow_refused(self):self.rejected(lambda d,*_:d['commands'][0].update(invalid='CollectionCapacity'))
 def test_missing_owned_redis_job_refused(self):self.rejected(lambda d,*_:d['jobs'].pop('source-redis'))
 def test_retained_owned_resources_refused(self):self.rejected(lambda d,*_:d['remaining'].update(containers='remaining'))
 def test_unverified_removed_origin_refused(self):self.rejected(lambda d,*_:d['removed_CIDs']['origin'].update(exact_CID_NotFound=False))
 def test_empty_root_cleanup_coverage_refused(self):self.rejected(lambda d,s,a,r,*_:r.update(fresh_cleanup_queries=[]))
 def test_root_nonempty_volume_inventory_refused(self):self.rejected(lambda d,s,a,r,*_:r['fresh_cleanup_queries'][2].update(stdout='remaining'))
 def test_missing_privacy_coverage_refused(self):self.rejected(lambda d,s,a,r,p,*_:p.update(body_rows_checked=0))
 def test_sensitive_body_match_refused(self):self.rejected(lambda d,s,a,r,p,*_:p.update(body_rows_with_sensitive_matches=1))
 def test_full_milestone_or_performance_scope_refused(self):
  for key in ['Main_accepted','full_M7_accepted','performance_accepted','default_worker_activation']:self.rejected(lambda d,*_,key=key:d.update({key:True}))
 def test_source_business_replay_refused(self):self.rejected(lambda d,*_:d.update(Source_business_requests=1))
 def test_failed_build_wait_refused(self):self.rejected(lambda d,s,a,r,p,b,*_:b['waits'][2].update(actual_exit=101))
 def test_current_build_cannot_claim_inherited_wholeworkspace_execution(self):self.rejected(lambda d,s,a,r,p,b,*_:b.update(wholeworkspace_execution_transferred=True))
 def test_wrong_actual_scoped_counts_refused(self):self.rejected(lambda d,s,a,r,p,b,*_:b['focused_XC'].update(passed=62))
 def test_actual_clock_cannot_be_current_wallclock_or_SQL_rewrite(self):
  self.rejected(lambda d,s,a,r,*_:r.update(fixed_eligibility_micros=1794528562638981))
  self.rejected(lambda d,s,a,r,*_:r.update(SQL_timestamp_rewrites=True))
  self.rejected(lambda d,s,a,r,*_:r.update(Main_wallclock_refresh_or_performance_acceptance=True))
 def test_mutated_payload_fails_manifest_without_private_fallback(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);shutil.copytree(V.HERE,root/'public',ignore=shutil.ignore_patterns('__pycache__'));p=root/'public/retained.json';p.write_bytes(p.read_bytes()+b'\n')
   with mock.patch.object(V,'HERE',root/'public'):
    with self.assertRaises(ValueError):V.verify()
 def test_reconstruction_overlay_corruption_refused_before_output(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);shutil.copytree(R.HERE,root/'public',ignore=shutil.ignore_patterns('__pycache__'));shutil.copytree(R.HERE.parent/'cached-catalogue-component',root/'cached-catalogue-component',ignore=shutil.ignore_patterns('__pycache__'));p=next((root/'public/overlay').rglob('*.rs'));p.write_bytes(p.read_bytes()+b'\n')
   with mock.patch.object(R,'HERE',root/'public'):
    with self.assertRaises(ValueError):R.load()
 def test_reconstruction_manifest_corruption_refused(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);shutil.copytree(R.HERE,root/'public',ignore=shutil.ignore_patterns('__pycache__'));p=root/'public/source-inputs.json';p.write_bytes(p.read_bytes()+b'\n')
   with mock.patch.object(R,'HERE',root/'public'):
    with self.assertRaises(ValueError):R.load()
 def test_reconstruction_requires_explicit_public_dependency(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);shutil.copytree(R.HERE,root/'public',ignore=shutil.ignore_patterns('__pycache__'))
   with mock.patch.object(R,'HERE',root/'public'),self.assertRaises(FileNotFoundError):R.load()
 def test_public_privacy_rejects_credential_fields_and_encoded_urls(self):
  for value in [{'username':'synthetic-private-user'},{'password':'synthetic-private-password'},{'cover':'http://synthetic:private@origin/image'},{'cover':'http://origin/image?key=synthetic-private-key'},{'cover':'http%253A%252F%252Forigin%252Fimage%253Ftoken%253Dsynthetic-private-token'}]:
   with self.assertRaises(ValueError):V.safe_public_body(json.dumps(value).encode())
 def test_public_privacy_accepts_only_noncredential_versioned_origin_example(self):
  self.assertTrue(V.safe_public_body(b'{"cover":"http://origin/image?type=movie_image&v=01234567","auth":0}'))
if __name__=='__main__':unittest.main()
