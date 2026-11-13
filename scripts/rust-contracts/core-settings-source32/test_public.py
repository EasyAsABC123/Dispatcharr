import copy,json,unittest
from unittest import mock
import verify as V
class PublicControls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.d=V.observations()
 def reject_observation(self,change):
  d=copy.deepcopy(self.d);change(d)
  with self.assertRaises((ValueError,KeyError,TypeError,AssertionError)):V.validate_observations(d)
 def test_retained_positive(self):self.assertEqual(V.validate_observations(self.d)['qualified_requests'],32)
 def test_original_capture_only_false_is_mandatory(self):self.reject_observation(lambda d:d.update(passed=True))
 def test_complete_request_coverage(self):
  for change in [lambda d:d.update(requests=[]),lambda d:d['requests'].pop(),lambda d:d['requests'].reverse(),lambda d:d['requests'][1].update(id=1),lambda d:d.update(actual_request_count=31),lambda d:d.update(completed_capture_count=31)]:
   with self.subTest(change=change):self.reject_observation(change)
 def test_each_response_status_body_role_and_clock(self):
  for key,value in [('status',200),('body_sha256','0'*64),('role','admin'),('actual_request_invoked',False),('began_utc','1970-01-01T00:00:00+00:00'),('completed_utc','2020-01-01T00:00:00')]:
   with self.subTest(key=key):self.reject_observation(lambda d:d['requests'][0].update({key:value}))
 def test_source_scope_and_real_auth_are_mandatory(self):
  for change in [lambda d:d.update(Original_git='wrong'),lambda d:d['startup'].update(default_graph_or_web_startup_acceptance=True),lambda d:d['database'].update(sha256='0'*64),lambda d:d['receiver_fixture_context'].update(Source_methods_replaced=True),lambda d:d.update(force_authenticate_used=True),lambda d:d.update(Source_TCP_or_Native=True),lambda d:d.update(APIClient_raise_request_exception=True),lambda d:d['requests'][4]['actual_auth'].update(user_id=2),lambda d:d['requests'][4].update(raw_headers_private={})]:
   with self.subTest(change=change):self.reject_observation(change)
 def test_snapshot_user_and_redis_coverage(self):
  for key in ['seed','final_before_receiver_cleanup']:
   for change in [lambda s:s.update(users=[]),lambda s:s.update(raw_Redis=[]),lambda s:s['raw_Redis'][1].update(rows=[]),lambda s:s['Channels'].update(frames=[])]:
    d=copy.deepcopy(self.d);change(d[key])
    if d==self.d:continue
    with self.subTest(key=key,change=change),self.assertRaises((ValueError,KeyError,TypeError)):V.validate_observations(d)
 def test_case12_warm_and_SQL_delta_are_required(self):
  for change in [lambda d:d.update(between_request_fixture_setup=[]),lambda d:d['requests'][11]['before']['named_cache'].update({'coresettings:group:owned.settings.new':None}),lambda d:d['requests'][11]['after']['named_cache'].update({'coresettings:group:owned.settings.new':None}),lambda d:d['requests'][11]['after']['named_cache'].update({'coresettings:group:owned.settings.renamed':{'wrong':True}})]:
   with self.subTest(change=change):self.reject_observation(change)
 def test_default_schema_mutation_and_sequence_changes(self):
  for change in [lambda d:d['requests'][0]['after']['public_manifest'].update(sequences={}),lambda d:d['requests'][0]['after']['public_manifest'].update(tables={}),lambda d:d['requests'][0]['after']['settings'].clear(),lambda d:d['requests'][0]['after']['public_manifest']['sequences'].update({'accounts_user_id_seq':[999,True]})]:
   with self.subTest(change=change):self.reject_observation(change)
 def test_lifecycle_positive(self):self.assertEqual(V.validate_lifecycle()['commands'],20)
 def test_lifecycle_coverage_and_physical_negatives(self):
  original=V.load;life=original('lifecycle.json')
  for change in [lambda x:x.update(actual_controller_exit=1),lambda x:x.update(raw_recorder_passed=True),lambda x:x.update(commands=[]),lambda x:x['commands'][0].update(physically_settled=False),lambda x:x.update(PID_absence=[]),lambda x:x['PID_absence'][0].update(original_process_absent=False),lambda x:x.update(independent_Docker_queries=[]),lambda x:x['independent_Docker_queries'][0].update(stdout='notempty'),lambda x:x.update(cleanup_elapsed_seconds=91),lambda x:x.update(owned_backends_after_Source=[{'pid':3}]),lambda x:x.update(project_inventory={'container':['x'],'network':[],'volume':[]})]:
   value=copy.deepcopy(life);change(value)
   with self.subTest(change=change),mock.patch.object(V,'load',lambda n:value if n=='lifecycle.json'else original(n)),self.assertRaises((ValueError,KeyError,TypeError)):V.validate_lifecycle()
 def test_safe_literal_key_and_empty_event_semantics_are_bound(self):
  self.reject_observation(lambda d:d['seed']['events'].append({'unexpected':True}))
  self.reject_observation(lambda d:d['seed']['raw_Redis'][1]['rows'][0].update(key_literal_UTF8='wrong'))
 def test_reconstructor_fails_before_output_on_existing_destination_or_bad_seal(self):
  import reconstruct as R,tempfile
  from pathlib import Path
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp)
   with self.assertRaisesRegex(ValueError,'FreshArtifactOutput'):R.run(root,root)
   (root/'executed-executor').mkdir();(root/'executed-executor/seal.json').write_text('{}')
   with mock.patch.object(R,'ROOT',root),self.assertRaisesRegex(ValueError,'ExactExecutedSource32Seal'):R.run(root,root/'absent')
   self.assertFalse((root/'absent').exists())
 def test_no_private_fallback(self):
  with mock.patch.object(V,'ROOT',V.ROOT/'missing-public-inputs'),self.assertRaises(FileNotFoundError):V.observations()
if __name__=='__main__':unittest.main()
