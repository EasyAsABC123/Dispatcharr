"""Socket-free finite Source24 corruption and controller ownership controls."""
import copy,hashlib,json,pickle,unittest
from pathlib import Path
import effects as E,synthetic as S,run as R
ROOT=Path(__file__).resolve().parent
class Oracle(unittest.TestCase):
 def setUp(self):self.value=S.build()
 def reject(self,change):
  change(self.value)
  with self.assertRaises((ValueError,KeyError)):E.validate(self.value,S.PLAN,S.SCHEMA)
 def test_synthetic24_positive_only(self):self.assertEqual(E.validate(self.value,S.PLAN,S.SCHEMA)['Source_requests'],24)
 def test_recorder_false_preserved(self):self.reject(lambda v:v.update(passed=True))
 def test_wrong_route_alias(self):self.reject(lambda v:v['requests'][0].update(path='/api/plugins/plugins/'))
 def test_wrong_target_pk(self):self.reject(lambda v:v['requests'][8]['resolver']['kwargs'].update(pk=802))
 def test_wrong_resolved_class(self):self.reject(lambda v:v['requests'][0]['resolver'].update(**{'class':'wrong.View'}))
 def test_wrong_body_byte(self):self.reject(lambda v:v['requests'][8].update(body_hex=b'[]'.hex()))
 def test_wrong_api_key(self):self.reject(lambda v:v['requests'][2]['raw_headers_private'].update(HTTP_X_API_KEY='00'))
 def test_wrong_backend(self):self.reject(lambda v:v['requests'][2]['actual_auth_observation'].update(backend_class='forced'))
 def test_auth_bool_integer(self):self.reject(lambda v:v['requests'][2]['actual_auth_observation'].update(is_authenticated=1))
 def test_wrong_seed_user_role(self):self.reject(lambda v:v['seed']['users'][0].update(user_level=1))
 def test_wrong_seed_actor_default(self):self.reject(lambda v:v['seed']['users'][0].update(is_staff=True))
 def test_wrong_seed_repo(self):self.reject(lambda v:v['seed']['repos'][1].update(name='swapped'))
 def test_wrong_config_pair(self):self.reject(lambda v:v['seed']['configs'][0].update(source_repo_id=802))
 def test_seed_sequence_bool(self):self.reject(lambda v:v['seed']['public_manifest']['sequences']['plugins_pluginrepo_id_seq'].__setitem__(1,1))
 def test_wrong_official_seed_flag(self):self.reject(lambda v:v['seed']['repos'][0].update(is_official=False))
 def test_persistent_version_ttl(self):self.reject(lambda v:v['seed']['cache']['rows'][0].update(pttl=300000))
 def test_missing_version_seed(self):self.reject(lambda v:v['seed']['cache']['rows'].pop())
 def test_wrong_seed_core_value(self):self.reject(lambda v:v['seed']['settings'][0].update(value={'UI':'0.0.0.0/0'}))
 def test_missing_request(self):self.reject(lambda v:v['requests'].pop())
 def test_wrong_status(self):self.reject(lambda v:v['requests'][0].update(status=200))
 def test_content_type(self):self.reject(lambda v:v['requests'][0]['headers'].update(**{'Content-Type':'text/html'}))
 def test_body_hash(self):self.reject(lambda v:v['requests'][2].update(response_body_sha256='0'*64))
 def test_readonly_field_mutation(self):self.reject(lambda v:v['requests'][11]['after']['repos'][1].update(url='https://bad.invalid'))
 def test_delete_cascade_clock_mutation(self):self.reject(lambda v:v['requests'][17]['after']['configs'][0].update(updated_at=S.stamp(20.2)))
 def test_forbidden_user_change(self):self.reject(lambda v:v['requests'][0]['after']['users'][0].update(last_login=S.stamp(3.2)))
 def test_schema_drop(self):self.reject(lambda v:v['requests'][0]['after']['public_manifest']['tables'].pop('accounts_user'))
 def test_foreign_table_digest(self):self.reject(lambda v:v['requests'][0]['after']['public_manifest']['tables']['accounts_user'].update(sha_like_md5='1'*32))
 def test_sequence_change(self):self.reject(lambda v:v['requests'][8]['after']['public_manifest']['sequences']['plugins_pluginrepo_id_seq'].__setitem__(0,999))
 def test_cache_key_loss(self):self.reject(lambda v:v['requests'][0]['after']['raw_Redis'][1]['rows'].pop())
 def test_cache_payload_mutation(self):self.reject(lambda v:v['requests'][0]['after']['raw_Redis'][1]['rows'][0].update(value_hex='00'))
 def test_extra_redis_key(self):self.reject(lambda v:v['requests'][0]['after']['raw_Redis'][2]['rows'].append({'key_hex':'00'}))
 def test_channel_consumption(self):self.reject(lambda v:v['requests'][0]['after']['Channels'].update(receive_pop_or_ack_calls=1))
 def test_wrong_channel_receiver(self):self.reject(lambda v:v['requests'][0]['after']['Channels'].update(receiver='another'))
 def test_fabricated_http_for_exception(self):self.reject(lambda v:v['requests'][-1].update(status=500))
 def test_exception_incomplete(self):self.reject(lambda v:v['requests'][-1]['framework_exception'].update(complete=False))
 def test_exception_wrong_sha(self):self.reject(lambda v:v['requests'][-1]['framework_exception'].update(traceback_sha256='0'*64))
 def test_literal_pickle_global_forbidden(self):
  with self.assertRaises(ValueError):E.literal_cache(b'cos\nsystem\n.')
 def test_request_window8(self):self.reject(lambda v:v['requests'][0].update(completed_utc=S.stamp(20)))
 def test_phase_window120(self):self.reject(lambda v:v.update(phase_elapsed_seconds=121))
 def test_delete_parser_materialized(self):self.reject(lambda v:v['requests'][17]['parsed_request_metadata']['cached']['_data'].update(**{'class':'builtins.dict','fields':[]}))
 def test_delete_parser_forced(self):self.reject(lambda v:v['requests'][17]['parsed_request_metadata'].update(forced_parse=True))
class ExceptionEvidence(unittest.TestCase):
 def test_cached_nested_JSON_metadata_without_forcing_parse(self):
  import source_fixture as F
  from types import SimpleNamespace
  request=SimpleNamespace(_data={'cached_manifest':{},'nested':[1,{'x':True}]});value=F.parsed_metadata(request);self.assertIs(value['forced_parse'],False);self.assertEqual(value['cached']['_data']['fields'][0]['values'],[{'kind':'json_literal','value':{}}])
 def test_cached_JSON_metadata_depth_cap(self):
  import source_fixture as F
  from types import SimpleNamespace
  value={}
  for _ in range(10):value={'nested':value}
  with self.assertRaises(ValueError):F.parsed_metadata(SimpleNamespace(_data={'x':value}))
 def test_full_literal_exception_metadata(self):
  import source_fixture as F
  try:raise ValueError('bounded literal')
  except ValueError:
   import sys
   value=F.exception(sys.exc_info())
  self.assertTrue(value['complete']);self.assertFalse(value['truncated']);self.assertEqual(value['message_original_bytes'],len(bytes.fromhex(value['message_hex'])));self.assertEqual(value['message_original_sha256'],value['message_sha256'])
 def test_capacity_keeps_original_count_hash_and_marks_incomplete(self):
  import source_fixture as F,sys
  message='x'*65537
  try:raise ValueError(message)
  except ValueError:value=F.exception(sys.exc_info())
  self.assertFalse(value['complete']);self.assertTrue(value['truncated']);self.assertTrue(value['message_truncated']);self.assertEqual(value['message_original_bytes'],65537);self.assertEqual(value['message_bytes'],65536);self.assertEqual(value['message_original_sha256'],hashlib.sha256(message.encode()).hexdigest())
class Controller(unittest.TestCase):
 def test_source_import_does_not_run_app(self):self.assertTrue(callable(R.main))
 def test_finite_normal_count_reserves_one_neutral_stop(self):
  with self.assertRaises(ValueError):R.command_budget(223,0,False)
  self.assertGreater(R.command_budget(223,0,True),0)
 def test_total_count_reserves_one_neutral_stop(self):
  with self.assertRaises(ValueError):R.command_budget(255,0,True)
 def test_output_reserve(self):
  with self.assertRaises(ValueError):R.command_budget(1,67108864-65536,True)
 def test_real_shell_migrate_only(self):
  text=(ROOT/'start.sh').read_text();self.assertIn('python manage.py shell -c',text);self.assertNotIn('python manage.py migrate',text);self.assertIn('should_skip_initialization() is True',text)
 def test_default_audit_readonly_before_snapshot(self):
  text=(ROOT/'default_audit.py').read_text();self.assertLess(text.index('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'),text.index("result={'named11'"));self.assertIn("sys.argv[1]!='shell'",text)
 def test_no_force_auth_or_patched_source(self):
  text=(ROOT/'source_fixture.py').read_text();self.assertNotIn('.force_authenticate(',text);self.assertIn('client.raise_request_exception=True',text);self.assertIn("matched.func.cls is expected",text);self.assertIn("vars(PluginManager).get('_instance')is None",text)
 def test_pre_request_seed_binding(self):
  text=(ROOT/'source_fixture.py').read_text();self.assertLess(text.index("report['seed_binding']=validate_seed(report,plan)"),text.index("for spec in plan['cases']:"))
 def test_cleanup_before_validation(self):
  text=(ROOT/'run.py').read_text();self.assertIn("begin_cleanup();actual=read(out/'plugin-repositories-private.json')",text);self.assertIn("report['cleanup_within90']",text)
 def test_exact_owned_database(self):
  self.assertEqual(R.database('rust_probe_pluginrepo_'+'a'*10),'rust_probe_pluginrepo_'+'a'*10)
  for name in ['dispatcharr','postgres','rust_probe_pluginrepo_bad']:
   with self.assertRaises(ValueError):R.database(name)
if __name__=='__main__':unittest.main(verbosity=2)
