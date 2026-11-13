"""Pure/synthetic finite Native19 binding/effect/clock/ownership controls."""
import ast,copy,datetime,hashlib,json,unittest
from pathlib import Path
from synthetic import example
from validate_native import validate,Clocks,SOURCE,clock,GROUP,SYS,equal
ROOT=Path(__file__).resolve().parents[1]
class Effects(unittest.TestCase):
 def reject(self,change):
  value=example();change(value)
  with self.assertRaises((ValueError,KeyError,TypeError,StopIteration,AssertionError)):validate(value)
 def test_complete_SYNTHETIC_only(self):
  result=validate(example());self.assertTrue(result['passed']);self.assertEqual(result['input_kind'],'synthetic-only-not-actual-observations');self.assertFalse(result['full_cache_parity_passed'])
 def test_missing_source_request(self):self.reject(lambda x:x['requests'].pop())
 def test_missing_transport_diagnostic(self):self.reject(lambda x:x['Native_transport_diagnostics'].pop())
 def test_raw_recorder_cannot_be_relabelled(self):self.reject(lambda x:x.update(passed=True))
 def test_wrong_projection_mode(self):self.reject(lambda x:x.update(Source_reference_mode='original-raw'))
 def test_wrong_projection_sha(self):self.reject(lambda x:x.update(Source_projection_sha256='0'*64))
 def test_wrong_original_sha(self):self.reject(lambda x:x.update(Original_raw_sha256='0'*64))
 def test_wrong_role(self):self.reject(lambda x:x['requests'][0].update(actor='standard'))
 def test_wrong_target(self):self.reject(lambda x:x['requests'][1].update(path='/api/core/notifications/11/'))
 def test_wrong_request_body(self):self.reject(lambda x:x['requests'][6].update(body_sha256='0'*64))
 def test_wrong_status(self):self.reject(lambda x:x['requests'][3].update(status=400))
 def test_raw_response_sha(self):self.reject(lambda x:x['requests'][0].update(response_body_sha256='0'*64))
 def test_raw_response_bytes(self):self.reject(lambda x:x['requests'][0].update(response_body_bytes=0))
 def test_seed_key_not_native(self):self.reject(lambda x:x['seed_users'][0].update(api_key={'sha256':'0'*64,'bytes':1}))
 def test_header_not_seed_credential(self):self.reject(lambda x:x['requests'][0]['raw_headers_private'].update(HTTP_X_API_KEY='00'))
 def test_user_field_mutation(self):self.reject(lambda x:x['requests'][2]['after_SQL']['tables']['accounts_user'][0].update(is_active=False))
 def test_event_not_masked(self):self.reject(lambda x:x['requests'][0]['after_events'].append({'id':99}))
 def test_target_title_mutation(self):self.reject(lambda x:x['requests'][1]['after_SQL']['tables']['core_systemnotification'][1].update(title='wrong'))
 def test_dismissal_cascade_missing(self):self.reject(lambda x:x['requests'][11]['after_SQL']['tables']['core_notificationdismissal'].append({'id':1}))
 def test_dismissal_sequence(self):self.reject(lambda x:x['requests'][11]['after_SQL']['sequences']['core_notificationdismissal'].update(last_value=4))
 def test_foreign_sequence(self):self.reject(lambda x:x['requests'][2]['after_SQL']['sequences']['m3u_m3uaccount'].update(last_value=99))
 def test_no_full_cache_parity_claim(self):self.reject(lambda x:x.update(full_cache_parity_claimed=True))
 def test_no_fake_backend_introspection(self):self.reject(lambda x:x.update(actual_auth_backend_introspection=True))
 def test_no_imported_warmed_system_group(self):self.reject(lambda x:x['requests'][0]['after_cache']['named_decoded'][SYS].update(raw_GET_hex='00',decoded={}))
 def test_no_imported_cache_rows(self):self.reject(lambda x:x['cache_after_seed']['rows'].append({'key_hex':'00'}))
 def test_missing_named_network(self):self.reject(lambda x:x['requests'][0]['after_cache']['named_decoded'].pop(GROUP))
 def test_wrong_network_decoded_type(self):self.reject(lambda x:x['requests'][0]['after_cache']['named_decoded'][GROUP].update(decoded={'UI':True}))
 def test_wrong_cache_get_bytes(self):self.reject(lambda x:x['requests'][0]['after_cache']['named_decoded'][GROUP].update(raw_GET_hex='00'))
 def test_dump_independent_binding(self):self.reject(lambda x:x['requests'][0]['after_raw_Redis'][1]['rows'][0].update(dump_hex='00'))
 def test_no_history_reset_or_addition(self):self.reject(lambda x:x['requests'][9]['after_cache'].update(api_key_failed_auth_history=[123.0]))
 def test_bad_history_raw_not_omitted(self):self.reject(lambda x:x['requests'][9]['after_cache'].update(api_key_failed_auth_raw_hex='00'))
 def test_missing_raw_redis_database(self):self.reject(lambda x:x['requests'][0]['after_raw_Redis'].pop())
 def test_raw_broker_key_added(self):self.reject(lambda x:x['requests'][0]['after_raw_Redis'][2]['rows'].append({'key_hex':'61','type':'list','dump_hex':'00','pttl':-1,'list_hex':[]}))
 def test_native_only_wrong_status(self):self.reject(lambda x:x['Native_transport_diagnostics'][1].update(status=413))
 def test_native_only_wrong_body_hash(self):self.reject(lambda x:x['Native_transport_diagnostics'][1].update(body_sha256='0'*64))
 def test_native_only_no_effects(self):self.reject(lambda x:x['Native_transport_diagnostics'][0]['after_SQL']['tables']['core_coresettings'].pop())
 def test_native_only_actor_not_anon(self):self.reject(lambda x:x['Native_transport_diagnostics'][0].update(actor='admin'))
 def test_final_SQL_continuity(self):self.reject(lambda x:x['final_SQL']['tables']['core_systemnotification'].pop())
 def test_final_server_join(self):self.reject(lambda x:x.update(server_graceful_wait0=False))
class ClockPolicy(unittest.TestCase):
 def test_existing_mapping_precedes_equality_rewind(self):
  c=Clocks(SOURCE);source=next(iter(c.generated));a=source.isoformat();b=(source+datetime.timedelta(hours=1)).isoformat();c.compare(a,b)
  with self.assertRaises(ValueError):c.compare(a,a)
 def test_coincident_generated_clock_registers(self):
  c=Clocks(SOURCE);source=next(iter(c.generated));a=source.isoformat();c.compare(a,a);self.assertIn(source,c.mapping)
  with self.assertRaises(ValueError):c.compare(a,(source+datetime.timedelta(microseconds=1)).isoformat())
 def test_only_unchanged_seed_equality_unmapped(self):
  c=Clocks(SOURCE);c.compare('2020-01-01T00:00:00Z','2020-01-01T00:00:00+00:00');self.assertEqual(c.mapping,{})
 def test_nested_bool_integer_types(self):self.assertFalse(equal({'flag':[True]},{'flag':[1]}))
class Ownership(unittest.TestCase):
 def test_runtime_test_selector_is_finite(self):
  text=(ROOT/'controller/run.py').read_text();self.assertIn("TEST='notifications::forms_sql_tests::real_api_keys_forms_twelve_and_seven_transport_regressions'",text);self.assertIn("'--exact','--ignored','--nocapture','--test-threads=1'",text)
 def test_report_path_matches_compiled_fixture_binding(self):
  text=(ROOT/'controller/run.py').read_text();self.assertIn("NOTIFICATION_FORMS_NATIVE_REPORT=/evidence/Native19-private.json",text);self.assertIn("capture=out/'Native19-private.json'",text);self.assertNotIn('native-source16.json',text)
 def test_cleanup_clock_before_every_post_ELF_work(self):
  text=(ROOT/'controller/run.py').read_text();i=text.index("'actual-Source12-plus7-Native-only-once'");self.assertLess(text.index('begin_cleanup()',i),text.index('qualified=validate(actual)',i));self.assertIn('finally:\n  begin_cleanup()',text);self.assertIn('cleanup_within_declared_90_seconds',text)
 def test_nonrecursive_bounded_stop(self):
  text=(ROOT/'controller/bounded_stop.py').read_text();self.assertIn('lambda:None',text);self.assertIn("receipt['acknowledged']=False",text)
 def test_exact_CID_before_bootstrap_and_partial_recovery(self):
  text=(ROOT/'controller/run.py').read_text();self.assertLess(text.index("'OneOwnedContainer'"),text.index("'StorageBootstrap180'"));self.assertIn('partial-up-exact-CID-recovery.json',text);self.assertIn("'--all','--no-trunc','--filter','id='+container",text)
 def test_real_PID_identity_before_absence(self):self.assertIn('ActualDockerTopHostPIDStartRequired',(ROOT/'controller/run.py').read_text())
 def test_once_reviewed_hash_before_fresh_output(self):
  text=(ROOT/'controller/run.py').read_text();self.assertLess(text.index("'ExactReviewedSeal'"),text.index('out.mkdir('));self.assertIn('exist_ok=False',text)
 def test_auditor_no_Django_imports(self):
  tree=ast.parse((ROOT/'controller/audit.py').read_text());self.assertFalse(any(isinstance(n,ast.ImportFrom)and n.module and n.module.startswith('django')for n in ast.walk(tree)));self.assertIn('default_transaction_read_only=on',(ROOT/'controller/audit.py').read_text());self.assertIn('REPEATABLE READ',(ROOT/'controller/audit.py').read_text())
if __name__=='__main__':unittest.main(verbosity=2)
