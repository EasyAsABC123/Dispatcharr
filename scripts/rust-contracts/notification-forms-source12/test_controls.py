"""Portable retained Source12 projection corruption controls; no app or Docker."""
import copy,hashlib,json,tempfile,unittest
from pathlib import Path
import verify
from forms_effects import compare
class Observation(unittest.TestCase):
 def setUp(self):self.source=verify.load_source_projection();self.plan=verify.load_plan()
 def reject(self,change):
  change(self.source)
  with self.assertRaises((ValueError,KeyError,TypeError,StopIteration,AssertionError)):compare(self.source,self.plan)
 def test_all_twelve(self):self.assertTrue(compare(self.source,self.plan)['passed'])
 def test_missing_request(self):self.reject(lambda x:x['requests'].pop())
 def test_wrong_role(self):self.reject(lambda x:x['requests'][0].update(actor='standard'))
 def test_wrong_target(self):self.reject(lambda x:x['requests'][1].update(path='/api/core/notifications/11/'))
 def test_alias_swap(self):self.reject(lambda x:x['aliases'].update(FormA=x['aliases']['FormB']))
 def test_response_body_sha(self):self.reject(lambda x:x['requests'][0].update(response_body_sha256='0'*64))
 def test_wrong_status(self):self.reject(lambda x:x['requests'][9].update(status=400))
 def test_original_twenty_bytes_rejected(self):self.reject(lambda x:x['requests'][6]['parsed_request_metadata']['cached']['_files']['fields'][0]['values'][0].update(size=20))
 def test_upload_class(self):self.reject(lambda x:x['requests'][6]['parsed_request_metadata']['cached']['_files']['fields'][0]['values'][0].update(**{'class':'unknown.File'}))
 def test_observer_read(self):self.reject(lambda x:x['requests'][6]['parsed_request_metadata']['cached']['_files']['fields'][0]['values'][0].update(file_content_read=True))
 def test_nested_bool_integer(self):self.reject(lambda x:x['requests'][6]['parsed_request_metadata']['cached']['_files']['fields'][0]['values'][0].update(file_content_read=0))
 def test_duplicate_title_order(self):
  def mutate(x):
   row=next(v for v in x['requests'][7]['parsed_request_metadata']['cached']['_data']['fields']if v['name']=='title');row['values'].reverse()
  self.reject(mutate)
 def test_ignored_delete_body_must_stay_unparsed(self):self.reject(lambda x:x['requests'][11]['parsed_request_metadata'].update(forced_parse=True))
 def test_no_jwt_or_force_auth_substitution(self):self.reject(lambda x:x.update(force_authenticate_used=True))
 def test_wrong_backend(self):self.reject(lambda x:x['requests'][0]['actual_auth_observation'].update(backend_class='rest_framework.authentication.ForcedAuthentication'))
 def test_header_credential_evidence(self):self.reject(lambda x:x['requests'][0]['raw_headers_private']['HTTP_X_API_KEY'].update(sha256='0'*64))
 def test_seed_credential_evidence(self):self.reject(lambda x:x['seed']['users'][0]['api_key'].update(sha256='0'*64))
 def test_extra_sequence_scope(self):self.reject(lambda x:x['requests'][0]['after']['public_manifest']['sequences'].update(unobserved_seq=[1,True]))
 def test_sequence_change(self):self.reject(lambda x:x['requests'][1]['after']['audit']['sequences']['public.core_notificationdismissal_id_seq'].update(last_value=4))
 def test_user_SQL_binding(self):self.reject(lambda x:x['requests'][0]['after']['audit']['tables']['accounts_user']['raw_rows'][0].update(is_staff=True))
 def test_user_SQL_fingerprint(self):self.reject(lambda x:x['requests'][0]['after']['audit']['tables']['accounts_user'].update(sha256='0'*64))
 def test_empty_delete_response(self):self.reject(lambda x:x['requests'][11].update(response_body_hex='20'))
 def test_own_request_clock(self):self.reject(lambda x:x['requests'][0].update(began_utc='2099-01-01T00:00:00Z'))
 def test_no_receiver_consumption(self):self.reject(lambda x:x['requests'][0]['after']['Channels'].update(receive_pop_or_ack_calls=1))
 def test_raw_redis_scope(self):self.reject(lambda x:x['requests'][0]['after']['raw_Redis'].pop())
 def test_failed_auth_history_not_erased(self):self.reject(lambda x:x['requests'][9]['after']['cache'].update(api_key_failed_auth_history={'unexpected':1}))
 def test_default_lifecycle_negative_preserved(self):self.assertEqual(verify.validate_lifecycle()['executed_controller_exit'],1)
class Reference(unittest.TestCase):
 def mutate(self,name,change):
  with tempfile.TemporaryDirectory(prefix='forms-public-control-')as d:
   target=Path(d)
   for filename in ('reference.json','Source12-projection-manifest.json','Source12-projection.json'):(target/filename).write_bytes((verify.ROOT/filename).read_bytes())
   p=target/name;value=json.loads(p.read_text());change(value);p.write_text(json.dumps(value));old=verify.ROOT
   try:
    verify.ROOT=target
    with self.assertRaises(ValueError):verify.load_source_projection()
   finally:verify.ROOT=old
 def test_wrong_projection_mode(self):self.mutate('reference.json',lambda x:x.update(reference_mode='original'))
 def test_raw_identity_even_consistent_new_manifest(self):self.mutate('reference.json',lambda x:x.update(Original_raw_sha256='0'*64))
 def test_projection_raw_not_substitutable(self):self.mutate('Source12-projection.json',lambda x:x.update(actual_request_count=11))
 def test_negative_controller_cannot_be_relabelled(self):self.mutate('reference.json',lambda x:x.update(executed_controller_exit=0))
 def test_projection_was_never_executed(self):self.mutate('Source12-projection-manifest.json',lambda x:x.update(projected_observation_executed=True))
if __name__=='__main__':unittest.main(verbosity=2)
