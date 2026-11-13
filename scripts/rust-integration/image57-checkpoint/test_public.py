"""Finite corruption, privacy and restoration controls; no Docker/app/network/media."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest import mock
import verify as V
import reconstruct as R
import projection_rules as P
H=Path(__file__).resolve().parent
class Publication57(unittest.TestCase):
 def matrix(self):return V.read(H/'matrix.json'),V.read(H/'authority.json')
 def rejects_matrix(self,fn):
  x,a=self.matrix();fn(x)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.matrix(x,a)
 def header(self):return[V.read(H/p)for p in ['evidence/Main/header/worker-header-native-private.json','evidence/Main/header/summary-private.json','header-auth-identity.json']]
 def rejects_header(self,fn):
  x=self.header();fn(*x)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.header(*x)
 def notice(self):return[V.read(H/p)for p in ['evidence/Main/notifications/notification-Main-private.json','evidence/Main/notifications/summary-private.json']]
 def rejects_notice(self,fn):
  x=self.notice();fn(*x)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.notifications(*x)
 def main(self):return V.load(H/'main4s/validate.py','controls_main55'),[V.read(H/'main4s'/p)for p in ['retained.json','independent-cleanup-projection.json','coverage-authority.json']]
 def rejects_main(self,fn):
  m,x=self.main();fn(*x)
  with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):m.validate(*x)
 def test_positive_full_public(self):self.assertTrue(V.verify()['passed'])
 def test_missing_matrix_node(self):self.rejects_matrix(lambda x:x['rows'].pop())
 def test_swap_required_node_names(self):self.rejects_matrix(lambda x:x['rows'][0].update(required_node=x['rows'][1]['required_node']))
 def test_worker_graph_activation(self):self.rejects_matrix(lambda x:x.update(worker_graph_activation=True))
 def test_controller_count_wrong(self):self.rejects_matrix(lambda x:x.update(actual_controller_attempts=31))
 def test_offline_controller_not_application(self):self.rejects_matrix(lambda x:x.update(application_fixture_children=28,offline_controller_children=0))
 def test_actual_time_authority_not_self_asserted(self):
  x,a=self.matrix();path=H/x['rows'][0]['controller'];original=V.read;changed=original(path);changed.update(started_epoch=1,completed_epoch=2)
  with mock.patch.object(V,'read',side_effect=lambda p:copy.deepcopy(changed)if Path(p)==path else original(p)):
   with self.assertRaises(ValueError):V.matrix(x,a)
 def test_actual_mapping_not_just_count(self):
  x,a=self.matrix();x['rows'][0]['controller']=x['rows'][1]['controller']
  with self.assertRaises(ValueError):V.matrix(x,a)
 def test_all_cleanup_queries_required(self):
  x,a=self.matrix();path=H/x['rows'][0]['cleanup'];original=V.read;changed=original(path);changed['queries']=[]
  with mock.patch.object(V,'read',side_effect=lambda p:copy.deepcopy(changed)if Path(p)==path else original(p)):
   with self.assertRaises(ValueError):V.matrix(x,a)
 def test_Original_wrong(self):
  original=V.read;changed=original(H/'scope.json');changed['Original_revision']='wrong'
  with mock.patch.object(V,'read',side_effect=lambda p:copy.deepcopy(changed)if Path(p)==H/'scope.json'else original(p)):
   with self.assertRaises(ValueError):V.verify()
 def test_main_missing_case(self):self.rejects_main(lambda d,c,a:d['summary']['cases'].pop())
 def test_main_missing_default_sequences(self):self.rejects_main(lambda d,c,a:d['summary']['cases'][0]['default_database_before'].update(sequences={}))
 def test_main_hold_over_max(self):self.rejects_main(lambda d,c,a:d['summary']['cases'][0]['native']['hold_observation'].update(actual_seconds=4.5))
 def test_main_missing_SQL_or_follower(self):
  for key in ['actual_SQL_Bearer_user_verified','early_follower_valid']:self.rejects_main(lambda d,c,a,k=key:d['summary']['cases'][0]['native'].update({k:False}))
 def test_main_missing_checkpoints(self):self.rejects_main(lambda d,c,a:d['summary']['cases'][0]['native'].update(checkpoints=[]))
 def test_main_missing_Source_signals(self):self.rejects_main(lambda d,c,a:d['Source_signals'].update(busy=[]))
 def test_main_missing_owned_processes(self):self.rejects_main(lambda d,c,a:d['owned_process_record_projection'].update(clientloss=[]))
 def test_main_missing_other_fence(self):self.rejects_main(lambda d,c,a:d['summary']['cases'][1]['native']['other_private_after']['ledger'].update(fence=None))
 def test_main_clientloss_not_fake_HTTP(self):self.rejects_main(lambda d,c,a:d['summary']['cases'][2]['native']['target_outcome'].update(status=503))
 def test_main_container_PID_namespace_not_host(self):self.rejects_main(lambda d,c,a:d.update(owned_process_PID_namespace_scope='host PIDs independently absent'))
 def test_header_extra_unobserved_audit_table(self):self.rejects_header(lambda p,s,a:s['default_before']['tables'].update(core_systemnotification={'rows':0,'sha256':'0'*64}))
 def test_header_missing_case(self):self.rejects_header(lambda p,s,a:p['cases'].pop())
 def test_header_wrong_hostname(self):self.rejects_header(lambda p,s,a:p['cases'][3].update(headers=[['X-Worker-ID','provider-canary']]))
 def test_header_missing_origin_request(self):self.rejects_header(lambda p,s,a:p['cases'][3]['origin_after'].update(requests=[]))
 def test_header_request_socket_unclosed(self):self.rejects_header(lambda p,s,a:p['cases'][0]['physical_close'].update(fd_after=5))
 def test_header_auth_missing(self):self.rejects_header(lambda p,s,a:a.update(auth_http_status=401))
 def test_header_full_scope_not_promoted(self):self.rejects_header(lambda p,s,a:p['qualification'].update(full_header_or_M4_parity=True))
 def test_notification_missing_case(self):self.rejects_notice(lambda p,s:p['cases'].pop())
 def test_notification_missing_credential_role(self):self.rejects_notice(lambda p,s:p['actual_auth_requests'].pop('standard'))
 def test_notification_wrong_real_SQL_ID(self):self.rejects_notice(lambda p,s:p['actual_auth_requests']['admin']['identity_projection'].update(id=-1))
 def test_notification_repeat_sequence_used(self):self.rejects_notice(lambda p,s:p['cases'][2]['after']['sequences']['core_notificationdismissal'].update(last_value=999))
 def test_notification_PUT_mutated(self):self.rejects_notice(lambda p,s:p['cases'][6]['after']['sequences']['core_systemnotification'].update(last_value=999))
 def test_notification_PATCH_outside_request(self):self.rejects_notice(lambda p,s:p['cases'][7]['after']['tables']['core_systemnotification'][-1].update(updated_at='2099-01-01T00:00:00Z'))
 def test_notification_default_changed(self):self.rejects_notice(lambda p,s:s['default_after']['sequences']['core_systemnotification'].update(last_value=999))
 def test_privacy_literal_JWT(self):
  token='ey'+'Jaaaaaaaaaaa.bbbbbbbbbbbbb.cccccccccccc';out=P.clean({'value':token});self.assertNotIn(token,json.dumps(out));self.assertIn('private_value_sha256',out['value'])
 def test_privacy_hex_DTO_and_cookie(self):
  original=json.dumps({'api_key':'private-example'}).encode().hex();out=P.clean({'body_hex':original,'headers':[['Set-Cookie','private-cookie']]});self.assertNotIn(original,json.dumps(out));self.assertNotIn('private-cookie',json.dumps(out))
 def test_privacy_hex_JWT(self):
  token='ey'+'Jaaaaaaaaaaa.bbbbbbbbbbbbb.cccccccccccc';original=('Bearer '+token).encode().hex();out=P.clean({'opaque':original});self.assertNotIn(original,json.dumps(out));self.assertIn('private_value_sha256',out['opaque'])
 def test_privacy_http_api_key(self):
  out=P.clean({'HTTP_X_API_KEY':'private-example'});self.assertNotIn('private-example',json.dumps(out))
 def test_three_exact_blocked_restorations(self):
  for name in ['main4s','header','notifications']:
   with self.subTest(name=name),tempfile.TemporaryDirectory()as d:
    result=R.tools(name,Path(d)/'restored');self.assertFalse(result['new_authorization_granted']);self.assertFalse(result['app_executed']);self.assertTrue(result['blocked']);self.assertFalse((Path(d)/'restored/execution-cue.json').exists())
 def test_historical_flag_is_not_new_authority(self):
  with tempfile.TemporaryDirectory()as d:self.assertFalse(R.tools('header',Path(d)/'old',True)['new_authorization_granted'])
 def test_existing_reconstruction_output_refused(self):
  with tempfile.TemporaryDirectory()as d:
   with self.assertRaises(ValueError):R.tools('header',Path(d))
 def test_corrupt_selected_payload_refused(self):
  original=V.sha
  with mock.patch.object(V,'sha',side_effect=lambda p:'0'*64 if str(p).endswith('scope.json')else original(p)):
   with self.assertRaises(ValueError):R.selected()
class Additional57(unittest.TestCase):
 def test_catalogue_positive(self):self.assertEqual(V.catalogue(V.read(H/'evidence/Main/catalogue26/Main26-private.json'))['original_controller_host_exit'],1)
 def rejects(self,change):
  v=V.read(H/'evidence/Main/catalogue26/Main26-private.json');change(v)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.catalogue(v)
 def test_catalogue_missing_case(self):self.rejects(lambda v:v['cases'].pop())
 def test_catalogue_duplicate(self):self.rejects(lambda v:v['cases'][0].update(name=v['cases'][1]['name']))
 def test_catalogue_status(self):self.rejects(lambda v:v['cases'][0].update(status=500))
 def test_catalogue_body(self):self.rejects(lambda v:v['cases'][0].update(body_hex='00'))
 def test_catalogue_SQL(self):self.rejects(lambda v:v['cases'][0].update(after={}))
 def test_catalogue_cfgtest(self):self.rejects(lambda v:v.update(actual_production_Main=False))
 def test_catalogue_info(self):self.rejects(lambda v:v.update(info_actions_or_fixed_clock_hook=True))
 def test_catalogue_full_parity(self):self.rejects(lambda v:v.update(full_M7_or_performance=True))
 def test_catalogue_predicate(self):self.rejects(lambda v:v['cases'][0]['parity'].update(extra=False))
 def test_fresh_queries_missing(self):
  with self.assertRaises(ValueError):V.fresh_queries([],['owned'])
 def test_sdk_changed_COPY(self):
  names=['offline-exact-review-corrected-label.json','all-source-inputs.json','verifier-selected-inputs.json','SDK-copy-closure.json','host-only-controls.json','all-source-after.json','actual-process-exit.json'];args=[V.read(H/('evidence/SDK/'+n))for n in names];args[3][next(iter(args[3]))]='0'*64
  with self.assertRaises(ValueError):V.sdk(*args)
 def test_sdk_unsettled(self):
  names=['offline-exact-review-corrected-label.json','all-source-inputs.json','verifier-selected-inputs.json','SDK-copy-closure.json','host-only-controls.json','all-source-after.json','actual-process-exit.json'];args=[V.read(H/('evidence/SDK/'+n))for n in names];args[-1]['wait_completed']=False
  with self.assertRaises(ValueError):V.sdk(*args)
class NewInputBinding57(unittest.TestCase):
 def test_new_bindings_blocked(self):
  import reconstruct_new_catalogue as N
  for kind in ['catalogue26','backend-observer']:
   with tempfile.TemporaryDirectory()as d:
    r=N.restore(kind,Path(d)/'new');self.assertTrue(r['execution_blocked']);self.assertFalse(r['authorization_granted']);self.assertFalse(r['original_input_or_result_relabelled'])
 def test_wrong_kind_refused(self):
  import reconstruct_new_catalogue as N
  with tempfile.TemporaryDirectory()as d:
   with self.assertRaises(ValueError):N.restore('other',Path(d)/'new')
if __name__=='__main__':unittest.main(verbosity=2)
