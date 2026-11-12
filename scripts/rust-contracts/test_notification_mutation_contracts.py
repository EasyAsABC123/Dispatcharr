"""Host-only Source48 assignment rejection and immutable register preservation.
Original Dispatcharr authors retain application/algorithm credit/licenses.
"""
import copy,json,unittest
from pathlib import Path
import notification_mutation_contracts as subject
ROOT=Path(__file__).resolve().parents[2]
class NotificationMutationControls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.value=json.loads((ROOT/subject.INPUT).read_text())
 def reject(self,change):
  value=copy.deepcopy(self.value);change(value)
  with self.assertRaises((AssertionError,ValueError,KeyError,IndexError,StopIteration)):subject.validate(ROOT,value)
 def test_exact3_distinct_methods48_source_observations(self):
  self.assertTrue(subject.validate(ROOT,self.value)['passed']);self.assertEqual(self.value['method_case_counts'],{'PUT':6,'PATCH':34,'DELETE':8});self.assertEqual(sorted(c['source_index']for k in self.value['contracts']for c in k['cases']),list(range(48)))
 def test_wrong_method(self):self.reject(lambda v:v['contracts'][0].update(method='HEAD'))
 def test_wrong_format_alias(self):self.reject(lambda v:v['contracts'][0].update(contract_id='route-abaabdde56bd6bb1afd8:PUT'))
 def test_wrong_route(self):self.reject(lambda v:v['contracts'][0].update(pattern_exact='api/system/^notifications/(?P<pk>[^/.]+)/$'))
 def test_wrong_handler(self):self.reject(lambda v:v['contracts'][0].update(source_handler='core.views.SystemNotificationViewSet'))
 def test_wrong_action(self):self.reject(lambda v:v['contracts'][1].update(source_action='update'))
 def test_method_indices_swapped(self):self.reject(lambda v:v['contracts'][0]['cases'][0].update(source_index=3))
 def test_duplicate_case(self):self.reject(lambda v:v['contracts'][0]['cases'].append(copy.deepcopy(v['contracts'][0]['cases'][0])))
 def test_missing_case(self):self.reject(lambda v:v['contracts'][1]['cases'].pop())
 def test_exact_captured_body(self):self.reject(lambda v:v['contracts'][0]['cases'][0]['actual_framework_observation'].update(body_hex='7b7d'))
 def test_exact_resolver_action(self):self.reject(lambda v:v['contracts'][0]['cases'][0]['actual_framework_observation']['resolver']['actions'].update(put='partial_update'))
 def test_exact_actor_network_override(self):self.reject(lambda v:v['contracts'][0]['source_role_specs']['adminDenied']['actual_actor'].update(custom_properties={}))
 def test_credential_bypass_not_native_auth(self):self.reject(lambda v:v['contracts'][0]['source_role_specs']['admin'].update(credential_backend_bypassed=False))
 def test_Source2_exit_not_Source1(self):self.reject(lambda v:v['contracts'][0]['result'].update(Source2_controller_exit=1))
 def test_Source1_negative_retained(self):self.reject(lambda v:v.update(Source1_negative_preserved=False))
 def test_no_new_Source_execution(self):self.reject(lambda v:v.update(new_Source_executions=1))
 def test_no_Native_coverage(self):self.reject(lambda v:v.update(Native_coverage_increased=True))
 def test_no_whole_endpoint(self):self.reject(lambda v:v['contracts'][0].update(fully_verified=True))
 def test_not_3362_suite(self):self.reject(lambda v:v.update(binding_kind='pinned_original_framework_tests'))
 def test_shared1449_not_inflated(self):self.reject(lambda v:v['contracts'][0]['result'].update(whole_fixture_shared_effect_checks=3362))
 def test_framework500_exception_not_hidden(self):self.reject(lambda v:v['contracts'][1]['cases'][-1]['actual_framework_observation']['framework_exception'].update(complete=False))
 def test_SQL_fields_not_normalized(self):self.reject(lambda v:v['contracts'][0]['cases'][-2]['actual_framework_observation']['after']['notifications'][0].update(updated_at='<clock>'))
 def test_cache_explicit_null_not_dropped(self):self.reject(lambda v:v['contracts'][0]['cases'][0]['actual_framework_observation']['before_condition_cache'].pop('D'))
 def test_nonHTTP_not_invented(self):self.reject(lambda v:v['non_HTTP_contracts'].append({'candidate_id':'invented'}))
 def test_register_preserves_all795_unselected_operations(self):
  old=json.loads((ROOT/'scripts/rust-contracts/notification-mutation-preservation.json').read_text());new=json.loads((ROOT/'docs/rust-contract-register.json').read_text());ops={o['contract_id']:o for r in new['runtime_routes']for o in r['operations']};selected=set(old['new_selected_contract_ids'])
  self.assertEqual(len(ops),798);self.assertEqual(len(old['unselected_operation_dicts']),795)
  for key,digest in old['unselected_operation_dicts'].items():self.assertEqual(subject.digest(ops[key]),digest,key)
  self.assertEqual(subject.digest([{k:v for k,v in r.items()if k!='operations'}for r in new['runtime_routes']]),old['routes_structural_projection_sha256'])
  self.assertEqual(subject.digest(new['openapi_operations']),old['openapi_operations_sha256']);self.assertEqual(subject.digest(new['non_HTTP_candidates']),old['non_HTTP_candidates_sha256']);self.assertEqual(subject.digest(new['observations_for_methods_not_declared_in_discovery']),old['observations_for_methods_not_declared_sha256'])
  self.assertEqual(new['executable_Source_non_HTTP_mapping'],old['non_HTTP_mapping']);self.assertEqual(new['executable_Source_mapping']['inputs'][:-1],old['existing_executable_Source_mapping_inputs']);self.assertEqual(new['executable_Source_mapping']['operations'],120)
  self.assertEqual(sum(o['method']is None for o in ops.values()),322);self.assertTrue(all(o['fully_verified']is False for o in ops.values()));self.assertEqual(sum('executable_Source_contract'in o for o in ops.values()),120)
  self.assertEqual({key for key,o in ops.items()if 'executable_Source_contract'in o and o['executable_Source_contract'].get('fixture_id')=='retained-Original-notification-mutations-Source2-48'},selected)
if __name__=='__main__':unittest.main()
