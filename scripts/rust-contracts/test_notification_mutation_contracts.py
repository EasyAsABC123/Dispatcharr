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
 def assert_preserved_register(self,new):
  import source_contracts
  old=json.loads((ROOT/'scripts/rust-contracts/notification-mutation-preservation.json').read_text())
  ops={o['contract_id']:o for r in new['runtime_routes']for o in r['operations']}
  selected=set(old['new_selected_contract_ids'])
  self.assertEqual(len(ops),798);self.assertEqual(len(old['unselected_operation_dicts']),795)
  inputs=new['executable_Source_mapping']['inputs'];prior=old['existing_executable_Source_mapping_inputs']
  self.assertEqual(inputs[:len(prior)],prior)
  self.assertEqual(inputs[len(prior)]['kind'],subject.KIND)
  # The receipt is immutable. Later schema/credential/VOD/logo/matching/station
  # assignments are validated by their existing exact public generators below.
  # Reverse ONLY their Source assignment fields to the register.py defaults,
  # then require every original row SHA. Identity, required roles, unrelated
  # evidence and all other fields retain the historical protection.
  later=inputs[len(prior)+1:]
  self.assertEqual([r['path']for r in later],[
   'scripts/rust-contracts/schema-cache/bindings.json',
   'scripts/rust-contracts/notification-credential-bindings.json',
   'scripts/rust-contracts/vod-access/bindings.json',
   'scripts/rust-contracts/logo-source/bindings.json',
   'scripts/rust-contracts/channel-matching-source51-bindings.json',
   'scripts/rust-contracts/station-aggregate-source53-55-bindings.json',
   'scripts/rust-contracts/retained-task-operator7/bindings.json',
   'scripts/rust-contracts/retained-management/bindings.json',
   'scripts/rust-contracts/core-settings-source32-assignments/bindings.json',
   'scripts/rust-contracts/retained-auth-plugin/bindings.json',
   'scripts/rust-contracts/retained-sd/bindings.json',
   'scripts/rust-contracts/framework-suite/bindings.json',
   'scripts/rust-contracts/retained-source12/bindings.json',
   'scripts/rust-contracts/retained-range-source10/bindings.json',
   'scripts/rust-contracts/retained-xc-refresh-source11/bindings.json',
   'scripts/rust-contracts/retained-epg-reads/bindings.json',
   'scripts/rust-contracts/retained-core-read/bindings.json',
   'scripts/rust-contracts/retained-serializer-null/bindings.json',
   'scripts/rust-contracts/retained-provider-channel-vod/bindings.json',
   'scripts/rust-contracts/retained-hdhr/bindings.json',
   'scripts/rust-contracts/retained-playback/bindings.json',
   'scripts/rust-contracts/retained-image22-controls/bindings.json',
   'scripts/rust-contracts/retained-repository-socket-switch/bindings.json',
   'scripts/rust-contracts/retained-admin-models/bindings.json',
   'scripts/rust-contracts/retained-dvr-xc/bindings.json',
   'scripts/rust-contracts/retained-connect/bindings.json',
   'scripts/rust-contracts/retained-connect-actions/bindings.json',
   'scripts/rust-contracts/retained-alias19/bindings.json',
   'scripts/rust-contracts/retained-connect-detail/bindings.json',
   'scripts/rust-contracts/retained-comskip-task/bindings.json',
   'scripts/rust-contracts/retained-clocked-recording/bindings.json',
   'scripts/rust-contracts/retained-alias-remainder/bindings.json',
   'scripts/rust-contracts/retained-connect-test/bindings.json',
   'scripts/rust-contracts/retained-set-epg-selected/bindings.json',
   'scripts/rust-contracts/retained-set-epg-batch/bindings.json',
   'scripts/rust-contracts/retained-paired-reads/bindings.json'])
  later_ids=[];supplemental_ids=[]
  import retained_management_contracts
  import retained_auth_plugin_contracts
  import retained_xc_refresh_contracts
  import retained_alias_remainder_contracts
  import retained_connect_test_contracts
  import retained_set_epg_selected_contracts
  import retained_paired_read_contracts
  for ref in later:
   value=source_contracts.checked(ROOT,ref)
   if ref['kind']==retained_management_contracts.KIND:
    retained_management_contracts.validate(ROOT,value)
    for contract in value['contracts']:
     identifier=contract['contract_id']
     if ops[identifier]['executable_Source_contract']['fixture_id'].startswith('retained:'):
      later_ids.append(identifier)
     else:supplemental_ids.append(identifier)
   elif ref['kind']==retained_auth_plugin_contracts.KIND:
    retained_auth_plugin_contracts.validate(ROOT,value)
    for contract in value['contracts']:
     identifier=contract['contract_id']
     if ops[identifier]['executable_Source_contract']['fixture_id'].startswith('retained-auth-plugin:'):
      later_ids.append(identifier)
     else:supplemental_ids.append(identifier)
   elif ref['kind']=='retained_original_Source12_search_M3U_resolver_HTTP':
    import retained_source12_contracts
    retained_source12_contracts.validate(ROOT,value)
    for contract in value['contracts']:
     if contract['method']is None:supplemental_ids.append(contract['contract_id'])
     else:later_ids.append(contract['contract_id'])
   elif ref['kind']=='retained_original_range_Source10_resolver_HTTP':
    import retained_range_source10_contracts
    retained_range_source10_contracts.validate(ROOT,value)
    supplemental_ids.extend(c['contract_id']for c in value['contracts'])
   elif ref['kind']==retained_xc_refresh_contracts.KIND:
    retained_xc_refresh_contracts.validate(ROOT,value)
   elif ref['kind']==retained_alias_remainder_contracts.KIND:
    retained_alias_remainder_contracts.validate(ROOT,value)
   elif ref['kind']==retained_connect_test_contracts.KIND:
    retained_connect_test_contracts.validate(ROOT,value)
   elif ref['kind']==retained_set_epg_selected_contracts.KIND:
    retained_set_epg_selected_contracts.validate(ROOT,value)
   elif ref['kind']==retained_set_epg_selected_contracts.BATCH_KIND:
    retained_set_epg_selected_contracts.validate_batch(ROOT,value)
   elif ref['kind']==retained_paired_read_contracts.KIND:
    retained_paired_read_contracts.validate(ROOT,value)
   else:later_ids.extend(c['contract_id']for c in value['contracts'])
  self.assertEqual(len(later_ids),282);self.assertEqual(len(set(later_ids)),282)
  self.assertEqual(len(supplemental_ids),11)
  self.assertFalse(selected.intersection(later_ids))
  historical=copy.deepcopy(new['runtime_routes'])
  retained_set_epg_selected_contracts.restore_pre_epg_assignments(ROOT,historical)
  retained_paired_read_contracts.restore_pre_paired_reads(ROOT,historical)
  retained_connect_test_contracts.restore_pre_connect_test(ROOT,historical)
  retained_alias_remainder_contracts.restore_pre_alias_remainder(ROOT,historical)
  retained_xc_refresh_contracts.restore_pre_xc(ROOT,historical)
  historical_ops={o['contract_id']:o for r in historical for o in r['operations']}
  for key in supplemental_ids:
   # Reverse only the new cohort's separate field. Original Source fields
   # still have to satisfy the immutable historical full-row digest.
   captures=historical_ops[key].pop('supplemental_executable_Source_captures')
   self.assertEqual(len(captures),1)
   self.assertIn(captures[0]['input']['kind'],[retained_management_contracts.KIND,retained_auth_plugin_contracts.KIND,retained_source12_contracts.KIND,retained_range_source10_contracts.KIND])
  for key in later_ids:
   operation=historical_ops[key]
   operation.pop('executable_Source_contract')
   operation.update(setup_fixture=None,assertions=[],result=None,
    coverage_state='scoped_evidence_needs_case_mapping'if operation['related_evidence']else'not_yet_mapped')
   for field in('observed_fixture_definitions','complete','observed_roles'):
    operation['role_matrix'].pop(field,None)
   operation['role_matrix']['state']='unmapped'
  for key,digest in old['unselected_operation_dicts'].items():
   self.assertEqual(subject.digest(historical_ops[key]),digest,key)
  # Exact reapplication validates each retained Source authority, not merely its
  # kind/ID, and checks every regenerated assignment field against the full row.
  bindings=json.loads((ROOT/'scripts/rust-contracts/case-bindings.json').read_text())
  mapping=source_contracts.apply(ROOT,historical,bindings)
  self.assertEqual(historical,new['runtime_routes'])
  self.assertEqual(mapping,new['executable_Source_mapping'])
  self.assertEqual(subject.digest([{k:v for k,v in r.items()if k!='operations'}for r in new['runtime_routes']]),old['routes_structural_projection_sha256'])
  self.assertEqual(subject.digest(new['openapi_operations']),old['openapi_operations_sha256']);self.assertEqual(subject.digest(new['observations_for_methods_not_declared_in_discovery']),old['observations_for_methods_not_declared_sha256'])
  # Only seven exact validator-pinned Source assignments supersede this old
  # non-HTTP digest. Reverse their known assignment fields, preserve every
  # source/identity/unrelated field, then require the immutable full old SHA.
  import retained_task_operator_contracts
  reference=next(r for r in later if r['kind']==retained_task_operator_contracts.KIND)
  retained=source_contracts.checked(ROOT,reference)
  retained_task_operator_contracts.validate(ROOT,retained)
  retained_ids={row['candidate_id']for row in retained['non_HTTP_contracts']}
  self.assertEqual(retained_ids,{row['candidate_id']for row in retained_task_operator_contracts.IDENTITIES})
  self.assertEqual(len(retained_ids),7)
  historical_non_HTTP=copy.deepcopy(new['non_HTTP_candidates'])
  import retained_comskip_task_contracts
  import retained_clocked_recording_contracts
  retained_clocked_recording_contracts.restore_pre_clocked_recording(ROOT,historical_non_HTTP)
  retained_comskip_task_contracts.restore_pre_comskip_task(ROOT,historical_non_HTTP)
  self.assertEqual(sum(row['candidate_id']not in retained_ids for row in historical_non_HTTP),243)
  for row in historical_non_HTTP:
   if row['candidate_id']in retained_ids:
    for field in('setup_fixture','owning_milestone','assertions','executable_Source_contract'):
     row.pop(field)
    row.update(state='static_candidate_requires_runtime_contract',result=None)
  self.assertEqual(subject.digest(historical_non_HTTP),old['non_HTTP_candidates_sha256'])
  # Reapply actual checked public Source data and require every new field too;
  # dropping arbitrary fields or accepting a changed seven-row oracle cannot pass.
  non_HTTP_mapping=source_contracts.apply_non_HTTP(ROOT,historical_non_HTTP)
  self.assertEqual(historical_non_HTTP,new['non_HTTP_candidates'])
  self.assertEqual(non_HTTP_mapping,new['executable_Source_non_HTTP_mapping'])
  self.assertEqual(non_HTTP_mapping['candidates'],old['non_HTTP_mapping']['candidates']+10)
  self.assertEqual(non_HTTP_mapping['fully_verified_added'],0)
  expected=old['base_HTTP_assignments']+len(selected)+len(later_ids)
  self.assertEqual(expected,402) # pre-XC preservation count
  expected += 4 + sum(row['binding']=='primary' for row in
                      retained_alias_remainder_contracts.load(ROOT)['mapping_decisions'].values())
  expected += retained_paired_read_contracts.load(ROOT)['primary_count']
  self.assertEqual(mapping['operations'],expected)
  self.assertEqual(sum(o['method']is None for o in ops.values()),322);self.assertTrue(all(o['fully_verified']is False for o in ops.values()));self.assertEqual(sum('executable_Source_contract'in o for o in ops.values()),expected)
  self.assertEqual({key for key,o in ops.items()if 'executable_Source_contract'in o and o['executable_Source_contract'].get('fixture_id')=='retained-Original-notification-mutations-Source2-48'},selected)
 def test_prior_rows_preserved_except_exact_validated_later_assignments(self):
  self.assert_preserved_register(json.loads((ROOT/'docs/rust-contract-register.json').read_text()))
 def test_later_assignment_cannot_change_historical_identity(self):
  value=json.loads((ROOT/'docs/rust-contract-register.json').read_text())
  operation=next(o for r in value['runtime_routes']for o in r['operations']if o['contract_id']=='route-a53ed9176d67c2be1915:GET')
  operation['source_action']='invented'
  with self.assertRaises(AssertionError):self.assert_preserved_register(value)
 def test_later_assignment_cannot_change_historical_required_roles(self):
  value=json.loads((ROOT/'docs/rust-contract-register.json').read_text())
  operation=next(o for r in value['runtime_routes']for o in r['operations']if o['contract_id']=='route-a53ed9176d67c2be1915:GET')
  operation['role_matrix']['required_roles']=['invented-required-role']
  with self.assertRaises(AssertionError):self.assert_preserved_register(value)
 def test_later_assignment_requires_exact_Source_fields(self):
  value=json.loads((ROOT/'docs/rust-contract-register.json').read_text())
  operation=next(o for r in value['runtime_routes']for o in r['operations']if o['contract_id']=='route-a53ed9176d67c2be1915:GET')
  operation['setup_fixture']['invented_success']=True
  with self.assertRaises(AssertionError):self.assert_preserved_register(value)
if __name__=='__main__':unittest.main()
