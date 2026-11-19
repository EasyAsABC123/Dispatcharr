"""Source12 retained authority and exact additive preservation; Original authors retain credit."""
import copy,json,shutil,tempfile,unittest
from pathlib import Path
import retained_epg_reads_contracts
import retained_core_read_contracts
import retained_serializer_null_contracts
import retained_provider_channel_vod_contracts
import retained_hdhr_contracts
import retained_playback_contracts
import retained_image22_controls_contracts
import retained_repository_socket_switch_contracts
import retained_admin_model_contracts
import retained_dvr_xc_contracts
import retained_connect_contracts
import retained_connect_actions_contracts
import retained_alias19_contracts
import retained_connect_detail_contracts
import retained_alias_remainder_contracts
import retained_connect_test_contracts
import retained_set_epg_selected_contracts
import retained_source12_contracts as subject
import source_contracts
import retained_xc_refresh_contracts
ROOT=Path(__file__).resolve().parents[2]
class RetainedSource12Controls(unittest.TestCase):
 def test_source12_exact_observations_and_unresolved_method(self):
  value=json.loads((ROOT/subject.INPUT).read_bytes());self.assertEqual(subject.validate(ROOT,value),{'new_assignments':1,'supplement_existing':1,'Source_observations':12,'fully_verified_added':0});self.assertEqual([len(c['cases'])for c in value['contracts']],[9,3]);self.assertIsNone(value['contracts'][1]['method']);self.assertIsNone(value['contracts'][1]['source_action']);self.assertTrue(all(c['observed_roles']==[]and c['source_role_specs']=={}for c in value['contracts']))
 def test_invented_role_method_response_or_green_rejected(self):
  original=json.loads((ROOT/subject.INPUT).read_bytes())
  for alter in [lambda v:v['contracts'][0].update(observed_roles=['admin']),lambda v:v['contracts'][1].update(method='GET'),lambda v:v['contracts'][0]['cases'][0].update(response_body_sha256='0'*64),lambda v:v['contracts'][0]['result'].update(fully_verified=True),lambda v:v.update(Source_observations=11)]:
   value=copy.deepcopy(original);alter(value)
   with self.assertRaises(AssertionError):subject.validate(ROOT,value)
 def test_corrupt_actual_terminal_denial_resolver_or_first_failure_rejected(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory)
   for n in subject.dependencies():
    p=root/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/n,p)
   for name,alter in [('resolver/UID-report.json',lambda v:v['actual_guard_counts'].update(network_attempts=0)),('resolver/observations.json',lambda v:v['observations'][0].update(actual_resolver_ordinal=158)),('resolver/Root-terminal.json',lambda v:v.update(actual_exit=1)),('first-failure/UID-report.json',lambda v:v.update(error_type='None'))]:
    p=root/subject.BASE/name;raw=p.read_bytes();v=json.loads(raw);alter(v);p.write_text(json.dumps(v))
    with self.assertRaises(AssertionError):subject.generate(root)
    p.write_bytes(raw)
 def test_previous173_and796_whole_rows_and_M3U_primary_preserved(self):
  register=json.loads((ROOT/'docs/rust-contract-register.json').read_bytes())
  retained_set_epg_selected_contracts.restore_pre_epg_assignments(ROOT,register['runtime_routes'])
  retained_connect_test_contracts.restore_pre_connect_test(ROOT,register['runtime_routes'])
  retained_alias_remainder_contracts.restore_pre_alias_remainder(ROOT,register['runtime_routes'])
  retained_connect_detail_contracts.restore_pre_connect_detail(ROOT,register['runtime_routes'])
  retained_alias19_contracts.restore_pre_alias19(ROOT,register['runtime_routes'])
  retained_connect_actions_contracts.restore_pre_connect_actions(ROOT,register['runtime_routes'])
  retained_connect_contracts.restore_pre_connect(ROOT,register['runtime_routes'])
  retained_dvr_xc_contracts.restore_pre_dvr_xc(ROOT,register['runtime_routes'])
  retained_admin_model_contracts.restore_pre_admin_models(ROOT,register['runtime_routes'])
  retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(ROOT,register['runtime_routes'])
  retained_image22_controls_contracts.restore_pre_image22_controls(ROOT,register['runtime_routes'])
  retained_playback_contracts.restore_pre_playback(ROOT,register['runtime_routes'])
  retained_hdhr_contracts.restore_pre_hdhr(ROOT,register['runtime_routes'])
  retained_provider_channel_vod_contracts.restore_pre_provider_channel_vod(ROOT,register['runtime_routes'])
  retained_serializer_null_contracts.restore_pre_serializer_null(ROOT,register['runtime_routes'])
  retained_core_read_contracts.restore_pre_core_reads(ROOT,register['runtime_routes'])
  retained_epg_reads_contracts.restore_pre_epg(ROOT,register['runtime_routes'])
  retained_xc_refresh_contracts.restore_pre_xc(ROOT,register['runtime_routes'])
  import retained_range_source10_contracts
  retained_range_source10_contracts.restore_pre_range(register['runtime_routes'])
  proof=json.loads((ROOT/subject.BASE/'preservation.json').read_bytes())
  rows=copy.deepcopy(register['runtime_routes'])
  current=copy.deepcopy(rows)
  ops={o['contract_id']:o for r in rows for o in r['operations']}
  new=proof['new_selected_original_unassigned_dict']['contract_id']
  old='route-56d9c31dd9fc91261e25:METHOD_UNRESOLVED'
  self.assertEqual(sum(bool(o.get('executable_Source_contract'))for o in ops.values()),174);self.assertEqual(subject.digest({k:v for k,v in ops.items()if k not in {new,old}}),proof['all_unselected796_whole_dicts_sha256'])
  subject.restore_pre_Source12(rows);prior={o['contract_id']:o for r in rows for o in r['operations']};self.assertEqual(prior[new],proof['new_selected_original_unassigned_dict']);self.assertEqual(subject.digest(prior[old]),proof['existing_supplement_target_whole_dict_sha256']);self.assertEqual(subject.digest({k:o['executable_Source_contract']for k,o in prior.items()if o.get('executable_Source_contract')}),proof['all_prior173_definitions_sha256']);self.assertEqual(subject.digest([{k:v for k,v in r.items()if k!='operations'}for r in rows]),proof['route_structure_sha256'])
  import retained_comskip_task_contracts
  import retained_clocked_recording_contracts
  retained_clocked_recording_contracts.restore_pre_clocked_recording(ROOT,register['non_HTTP_candidates'])
  retained_comskip_task_contracts.restore_pre_comskip_task(ROOT,register['non_HTTP_candidates'])
  for field,name in [('non_HTTP_candidates','non_HTTP_sha256'),('openapi_operations','OpenAPI_sha256'),('observations_for_methods_not_declared_in_discovery','legacy_undeclared_observations_sha256')]:self.assertEqual(subject.digest(register[field]),proof[name])
  bindings=json.loads((ROOT/'scripts/rust-contracts/case-bindings.json').read_bytes())
  mapping=source_contracts.apply(ROOT,rows,bindings)
  self.assertEqual(mapping['operations'],470)
  retained_set_epg_selected_contracts.restore_pre_epg_assignments(ROOT,rows)
  retained_connect_test_contracts.restore_pre_connect_test(ROOT,rows)
  retained_alias_remainder_contracts.restore_pre_alias_remainder(ROOT,rows)
  retained_connect_detail_contracts.restore_pre_connect_detail(ROOT,rows)
  retained_alias19_contracts.restore_pre_alias19(ROOT,rows)
  retained_connect_actions_contracts.restore_pre_connect_actions(ROOT,rows)
  retained_connect_contracts.restore_pre_connect(ROOT,rows)
  retained_dvr_xc_contracts.restore_pre_dvr_xc(ROOT,rows)
  retained_admin_model_contracts.restore_pre_admin_models(ROOT,rows)
  retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(ROOT,rows)
  retained_image22_controls_contracts.restore_pre_image22_controls(ROOT,rows)
  retained_playback_contracts.restore_pre_playback(ROOT,rows)
  retained_hdhr_contracts.restore_pre_hdhr(ROOT,rows)
  retained_provider_channel_vod_contracts.restore_pre_provider_channel_vod(ROOT,rows)
  retained_serializer_null_contracts.restore_pre_serializer_null(ROOT,rows)
  retained_core_read_contracts.restore_pre_core_reads(ROOT,rows)
  retained_epg_reads_contracts.restore_pre_epg(ROOT,rows)
  retained_xc_refresh_contracts.restore_pre_xc(ROOT,rows)
  retained_range_source10_contracts.restore_pre_range(rows)
  self.assertEqual(rows,current)
  source_contracts.apply(ROOT,rows,bindings)
  retained_set_epg_selected_contracts.restore_pre_epg_assignments(ROOT,rows)
  retained_connect_test_contracts.restore_pre_connect_test(ROOT,rows)
  retained_alias_remainder_contracts.restore_pre_alias_remainder(ROOT,rows)
  retained_connect_detail_contracts.restore_pre_connect_detail(ROOT,rows)
  retained_alias19_contracts.restore_pre_alias19(ROOT,rows)
  retained_connect_actions_contracts.restore_pre_connect_actions(ROOT,rows)
  retained_connect_contracts.restore_pre_connect(ROOT,rows)
  retained_dvr_xc_contracts.restore_pre_dvr_xc(ROOT,rows)
  retained_admin_model_contracts.restore_pre_admin_models(ROOT,rows)
  retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(ROOT,rows)
  retained_image22_controls_contracts.restore_pre_image22_controls(ROOT,rows)
  retained_playback_contracts.restore_pre_playback(ROOT,rows)
  retained_hdhr_contracts.restore_pre_hdhr(ROOT,rows)
  retained_provider_channel_vod_contracts.restore_pre_provider_channel_vod(ROOT,rows)
  retained_serializer_null_contracts.restore_pre_serializer_null(ROOT,rows)
  retained_core_read_contracts.restore_pre_core_reads(ROOT,rows)
  retained_epg_reads_contracts.restore_pre_epg(ROOT,rows)
  retained_xc_refresh_contracts.restore_pre_xc(ROOT,rows)
  retained_range_source10_contracts.restore_pre_range(rows)
  self.assertEqual(rows,current)
  self.assertEqual(sum(o['method']is None for o in ops.values()),322)
  self.assertEqual(sum(o['method']is None and bool(o.get('executable_Source_contract'))for o in ops.values()),63)
  self.assertTrue(all(not o['fully_verified']for o in ops.values()))
if __name__=='__main__':unittest.main()
