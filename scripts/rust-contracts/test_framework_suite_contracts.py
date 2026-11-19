"""Offline framework authority and exact additive preservation controls.
Original Dispatcharr authors retain all application and algorithm credit.
"""
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
import framework_suite_contracts as subject
import source_contracts
import retained_xc_refresh_contracts
ROOT=Path(__file__).resolve().parents[2]
class FrameworkSuiteControls(unittest.TestCase):
 def test_retained_actual_passes_and_framework_scope(self):
  value=json.loads((ROOT/subject.INPUT).read_bytes())
  self.assertEqual(subject.validate(ROOT,value),{'new_assignments':10,'request_sites':36,'historical_test_methods':34,'fully_verified_added':0})
  self.assertEqual(sum(c['method']is None for c in value['contracts']),2)
  self.assertTrue(all(c['observed_roles']==[]and c['source_role_specs']=={}for c in value['contracts']))
  self.assertTrue(all(not f['Source_HTTP_transport_proven']and not f['effective_actor_roles_qualified']for f in value['fixtures'].values()))
 def test_changed_action_role_pass_body_or_green_rejected(self):
  original=json.loads((ROOT/subject.INPUT).read_bytes())
  for alter in [lambda v:v['contracts'][0].update(source_action='invented'),lambda v:v['contracts'][0].update(observed_roles=['admin']),lambda v:v['contracts'][0]['cases'][0].update(actual_retained_test_log_block_sha256='0'*64),lambda v:v['contracts'][0]['cases'][0].update(response_status_assertions=['invented']),lambda v:v['contracts'][0]['result'].update(fully_verified=True),lambda v:v['contracts'][0].update(method='HEAD')]:
   v=copy.deepcopy(original);alter(v)
   with self.assertRaises(AssertionError):subject.validate(ROOT,v)
 def test_source_class_or_attestation_corruption_rejected(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory)
   for name in subject.dependencies():
    p=root/name;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/name,p)
   for name in ['selected-test-attestation.json','original/apps/channels/tests/test_recording_metadata.py']:
    p=root/subject.BASE/name;raw=p.read_bytes();p.write_bytes(raw+b'\n')
    with self.assertRaises(AssertionError):subject.generate(root)
    p.write_bytes(raw)
 def test_ten_additions_preserve_prior163_and788_whole_dictionaries(self):
  r=json.loads((ROOT/'docs/rust-contract-register.json').read_bytes())
  retained_dvr_xc_contracts.restore_pre_dvr_xc(ROOT,r['runtime_routes'])
  retained_admin_model_contracts.restore_pre_admin_models(ROOT,r['runtime_routes'])
  retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(ROOT,r['runtime_routes'])
  retained_image22_controls_contracts.restore_pre_image22_controls(ROOT,r['runtime_routes'])
  retained_playback_contracts.restore_pre_playback(ROOT,r['runtime_routes'])
  retained_hdhr_contracts.restore_pre_hdhr(ROOT,r['runtime_routes'])
  retained_provider_channel_vod_contracts.restore_pre_provider_channel_vod(ROOT,r['runtime_routes'])
  retained_serializer_null_contracts.restore_pre_serializer_null(ROOT,r['runtime_routes'])
  retained_core_read_contracts.restore_pre_core_reads(ROOT,r['runtime_routes'])
  retained_epg_reads_contracts.restore_pre_epg(ROOT,r['runtime_routes'])
  retained_xc_refresh_contracts.restore_pre_xc(ROOT,r['runtime_routes'])
  import retained_range_source10_contracts
  retained_range_source10_contracts.restore_pre_range(r['runtime_routes'])
  import retained_source12_contracts
  retained_source12_contracts.restore_pre_Source12(r['runtime_routes'])
  proof=json.loads((ROOT/subject.BASE/'preservation.json').read_bytes())
  rows=copy.deepcopy(r['runtime_routes'])
  current=copy.deepcopy(rows)
  ops={o['contract_id']:o for route in rows for o in route['operations']}
  selected=proof['selected_original_unassigned_dicts']
  self.assertEqual(len(ops),798);self.assertEqual(sum(bool(o.get('executable_Source_contract'))for o in ops.values()),173)
  self.assertEqual(subject.digest({k:v for k,v in ops.items()if k not in selected}),proof['unselected788_full_dicts_sha256'])
  subject.restore_pre_framework(rows);prior={o['contract_id']:o for route in rows for o in route['operations']}
  self.assertEqual(sum(bool(o.get('executable_Source_contract'))for o in prior.values()),163)
  self.assertEqual({k:prior[k]for k in selected},selected)
  self.assertEqual(subject.digest({k:v['executable_Source_contract']for k,v in prior.items()if v.get('executable_Source_contract')}),proof['prior163_definitions_sha256'])
  self.assertEqual(subject.digest([{k:v for k,v in route.items()if k!='operations'}for route in rows]),proof['all_routes_structure_sha256'])
  self.assertEqual(subject.digest(r['non_HTTP_candidates']),proof['non_HTTP_sha256'])
  self.assertEqual(subject.digest(r['openapi_operations']),proof['OpenAPI_sha256'])
  bindings=json.loads((ROOT/'scripts/rust-contracts/case-bindings.json').read_bytes())
  mapping=source_contracts.apply(ROOT,rows,bindings)
  self.assertEqual(mapping['operations'],373)
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
  retained_source12_contracts.restore_pre_Source12(rows)
  self.assertEqual(rows,current)
  source_contracts.apply(ROOT,rows,bindings)
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
  retained_source12_contracts.restore_pre_Source12(rows)
  self.assertEqual(rows,current)
  self.assertEqual(sum(o['method']is None for o in ops.values()),322);self.assertEqual(sum(o['method']is None and bool(o.get('executable_Source_contract'))for o in ops.values()),63)
  self.assertTrue(all(not o['fully_verified']for o in ops.values()))
if __name__=='__main__':unittest.main()
