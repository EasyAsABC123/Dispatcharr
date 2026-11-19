"""Range Source10 supplemental custody/preservation; Original authors retain credit."""
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
import retained_range_source10_contracts as subject
import source_contracts
import retained_xc_refresh_contracts
ROOT=Path(__file__).resolve().parents[2]
class RangeSource10Controls(unittest.TestCase):
 def test_capture_pending_artifact_and_separate_behavior_remain_distinct(self):
  value=json.loads((ROOT/subject.INPUT).read_bytes());self.assertEqual(subject.validate(ROOT,value),{'new_assignments':0,'supplement_existing':1,'Source_observations':10,'fully_verified_added':0});c,=value['contracts'];self.assertEqual(c['contract_id'],'route-ef627283fb23a277c00e:GET');self.assertEqual(c['resolver_ordinal'],89);self.assertEqual(c['observed_roles'],[]);self.assertTrue(all(r['raw_capture_artifact_behavior_pending_flag_preserved']and r['separate_behavior_review_bound']for r in c['cases']))
 def test_invented_alias_role_response_or_fullgreen_rejected(self):
  original=json.loads((ROOT/subject.INPUT).read_bytes())
  for alter in [lambda v:v.update(new_assignments=1),lambda v:v['contracts'][0].update(resolver_ordinal=90),lambda v:v['contracts'][0].update(observed_roles=['admin']),lambda v:v['contracts'][0]['cases'][0].update(body_sha256='0'*64),lambda v:v['contracts'][0]['result'].update(fully_verified=True)]:
   value=copy.deepcopy(original);alter(value)
   with self.assertRaises(AssertionError):subject.validate(ROOT,value)
 def test_raw_capture_behavior_resolver_firstfailure_or_denial_corruption_rejected(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory)
   for n in subject.dependencies():
    p=root/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/n,p)
   for name,alter in [('capture-qualification.json',lambda v:v.update(status='BUSINESS_GREEN')),('behavior-review.json',lambda v:v.update(Source_HTTP_observed_and_reviewed=0)),('resolver/UID-report.json',lambda v:v['actual_guard_counts'].update(network_attempts=0)),('resolver/observations.json',lambda v:v['observations'][0].update(actual_resolver_ordinal=90)),('first-failure/UID-report.json',lambda v:v.update(passed=True))]:
    p=root/subject.BASE/name;raw=p.read_bytes();v=json.loads(raw);alter(v);p.write_text(json.dumps(v))
    with self.assertRaises(AssertionError):subject.generate(root)
    p.write_bytes(raw)
 def test_all174_definitions_797_whole_rows_and_selected_primary_unchanged(self):
  register=json.loads((ROOT/'docs/rust-contract-register.json').read_bytes())
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
  proof=json.loads((ROOT/subject.BASE/'preservation.json').read_bytes())
  rows=copy.deepcopy(register['runtime_routes'])
  current=copy.deepcopy(rows)
  ops={o['contract_id']:o for r in rows for o in r['operations']}
  key=proof['selected_id']
  self.assertEqual(sum(bool(o.get('executable_Source_contract'))for o in ops.values()),174)
  self.assertEqual(subject.digest({k:o for k,o in ops.items()if k!=key}),proof['all_unselected797_whole_operations_sha256'])
  self.assertEqual(subject.digest({k:o['executable_Source_contract']for k,o in ops.items()if o.get('executable_Source_contract')}),proof['all_prior174_definitions_sha256'])
  subject.restore_pre_range(rows);prior={o['contract_id']:o for r in rows for o in r['operations']};self.assertEqual(subject.digest(prior[key]),proof['selected_original_whole_operation_sha256']);self.assertEqual(subject.digest([{k:v for k,v in r.items()if k!='operations'}for r in rows]),proof['route_structure_sha256'])
  import retained_comskip_task_contracts
  import retained_clocked_recording_contracts
  retained_clocked_recording_contracts.restore_pre_clocked_recording(ROOT,register['non_HTTP_candidates'])
  retained_comskip_task_contracts.restore_pre_comskip_task(ROOT,register['non_HTTP_candidates'])
  for field,name in [('non_HTTP_candidates','non_HTTP_sha256'),('openapi_operations','OpenAPI_sha256'),('observations_for_methods_not_declared_in_discovery','legacy_undeclared_observations_sha256')]:self.assertEqual(subject.digest(register[field]),proof[name])
  bindings=json.loads((ROOT/'scripts/rust-contracts/case-bindings.json').read_bytes())
  mapping=source_contracts.apply(ROOT,rows,bindings)
  self.assertEqual(mapping['operations'],467)
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
  self.assertEqual(rows,current)
  source_contracts.apply(ROOT,rows,bindings)
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
  self.assertEqual(rows,current)
  self.assertEqual(sum(o['method']is None for o in ops.values()),322)
  self.assertEqual(sum(o['method']is None and bool(o.get('executable_Source_contract'))for o in ops.values()),63)
  self.assertTrue(all(not o['fully_verified']for o in ops.values()))
if __name__=='__main__':unittest.main()
