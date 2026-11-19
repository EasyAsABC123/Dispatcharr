"""Retained auth/plugin route-custody, nonzero denial and preservation controls.
Original Dispatcharr, Django/DRF and urllib3 authors retain their full credit.
"""
import copy
import json
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
import retained_paired_read_contracts
import retained_live_stop_contracts
import retained_ts_controls_contracts
import retained_canonical_management_contracts
import retained_exact_vod_contracts
import retained_image57_accounts_contracts
import retained_image57_m3u_contracts
import retained_comskip_task_contracts
import retained_clocked_recording_contracts
import unittest
import retained_auth_plugin_contracts as subject
import source_contracts

ROOT = Path(__file__).resolve().parents[2]


class RetainedAuthPluginControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = json.loads((ROOT / subject.INPUT).read_bytes())

    def test_exact_separate_component_keeps_raw_failed_exit_and_nonzero_attempt(self):
        component, resolutions, routes = subject.qualify_resolver(ROOT)
        self.assertTrue(component['qualified_route_binding_component'])
        self.assertEqual(component['actual_original_UID_controller_exit'], 1)
        self.assertIs(component['actual_original_raw_passed'], False)
        self.assertIs(component['actual_original_UID_scope_qualified'], False)
        self.assertEqual(component['actual_guard_counts']['network_attempts'], 1)
        self.assertEqual(component['actual_denied_import_probe']['stage'], 'django_setup')
        self.assertIs(component['actual_denied_import_probe']['original_operation_executed'], False)
        self.assertEqual((len(resolutions), len(routes)), (86, 592))

    def test_wrong_phase_zeroed_counter_green_controller_alias_or_case_rejected(self):
        mutations = [lambda value: value['resolver_component_qualification'].update(actual_original_UID_controller_exit=0),
                     lambda value: value['resolver_component_qualification'].update(actual_original_raw_passed=True),
                     lambda value: value['resolver_component_qualification']['actual_guard_counts'].update(network_attempts=0),
                     lambda value: value['resolver_component_qualification']['actual_denied_import_probe'].update(stage='resolver_forest_load'),
                     lambda value: value['contracts'][0].update(pattern_exact='api/accounts/auth/login.json/'),
                     lambda value: value['contracts'][0]['cases'][0].update(role='invented-role'),
                     lambda value: value['contracts'][0]['cases'][0].update(response_body_sha256='0' * 64),
                     lambda value: value['contracts'][0]['result'].update(Native_coverage_increased=True),
                     lambda value: value['unmapped_method_Source_observations'][0].update(contract_id='invented:GET')]
        for index, mutate in enumerate(mutations):
            value = copy.deepcopy(self.value)
            mutate(value)
            with self.subTest(index=index), self.assertRaises(AssertionError):
                subject.validate(ROOT, value)

    def test_declared_methods_and_fourteen_refusals_stay_separate(self):
        subject.validate(ROOT, self.value)
        self.assertEqual(len(self.value['contracts']), 13)
        self.assertEqual(sum(len(row['cases']) for row in self.value['contracts']), 72)
        side = self.value['unmapped_method_Source_observations']
        self.assertEqual(len(side), 14)
        self.assertTrue(all(row['contract_id'] is None and not row['declared_method_set_expanded'] for row in side))
        self.assertTrue(all(not row['fully_verified'] for row in self.value['contracts']))
        observed = {(case['source_group'], case['Source_index']) for row in self.value['contracts'] for case in row['cases']}
        indirect = {(row['observation']['source_group'], row['observation']['Source_index']) for row in side}
        self.assertFalse(observed & indirect)
        self.assertEqual(len(observed | indirect), 86)

    def test_nine_new_four_supplemental_and_all_prior_fields_preserved(self):
        proof = json.loads((ROOT / 'scripts/rust-contracts/retained-auth-plugin/preservation.json').read_bytes())
        register = json.loads((ROOT / 'docs/rust-contract-register.json').read_bytes())
        current_rows = copy.deepcopy(register['runtime_routes'])
        import retained_xc_refresh_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(ROOT, register['runtime_routes'])
        retained_paired_read_contracts.restore_pre_paired_reads(ROOT, register['runtime_routes'])
        retained_live_stop_contracts.restore_pre_live_stop(ROOT, register['runtime_routes'])
        retained_ts_controls_contracts.restore_pre_ts_controls(ROOT, register['runtime_routes'])
        retained_canonical_management_contracts.restore_pre_management(ROOT, register['runtime_routes'])
        retained_exact_vod_contracts.restore_pre_vod_split(ROOT, register['runtime_routes'])
        retained_image57_accounts_contracts.restore_pre_image57_accounts(ROOT, register['runtime_routes'])
        retained_image57_m3u_contracts.restore_pre_image57_m3u(ROOT, register['runtime_routes'])
        retained_connect_test_contracts.restore_pre_connect_test(ROOT, register['runtime_routes'])
        retained_alias_remainder_contracts.restore_pre_alias_remainder(ROOT, register['runtime_routes'])
        retained_connect_detail_contracts.restore_pre_connect_detail(ROOT, register['runtime_routes'])
        retained_alias19_contracts.restore_pre_alias19(ROOT, register['runtime_routes'])
        retained_connect_actions_contracts.restore_pre_connect_actions(ROOT, register['runtime_routes'])
        retained_connect_contracts.restore_pre_connect(ROOT, register['runtime_routes'])
        retained_dvr_xc_contracts.restore_pre_dvr_xc(ROOT, register['runtime_routes'])
        retained_admin_model_contracts.restore_pre_admin_models(ROOT, register['runtime_routes'])
        retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(ROOT, register['runtime_routes'])
        retained_image22_controls_contracts.restore_pre_image22_controls(ROOT, register['runtime_routes'])
        retained_playback_contracts.restore_pre_playback(ROOT, register['runtime_routes'])
        retained_hdhr_contracts.restore_pre_hdhr(ROOT, register['runtime_routes'])
        retained_provider_channel_vod_contracts.restore_pre_provider_channel_vod(ROOT, register['runtime_routes'])
        retained_serializer_null_contracts.restore_pre_serializer_null(ROOT, register['runtime_routes'])
        retained_core_read_contracts.restore_pre_core_reads(ROOT, register['runtime_routes'])
        retained_epg_reads_contracts.restore_pre_epg(ROOT, register['runtime_routes'])
        retained_xc_refresh_contracts.restore_pre_xc(ROOT, register['runtime_routes'])
        import retained_range_source10_contracts
        retained_range_source10_contracts.restore_pre_range(register['runtime_routes'])
        import retained_source12_contracts
        retained_source12_contracts.restore_pre_Source12(register['runtime_routes'])
        import framework_suite_contracts
        framework_suite_contracts.restore_pre_framework(register['runtime_routes'])
        import retained_sd_contracts
        retained_sd_contracts.restore_pre_SD(register['runtime_routes'])
        operations = {o['contract_id']: o for r in register['runtime_routes'] for o in r['operations']}
        selected = set(proof['selected_contract_ids'])
        self.assertEqual(len(operations), 798)
        self.assertEqual(len(proof['new_contract_ids']), 9)
        self.assertEqual(len(proof['existing_supplemental_target_ids']), 4)
        self.assertEqual(subject.digest({k: v for k, v in operations.items() if k not in selected}), proof['unselected_full_operation_dicts_sha256'])
        for identifier in proof['existing_supplemental_target_ids']:
            operation = copy.deepcopy(operations[identifier])
            additions = operation.pop('supplemental_executable_Source_captures')
            self.assertEqual(len(additions), 1)
            self.assertEqual(additions[0]['input']['kind'], subject.KIND)
            self.assertEqual(subject.digest(operation), proof['selected_existing_primary_dict_sha256'][identifier])
        self.assertEqual(subject.digest([{k: v for k, v in row.items() if k != 'operations'} for row in register['runtime_routes']]), proof['route_structural_sha256'])
        retained_clocked_recording_contracts.restore_pre_clocked_recording(ROOT, register['non_HTTP_candidates'])
        retained_comskip_task_contracts.restore_pre_comskip_task(ROOT, register['non_HTTP_candidates'])
        for key, field in [('non_HTTP_candidates', 'non_HTTP_candidates_sha256'), ('openapi_operations', 'OpenAPI_operations_sha256'), ('observations_for_methods_not_declared_in_discovery', 'legacy_undeclared_method_observations_sha256')]:
            self.assertEqual(subject.digest(register[key]), proof[field])
        rows = copy.deepcopy(current_rows)
        mapping = source_contracts.apply(ROOT, rows, json.loads((ROOT / 'scripts/rust-contracts/case-bindings.json').read_bytes()))
        self.assertEqual(rows, current_rows)
        self.assertEqual([row for row in mapping['unmapped_method_Source_observations'] if row['route_id'] != 'route-7e6941c073eccaa2b6dd' and row.get('fixture_id') != retained_epg_reads_contracts.FIXTURE], self.value['unmapped_method_Source_observations'])
        self.assertEqual(mapping['operations'], 509)
        self.assertEqual(sum(o['method'] is None for o in operations.values()), 322)
        self.assertTrue(all(o['fully_verified'] is False for o in operations.values()))


if __name__ == '__main__':
    unittest.main()
