"""Source32 exact resolver/definition/privacy and unchanged-row controls.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import copy
import hashlib
import json
import unittest
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
import retained_comskip_task_contracts
import retained_clocked_recording_contracts
import core_settings_source_contracts as subject
import source_contracts

ROOT = Path(__file__).resolve().parents[2]


class CoreSettingsSourceControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = json.loads((ROOT / subject.INPUT).read_text())

    def test_six_definitions_join_all32_actual_resolver_cases(self):
        self.assertEqual(subject.validate(ROOT, self.value)['operation_assignments'], 6)
        self.assertEqual(sorted(case['Source_index'] for c in self.value['contracts'] for case in c['cases']), list(range(32)))
        self.assertEqual([len(c['cases']) for c in self.value['contracts']], [8, 1, 11, 2, 5, 5])
        self.assertTrue(all(not f['raw_recorder_passed'] and not f['projected_observations_executed'] for f in self.value['fixtures'].values()))

    def test_changed_role_resolver_body_definition_or_privacy_claim_rejected(self):
        mutations = [lambda v: v['contracts'][0].update(method='GET'),
                     lambda v: v['contracts'][0].update(pattern_exact='api/core/^settings.json$'),
                     lambda v: v['contracts'][0]['cases'][0]['actual_resolver'].update(action='list'),
                     lambda v: v['contracts'][0]['cases'][0].update(role='admin'),
                     lambda v: v['contracts'][0]['cases'][0].update(response_body_sha256='0' * 64),
                     lambda v: v['contracts'][0]['cases'][0].update(case_definition_sha256='0' * 64),
                     lambda v: v['contracts'][0]['cases'][0].update(projection_executed=True),
                     lambda v: v['contracts'][0]['result'].update(raw_recorder_passed=True),
                     lambda v: v['contracts'][0]['result'].update(Native_coverage_increased=True)]
        for index, mutate in enumerate(mutations):
            value = copy.deepcopy(self.value)
            mutate(value)
            with self.subTest(index=index), self.assertRaises(AssertionError):
                subject.validate(ROOT, value)

    def test_original146_definitions_and_all_unrelated_rows_preserved(self):
        proof = json.loads((ROOT / 'scripts/rust-contracts/core-settings-source32-assignments/preservation.json').read_text())
        register = json.loads((ROOT / 'docs/rust-contract-register.json').read_text())
        # Validate and reverse only the exact later auth/plugin assignments,
        # then require the immutable preceding Core32 full-row digests. Exact
        # reapplication must reproduce every current field, so no corruption is
        # hidden by restoring an older partial-source snapshot.
        import retained_auth_plugin_contracts
        auth = json.loads((ROOT / retained_auth_plugin_contracts.INPUT).read_text())
        retained_auth_plugin_contracts.validate(ROOT, auth)
        auth_proof = json.loads((ROOT / 'scripts/rust-contracts/retained-auth-plugin/preservation.json').read_text())
        original_rows = copy.deepcopy(register['runtime_routes'])
        import retained_xc_refresh_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(ROOT, register['runtime_routes'])
        retained_paired_read_contracts.restore_pre_paired_reads(ROOT, register['runtime_routes'])
        retained_live_stop_contracts.restore_pre_live_stop(ROOT, register['runtime_routes'])
        retained_ts_controls_contracts.restore_pre_ts_controls(ROOT, register['runtime_routes'])
        retained_canonical_management_contracts.restore_pre_management(ROOT, register['runtime_routes'])
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
        prior_rows = register['runtime_routes']
        by_id = {o['contract_id']: o for r in prior_rows for o in r['operations']}
        for identifier in auth_proof['existing_supplemental_target_ids']:
            operation = by_id[identifier]
            captures = operation.pop('supplemental_executable_Source_captures')
            self.assertEqual(len(captures), 1)
            self.assertEqual(captures[0]['input']['kind'], retained_auth_plugin_contracts.KIND)
        for identifier in auth_proof['new_contract_ids']:
            operation = by_id[identifier]
            operation.pop('executable_Source_contract')
            operation.update(setup_fixture=None, assertions=[], result=None,
                             coverage_state='scoped_evidence_needs_case_mapping' if operation['related_evidence'] else 'not_yet_mapped')
            for field in ['observed_fixture_definitions', 'complete', 'observed_roles']:
                operation['role_matrix'].pop(field, None)
            operation['role_matrix']['state'] = 'unmapped'
        rebuilt = copy.deepcopy(prior_rows)
        source_contracts.apply(ROOT, rebuilt, json.loads((ROOT / 'scripts/rust-contracts/case-bindings.json').read_text()))
        self.assertEqual(rebuilt, original_rows)
        selected = set(proof['new_selected_contract_ids'])
        operations = {o['contract_id']: o for r in register['runtime_routes'] for o in r['operations']}
        self.assertEqual(len(selected), 6)
        self.assertEqual(len(operations), 798)
        self.assertEqual(subject.digest({k: v for k, v in operations.items() if k not in selected}), proof['all_unselected_operation_dicts_sha256'])
        self.assertEqual(subject.digest([{k: v for k, v in r.items() if k != 'operations'} for r in register['runtime_routes']]), proof['routes_structural_sha256'])
        retained_clocked_recording_contracts.restore_pre_clocked_recording(ROOT, register['non_HTTP_candidates'])
        retained_comskip_task_contracts.restore_pre_comskip_task(ROOT, register['non_HTTP_candidates'])
        for field, name in [('non_HTTP_candidates', 'non_HTTP_candidates_sha256'), ('openapi_operations', 'OpenAPI_operations_sha256'), ('observations_for_methods_not_declared_in_discovery', 'observations_for_methods_not_declared_sha256')]:
            self.assertEqual(subject.digest(register[field]), proof[name])
        self.assertEqual(sum('executable_Source_contract' in o for o in operations.values()), 152)
        self.assertEqual(sum(bool(o.get('supplemental_executable_Source_captures')) for o in operations.values()), 5)
        self.assertEqual(sum(o['method'] is None for o in operations.values()), 322)
        self.assertTrue(all(not o['fully_verified'] for o in operations.values()))


if __name__ == '__main__':
    unittest.main()
