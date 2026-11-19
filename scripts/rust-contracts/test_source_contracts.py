"""Public-only rejection/preservation controls for partial Source mapping."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import source_contracts
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
import retained_provider_admin_contracts
import retained_c98_hdhr_contracts
import retained_core_profile_json_contracts
import retained_hdhr_literal_contracts
import retained_c106_connect_replay_contracts
import retained_logo_m3u_contracts

ROOT = Path(__file__).resolve().parents[2]


class SourceContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        names = ['executable-source-inputs.json', 'executable-source-bindings.json',
                 'case-bindings.json', 'datasets/image44-scoped-cases.json',
                 'framework-auth/bindings.json', 'framework-auth/full-suite-reference.json',
                 'framework-auth/observations.json', 'framework-auth/original/test_accounts.py',
                 'framework-auth/original/test_login_rate_limit_inbox.py']
        for name in names:
            p = self.root / 'scripts/rust-contracts' / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / 'scripts/rust-contracts' / name, p)
        for name in ['docs/rust-contract-register.json', 'scripts/rust-integration/verified-checkpoint-image44-1.json',
                     'scripts/rust-vod-api-contracts/verified-unified-image36-1.json']:
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        account = json.loads((ROOT / 'scripts/rust-contracts/accounts-settings/bindings.json').read_text())
        dependencies = ['scripts/rust-contracts/accounts-settings/bindings.json']
        dependencies += [r['path'] for r in account['authorities'] + account['original_test_files']]
        for name in dependencies:
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import scanner_task_contracts
        for name in scanner_task_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import channel_guide_contracts
        for name in channel_guide_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import dvr_import_contracts
        for name in dvr_import_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import provider_output_contracts
        for name in provider_output_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import admin_rest_contracts
        for name in admin_rest_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import proxy_plugin_contracts
        for name in proxy_plugin_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import notification_source_contracts
        for name in notification_source_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import notification_mutation_contracts
        for name in notification_mutation_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import schema_cache_contracts
        for name in schema_cache_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import notification_credential_contracts
        for name in notification_credential_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import vod_access_contracts
        for name in vod_access_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import logo_source_contracts
        for name in logo_source_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        matching = json.loads((ROOT / 'scripts/rust-contracts/channel-matching-source51-bindings.json').read_text())
        for name in ['scripts/rust-contracts/channel-matching-source51-bindings.json', *[r['path'] for r in matching['authorities']]]:
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        additional = json.loads((ROOT / 'scripts/rust-contracts/station-aggregate-source53-55-bindings.json').read_text())
        for name in ['scripts/rust-contracts/station-aggregate-source53-55-bindings.json', *[r['path'] for r in additional['authorities']]]:
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_task_operator_contracts
        for name in retained_task_operator_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_comskip_task_contracts
        for name in retained_comskip_task_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_clocked_recording_contracts
        import retained_c105_guide_startup_contracts
        for name in retained_clocked_recording_contracts.dependencies() | retained_c105_guide_startup_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_management_contracts
        for name in retained_management_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import core_settings_source_contracts
        for name in core_settings_source_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_auth_plugin_contracts
        for name in retained_auth_plugin_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_xc_refresh_contracts
        for name in retained_xc_refresh_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_range_source10_contracts
        for name in retained_range_source10_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_source12_contracts
        for name in retained_source12_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import framework_suite_contracts
        for name in framework_suite_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_sd_contracts
        for name in retained_sd_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_epg_reads_contracts
        for name in retained_epg_reads_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_core_read_contracts
        for name in retained_core_read_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_serializer_null_contracts
        for name in retained_serializer_null_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_provider_channel_vod_contracts
        for name in retained_provider_channel_vod_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_hdhr_contracts
        for name in retained_hdhr_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_playback_contracts
        for name in retained_playback_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_image22_controls_contracts
        for name in retained_image22_controls_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_repository_socket_switch_contracts
        for name in retained_repository_socket_switch_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_admin_model_contracts
        for name in retained_admin_model_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import retained_dvr_xc_contracts
        for name in retained_dvr_xc_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        for name in retained_connect_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        for name in retained_connect_actions_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        for name in retained_alias19_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        for name in retained_connect_detail_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        for name in retained_alias_remainder_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        for name in retained_connect_test_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        for name in retained_set_epg_selected_contracts.dependencies() | retained_paired_read_contracts.dependencies() | retained_live_stop_contracts.dependencies() | retained_ts_controls_contracts.dependencies() | retained_canonical_management_contracts.dependencies() | retained_exact_vod_contracts.dependencies() | retained_image57_accounts_contracts.dependencies() | retained_image57_m3u_contracts.dependencies() | retained_provider_admin_contracts.dependencies() | retained_c98_hdhr_contracts.dependencies() | retained_core_profile_json_contracts.dependencies() | retained_hdhr_literal_contracts.dependencies() | retained_c106_connect_replay_contracts.dependencies() | retained_logo_m3u_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        self.value = json.loads((self.root / 'scripts/rust-contracts/executable-source-bindings.json').read_text())
        self.bindings = json.loads((self.root / 'scripts/rust-contracts/case-bindings.json').read_text())
        self.register = json.loads((self.root / 'docs/rust-contract-register.json').read_text())

    def apply(self):
        rows = copy.deepcopy(self.register['runtime_routes'])
        return source_contracts.apply(self.root, rows, self.bindings), rows

    def corrupt(self, mutate):
        value = copy.deepcopy(self.value)
        mutate(value)
        with self.assertRaises((AssertionError, KeyError, StopIteration)):
            source_contracts.validate_captured(self.root, value, self.bindings)

    def test_exact_partial_assignments(self):
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        assigned = [o for r in rows for o in r['operations'] if 'executable_Source_contract' in o]
        self.assertEqual(len(assigned), 592)
        self.assertEqual(sum(o['method'] is None for o in assigned), 218)
        self.assertEqual(sum(o['result']['new_Source_cases_this_task'] for o in assigned), 78)
        self.assertTrue(all(not o['fully_verified'] for r in rows for o in r['operations']))

    def test_retained_xc_refresh_adds_only_observed_post_reference(self):
        import m0_source_assignments
        import retained_xc_refresh_contracts as xc

        result, rows = self.apply()
        packet = xc.generate(self.root)
        route, = [row for row in rows if row["resolver_ordinal"] == 184]
        operation, = route["operations"]
        self.assertFalse(route["declared_methods_resolved"])
        self.assertIsNone(operation["method"])
        self.assertIsNone(operation["source_action"])
        self.assertFalse(operation["fully_verified"])
        self.assertEqual(operation["executable_Source_contract"], packet["contracts"][0])
        case, = operation["executable_Source_contract"]["cases"]
        self.assertEqual((case["method"], case["literal_path"], case["role"], case["status"]),
                         ("POST", "/api/m3u/refresh/6/", "admin", 202))
        self.assertTrue(case["profile_parent_is_refresh_root"])
        self.assertEqual(result["operations"], 592)
        xc.restore_pre_xc(self.root, rows)
        self.assertEqual(operation, xc.reference(self.root)["prior_operation"])
        self.assertEqual(sum("executable_Source_contract" in op for row in rows for op in row["operations"]), 591)
        audit = m0_source_assignments.generate(self.root)
        self.assertEqual(audit["counts"]["executable_Source_partial_operations"], 592)
        self.assertEqual(audit["counts"]["operations_without_executable_Source_assignment"], 206)
        self.assertEqual(audit["counts"]["method_unresolved_with_Source_fixture"], 218)
        self.assertEqual(audit["counts"]["fully_verified_operations"], 0)

    def test_alias_remainder_preserves_prior_primaries_and_reported_failures(self):
        subject = retained_alias_remainder_contracts
        packet = subject.load(self.root)
        subject.validate(self.root, packet)
        self.assertEqual(packet['capture_boundary']['observed_HTTP_status_counts']['500'], 75)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        subject.restore_pre_alias_remainder(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 403)
        for field in ('raw_method', 'observed_method', 'partial_route_observation_only'):
            invalid = copy.deepcopy(packet)
            invalid['contracts'][0]['cases'][0][field] = 'fabricated'
            with self.assertRaises(AssertionError):
                subject.validate(self.root, invalid)

    def test_connect_test_only_assigns_the_unformatted_saved_webhook_action(self):
        subject = retained_connect_test_contracts
        binding = subject.load(self.root)
        subject.validate(self.root, binding)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        operations = {op['contract_id']: op for route in rows for op in route['operations']}
        self.assertFalse(operations[subject.CONTRACT]['role_matrix']['complete'])
        self.assertNotIn('executable_Source_contract', operations['route-247b2930daac158ddf19:POST'])
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        subject.restore_pre_connect_test(self.root, rows)
        self.assertNotIn('executable_Source_contract', operations[subject.CONTRACT])

    def test_selected_set_epg_retains_only_two_unformatted_controls(self):
        subject = retained_set_epg_selected_contracts
        binding = subject.load(self.root)
        subject.validate(self.root, binding)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        records = {op['contract_id']: op for route in rows for op in route['operations']}
        assigned = records[subject.CONTRACT]
        self.assertEqual([case['status'] for case in assigned['executable_Source_contract']['cases']], [200, 400])
        self.assertFalse(assigned['role_matrix']['complete'])
        self.assertNotIn('executable_Source_contract', records['route-2b118717b5fd2a6cfe3c:POST'])
        self.assertNotIn('executable_Source_contract', records['route-61d15dceb3b6411c9090:POST'])
        subject.restore_pre_set_epg_selected(self.root, rows)
        self.assertNotIn('executable_Source_contract', assigned)

    def test_batch_epg_preserves_native_publisher_absent_guard(self):
        subject = retained_set_epg_selected_contracts
        packet = subject.load_batch(self.root)
        subject.validate_batch(self.root, packet)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        operations = {op['contract_id']: op for route in rows for op in route['operations']}
        operation = operations[subject.BATCH_CONTRACT]
        self.assertIn('executable_Source_contract', operation)
        self.assertEqual(operation['setup_fixture']['Native_publisher_absent_guard']['status'], 501)
        self.assertEqual(len(operation['executable_Source_contract']['cases']), 17)
        self.assertFalse(operation['role_matrix']['complete'])
        self.assertNotIn('executable_Source_contract', operations['route-61d15dceb3b6411c9090:POST'])

    def test_set_epg_requires_exact_public_acceptance_receipts(self):
        subject = retained_set_epg_selected_contracts
        for receipt_path, packet, validate in (
            (subject.SELECTED_PUBLISHED, subject.load(self.root), subject.validate),
            (subject.BATCH_PUBLISHED, subject.load_batch(self.root), subject.validate_batch),
        ):
            receipt = self.root / receipt_path
            original = receipt.read_bytes()
            detached = receipt.with_suffix('.missing')
            receipt.rename(detached)
            try:
                with self.assertRaises(AssertionError):
                    validate(self.root, packet)
            finally:
                detached.rename(receipt)
            receipt.write_bytes(original + b'\n')
            try:
                with self.assertRaises(AssertionError):
                    validate(self.root, packet)
            finally:
                receipt.write_bytes(original)
            validate(self.root, packet)

    def test_connect_detail_keeps_successes_without_literal_paths_separate(self):
        packet = retained_connect_detail_contracts.load(self.root)
        retained_connect_detail_contracts.validate(self.root, packet)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 401)
        fixture = packet['fixtures']['m7-connect-delete-candidate80-source-1']
        fixture['actual_successes_with_symbolic_ID'][0]['literal_path_verified'] = True
        with self.assertRaises(AssertionError):
            retained_connect_detail_contracts.validate(self.root, packet)

    def test_alias19_uses_only_completed_anonymous_observations(self):
        packet = retained_alias19_contracts.load(self.root)
        retained_alias19_contracts.validate(self.root, packet)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 382)
        packet['contracts'][0]['cases'][0]['role'] = 'admin'
        with self.assertRaises(AssertionError):
            retained_alias19_contracts.validate(self.root, packet)

    def test_connect_actions_preserve_literal_routes_and_symbolic_fixture_paths(self):
        packet = retained_connect_actions_contracts.load(self.root)
        retained_connect_actions_contracts.validate(self.root, packet)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 380)
        packet['contracts'][1]['cases'][0]['literal_path_verified'] = False
        with self.assertRaises(AssertionError):
            retained_connect_actions_contracts.validate(self.root, packet)

    def test_connect_collection_preserves_method_scoped_responses_and_failed_outer_run(self):
        packet = retained_connect_contracts.load(self.root)
        retained_connect_contracts.validate(self.root, packet)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 378)
        packet['contracts'][1]['cases'][0]['method'] = 'GET'
        with self.assertRaises(AssertionError):
            retained_connect_contracts.validate(self.root, packet)

    def test_retained_dvr_xc_preserves_role_scope_and_admin_bindings(self):
        import retained_dvr_xc_contracts as dvr

        packet = dvr.load(self.root)
        dvr.validate(self.root, packet)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        dvr.restore_pre_dvr_xc(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 373)
        socket = next(row for row in packet['contracts'] if row['fixture_id'] == 'XC-live-entry-paired')
        socket['cases'][0]['fixture_role_is_auth_success'] = True
        with self.assertRaises(AssertionError):
            dvr.validate(self.root, packet)

    def test_retained_admin_models_preserve_actor_modes_and_failed_expectations(self):
        import retained_admin_model_contracts as admin

        value = admin.load(self.root)
        admin.validate(self.root, value)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        import retained_dvr_xc_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(self.root, rows)
        admin.restore_pre_admin_models(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 270)
        value['contracts'][0]['cases'][0]['role'] = 'unchecked'
        with self.assertRaises(AssertionError):
            admin.validate(self.root, value)

    def test_retained_repository_socket_switch_preserves_exception_boundary(self):
        import retained_repository_socket_switch_contracts as packet

        value = packet.load(self.root)
        packet.validate(self.root, value)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        import retained_admin_model_contracts
        import retained_dvr_xc_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(self.root, rows)
        retained_admin_model_contracts.restore_pre_admin_models(self.root, rows)
        packet.restore_pre_repository_socket_switch(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 264)
        value['excluded_non_HTTP_outcome'][0]['HTTP_response_returned'] = True
        with self.assertRaises(AssertionError):
            packet.validate(self.root, value)

    def test_retained_image22_controls_preserve_patterns_and_playback(self):
        import retained_image22_controls_contracts as image22

        packet = image22.load(self.root)
        image22.validate(self.root, packet)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        import retained_repository_socket_switch_contracts
        import retained_admin_model_contracts
        import retained_dvr_xc_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(self.root, rows)
        retained_admin_model_contracts.restore_pre_admin_models(self.root, rows)
        retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(self.root, rows)
        image22.restore_pre_image22_controls(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 257)
        packet['contracts'][0]['cases'][0]['role'] = 'unsupported'
        with self.assertRaises(AssertionError):
            image22.validate(self.root, packet)

    def test_retained_playback_preserves_patterns_and_prior_assignments(self):
        import retained_playback_contracts as playback

        value = playback.load(self.root)
        playback.validate(self.root, value)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        self.assertEqual(sum(o['method'] is None for route in rows for o in route['operations']), 322)
        import retained_image22_controls_contracts
        import retained_repository_socket_switch_contracts
        import retained_admin_model_contracts
        import retained_dvr_xc_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(self.root, rows)
        retained_admin_model_contracts.restore_pre_admin_models(self.root, rows)
        retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(self.root, rows)
        retained_image22_controls_contracts.restore_pre_image22_controls(self.root, rows)
        playback.restore_pre_playback(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows
                             for op in route['operations']), 250)
        value['contracts'][0]['cases'][0]['path'] = '/proxy/vod/stop_client/'
        with self.assertRaises(AssertionError):
            playback.validate(self.root, value)

    def test_retained_hdhr_preserves_unresolved_methods(self):
        import retained_hdhr_contracts as hdhr
        value = json.loads((self.root / hdhr.INPUT).read_bytes())
        hdhr.validate(self.root, value)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        self.assertEqual(sum(len(contract['cases']) for contract in value['contracts']), 120)
        self.assertEqual(sum(o['method'] is None for route in rows for o in route['operations']), 322)
        declared = {o['contract_id']: o for route in rows for o in route['operations']}
        unresolved = [c for c in value['contracts'] if c['method'] is None]
        self.assertEqual(len(unresolved), 16)
        self.assertTrue(all(declared[c['contract_id']]['method'] is None for c in unresolved))
        import retained_playback_contracts
        import retained_image22_controls_contracts
        import retained_repository_socket_switch_contracts
        import retained_admin_model_contracts
        import retained_dvr_xc_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(self.root, rows)
        retained_admin_model_contracts.restore_pre_admin_models(self.root, rows)
        retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(self.root, rows)
        retained_image22_controls_contracts.restore_pre_image22_controls(self.root, rows)
        retained_playback_contracts.restore_pre_playback(self.root, rows)
        hdhr.restore_pre_hdhr(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows for op in route['operations']), 225)
        unresolved[0]['method'] = 'GET'
        with self.assertRaises(AssertionError):
            hdhr.validate(self.root, value)

    def test_retained_provider_channel_vod_joins_exact_observations(self):
        import retained_provider_channel_vod_contracts as packet
        value = json.loads((self.root / packet.INPUT).read_bytes())
        packet.validate(self.root, value)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        self.assertEqual(sum(len(contract['cases']) for contract in value['contracts']), 132)
        import retained_hdhr_contracts
        import retained_playback_contracts
        import retained_image22_controls_contracts
        import retained_repository_socket_switch_contracts
        import retained_admin_model_contracts
        import retained_dvr_xc_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(self.root, rows)
        retained_admin_model_contracts.restore_pre_admin_models(self.root, rows)
        retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(self.root, rows)
        retained_image22_controls_contracts.restore_pre_image22_controls(self.root, rows)
        retained_playback_contracts.restore_pre_playback(self.root, rows)
        retained_hdhr_contracts.restore_pre_hdhr(self.root, rows)
        packet.restore_pre_provider_channel_vod(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows for op in route['operations']), 211)
        value['contracts'][0]['cases'][0]['HTTP_status'] = 299
        with self.assertRaises(AssertionError):
            packet.validate(self.root, value)

    def test_retained_serializer_null_bind_exact_refusals(self):
        import retained_serializer_null_contracts as core
        value = json.loads((self.root / core.INPUT).read_bytes())
        core.validate(self.root, value)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        self.assertEqual(sum(len(contract['cases']) for contract in value['contracts']), 96)
        import retained_provider_channel_vod_contracts
        import retained_hdhr_contracts
        import retained_playback_contracts
        import retained_image22_controls_contracts
        import retained_repository_socket_switch_contracts
        import retained_admin_model_contracts
        import retained_dvr_xc_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(self.root, rows)
        retained_admin_model_contracts.restore_pre_admin_models(self.root, rows)
        retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(self.root, rows)
        retained_image22_controls_contracts.restore_pre_image22_controls(self.root, rows)
        retained_playback_contracts.restore_pre_playback(self.root, rows)
        retained_hdhr_contracts.restore_pre_hdhr(self.root, rows)
        retained_provider_channel_vod_contracts.restore_pre_provider_channel_vod(self.root, rows)
        core.restore_pre_serializer_null(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows for op in route['operations']), 188)
        value['contracts'][0]['cases'][0]['HTTP_status'] = 299
        with self.assertRaises(AssertionError):
            core.validate(self.root, value)

    def test_retained_core_reads_bind_exact_natural_gets(self):
        import retained_core_read_contracts as core
        value = json.loads((self.root / core.INPUT).read_bytes())
        core.validate(self.root, value)
        result, rows = self.apply()
        self.assertEqual(result['operations'], 592)
        self.assertEqual(sum(len(contract['cases']) for contract in value['contracts']), 29)
        import retained_serializer_null_contracts
        import retained_provider_channel_vod_contracts
        import retained_hdhr_contracts
        import retained_playback_contracts
        import retained_image22_controls_contracts
        import retained_repository_socket_switch_contracts
        import retained_admin_model_contracts
        import retained_dvr_xc_contracts
        retained_set_epg_selected_contracts.restore_pre_epg_assignments(self.root, rows)
        retained_paired_read_contracts.restore_pre_paired_reads(self.root, rows)
        retained_live_stop_contracts.restore_pre_live_stop(self.root, rows)
        retained_ts_controls_contracts.restore_pre_ts_controls(self.root, rows)
        retained_canonical_management_contracts.restore_pre_management(self.root, rows)
        retained_exact_vod_contracts.restore_pre_vod_split(self.root, rows)
        retained_image57_accounts_contracts.restore_pre_image57_accounts(self.root, rows)
        retained_image57_m3u_contracts.restore_pre_image57_m3u(self.root, rows)
        retained_provider_admin_contracts.restore_pre_provider_admin(self.root, rows)
        retained_c98_hdhr_contracts.restore_pre_c98_hdhr(self.root, rows)
        retained_core_profile_json_contracts.restore_pre_core_profiles(self.root, rows)
        retained_hdhr_literal_contracts.restore_pre_literal_hdhr(self.root, rows)
        retained_c106_connect_replay_contracts.restore_pre_c106_replay(self.root, rows)
        retained_logo_m3u_contracts.restore_pre_logo_m3u(self.root, rows)
        retained_connect_test_contracts.restore_pre_connect_test(self.root, rows)
        retained_alias_remainder_contracts.restore_pre_alias_remainder(self.root, rows)
        retained_connect_detail_contracts.restore_pre_connect_detail(self.root, rows)
        retained_alias19_contracts.restore_pre_alias19(self.root, rows)
        retained_connect_actions_contracts.restore_pre_connect_actions(self.root, rows)
        retained_connect_contracts.restore_pre_connect(self.root, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(self.root, rows)
        retained_admin_model_contracts.restore_pre_admin_models(self.root, rows)
        retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(self.root, rows)
        retained_image22_controls_contracts.restore_pre_image22_controls(self.root, rows)
        retained_playback_contracts.restore_pre_playback(self.root, rows)
        retained_hdhr_contracts.restore_pre_hdhr(self.root, rows)
        retained_provider_channel_vod_contracts.restore_pre_provider_channel_vod(self.root, rows)
        retained_serializer_null_contracts.restore_pre_serializer_null(self.root, rows)
        core.restore_pre_core_reads(self.root, rows)
        self.assertEqual(sum('executable_Source_contract' in op for route in rows for op in route['operations']), 181)
        value['contracts'][0]['cases'][0]['HTTP_status'] = 299
        with self.assertRaises(AssertionError):
            core.validate(self.root, value)

    def test_stored_epg_framework_capture_and_unmapped_head(self):
        import retained_epg_reads_contracts as epg
        value = json.loads((self.root / epg.INPUT).read_bytes())
        epg.validate(self.root, value)
        result, rows = self.apply()
        self.assertEqual(result['unmapped_method_Source_observations'][-3:], value['unmapped_method_Source_observations'])
        ids = {contract['contract_id'] for contract in value['contracts']}
        assigned = [o for r in rows for o in r['operations'] if o['contract_id'] in ids]
        self.assertEqual(len(assigned), 6)
        self.assertEqual(sum(len(o['executable_Source_contract']['cases']) for o in assigned), 101)
        value['contracts'][0]['cases'][0]['HTTP_status'] = 299
        with self.assertRaises(AssertionError):
            epg.validate(self.root, value)

    def test_captured_aggregate_does_not_resolve_operation_method(self):
        value = json.loads((self.root / 'scripts/rust-contracts/station-aggregate-source53-55-bindings.json').read_text())
        next(c for c in value['contracts'] if c['method'] is None)['method'] = 'POST'
        with self.assertRaises(AssertionError):
            source_contracts.validate_station_aggregate_source53_55(self.root, value)

    def test_station_aggregate_cannot_invent_native_coverage(self):
        value = json.loads((self.root / 'scripts/rust-contracts/station-aggregate-source53-55-bindings.json').read_text())
        value['Native_coverage_increased'] = True
        with self.assertRaises(AssertionError):
            source_contracts.validate_station_aggregate_source53_55(self.root, value)

    def test_station_aggregate_requires_actual_record_and_backend(self):
        path = self.root / 'scripts/rust-contracts/station-aggregate-source53-55-bindings.json'
        for field, replacement in [('record_sha256', '0' * 64), ('backend_status', 'FAILURE')]:
            value = json.loads(path.read_text())
            value['contracts'][0]['cases'][0][field] = replacement
            with self.assertRaises(AssertionError):
                source_contracts.validate_station_aggregate_source53_55(self.root, value)

    def test_identity_methods_roles_and_unrelated_rows_preserved(self):
        before = copy.deepcopy(self.register['runtime_routes'])
        # Insert an unmapped role requirement to prove it is never erased.
        target = self.value['contracts'][0]['contract_id']
        operation = next(o for r in before for o in r['operations'] if o['contract_id'] == target)
        operation['role_matrix']['required_roles'] = ['sentinel-required-role']
        after = copy.deepcopy(before)
        source_contracts.apply(self.root, after, self.bindings)
        self.assertEqual(before, after)

    def test_missing_manifest_fails(self):
        (self.root / 'scripts/rust-contracts/executable-source-inputs.json').unlink()
        with self.assertRaisesRegex(AssertionError, 'manifest absent'):
            self.apply()

    def test_missing_input_fails(self):
        (self.root / 'scripts/rust-contracts/executable-source-bindings.json').unlink()
        with self.assertRaisesRegex(AssertionError, 'authority absent'):
            self.apply()

    def test_changed_input_hash_fails(self):
        p = self.root / 'scripts/rust-contracts/executable-source-bindings.json'
        p.write_bytes(p.read_bytes() + b'\n')
        with self.assertRaisesRegex(AssertionError, 'authority hash'):
            self.apply()

    def test_unknown_fixture(self):
        self.corrupt(lambda v: v['contracts'][0].update(fixture_id='invented'))

    def test_wrong_method(self):
        self.corrupt(lambda v: v['contracts'][0].update(method='HEAD'))

    def test_wrong_handler(self):
        self.corrupt(lambda v: v['contracts'][0].update(source_handler='wrong.View'))

    def test_wrong_alias(self):
        self.corrupt(lambda v: v['contracts'][0].update(pattern_exact='api/vod/all'))

    def test_wrong_role(self):
        self.corrupt(lambda v: v['contracts'][0].update(observed_roles=['admin']))

    def test_wrong_role_fixture(self):
        self.corrupt(lambda v: v['contracts'][0]['source_role_specs'].update(anonymous={'user_level': 10}))

    def test_wrong_case_coordinate(self):
        self.corrupt(lambda v: v['contracts'][0]['cases'][0].update(source_index=999))

    def test_wrong_case_response(self):
        self.corrupt(lambda v: v['contracts'][2]['cases'][0]['response'].update(status=418))

    def test_wrong_tie_authority(self):
        self.corrupt(lambda v: v['contracts'][2]['cases'][0].update(Original_authority='arbitrary'))

    def test_historical_not_fresh(self):
        self.corrupt(lambda v: v['contracts'][0]['result'].update(new_Source_cases_this_task=1))

    def test_no_endpoint_promotion(self):
        self.corrupt(lambda v: v['contracts'][0].update(fully_verified=True))

    def test_fresh_wire_corruption(self):
        self.corrupt(lambda v: v['fresh_Original_qualification']['wire_projection'][0].update(body_sha256='0' * 64))

    def test_original_growth_or_other_cases_not_added(self):
        self.corrupt(lambda v: v.update(historical_case_count=404))

    def test_conflicting_inputs_rejected(self):
        p = self.root / 'scripts/rust-contracts/executable-source-inputs.json'
        value = json.loads(p.read_text())
        value['inputs'].append(value['inputs'][0])
        p.write_text(json.dumps(value))
        with self.assertRaisesRegex(AssertionError, 'conflicting Source'):
            self.apply()

    def test_owner_guard_on_application(self):
        rows = copy.deepcopy(self.register['runtime_routes'])
        target = self.value['contracts'][0]['contract_id']
        operation = next(o for r in rows for o in r['operations'] if o['contract_id'] == target)
        operation['owning_milestone'] = 'M4'
        with self.assertRaisesRegex(AssertionError, 'stable operation'):
            source_contracts.apply(self.root, rows, self.bindings)


if __name__ == '__main__':
    unittest.main()
