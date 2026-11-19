"""Offline SD resolver custody and immutable preceding-row controls.
Original Dispatcharr and dependency authors retain all application credit.
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
import shutil
import tempfile
import unittest
import retained_sd_contracts as subject
import source_contracts
ROOT = Path(__file__).resolve().parents[2]

class RetainedSDControls(unittest.TestCase):
    def test_actual_resolver_and_original_HTTP_controller_outcomes_stay_distinct(self):
        value = json.loads((ROOT / subject.INPUT).read_bytes())
        self.assertEqual(subject.validate(ROOT, value), {'new_assignments': 2, 'Source_observations': 25, 'operation_observations': 19, 'method_side_observations': 6, 'fully_verified_added': 0})
        self.assertEqual([len(c['cases']) for c in value['contracts']], [8, 11])
        self.assertEqual(value['actual_resolver_network_attempts'], 1)
        self.assertEqual(value['original_network_operations_executed'], 0)
        self.assertTrue(all(f['actual_Source_controller_exits'] == {'Source3': 1, 'Source10': 1} and f['resolver_addendum_exit'] == 0 for f in value['fixtures'].values()))

    def test_fabricated_role_body_method_route_green_or_side_alias_rejected(self):
        original = json.loads((ROOT / subject.INPUT).read_bytes())
        mutations = [lambda x: x['contracts'][0]['cases'][0].update(role='invented'),
                     lambda x: x['contracts'][0]['cases'][0].update(response_body_sha256='0' * 64),
                     lambda x: x['contracts'][0].update(method='HEAD'),
                     lambda x: x['contracts'][0].update(resolver_ordinal=156),
                     lambda x: x.update(actual_resolver_network_attempts=0),
                     lambda x: x.update(Native_coverage_increased=True),
                     lambda x: x['unmapped_method_Source_observations'][0].update(contract_id='invented:OPTIONS')]
        for index, mutation in enumerate(mutations):
            value = copy.deepcopy(original); mutation(value)
            with self.subTest(index=index), self.assertRaises(AssertionError): subject.validate(ROOT, value)

    def test_raw_counter_phase_route_controller_or_custody_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in subject.dependencies():
                p = root / name; p.parent.mkdir(parents=True, exist_ok=True); shutil.copyfile(ROOT / name, p)
            mutations = [('resolver/UID-report.json', lambda x: x['actual_guard_counts'].update(network_attempts=0)),
                         ('resolver/observations.json', lambda x: x['denied_operations'][0].update(stage='resolver_forest_load')),
                         ('resolver/observations.json', lambda x: x['observations'][0].update(actual_resolver_ordinal=156)),
                         ('resolver/Root-terminal.json', lambda x: x.update(actual_exit=1)),
                         ('Source3-qualification.json', lambda x: x.update(actual_original_controller_exit=0))]
            for name, mutation in mutations:
                p = root / subject.BASE / name; raw = p.read_bytes(); value = json.loads(raw); mutation(value); p.write_text(json.dumps(value))
                with self.subTest(name=name), self.assertRaises(AssertionError): subject.generate(root)
                p.write_bytes(raw)

    def test_only_two_assignments_preserve_796_full_rows_and_161_prior_definitions(self):
        register = json.loads((ROOT / 'docs/rust-contract-register.json').read_bytes())
        import retained_xc_refresh_contracts
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
        proof = json.loads((ROOT / subject.BASE / 'preservation.json').read_bytes())
        rows = copy.deepcopy(register['runtime_routes']); current = copy.deepcopy(rows)
        ops = {o['contract_id']: o for r in rows for o in r['operations']}
        self.assertEqual(len(ops), 798)
        self.assertEqual(sum(bool(o.get('executable_Source_contract')) for o in ops.values()), 163)
        self.assertEqual(subject.digest({k: v for k, v in ops.items() if k not in proof['selected']}), proof['unselected_full_operation_dicts_sha256'])
        subject.restore_pre_SD(rows)
        prior = {o['contract_id']: o for r in rows for o in r['operations']}
        self.assertEqual(sum(bool(o.get('executable_Source_contract')) for o in prior.values()), 161)
        self.assertEqual({k: prior[k] for k in proof['selected']}, proof['selected'])
        self.assertEqual(subject.digest([{k: v for k, v in r.items() if k != 'operations'} for r in rows]), proof['route_structural_sha256'])
        for key, name in [('non_HTTP_candidates', 'non_HTTP_candidates_sha256'), ('openapi_operations', 'OpenAPI_operations_sha256'), ('observations_for_methods_not_declared_in_discovery', 'legacy_undeclared_method_observations_sha256')]: self.assertEqual(subject.digest(register[key]), proof[name])
        bindings = json.loads((ROOT / 'scripts/rust-contracts/case-bindings.json').read_bytes())
        mapping = source_contracts.apply(ROOT, rows, bindings)
        retained_connect_contracts.restore_pre_connect(ROOT, rows)
        retained_dvr_xc_contracts.restore_pre_dvr_xc(ROOT, rows)
        retained_admin_model_contracts.restore_pre_admin_models(ROOT, rows)
        retained_repository_socket_switch_contracts.restore_pre_repository_socket_switch(ROOT, rows)
        retained_image22_controls_contracts.restore_pre_image22_controls(ROOT, rows)
        retained_playback_contracts.restore_pre_playback(ROOT, rows)
        retained_hdhr_contracts.restore_pre_hdhr(ROOT, rows)
        retained_provider_channel_vod_contracts.restore_pre_provider_channel_vod(ROOT, rows)
        retained_serializer_null_contracts.restore_pre_serializer_null(ROOT, rows)
        retained_core_read_contracts.restore_pre_core_reads(ROOT, rows)
        retained_epg_reads_contracts.restore_pre_epg(ROOT, rows)
        retained_xc_refresh_contracts.restore_pre_xc(ROOT, rows)
        retained_range_source10_contracts.restore_pre_range(rows)
        retained_source12_contracts.restore_pre_Source12(rows)
        framework_suite_contracts.restore_pre_framework(rows)
        self.assertEqual(rows, current); self.assertEqual(mapping['operations'], 380)
        self.assertEqual(len(mapping['unmapped_method_Source_observations']), 23)
        self.assertEqual(len([x for x in mapping['unmapped_method_Source_observations'] if x['route_id'] == 'route-7e6941c073eccaa2b6dd']), 6)
        source_contracts.apply(ROOT, rows, bindings)
        again = copy.deepcopy(rows); source_contracts.apply(ROOT, again, bindings); self.assertEqual(again, rows)

if __name__ == '__main__': unittest.main()
