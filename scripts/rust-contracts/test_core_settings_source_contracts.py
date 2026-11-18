"""Source32 exact resolver/definition/privacy and unchanged-row controls.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import copy
import hashlib
import json
import unittest
from pathlib import Path
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
        selected = set(proof['new_selected_contract_ids'])
        operations = {o['contract_id']: o for r in register['runtime_routes'] for o in r['operations']}
        self.assertEqual(len(selected), 6)
        self.assertEqual(len(operations), 798)
        self.assertEqual(subject.digest({k: v for k, v in operations.items() if k not in selected}), proof['all_unselected_operation_dicts_sha256'])
        self.assertEqual(subject.digest([{k: v for k, v in r.items() if k != 'operations'} for r in register['runtime_routes']]), proof['routes_structural_sha256'])
        for field, name in [('non_HTTP_candidates', 'non_HTTP_candidates_sha256'), ('openapi_operations', 'OpenAPI_operations_sha256'), ('observations_for_methods_not_declared_in_discovery', 'observations_for_methods_not_declared_sha256')]:
            self.assertEqual(subject.digest(register[field]), proof[name])
        self.assertEqual(sum('executable_Source_contract' in o for o in operations.values()), 152)
        self.assertEqual(sum(bool(o.get('supplemental_executable_Source_captures')) for o in operations.values()), 5)
        self.assertEqual(sum(o['method'] is None for o in operations.values()), 322)
        self.assertTrue(all(not o['fully_verified'] for o in operations.values()))


if __name__ == '__main__':
    unittest.main()
