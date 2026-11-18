"""Retained Source37 packet integrity and additive mapping preservation controls.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import copy
import json
import unittest
import retained_management_contracts as retained
import source_contracts
import test_source_contracts


class RetainedManagementControls(unittest.TestCase):
    def setUp(self):
        test_source_contracts.SourceContracts.setUp(self)
        self.packet = json.loads((self.root / retained.PATH).read_text())

    def test_qualified_leaves_keep_three_distinct_controllers(self):
        result = retained.validate(self.root, self.packet)
        self.assertEqual(result['Source_positive_leaves'], 37)
        self.assertEqual(result['distinct_operation_roots'], 12)
        self.assertEqual(self.packet['fixtures']['management-first4']['actual_controller_exit'], 1)
        self.assertEqual(self.packet['fixtures']['management-second8']['actual_controller_exit'], 0)

    def test_wire_role_method_effect_and_controller_corruption_rejected(self):
        for field, replacement in [('role', 'viewer'), ('method', 'DELETE'),
                                   ('response_body_sha256', '0' * 64),
                                   ('HTTP_status', 500), ('contract_id', 'other')]:
            bad = copy.deepcopy(self.packet)
            bad['contracts'][0]['cases'][0][field] = replacement
            with self.subTest(field=field), self.assertRaises(AssertionError):
                retained.validate(self.root, bad)
        bad = copy.deepcopy(self.packet)
        bad['fixtures']['management-first4']['actual_controller_exit'] = 0
        with self.assertRaises(AssertionError):
            retained.validate(self.root, bad)

    def test_repeated_retained_input_is_rejected(self):
        path = self.root / 'scripts/rust-contracts/executable-source-inputs.json'
        manifest = json.loads(path.read_text())
        manifest['inputs'].append(copy.deepcopy(next(x for x in manifest['inputs'] if x['kind'] == retained.KIND)))
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(AssertionError, 'conflicting Source contract inputs'):
            source_contracts.apply(self.root, copy.deepcopy(self.register['runtime_routes']), self.bindings)

    def test_seven_new_five_supplemental_and_existing_fields_preserved(self):
        path = self.root / 'scripts/rust-contracts/executable-source-inputs.json'
        manifest = json.loads(path.read_text())
        old = copy.deepcopy(manifest)
        old['inputs'] = [x for x in old['inputs'] if x['kind'] != retained.KIND]
        path.write_text(json.dumps(old))
        before = copy.deepcopy(self.register['runtime_routes'])
        # Reconstruct the previous primary definitions with no new cohort state.
        for route in before:
            for operation in route['operations']:
                operation.pop('supplemental_executable_Source_captures', None)
                if operation.get('executable_Source_contract', {}).get('fixture_id', '').startswith('retained:'):
                    for key in ['executable_Source_contract', 'setup_fixture', 'result']:
                        operation.pop(key, None)
                    operation['setup_fixture'] = operation['result'] = None
                    operation['assertions'] = []
        source_contracts.apply(self.root, before, self.bindings)
        after = copy.deepcopy(before)
        path.write_text(json.dumps(manifest))
        source_contracts.apply(self.root, after, self.bindings)
        old_index = {o['contract_id']: o for r in before for o in r['operations']}
        selected = {c['contract_id'] for c in self.packet['contracts']}
        new = supplementary = 0
        for route in after:
            for operation in route['operations']:
                prior = old_index[operation['contract_id']]
                if operation['contract_id'] not in selected:
                    self.assertEqual(operation, prior)
                elif prior.get('executable_Source_contract') is not None:
                    self.assertEqual(len(operation['supplemental_executable_Source_captures']), 1)
                    supplementary += 1
                    for key in ['executable_Source_contract', 'setup_fixture', 'assertions', 'result', 'role_matrix', 'case_bindings']:
                        self.assertEqual(operation.get(key), prior.get(key))
                else:
                    self.assertTrue(operation['setup_fixture'] and operation['assertions'] and operation['result'])
                    new += 1
        self.assertEqual((new, supplementary), (7, 5))
        again = copy.deepcopy(after)
        source_contracts.apply(self.root, again, self.bindings)
        self.assertEqual(again, after, 'assignment remains idempotent')


if __name__ == '__main__':
    unittest.main()
