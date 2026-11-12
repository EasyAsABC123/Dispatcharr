"""Public-only rejection/preservation controls for partial Source mapping."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import source_contracts

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

    def test_exact13_partial_assignments(self):
        result, rows = self.apply()
        self.assertEqual(result['operations'], 29)
        assigned = [o for r in rows for o in r['operations'] if 'executable_Source_contract' in o]
        self.assertEqual(len(assigned), 29)
        self.assertEqual(sum(o['method'] is None for o in assigned), 8)
        self.assertEqual(sum(o['result']['new_Source_cases_this_task'] for o in assigned), 78)
        self.assertTrue(all(not o['fully_verified'] for r in rows for o in r['operations']))

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
