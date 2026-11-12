"""Portable corruption controls for exact account/model Original assignments."""
import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import source_contracts
import account_settings_contracts as module

ROOT = Path(__file__).resolve().parents[2]
ORIGINAL = json.loads((ROOT / 'scripts/rust-contracts/accounts-settings/bindings.json').read_text())


class AccountSettingsControls(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        names = [r['path'] for r in ORIGINAL['authorities'] + ORIGINAL['original_test_files']]
        names.append('docs/rust-contract-register.json')
        for name in names:
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        for name in ['scripts/rust-contracts/executable-source-inputs.json', 'scripts/rust-contracts/accounts-settings/bindings.json']:
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import scanner_task_contracts
        for name in scanner_task_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        import provider_output_contracts
        for name in provider_output_contracts.dependencies():
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, p)
        self.value = copy.deepcopy(ORIGINAL)

    def reject(self, mutate):
        mutate(self.value)
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            module.validate(self.root, self.value)

    def test_nonHTTP_apply_preserves_all250_identities(self):
        before = json.loads((self.root / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        after = copy.deepcopy(before)
        result = source_contracts.apply_non_HTTP(self.root, after)
        self.assertEqual(result['candidates'], 8)
        self.assertEqual(before, after)
        self.assertEqual(len(after), 250)
        self.assertTrue(all(not item['fully_verified'] for item in after))

    def test_nonHTTP_apply_rejects_changed_source(self):
        rows = json.loads((self.root / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        entry = next(x for x in rows if x['candidate_id'] == module.MODEL_ID)
        entry['source']['kind'] = 'management_command'
        with self.assertRaisesRegex(AssertionError, 'source identity'):
            source_contracts.apply_non_HTTP(self.root, rows)

    def test_nonHTTP_apply_rejects_already_complete(self):
        rows = json.loads((self.root / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        next(x for x in rows if x['candidate_id'] == module.MODEL_ID)['fully_verified'] = True
        with self.assertRaises(AssertionError):
            source_contracts.apply_non_HTTP(self.root, rows)

    def test_nonHTTP_apply_rejects_conflicting_inputs(self):
        p = self.root / 'scripts/rust-contracts/executable-source-inputs.json'
        value = json.loads(p.read_text())
        selected = next(x for x in value['inputs'] if x['kind'] == 'pinned_original_account_settings_tests')
        value['inputs'].append(selected)
        p.write_text(json.dumps(value))
        rows = json.loads((self.root / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        with self.assertRaisesRegex(AssertionError, 'conflicting nonHTTP'):
            source_contracts.apply_non_HTTP(self.root, rows)

    def test_exact80_historical_successes(self):
        value = module.validate(self.root, self.value)
        self.assertEqual(value['HTTP_assignments'], 16)
        self.assertEqual(value['nonHTTP_assignments'], 1)
        self.assertEqual(value['historical_unique_tests'], 80)

    def test_wrong_source_revision_file(self):
        p = self.root / ORIGINAL['original_test_files'][0]['path']
        p.write_bytes(p.read_bytes() + b'\n')
        self.reject(lambda v: None)

    def test_wrong_original_authority(self):
        self.reject(lambda v: v['original_test_files'][0].update(sha256='0' * 64))

    def test_wrong_methodless_declaration(self):
        self.reject(lambda v: v['contracts'][1].update(method='PUT'))

    def test_wrong_alias(self):
        self.reject(lambda v: v['contracts'][0].update(pattern_exact='api/accounts/users/me.json'))

    def test_wrong_action(self):
        self.reject(lambda v: v['contracts'][0].update(source_action='update'))

    def test_wrong_milestone(self):
        self.reject(lambda v: v['contracts'][0].update(owning_milestone='M4'))

    def test_wrong_named_test(self):
        self.reject(lambda v: v['contracts'][0]['cases'][0].update(test_id='invented.test'))

    def test_wrong_ast(self):
        self.reject(lambda v: v['contracts'][0]['cases'][0].update(source_ast_sha256='0' * 64))

    def test_erased_assertions(self):
        self.reject(lambda v: v['contracts'][0]['cases'][0].update(assertion_expressions=[]))

    def test_faked_workflow_assertions(self):
        self.reject(lambda v: v['contracts'][0]['assertions'].__setitem__(0, 'all Native roles pass'))

    def test_wrong_request_coordinate(self):
        self.reject(lambda v: v['contracts'][0]['cases'][0]['selected_request_sites'][0].update(HTTP_method_observed='POST'))

    def test_actor_is_not_target_user(self):
        self.reject(lambda v: v['contracts'][0]['cases'][0]['selected_request_sites'][0]['actor'].update(expression='self.admin'))

    def test_api_key_not_forced_auth(self):
        contract = next(c for c in self.value['contracts'] if any('test_api_key_auth_works' in x['test_id'] for x in c['cases']))
        case = next(c for c in contract['cases'] if c['method'] == 'test_api_key_auth_works')
        self.assertIn('API-key', case['selected_request_sites'][0]['actor']['kind'])
        self.reject(lambda v: case['selected_request_sites'][0]['actor'].update(kind='forced administrator'))

    def test_missing_level_is_streamer0(self):
        self.reject(lambda v: v['missing_user_level_authority'].update(value=1))

    def test_wrong_created_role_definition(self):
        fixture = self.value['fixtures'][self.value['contracts'][0]['fixture_id']]
        key = next(iter(fixture['roles']))
        self.reject(lambda v: fixture['roles'][key].update(expression='admin'))

    def test_fixture_labels_not_arbitrary(self):
        fixture = self.value['fixtures'][self.value['contracts'][0]['fixture_id']]
        self.reject(lambda v: fixture['execution']['argv'].append('apps.vod.tests'))

    def test_not_fresh_execution(self):
        self.reject(lambda v: v['contracts'][0]['result'].update(new_Source_cases_this_task=1))

    def test_no_endpoint_completion(self):
        self.reject(lambda v: v['contracts'][0].update(fully_verified=True))

    def test_wrong_model_candidate(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0].update(candidate_id='candidate-invented'))

    def test_wrong_model_source_kind(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['source'].update(kind='management_command'))

    def test_model_no_REST_promotion(self):
        model = self.value['non_HTTP_contracts'][0]
        fixture = self.value['fixtures'][model['fixture_id']]
        self.reject(lambda v: fixture.update(scope='CoreSettings REST allmethods'))

    def test_model_test_not_HTTP(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['cases'][0].update(transport='actual TCP'))

    def test_model_result_not_fresh(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['result'].update(new_Source_cases_this_task=35))

    def test_model_no_complete_candidate(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0].update(fully_verified=True))

    def test_actual_named_log_authority_required(self):
        p = self.root / 'scripts/rust-contracts/accounts-settings/observations.json'
        text = p.read_text()
        p.write_text(text.replace('historical_result', 'invented_result', 1))
        self.reject(lambda v: None)

    def test_unrelated_methods_not_source_paths(self):
        self.reject(lambda v: v.update(unique_historical_tests=81))

    def test_no_native_coverage_claim(self):
        self.reject(lambda v: v.update(Native_coverage_increased=True))


if __name__ == '__main__':
    unittest.main()
