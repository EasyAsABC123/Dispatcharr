"""Portable rejection controls, no Django imports/token/request execution."""
import copy
import json
from pathlib import Path
import re
import tempfile
import unittest
import admin_rest_contracts as module
import source_contracts

ROOT = Path(__file__).resolve().parents[2]


class AdminRestControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = json.loads((ROOT / (module.PREFIX + 'bindings.json')).read_text())

    def reject(self, mutate):
        value = copy.deepcopy(self.value); mutate(value)
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            module.validate(ROOT, value)

    def jwt(self, value):
        return next(site for contract in value['contracts'] for case in contract['cases'] for site in case['sites'] if 'JWT_helper_ast_sha256' in site['actor'])

    def branch(self, value):
        return next(site for contract in value['contracts'] for case in contract['cases'] for site in case['sites'] if site['finite_loop_branch'])

    def test_exact18_60_87(self):
        self.assertEqual(module.validate(ROOT, self.value)['unique_historical_workflows'], 60)
        self.assertEqual(self.value['operation_count'], 18)
        self.assertEqual(self.value['static_source_branch_sites'], 87)

    def test_fourteen_null_methods_stay_null(self):
        self.assertEqual(sum(contract['method'] is None for contract in self.value['contracts']), 14)
        self.reject(lambda value: value['contracts'][0].update(method='GET'))

    def test_JWT_helper_is_source_only_here(self):
        site = self.jwt(self.value)
        self.assertIn('no token helper execution/public token/new authentication run here', site['actor']['authentication_path'])
        self.reject(lambda value: self.jwt(value)['actor'].update(authentication_path='new actual credential execution'))

    def test_no_actual_JWT_in_bindings(self):
        self.assertIsNone(re.search(r'eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}', json.dumps(self.value)))

    def test_JWT_helper_AST_guard(self):
        self.reject(lambda value: self.jwt(value)['actor'].update(JWT_helper_ast_sha256='0'*64))

    def test_api_admin_flags_preserved(self):
        sites = [site for contract in self.value['contracts'] for case in contract['cases'] for site in case['sites'] if site['actor'].get('expression') == 'self.api_admin']
        self.assertTrue(sites)
        self.assertTrue(all(site['actor']['effective_user_level'] == 10 and site['actor']['explicit_staff_superuser_writes'] == ['self.api_admin.is_staff = False', 'self.api_admin.is_superuser = False'] for site in sites))

    def test_superuser_manager_default_not_guessed(self):
        sites = [site for contract in self.value['contracts'] for case in contract['cases'] for site in case['sites'] if 'source_manager_default' in site['actor']]
        self.assertTrue(sites)
        self.assertTrue(all(site['actor']['source_manager_default'] == 10 and site['actor']['manager_default_source_ast_sha256'] for site in sites))

    def test_omitted_User_field_default_still_zero(self):
        self.assertTrue(all(fixture['omitted_User_level_actual_default'] == 0 for fixture in self.value['fixtures'].values()))
        self.reject(lambda value: next(iter(value['fixtures'].values())).update(omitted_User_level_actual_default=1))

    def test_wrong_actor_level(self):
        self.reject(lambda value: self.jwt(value)['actor'].update(effective_user_level=999))

    def test_anonymous_token_path_not_Bearer(self):
        sites = [site for contract in self.value['contracts'] for case in contract['cases'] for site in case['sites'] if '?token=' in site['supplied_framework_path']]
        self.assertTrue(sites)
        self.assertTrue(all(site['actor']['effective_user_level'] is None for site in sites))

    def test_finite_URL_branch_guard(self):
        self.reject(lambda value: self.branch(value)['finite_loop_branch'][0].update(literal='/invented'))

    def test_finite_URL_not_runtime_count(self):
        self.assertIn('not a runtime request count', self.branch(self.value)['site_scope'])
        self.reject(lambda value: self.branch(value).update(site_scope='actual wire count'))

    def test_log_helper_methods_excluded(self):
        self.assertEqual(len(self.value['excluded_helper_only_methods']), 3)
        self.assertTrue(any('resolver_rejects_traversal' in label for label in self.value['excluded_helper_only_methods']))

    def test_backup_mocked_task_scope_kept(self):
        def mutate(value):
            case = next(case for contract in value['contracts'] for case in contract['cases'] if case['method'] == 'test_create_backup_success')
            case['decorators'].clear()
        self.reject(mutate)

    def test_notification_wrong_path_not_assigned(self):
        ids = {contract['contract_id'] for contract in self.value['contracts']}
        self.assertFalse(any(identifier.startswith('route-025c0d74fa99218d4728') for identifier in ids))
        self.assertFalse(any('SystemNotificationPermissionTests' in case['test_id'] for contract in self.value['contracts'] for case in contract['cases']))

    def test_old_slash_image_assignments_not_replaced(self):
        ids = {contract['contract_id'] for contract in self.value['contracts']}
        self.assertNotIn('route-56a108b56d175586118c:GET', ids)
        self.assertNotIn('route-715313bb016b528e1671:GET', ids)
        self.assertIn('route-b75988847c0358b96a1d:GET', ids)
        self.assertIn('route-b2b53c6ce1dbeea3e3d2:GET', ids)

    def test_image_outbound_mocks_kept(self):
        def mutate(value):
            case = next(case for contract in value['contracts'] for case in contract['cases'] if case['method'] == 'test_movie_image_accepts_slash_less_url')
            case['decorators'].clear()
        self.reject(mutate)

    def test_plugin_assertions_kept(self):
        def mutate(value):
            case = next(case for contract in value['contracts'] for case in contract['cases'] if case['class'] == 'PluginLogoAcceptHeaderTests')
            case['assertion_expressions'].clear()
        self.reject(mutate)

    def test_wrong_operation_id(self):
        self.reject(lambda value: value['contracts'][0].update(contract_id='invented'))

    def test_wrong_route_pattern(self):
        self.reject(lambda value: value['contracts'][0].update(pattern_exact='api/invented/'))

    def test_wrong_owner_or_action(self):
        self.reject(lambda value: value['contracts'][0].update(owning_milestone='M4', source_action='destroy'))

    def test_wrong_named_result_block(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(framework_log_block='invented ... ok'))

    def test_wrong_test_AST(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(source_ast_sha256='0'*64))

    def test_wrong_full_log_byte_offset(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(full_log_byte_start=0))

    def test_source_assertions_kept(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['assertion_expressions'].clear())

    def test_Native_not_invented(self):
        self.reject(lambda value: value.update(Native_coverage_increased=True))

    def test_new_Source_not_invented(self):
        self.reject(lambda value: value.update(new_Source_cases_this_task=60))

    def test_full_verification_not_invented(self):
        self.reject(lambda value: value['contracts'][0].update(fully_verified=True))

    def test_missing_Source_authority_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(FileNotFoundError):
                module.validate(Path(temp), self.value)

    def test_nonHTTP_all250_unchanged_idempotent(self):
        before = json.loads((ROOT / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        after = copy.deepcopy(before)
        self.assertEqual(source_contracts.apply_non_HTTP(ROOT, after)['candidates'], 8)
        self.assertEqual(before, after)
