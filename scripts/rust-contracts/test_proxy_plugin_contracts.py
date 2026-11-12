"""Portable exact Source-definition controls; no Django, credentials or requests."""
import copy
import json
from pathlib import Path
import re
import tempfile
import unittest
import proxy_plugin_contracts as module
import source_contracts

ROOT = Path(__file__).resolve().parents[2]


class ProxyPluginControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = json.loads((ROOT / (module.PREFIX + 'bindings.json')).read_text())

    def reject(self, change):
        value = copy.deepcopy(self.value)
        change(value)
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            module.validate(ROOT, value)

    def cases(self, value):
        return [case for contract in value['contracts'] for case in contract['cases']]

    def sites(self, value):
        return [site for case in self.cases(value) for site in case['sites']]

    def test_exact19_148_258(self):
        self.assertEqual(module.validate(ROOT, self.value)['historical_named_cases'], 148)
        self.assertEqual(self.value['operation_count'], 19)
        self.assertEqual(self.value['static_source_helper_sites'], 258)

    def test_eighteen_method_null_not_guessed(self):
        self.assertEqual(sum(c['method'] is None for c in self.value['contracts']), 18)
        self.reject(lambda v: v['contracts'][1].update(method='POST'))

    def test_static_sites_not_runtime_requests(self):
        self.assertTrue(all(s['static_site_not_runtime_request_or_branch_count'] for s in self.sites(self.value)))
        self.reject(lambda v: self.sites(v)[0].update(static_site_not_runtime_request_or_branch_count=False))

    def test_factory_dispatch_not_TCP(self):
        sites = [s for s in self.sites(self.value) if s['transport'] == 'direct framework factory endpoint only']
        self.assertTrue(sites)
        self.assertTrue(all(s['matching_endpoint_dispatch'] for s in sites))
        self.reject(lambda v: next(s for s in self.sites(v) if s['matching_endpoint_dispatch']).update(matching_endpoint_dispatch=[]))

    def test_dispatch_AST_kept(self):
        self.reject(lambda v: next(s for s in self.sites(v) if s['matching_endpoint_dispatch'])['matching_endpoint_dispatch'][0].update(source_ast_sha256='0'*64))

    def test_literal_URI_kept(self):
        self.reject(lambda v: self.sites(v)[0].update(fixture_URI_or_template='/invented/'))

    def test_helper_call_arguments_kept(self):
        self.reject(lambda v: next(s for s in self.sites(v) if len(s['helper_path']) > 1).update(helper_path=['invented']))

    def test_symbolic_UUID_not_actual_token(self):
        sites = [s for s in self.sites(self.value) if '{UUID4:uuid.uuid4()}' in s['fixture_URI_or_template']]
        self.assertTrue(sites)
        self.reject(lambda v: next(s for s in self.sites(v) if '{UUID4:uuid.uuid4()}' in s['fixture_URI_or_template']).update(fixture_URI_or_template='/concrete-invented-uuid'))

    def test_response_ID_template_not_executed(self):
        sites = [s for s in self.sites(self.value) if any('session_id' in sym for sym in s['unresolved_template_symbols'])]
        self.assertTrue(sites)
        self.reject(lambda v: next(s for s in self.sites(v) if any('session_id' in sym for sym in s['unresolved_template_symbols'])).update(unresolved_template_symbols=[]))

    def test_super_helper_AST_kept(self):
        cases = [c for c in self.cases(self.value) if any(name.startswith('super:') for name in c['helper_definitions'])]
        self.assertTrue(cases)
        self.reject(lambda v: next(c for c in self.cases(v) if any(name.startswith('super:') for name in c['helper_definitions'])).update(helper_definitions={}))

    def test_User_omitted_level_zero(self):
        self.assertTrue(all(c['role_context']['omitted_User_level_default'] == 0 for c in self.cases(self.value)))
        self.reject(lambda v: self.cases(v)[0]['role_context'].update(omitted_User_level_default=1))

    def test_fixture_role_chronology_not_complete_matrix(self):
        self.assertTrue(all(c['role_context']['complete_role_matrix'] is False for c in self.cases(self.value)))
        self.reject(lambda v: self.cases(v)[0]['role_context'].update(complete_role_matrix=True))

    def test_permission_bypass_retained(self):
        self.assertTrue(any(c['role_context']['permission_bypass_expressions'] for c in self.cases(self.value)))
        self.reject(lambda v: next(c for c in self.cases(v) if c['role_context']['permission_bypass_expressions'])['role_context'].update(permission_bypass_expressions=[]))

    def test_forced_authentication_retained(self):
        self.assertTrue(any(c['role_context']['forced_authentication_expressions'] for c in self.cases(self.value)))
        self.reject(lambda v: next(c for c in self.cases(v) if c['role_context']['forced_authentication_expressions'])['role_context'].update(forced_authentication_expressions=[]))

    def test_synthetic_actors_not_DB_users(self):
        declarations = [d for c in self.cases(self.value) for d in c['role_context']['fixture_user_declarations']]
        self.assertTrue(any(d['synthetic_mock_not_DB_user'] for d in declarations))
        self.reject(lambda v: next(d for c in self.cases(v) for d in c['role_context']['fixture_user_declarations'] if d['synthetic_mock_not_DB_user']).update(synthetic_mock_not_DB_user=False))

    def test_XC_auth_error_scope_not_info_helpers(self):
        cases = [c for c in self.cases(self.value) if c['source_path'].endswith('test_xc_auth_responses.py')]
        self.assertTrue(cases)
        self.assertTrue(all('auth' in c['test_id'].lower() or 'error' in c['test_id'].lower() or 'reject' in c['test_id'].lower() for c in cases))
        self.reject(lambda v: next(c for c in self.cases(v) if c['source_path'].endswith('test_xc_auth_responses.py')).update(assertion_scope='real upstream and full endpoint parity'))

    def test_wrong_timeshift_entry_excluded(self):
        self.assertFalse(any(c['method'] == 'test_xc_entry_delegates_to_serve_catchup' for c in self.cases(self.value)))
        excluded = json.loads((ROOT / (module.PREFIX + 'excluded-coordinates.json')).read_text())
        self.assertIn('test_xc_entry_delegates_to_serve_catchup', json.dumps(excluded))

    def test_no_wrong_notification_alias(self):
        self.assertFalse(any('/api/system/notifications/' in s['fixture_URI_or_template'] for s in self.sites(self.value)))

    def test_no_actual_JWT(self):
        self.assertIsNone(re.search(r'eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}', json.dumps(self.value)))

    def test_wrong_stable_ID(self):
        self.reject(lambda v: v['contracts'][0].update(contract_id='invented'))

    def test_wrong_pattern(self):
        self.reject(lambda v: v['contracts'][0].update(pattern_exact='invented'))

    def test_wrong_handler(self):
        self.reject(lambda v: v['contracts'][0].update(source_handler='invented'))

    def test_wrong_owner(self):
        self.reject(lambda v: v['contracts'][0].update(owning_milestone='invented'))

    def test_wrong_action(self):
        self.reject(lambda v: v['contracts'][0].update(source_action='invented'))

    def test_missing_named_case(self):
        self.reject(lambda v: v['contracts'][0]['cases'].pop())

    def test_wrong_historical_result(self):
        self.reject(lambda v: self.cases(v)[0].update(historical_result='new execution'))

    def test_wrong_success_log(self):
        self.reject(lambda v: self.cases(v)[0].update(framework_log_block='invented ... ok'))

    def test_wrong_log_offset(self):
        self.reject(lambda v: self.cases(v)[0].update(full_log_byte_start=0))

    def test_wrong_test_AST(self):
        self.reject(lambda v: self.cases(v)[0].update(source_ast_sha256='0'*64))

    def test_assertion_definitions_kept(self):
        self.reject(lambda v: next(c for c in self.cases(v) if c['assertion_expressions']).update(assertion_expressions=[]))

    def test_inherited_setup_kept(self):
        self.reject(lambda v: self.cases(v)[0].update(class_and_inherited_setup=[]))

    def test_decorators_mocks_kept(self):
        self.reject(lambda v: next(c for c in self.cases(v) if c['decorators']).update(decorators=[]))

    def test_no_new_Source_execution(self):
        self.reject(lambda v: v.update(new_Source_cases_this_task=148))

    def test_no_Native_coverage(self):
        self.reject(lambda v: v.update(Native_coverage_increased=True))

    def test_no_full_verification(self):
        self.reject(lambda v: v['contracts'][0].update(fully_verified=True))

    def test_missing_Source_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(FileNotFoundError):
                module.validate(Path(temp), self.value)

    def test_nonHTTP250_unchanged(self):
        before = json.loads((ROOT / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        after = copy.deepcopy(before)
        self.assertEqual(source_contracts.apply_non_HTTP(ROOT, after)['candidates'], 8)
        self.assertEqual(before, after)
