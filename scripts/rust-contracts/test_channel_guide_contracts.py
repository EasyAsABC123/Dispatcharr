"""Pure Source-assignment corruption controls; no Original import/workload."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import channel_guide_contracts as module

ROOT = Path(__file__).resolve().parents[2]


class ChannelGuideControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = json.loads((ROOT / (module.PREFIX + 'bindings.json')).read_text())

    def reject(self, mutate):
        value = copy.deepcopy(self.value)
        mutate(value)
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            module.validate(ROOT, value)

    def test_exact26_167_206(self):
        result = module.validate(ROOT, self.value)
        self.assertEqual(result['operations'], 26)
        self.assertEqual(result['unique_historical_tests'], 167)
        self.assertEqual(result['static_request_sites'], 206)

    def test_qualified_user_levels_and_anonymous(self):
        sites = [site for contract in self.value['contracts'] for case in contract['cases'] for site in case['request_sites']]
        self.assertEqual({site['actor']['effective_user_level'] for site in sites}, {None, 0, 1, 5, 10})
        self.assertTrue(all('credential backend bypassed' in site['actor']['kind'] or 'anonymous' in site['actor']['kind'] for site in sites))

    def test_five_methods_stay_null(self):
        self.assertEqual(sum(contract['method'] is None for contract in self.value['contracts']), 5)
        self.assertTrue(all(not contract['fully_verified'] for contract in self.value['contracts']))

    def test_changed_candidate_operation(self):
        self.reject(lambda value: value['contracts'][0].update(contract_id='invented'))

    def test_changed_route_pattern(self):
        self.reject(lambda value: value['contracts'][0].update(pattern_exact='api/channels/channels.json'))

    def test_changed_view_handler(self):
        self.reject(lambda value: value['contracts'][0].update(source_handler='OtherView'))

    def test_changed_action(self):
        self.reject(lambda value: value['contracts'][0].update(source_action='destroy'))

    def test_observed_method_not_declaration(self):
        self.reject(lambda value: next(contract for contract in value['contracts'] if contract['method'] is None).update(method='GET'))

    def test_invented_HEAD(self):
        self.reject(lambda value: value['contracts'][0].update(observed_request_methods=['HEAD']))

    def test_original_role_level_changed(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['request_sites'][0]['actor'].update(effective_user_level=0))

    def test_forced_auth_not_backend_auth(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['request_sites'][0]['actor'].update(kind='actual JWT authentication'))

    def test_factory_not_TCP(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['request_sites'][0].update(transport='APIRequestFactory full TCP'))

    def test_static_sites_not_live_counts(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['request_sites'][0].update(site_kind='actual wire request count'))

    def test_invented_successful_test(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(test_id='made_up'))

    def test_changed_test_AST(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(source_ast_sha256='0'*64))

    def test_removed_assertion(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['assertion_expressions'].pop())

    def test_changed_setup(self):
        self.reject(lambda value: next(iter(value['fixtures'].values()))['class_fixture_definitions'].clear())

    def test_removed_mock_context(self):
        def mutate(value):
            case = next(case for contract in value['contracts'] for case in contract['cases'] if case['patch_contexts'])
            case['patch_contexts'].clear()
        self.reject(mutate)

    def test_changed_helper_chain(self):
        def mutate(value):
            site = next(site for contract in value['contracts'] for case in contract['cases'] for site in case['request_sites'] if site['helper_call_chain'])
            site['helper_call_chain'][0]['helper_ast_sha256'] = '0'*64
        self.reject(mutate)

    def test_changed_helper_assertion(self):
        def mutate(value):
            case = next(case for contract in value['contracts'] for case in contract['cases'] if any(case['helper_assertion_expressions'].values()))
            next(assertions for assertions in case['helper_assertion_expressions'].values() if assertions).pop()
        self.reject(mutate)

    def test_changed_log_block(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(framework_log_block='invented ... ok'))

    def test_changed_log_offset(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(full_log_byte_start=0))

    def test_changed_historical_result(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(historical_result='skipped'))

    def test_fresh_execution_claim(self):
        self.reject(lambda value: value.update(fresh_Source_case_count=167))

    def test_Native_claim(self):
        self.reject(lambda value: value.update(Native_coverage_increased=True))

    def test_fully_verified_claim(self):
        self.reject(lambda value: value['contracts'][0].update(fully_verified=True))

    def test_complete_role_matrix_claim(self):
        self.reject(lambda value: next(iter(value['contracts'][0]['source_role_specs'].values())).update(complete_role_matrix=True))

    def test_omitted_level_default_remains_Original_zero(self):
        fixture = next(iter(self.value['fixtures'].values()))
        self.assertEqual(fixture['omitted_user_level_actual_default'], 0)
        self.reject(lambda value: next(iter(value['fixtures'].values())).update(omitted_user_level_actual_default=1))

    def test_excluded_model_helper_not_assigned(self):
        self.assertTrue(any('ChannelManagerEffectiveValuesTests' in label for label in self.value['excluded_test_methods']))
        self.assertFalse(any('SeriesRuleAPITests' in case['test_id'] for contract in self.value['contracts'] for case in contract['cases']))

    def test_wrong_original_authority(self):
        self.reject(lambda value: value['original_test_files'][0].update(sha256='0'*64))

    def test_missing_authority_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                module.validate(Path(tmp), self.value)

    def test_unbounded_unknown_URL_helper_refused(self):
        import ast
        with self.assertRaises(ValueError):
            module.source_url(ast.parse('untrusted_url_builder()', mode='eval').body, {}, {}, {})
