"""Pure corruption controls for explicit DVR/EPG framework-source scope."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import dvr_import_contracts as module

ROOT = Path(__file__).resolve().parents[2]


class DvrImportControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = json.loads((ROOT / (module.PREFIX + 'bindings.json')).read_text())

    def reject(self, mutate):
        value = copy.deepcopy(self.value)
        mutate(value)
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            module.validate(ROOT, value)

    def factory(self, value):
        return next(site for contract in value['contracts'] for case in contract['cases'] for site in case['request_sites'] if 'view_action' in site)

    def test_exact13_55_90(self):
        self.assertEqual(module.validate(ROOT, self.value)['unique_historical_tests'], 55)
        self.assertEqual(self.value['operation_count'], 13)
        self.assertEqual(self.value['static_request_sites'], 90)

    def test_five_null_methods_preserved(self):
        self.assertEqual(sum(row['method'] is None for row in self.value['contracts']), 5)
        self.reject(lambda value: next(row for row in value['contracts'] if row['method'] is None).update(method='POST'))

    def test_wrong_operation(self):
        self.reject(lambda value: value['contracts'][0].update(contract_id='invented'))

    def test_wrong_source_action(self):
        self.reject(lambda value: value['contracts'][0].update(source_action='retrieve'))

    def test_wrong_pattern(self):
        self.reject(lambda value: value['contracts'][0].update(pattern_exact='api/channels/recordings.json'))

    def test_factory_is_not_HTTP(self):
        self.reject(lambda value: self.factory(value).update(HTTP_dispatch_proved=True))

    def test_factory_is_not_router_dispatch(self):
        self.reject(lambda value: self.factory(value).update(router_dispatch_proved=True))

    def test_factory_is_not_TCP(self):
        self.reject(lambda value: self.factory(value).update(transport='actual HTTP/TCP'))

    def test_factory_as_view_action(self):
        self.reject(lambda value: self.factory(value).update(view_action={'post': 'destroy'}))

    def test_factory_explicit_PK_binding(self):
        self.reject(lambda value: self.factory(value).update(symbolic_view_pk='foreign'))

    def test_factory_force_authenticate_preserved(self):
        self.reject(lambda value: self.factory(value).update(force_authenticate_expression='missing'))

    def test_factory_admin_default_is_conditional(self):
        actor = self.factory(self.value)['actor']
        self.assertIsNone(actor['effective_user_level'])
        self.assertEqual(actor['declared_create_default_user_level'], 10)
        self.assertFalse(actor['existing_row_level_asserted'])
        self.reject(lambda value: self.factory(value)['actor'].update(existing_row_level_asserted=True))

    def test_keyword_only_fixture_default_not_User_field_default(self):
        fixtures = list(self.value['fixtures'].values())
        self.assertTrue(all(fixture['omitted_User_field_level_default'] == 0 for fixture in fixtures))
        sites = [site for row in self.value['contracts'] for case in row['cases'] for site in case['request_sites'] if 'DVR_fixture_argument' in site]
        self.assertTrue(any(site['actor']['effective_user_level'] == 1 and not site['DVR_fixture_argument']['fixture_key_added'] for site in sites))

    def test_DVR_access_argument_kept(self):
        def mutate(value):
            site = next(site for row in value['contracts'] for case in row['cases'] for site in case['request_sites'] if 'DVR_fixture_argument' in site)
            site['DVR_fixture_argument']['fixture_dvr_access_argument'] = 'manage'
        self.reject(mutate)

    def test_DVR_constants_not_invented(self):
        self.reject(lambda value: next(iter(value['fixtures'].values()))['DVR_fixture_constants'].update(DVR_ACCESS_VIEW='manage'))

    def test_EPG_delay_is_not_Celery_execution(self):
        def mutate(value):
            case = next(case for row in value['contracts'] for case in row['cases'] if case['class'] == 'EPGImportAPITests')
            case['Celery_execution_proved'] = True
        self.reject(mutate)

    def test_EPG_delay_mock_retained(self):
        def mutate(value):
            case = next(case for row in value['contracts'] for case in row['cases'] if case['class'] == 'EPGImportAPITests')
            case['decorators'].clear()
        self.reject(mutate)

    def test_original_assertions_retained(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['assertion_expressions'].clear())

    def test_helper_chain_retained(self):
        self.reject(lambda value: self.factory(value)['helper_call_chain'].clear())

    def test_original_test_AST(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(source_ast_sha256='0'*64))

    def test_mocked_thread_scope_retained(self):
        def mutate(value):
            case = next(case for row in value['contracts'] for case in row['cases'] if case['class'] == 'StopEndpointTests' and case['patch_contexts'])
            case['patch_contexts'].clear()
        self.reject(mutate)

    def test_source_setup_context_retained(self):
        self.reject(lambda value: next(iter(value['fixtures'].values()))['class_fixture_definitions'].clear())

    def test_historical_ok_not_invented(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(historical_result='newly passed'))

    def test_log_block_not_invented(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(framework_log_block='fake ... ok'))

    def test_raw_log_offsets_not_invented(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0].update(full_log_byte_start=0))

    def test_new_Source_run_not_invented(self):
        self.reject(lambda value: value.update(fresh_Source_case_count=55))

    def test_Native_not_invented(self):
        self.reject(lambda value: value.update(Native_coverage_increased=True))

    def test_full_verification_not_invented(self):
        self.reject(lambda value: value['contracts'][0].update(fully_verified=True))

    def test_queryset_helpers_and_existing_me_PATCH_excluded(self):
        self.assertEqual(len(self.value['excluded_test_methods']), 3)
        self.assertTrue(any('queryset_helper' in label for label in self.value['excluded_test_methods']))
        self.assertTrue(any('self_grant' in label for label in self.value['excluded_test_methods']))

    def test_missing_Source_authority_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                module.validate(Path(tmp), self.value)
