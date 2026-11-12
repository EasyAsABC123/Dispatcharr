"""Public-only provider/output/consumer rejection and typed-apply controls."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import provider_output_contracts as module
import source_contracts

ROOT = Path(__file__).resolve().parents[2]


class ProviderOutputControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = json.loads((ROOT / (module.PREFIX + 'bindings.json')).read_text())

    def reject(self, mutate):
        value = copy.deepcopy(self.value); mutate(value)
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            module.validate(ROOT, value)

    def output(self, value):
        return next(contract for contract in value['contracts'] if contract['source_handler'] == 'apps.output.views.m3u_endpoint')

    def consumer(self, value):
        return value['non_HTTP_contracts'][0]

    def test_exact9_1_34(self):
        self.assertEqual(module.validate(ROOT, self.value)['unique_historical_workflows'], 34)
        self.assertEqual(self.value['operation_count'], 9)
        self.assertEqual(len(self.value['non_HTTP_contracts']), 1)

    def test_two_output_methods_remain_null(self):
        self.assertEqual(sum(contract['method'] is None for contract in self.value['contracts']), 2)
        self.reject(lambda value: self.output(value).update(method='GET'))

    def test_old250_identity_idempotent_apply(self):
        before = json.loads((ROOT / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        after = copy.deepcopy(before)
        self.assertEqual(source_contracts.apply_non_HTTP(ROOT, after)['candidates'], 8)
        self.assertEqual(before, after)
        self.assertTrue(all(not row['fully_verified'] for row in after))

    def test_changed_consumer_source_rejected(self):
        rows = json.loads((ROOT / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        next(row for row in rows if row['candidate_id'] == module.CONSUMER['candidate_id'])['source']['line'] += 1
        with self.assertRaises(AssertionError):
            source_contracts.apply_non_HTTP(ROOT, rows)

    def test_consumer_already_fully_verified_rejected(self):
        rows = json.loads((ROOT / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        next(row for row in rows if row['candidate_id'] == module.CONSUMER['candidate_id'])['fully_verified'] = True
        with self.assertRaises(AssertionError):
            source_contracts.apply_non_HTTP(ROOT, rows)

    def test_consumer_candidate_kind_guard(self):
        self.reject(lambda value: self.consumer(value)['source'].update(kind='task_candidate'))

    def test_consumer_id_guard(self):
        self.reject(lambda value: self.consumer(value).update(candidate_id='invented'))

    def test_consumer_class_AST_guard(self):
        self.reject(lambda value: self.consumer(value).update(definition_ast_sha256='0'*64))

    def test_inherited_output_mixin_kept(self):
        def mutate(value):
            fixture = value['fixtures'][self.output(value)['fixture_id']]
            next(iter(fixture['class_fixture_definitions'].values()))['inherited_OutputEndpointTestMixin'] = None
        self.reject(mutate)

    def test_output_real_ACL_claim_rejected(self):
        self.reject(lambda value: self.output(value)['cases'][0]['sites'][0].update(transport='real network ACL and TCP'))

    def test_output_real_Redis_cache_ownership_rejected(self):
        def mutate(value):
            fixture = value['fixtures'][self.output(value)['fixture_id']]
            next(iter(fixture['class_fixture_definitions'].values()))['real_ACL_Redis_cache_ownership_proven'] = True
        self.reject(mutate)

    def test_named_reverse_not_guessed_alias(self):
        self.reject(lambda value: self.output(value)['cases'][0]['sites'][0]['named_reverse_resolution'].update(reverse_name='output:unobserved'))

    def test_root_namespace_mount_guard(self):
        self.reject(lambda value: self.output(value)['cases'][0]['sites'][0]['named_reverse_resolution'].update(root_namespace_mount_ast_sha256='0'*64))

    def test_output_credential_claim_rejected(self):
        self.reject(lambda value: self.output(value)['cases'][0]['sites'][0]['actor'].update(kind='actual API-key authentication'))

    def test_provider_delay_mock_kept(self):
        def mutate(value):
            case = next(case for contract in value['contracts'] for case in contract['cases'] if case['method'] == 'test_admin_can_call_custom_actions')
            case['decorators'].clear()
        self.reject(mutate)

    def test_provider_proxy_stop_mock_kept(self):
        def mutate(value):
            contract = next(contract for contract in value['contracts'] if contract['method'] == 'DELETE')
            fixture = value['fixtures'][contract['fixture_id']]
            next(iter(fixture['class_fixture_definitions'].values()))['setup'] = 'unmocked proxy stop'
        self.reject(mutate)

    def test_provider_actual_level_guard(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['sites'][0]['actor'].update(effective_user_level=999))

    def test_synthetic_consumer_user_not_credentials(self):
        self.reject(lambda value: self.consumer(value)['cases'][0]['sites'][0]['synthetic_user'].update(constructor='real JWT authenticated ASGI scope'))

    def test_real_WebSocket_broker_claim_rejected(self):
        self.reject(lambda value: self.consumer(value)['cases'][0]['sites'][0].update(actual_WebSocket_ASGI_credential_broker_claim=True))

    def test_AsyncMock_send_not_real_socket(self):
        self.reject(lambda value: self.consumer(value)['cases'][0]['sites'][0].update(send='real connected socket'))

    def test_connect_method_not_invented(self):
        self.reject(lambda value: self.consumer(value).update(observed_methods=['connect', 'receive', 'update']))

    def test_serializer_only_factory_not_account_GET(self):
        self.assertFalse(any(contract['contract_id'] == 'route-ab83774ea9ffdfb35ed4:GET' for contract in self.value['contracts']))
        self.reject(lambda value: value['contracts'].append(dict(value['contracts'][0], contract_id='route-ab83774ea9ffdfb35ed4:GET')))

    def test_Original_assertions_kept(self):
        self.reject(lambda value: value['contracts'][0]['cases'][0]['assertion_expressions'].clear())

    def test_named_historical_ok_not_invented(self):
        self.reject(lambda value: self.consumer(value)['cases'][0].update(framework_log_block='fake ... ok'))

    def test_historical_byte_offset_guard(self):
        self.reject(lambda value: self.consumer(value)['cases'][0].update(full_log_byte_start=0))

    def test_new_Source_not_invented(self):
        self.reject(lambda value: value.update(new_Source_cases_this_task=34))

    def test_Native_and_full_verification_not_invented(self):
        self.reject(lambda value: value.update(Native_coverage_increased=True, fully_verified_added=10))

    def test_missing_authority_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FileNotFoundError):
                module.validate(Path(temporary), self.value)
