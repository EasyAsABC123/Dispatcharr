"""Validate literal Original Connect delivery-log and subscription-set bindings."""
from collections import Counter
import re

from source_contracts import checked

KIND = 'retained_original_Connect_log_read_and_subscription_set_HTTP'
INPUT = 'scripts/rust-contracts/retained-connect-actions/bindings.json'
SHA256 = 'b132c92fd411d9a6aa28afdb42972788ab84405f9062d105411856182e88f708'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert load(root) == value and value['binding_kind'] == KIND
    assert value['operation_count'] == len(value['contracts']) == 2
    assert value['retained_Source_responses_with_literal_route_path'] == 27
    assert value['additional_symbolic_fixture_path_responses'] == 4
    assert value['fully_verified_operations'] == value['new_Source_executions'] == 0
    assert value['Native_coverage_increased'] is False
    expected = {'GET': ('route-de258e62c08a0966ba7f:GET', 'list', Counter({200: 10, 401: 1, 403: 2, 404: 1})),
                'PUT': ('route-97c801e89fbcca3b37ea:PUT', 'set_subscriptions', Counter({400: 5, 401: 2, 403: 3, 404: 1, 500: 2}))}
    for contract in value['contracts']:
        method = contract['method']
        identifier, action, statuses = expected.pop(method)
        assert contract['contract_id'] == identifier and contract['source_action'] == action
        assert contract['fully_verified'] is False and contract['observed_methods'] == [method]
        fixture = value['fixtures'][contract['fixture_id']]
        assert fixture['actual_runner_exit'] == 0 and fixture['exact_cleanup_passed'] is True
        cases = contract['cases']
        assert statuses == Counter(case['status'] for case in cases)
        assert contract['observed_roles'] == sorted({case['role'] for case in cases})
        assert len({case['observation_pointer'] for case in cases}) == len(cases)
        assert contract['result']['Source_case_observations'] == len(cases)
        for case in cases:
            assert case['method'] == method
            assert re.fullmatch('[0-9a-f]{64}', case['response_projection_sha256'])
            if method == 'GET':
                assert case['path'] == '/api/connect/logs/'
                assert case['generated_query_values_are_symbolic'] == ('{' in case['query_scope'])
            else:
                assert re.fullmatch(r'/api/connect/integrations/[0-9]+/subscriptions/set/', case['path'])
                assert case['literal_path_verified'] and case['original_log_line'] > 0
                if case['case'] == 'stored-duplicate-partial-failure':
                    assert case['observed_effects'] == {'stale_log_and_subscription_removed_with_earlier_upserts_committed': True}
                else:
                    assert case['observed_effects'] == {'database_and_subscription_sequence_unchanged': True}
        if method == 'PUT':
            guard = fixture['owned_receiver_guard']
            assert guard['only_owned_subscription_put'] and guard['put_count'] == 17
            assert guard['owned_receiver_before'] == guard['owned_receiver_after'] == 0
            assert fixture['native_replace_exercised'] is False
            symbolic = fixture['actual_successes_with_symbolic_ID']
            assert len(symbolic) == 4 and all(row['status'] == 200 for row in symbolic)
            assert all(not row['literal_path_verified'] and row['original_log_line'] is None for row in symbolic)
            assert all(row['observed_effects']['exact_subscription_log_and_sequence_state'] for row in symbolic)
    assert not expected


def restore_pre_connect_actions(root, rows):
    """Recover the exact preceding register for historical equality assertions."""
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for contract in load(root)['contracts']:
        operation = operations[contract['contract_id']]
        assert operation.pop('executable_Source_contract') == contract
        assert not operation['related_evidence']
        operation.update(setup_fixture=None, assertions=[], result=None, coverage_state='not_yet_mapped')
        operation['role_matrix'] = {'state': 'unmapped', 'required_roles': []}
