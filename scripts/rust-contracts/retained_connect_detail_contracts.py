"""Validate retained Original Connect detail update and delete observations."""
from collections import Counter
import hashlib
import re

from source_contracts import checked


INPUT = 'scripts/rust-contracts/retained-connect-detail/bindings.json'
KIND = 'retained_original_Connect_detail_PUT_DELETE_HTTP'
SHA256 = '6583922626cc3930a8c9f85fee85229c6d9bf1a1f290e34bb0a39b36ed4c9dbb'
DETAIL = '/api/connect/integrations/'
ROUTE = 'route-a6f08b2b757cba093f6e'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert value == load(root) and value['binding_kind'] == KIND
    assert value['operation_count'] == len(value['contracts']) == len(value['fixtures']) == 2
    assert value['retained_Source_responses_with_literal_route_path'] == 21
    assert value['additional_symbolic_fixture_path_responses'] == 6
    assert value['fully_verified_operations'] == value['new_Source_executions'] == 0
    assert value['Native_coverage_increased'] is False
    expected = {
        'PUT': ('update', 18, 200, Counter({400: 8, 401: 2, 403: 3, 404: 1})),
        'DELETE': ('destroy', 9, 204, Counter({401: 2, 403: 3, 404: 2})),
    }
    for contract in value['contracts']:
        method = contract['method']
        source_action, count, success_status, error_counts = expected.pop(method)
        assert contract['contract_id'] == f'{ROUTE}:{method}' and contract['source_action'] == source_action
        assert contract['observed_methods'] == [method] and not contract['fully_verified']
        fixture = value['fixtures'][contract['fixture_id']]
        assert fixture['actual_runner_exit'] == 0 and fixture['exact_cleanup_passed']
        assert fixture['candidate_detail_method_exercised'] is False
        guard = fixture['owned_receiver_guard']
        assert guard['owned_receiver_before'] == guard['owned_receiver_after'] == 0
        assert guard[f'only_owned_integration_detail_{method.lower()}']
        if method == 'PUT':
            assert guard['put_count'] == count
        else:
            assert guard['delete_attempted'] == guard['delete_completed'] == count
        cases = contract['cases']
        symbolic = fixture['actual_successes_with_symbolic_ID']
        assert Counter(row['status'] for row in cases) == error_counts
        assert len(cases) + len(symbolic) == count
        assert len({row['observation_pointer'] for row in cases + symbolic}) == count
        assert contract['observed_roles'] == sorted({row['role'] for row in cases})
        assert contract['result']['Source_case_observations'] == len(cases)
        for row in cases + symbolic:
            assert row['method'] == method and re.fullmatch('[0-9a-f]{64}', row['body_sha256'])
        for case in cases:
            assert re.fullmatch(r'/api/connect/integrations/\d+/', case['path'])
            assert case['literal_path_verified'] and case['original_log_line'] > 0
            effect = ('database_references_and_sequences_unchanged' if method == 'PUT'
                      else 'all_connect_rows_and_sequences_unchanged')
            assert case['observed_effects'] == {effect: True}
        for case in symbolic:
            assert case['status'] == success_status and case['role'] == 'admin'
            assert case['literal_path_verified'] is False and case['original_log_line'] is None
            assert 'path' not in case
            if method == 'PUT':
                assert case['path_template'] == DETAIL + '{fixture_edited_id}/'
                assert case['observed_effects'] == {'exact_only_target_updated_with_references_and_sequences_preserved': True}
            else:
                assert case['body_bytes'] == 0 and case['body_sha256'] == hashlib.sha256(b'').hexdigest()
                expected_symbol, subscriptions, logs = {
                    'stock-delete-exact-cascade': ('target', 2, 3),
                    'childless-delete-ignores-unused-body': ('childless', 0, 0),
                }[case['case']]
                assert case['path_template'] == DETAIL + '{fixture_' + expected_symbol + '_id}/'
                assert case['observed_effects'] == {'exact_fixture_shape_observed': True,
                              'exact_cascade_with_unrelated_rows_and_sequences_preserved': True}
                assert case['removed_owned_SQL_rows'] == {'integrations': 1, 'subscriptions': subscriptions, 'delivery_logs': logs}
    assert not expected


def restore_pre_connect_detail(root, rows):
    """Restore the original unassigned operation dictionaries for history checks."""
    operations = {operation['contract_id']: operation for route in rows for operation in route['operations']}
    for contract in load(root)['contracts']:
        operation = operations[contract['contract_id']]
        assert operation.pop('executable_Source_contract') == contract
        assert not operation['related_evidence']
        operation.update(setup_fixture=None, assertions=[], result=None, coverage_state='not_yet_mapped')
        operation['role_matrix'] = {'state': 'unmapped', 'required_roles': []}
