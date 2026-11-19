"""Validate retained Original Connect collection response scopes and SQL results."""
from collections import Counter
import re

from source_contracts import checked

KIND = 'retained_original_Connect_collection_HTTP'
INPUT = 'scripts/rust-contracts/retained-connect/bindings.json'
SHA256 = '2baed770de482644dd8a6aa5a65d98fe196c6210aecca292093dcecac4afef5b'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert load(root) == value and value['binding_kind'] == KIND
    assert value['operation_count'] == len(value['contracts']) == 2
    assert value['retained_Source_responses'] == 24
    assert value['fully_verified_operations'] == value['new_Source_executions'] == 0
    assert value['Native_coverage_increased'] is False
    expected = {'GET': ('list', Counter({200: 3, 401: 3, 403: 2})),
                'POST': ('create', Counter({400: 8, 401: 2, 403: 3, 201: 3}))}
    for contract in value['contracts']:
        method = contract['method']
        action, counts = expected.pop(method)
        assert contract['contract_id'] == f'route-04cdc39881caace6c343:{method}'
        assert contract['source_action'] == action and contract['fully_verified'] is False
        assert contract['pattern_exact'] == 'api/connect/^integrations/$'
        assert contract['source_handler'] == 'apps.connect.api_views.IntegrationViewSet'
        fixture = value['fixtures'][contract['fixture_id']]
        assert fixture['actual_runner_exit'] == 1 and fixture['outer_passed'] is False
        assert fixture['other_failed_gate']['name'] == 'status-parity:/proxy/ts/stream/not-a-channel'
        assert fixture['other_failed_gate']['passed'] is False and fixture['exact_cleanup_passed'] is True
        cases = contract['cases']
        assert counts == Counter(case['status'] for case in cases)
        assert contract['observed_roles'] == sorted({case['role'] for case in cases})
        assert len({case['observation_pointer'] for case in cases}) == len(cases)
        for case in cases:
            assert case['method'] == method and case['path'] == '/api/connect/integrations/'
            assert re.fullmatch('[0-9a-f]{64}', case['response_projection_sha256'])
            if method == 'GET':
                assert case['observed_effects'] == []
            elif case['status'] == 201:
                assert case['observed_effects'] == ['exactly_one_integration_and_sequence_advance_without_subscriptions_or_deliveries',
                                                   'utc_creation_in_request_window']
            else:
                assert case['observed_effects'] == ['database_rows_and_sequence_unchanged']
        if method == 'POST':
            guard = fixture['owned_webhook_guard']
            assert guard['issued_only_original_collection_post'] and guard['original_creation_post_count'] == 16
            assert guard['actual_owned_webhook_requests_before'] == guard['actual_owned_webhook_requests_after'] == 0
            assert fixture['native_create_exercised'] is False
    assert not expected


def restore_pre_connect(root, rows):
    """Recover the exact preceding register for historical equality assertions."""
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for contract in load(root)['contracts']:
        operation = operations[contract['contract_id']]
        assert operation.pop('executable_Source_contract') == contract
        assert not operation['related_evidence']
        operation.update(setup_fixture=None, assertions=[], result=None, coverage_state='not_yet_mapped')
        operation['role_matrix'] = {'state': 'unmapped', 'required_roles': []}
