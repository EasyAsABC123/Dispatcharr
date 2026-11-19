"""Validate sanitized, terminal Original Django admin observations.

Application and algorithm credit belongs to the original Dispatcharr authors.
"""
from collections import Counter
import re

from source_contracts import checked

KIND = 'retained_original_admin_model_TCP'
INPUT = 'scripts/rust-contracts/retained-admin-models/bindings.json'
SHA256 = '549a30ca43da1120ed4a3108d314a1ff79123ecc1d890f6acb8b24b683fc7e66'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert load(root) == value and value['binding_kind'] == KIND
    assert value['operation_count'] == len(value['contracts']) == 103
    assert value['unique_Source_cases'] == 283 and value['actual_HTTP_attempts'] == 286
    assert value['fully_verified_operations'] == value['new_Source_executions'] == 0
    assert not value['Native_coverage_increased']
    fixture = value['fixtures']['Original-admin-model-callbacks-actual']
    terminal = fixture['actual_Terminal_cleanup_verification']
    assert terminal['actual_controller_exit'] == 1
    assert terminal['exact_owned_cleanup_verified'] is True
    assert terminal['selected_application_cells_passed'] is False
    assert value['actual_blocker_count'] == 10
    assert value['actual_model_outcome_counts'] == {
        'observed': 199, 'blocked_or_expectation_failed_after_capture': 7,
        'NOT_SELECTED_IN_133_MODEL_CALLBACK_SET': 1,
    }
    assert len(value['login_controls_already_scoped']) == 3
    assert not any(row['admitted_as_new_operation'] for row in value['login_controls_already_scoped'])
    observed = Counter()
    seen = set()
    failed_expectation_responses = 0
    callbacks = Counter()
    for contract in value['contracts']:
        assert contract['method'] is None and contract['source_action'] is None
        assert contract['contract_id'].endswith(':METHOD_UNRESOLVED')
        assert not contract['fully_verified']
        assert contract['fixture_id'] == 'Original-admin-model-callbacks-actual'
        assert contract['route_name'].startswith('admin:')
        assert contract['pattern_exact'].startswith('admin/')
        pattern = contract['pattern_exact'].replace('<path:object_id>', r'[0-9]+')
        cases = contract['cases']
        assert contract['observed_roles'] == sorted({row['role'] for row in cases})
        assert contract['observed_methods'] == sorted({row['method'] for row in cases})
        assert contract['result']['Source_case_observations'] == len(cases)
        assert contract['result']['new_Source_cases_this_task'] == 0
        assert contract['result']['Native_coverage_increased'] is False
        callbacks[cases[0]['admin_callback']] += 1
        for row in cases:
            assert row['source_index'] not in seen
            seen.add(row['source_index'])
            assert re.fullmatch(pattern, row['path'].lstrip('/'))
            expected_name = 'admin:' + row['model'].replace('.', '_') + '_' + row['admin_callback']
            assert contract['route_name'] == expected_name
            assert row['role'] in fixture['actor_roles']
            assert row['method'] in ('GET', 'POST')
            if row['role'] == 'staff_without_model_permission':
                assert row['method'] == 'GET' and row['status'] == 403
            for column in ('private_wire_sha256', 'response_body_sha256', 'SQL_before_sha256',
                           'SQL_after_sha256'):
                assert re.fullmatch(r'[0-9a-f]{64}', row[column])
            if row['semantic_outcome'] == 'blocked_or_expectation_failed_after_capture':
                assert not row['semantic_workflow_witness']
                failed_expectation_responses += 1
            observed[row['method'], row['role'], row['status']] += 1
    assert seen == set(range(4, 287))
    assert callbacks == {'changelist': 26, 'add': 25, 'change': 26, 'delete': 26}
    assert failed_expectation_responses == 12
    assert observed == {
        ('GET', 'staff_without_model_permission', 403): 103,
        ('GET', 'superuser', 200): 101,
        ('GET', 'superuser', 500): 2,
        ('POST', 'superuser', 200): 5,
        ('POST', 'superuser', 302): 72,
    }


def restore_pre_admin_models(root, rows):
    """Reverse only this retained binding when reviewing earlier exact registries."""
    value = load(root)
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for contract in value['contracts']:
        operation = operations[contract['contract_id']]
        assert operation.pop('executable_Source_contract') == contract
        operation.update(setup_fixture=None, assertions=[], result=None,
                         coverage_state='scoped_evidence_needs_case_mapping'
                         if operation['related_evidence'] else 'not_yet_mapped')
        for key in ('observed_fixture_definitions', 'complete', 'observed_roles'):
            operation['role_matrix'].pop(key)
        operation['role_matrix']['state'] = 'unmapped'
