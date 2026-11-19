"""Bind retained malformed-request and authorization observations.

Original Dispatcharr and dependency authors retain application and algorithm credit.
"""
import collections
import hashlib
import json
import re
from pathlib import Path

from source_contracts import checked, digest

KIND = 'retained_original_image22_serializer_null_TCP'
INPUT = 'scripts/rust-contracts/retained-serializer-null/bindings.json'
INPUT_SHA256 = '48462d0ec0a7404a1ab583bcfc9b2f68196fdecc0d4ed339b3ae909fb4fa30c2'
PUBLIC_RECEIPT = 'scripts/rust-integration/verified-integration-m5-auth-image22-1.json'
PUBLIC_SHA256 = 'e7cb41ab0e72d66c705d254ebd416f3098dd2c4ed797305dcd012883fd0fc4e8'
FIXTURE = 'retained-Original-Image22-serializer-null'


def dependencies():
    return {INPUT, PUBLIC_RECEIPT}


def validate(root, value):
    assert value == checked(root, {'path': INPUT, 'sha256': INPUT_SHA256})
    fixture = value['fixtures'][FIXTURE]
    assert fixture['public_historical_receipt'] == {'path': PUBLIC_RECEIPT, 'sha256': PUBLIC_SHA256}
    receipt = checked(root, fixture['public_historical_receipt'])
    assert receipt['passed'] is True
    authority = fixture['private_historical_capture']
    historical, = (row for row in receipt['runs'] if row['report'] == authority['controller']['path'])
    assert historical['sha256'] == authority['controller']['sha256']
    assert historical['cleanup'] == fixture['cleanup'] == 'project removed'
    assert historical['tool_source'] == authority['tool_source']
    assert historical['source_cases'] == value['original_corpus_size'] == 141
    assert historical['protected_tables'] == fixture['protected_model_tables'] == 16
    assert historical['no_model_mutations'] is fixture['all_original_no_model_mutations'] is True
    assert value['binding_kind'] == KIND
    assert value['operation_count'] == len(value['contracts']) == 23
    assert value['new_Source_executions'] == value['fully_verified_operations'] == 0
    assert value['Native_coverage_increased'] is False
    assert collections.Counter(row['method'] for row in value['contracts']) == {'POST':8,'PUT':9,'PATCH':6}
    matched = set()
    for contract in value['contracts']:
        assert contract['fixture_id'] == FIXTURE and contract['fully_verified'] is False
        assert contract['contract_id'].endswith(':' + contract['method'])
        assert contract['owning_milestone'] == 'M5'
        route = re.compile(contract['pattern_exact'].replace('/^','/'))
        cases = contract['cases']
        result = contract['result']
        assert result['Source_case_observations'] == len(cases)
        assert result['new_Source_cases_this_task'] == 0
        assert result['Native_coverage_increased'] is result['complete_role_matrix'] is False
        assert contract['observed_roles'] == sorted({case['role'] for case in cases})
        assert contract['source_role_specs'] == {
            role:fixture['roles'][role] for role in contract['observed_roles']
        }
        for case in cases:
            assert case['method'] == contract['method']
            assert case['HTTP_status'] in {400,401,403,404}
            assert case['no_model_mutations'] and route.fullmatch(case['path'].lstrip('/'))
            assert case['path'] != '/api/accounts/users/me/'
            index = case['source_index']
            assert index not in matched and case['record_json_pointer'] == f'/cases/{index}'
            matched.add(index)
            assert all(re.fullmatch('[0-9a-f]{64}',case[key])
                       for key in ('request_body_sha256','expected_response_projection_sha256'))
    prior = set(value['already_assigned_source_indices'])
    ambiguous = set(value['excluded_ambiguous_self_route_indices'])
    assert len(matched) == value['unique_Source_cases'] == 96
    assert len(prior) == 36 and ambiguous == set(range(132,141))
    assert len(matched | prior | ambiguous) == 141


def restore_pre_serializer_null(root, rows):
    """Remove only this cohort when verifying older register snapshots."""
    value = checked(root, {'path': INPUT, 'sha256': INPUT_SHA256})
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for contract in value['contracts']:
        operation = operations[contract['contract_id']]
        assert operation.pop('executable_Source_contract') == contract
        operation.update(setup_fixture=None, assertions=[], result=None,
                         coverage_state='scoped_evidence_needs_case_mapping'
                         if operation['related_evidence'] else 'not_yet_mapped')
        for field in ('observed_fixture_definitions','complete','observed_roles'):
            operation['role_matrix'].pop(field)
        operation['role_matrix']['state'] = 'unmapped'


def verify_private(root):
    root = Path(root)
    binding = checked(root, {'path': INPUT, 'sha256': INPUT_SHA256})
    validate(root, binding)
    fixture = binding['fixtures'][FIXTURE]
    authority = fixture['private_historical_capture']
    controller = checked(root, authority['controller'])
    source = checked(root, authority['raw_fixture'])
    comparison = checked(root, authority['comparison'])
    assert controller['passed'] and controller['cleanup'] == 'project removed'
    assert not controller['resources_retained']
    assert controller['packaged_checks_sha256'] == authority['comparison']['sha256']
    assert len(comparison['checks']) == len(source['cases']) == 141
    assert source['source_capture_only'] is source['all_no_model_mutations'] is True
    for name, wanted in authority['tool_sha256'].items():
        raw = (root/authority['tool_source']/name).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == controller['tool_source_sha256'][name] == wanted
    for definition in binding['contracts']:
        for row in definition['cases']:
            index = row['source_index']
            actual = source['cases'][index]
            check = comparison['checks'][index]
            assert actual['no_model_mutations'] is True and check['passed'] is True
            assert actual['name'] == row['label'] == check['name']
            assert all(actual[key] == row[key] for key in ('path','method','request_body_sha256'))
            assert row['role'] == (actual['role'] or 'anonymous')
            assert actual['expected']['status'] == row['HTTP_status'] == check['source_status']
            assert row['expected_response_projection_sha256'] == digest(actual['expected'])
    return {'retained_Original_HTTP_cases_verified': binding['unique_Source_cases'],
            'new_application_executions': 0}


if __name__ == '__main__':
    print(json.dumps(verify_private(Path(__file__).resolve().parents[2])))
