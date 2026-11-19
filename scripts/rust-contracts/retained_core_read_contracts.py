"""Bind retained Original natural HTTP Core reads to existing route operations.

Original Dispatcharr and dependency authors retain application and algorithm credit.
"""
import collections
import hashlib
import json
import re
from pathlib import Path

from source_contracts import checked, digest

KIND = 'retained_original_image22_core_reads_TCP'
INPUT = 'scripts/rust-contracts/retained-core-read/bindings.json'
INPUT_SHA256 = 'e76dba659b059f62fd7ff7cc552b4fbdc9cb6181387e61019f3e67953a2ad7c9'
PUBLIC_RECEIPT = 'scripts/rust-integration/verified-integration-m5-auth-image22-1.json'
PUBLIC_SHA256 = 'e7cb41ab0e72d66c705d254ebd416f3098dd2c4ed797305dcd012883fd0fc4e8'
FIXTURE = 'retained-Original-Image22-core-read'
RAW_SHA256 = 'bc1b852bd233b03bff581d52349a9ee1d7e27e96664885f86e2be4b3c96c99ae'
CASE = re.compile(r'core-read:(settings|useragents|streamprofiles|outputprofiles):(admin|standard|viewer|inactive|None)(?::detail:([a-z0-9-]+))?')
LIST_STATUSES = {'admin':200, 'standard':200, 'viewer':403, 'inactive':401, 'anonymous':401}


def dependencies():
    return {INPUT, PUBLIC_RECEIPT}


def validate(root, value):
    assert value == checked(root, {'path': INPUT, 'sha256': INPUT_SHA256}), 'exact retained Core input'
    fixture = value['fixtures'][FIXTURE]
    assert fixture['public_historical_receipt'] == {'path': PUBLIC_RECEIPT, 'sha256': PUBLIC_SHA256}
    receipt = checked(root, fixture['public_historical_receipt'])
    assert receipt['passed'] is True
    raw = fixture['private_historical_capture']
    historical, = (run for run in receipt['runs'] if run['report'] == raw['path'])
    assert historical['sha256'] == raw['sha256'] == RAW_SHA256
    assert historical['tool_source'] == raw['tool_source']
    assert historical['checks'] == 1024 and 'core' in historical['contract_suites']
    assert historical['cleanup'] == fixture['historical_cleanup'] == 'project removed'
    assert fixture['historical_controller_passed'] is True
    assert value['binding_kind'] == KIND and value['operation_count'] == len(value['contracts']) == 7
    assert value['new_Source_executions'] == value['fully_verified_operations'] == 0
    assert value['Native_coverage_increased'] is False
    omitted = value['already_assigned_settings_retrieve_indices']
    assert omitted == [137,138,139]
    identity_counts = collections.Counter()
    seen = set()
    for contract in value['contracts']:
        action = contract['source_action']
        identity_counts[action] += 1
        assert contract['owning_milestone'] == 'M5'
        assert contract['fixture_id'] == FIXTURE and contract['method'] == 'GET'
        assert contract['fully_verified'] is False
        result = contract['result']
        cases = contract['cases']
        assert result['Source_case_observations'] == len(cases)
        assert result['new_Source_cases_this_task'] == 0
        assert result['Native_coverage_increased'] is result['complete_role_matrix'] is False
        assert contract['observed_roles'] == sorted({case['role'] for case in cases})
        assert contract['source_role_specs'] == {name: fixture['roles'][name] for name in contract['observed_roles']}
        if action == 'list':
            assert {row['role']:row['HTTP_status'] for row in cases} == LIST_STATUSES and len(cases) == 5
        else:
            assert [row['HTTP_status'] for row in cases] == [200,404,404]
            assert contract['observed_roles'] == ['standard']
        route = re.compile(contract['pattern_exact'].replace('/^','/'))
        for row in cases:
            resource, role, detail = CASE.fullmatch(row['label']).groups()
            path = f'/api/core/{resource}/' + (f'{detail}/' if detail else '')
            assert row['path'] == path and route.fullmatch(path.lstrip('/'))
            assert row['role'] == ('anonymous' if role == 'None' else role)
            assert row['method'] == 'GET' and (action == 'retrieve') == bool(detail)
            index = row['source_index']
            assert index not in seen and row['record_json_pointer'] == f'/checks/{index}'
            seen.add(index)
            assert all(re.fullmatch('[0-9a-f]{64}',row[key])
                       for key in ('json_projection_sha256','record_sha256'))
    assert identity_counts == {'list':4, 'retrieve':3}
    assert len(seen) == value['unique_Source_cases'] == 29
    assert not seen.intersection(omitted) and seen.union(omitted) == set(range(135,167))


def restore_pre_core_reads(root, rows):
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
    capture = checked(root, authority)
    assert capture['passed'] and not capture['resources_retained']
    assert capture['source_baseline'] == fixture['baseline_revision']
    assert capture['baseline_archive_sha256'] == fixture['baseline_archive_sha256']
    assert any(item['ID'] == fixture['baseline_image'] and item['ContainerName'].endswith('-baseline-1')
               for item in capture['images'])
    for name, wanted in authority['tool_sha256'].items():
        assert capture['tool_source_sha256'][name] == wanted
        actual = (root/authority['tool_source']/name).read_bytes()
        assert hashlib.sha256(actual).hexdigest() == wanted
    for contract in binding['contracts']:
        for row in contract['cases']:
            actual = capture['checks'][row['source_index']]
            assert actual['passed'] is True and actual['name'] == row['label']
            assert digest(actual) == row['record_sha256']
            assert actual['details']['python_status'] == row['HTTP_status']
            assert actual['details']['python_body_sha256'] == row['json_projection_sha256']
    return {'retained_Original_TCP_cases_verified': binding['unique_Source_cases'],
            'new_application_executions': 0}


if __name__ == '__main__':
    print(json.dumps(verify_private(Path(__file__).resolve().parents[2])))
