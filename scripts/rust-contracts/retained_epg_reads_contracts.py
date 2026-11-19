"""Bind retained Original EPG view observations without executing the application.

Original Dispatcharr and dependency authors retain their credit and licences.
"""

import collections
import hashlib
import json
import re
from pathlib import Path

from source_contracts import checked, digest

KIND = 'retained_original_stored_epg_104_framework'
INPUT = 'scripts/rust-contracts/retained-epg-reads/bindings.json'
INPUT_SHA256 = 'c8348e37391aefa99c2a4cf367dd2a470a1ecc0cb976877ddc3b8f3f263e20bb'
RECEIPT = 'scripts/rust-epg-contracts/verified-program-http-8.json'
RECEIPT_SHA256 = 'a07cc5dc1b8137f061bfa55e07583960fa59c130e6d4574594cb61a589b8b58e'
CAPTURE_SHA256 = '09969484794184a2b8754b27935e75e7c60af3529bd569dee5cb941a99aefedc'
RESULT_SHA256 = 'cedc2df95ddd9b6ca382b5fa1aafa1fc6fe12b987cb91f6de9226549ad5b2e1f'
FIXTURE = 'retained-Original-stored-EPG-reads-104'


def dependencies():
    return {INPUT, RECEIPT}


def validate(root, value):
    assert value == checked(root, {'path': INPUT, 'sha256': INPUT_SHA256}), 'exact retained EPG input'
    fixture = value['fixtures'][FIXTURE]
    historical = {'path': RECEIPT, 'sha256': RECEIPT_SHA256}
    assert fixture['public_historical_receipt'] == historical
    receipt = checked(root, historical)
    assert receipt['passed'] is True and receipt['captured_source_http_cases'] == 104
    authority = fixture['private_authority']
    assert receipt['artifact_sha256']['source-reads.json'] == authority['capture_sha256'] == CAPTURE_SHA256
    assert receipt['artifact_sha256']['result.json'] == authority['historical_result_sha256'] == RESULT_SHA256
    assert authority['root'] == receipt['evidence_directory']
    assert fixture['historical_cleanup'] == 'project removed'
    assert fixture['historical_controller_passed'] is True and fixture['Source_TCP_verified'] is False
    assert value['binding_kind'] == KIND and value['actor_credential_values_projected_out'] is True
    assert value['operation_count'] == len(value['contracts']) == 6
    assert value['fully_verified_operations'] == value['new_Source_executions'] == 0
    assert value['Native_coverage_increased'] is False
    counts = collections.Counter()
    roles = fixture['roles']
    seen = set()
    for contract in value['contracts']:
        assert contract['fixture_id'] == FIXTURE
        assert contract['method'] == 'GET' and contract['fully_verified'] is False
        assert contract['owning_milestone'] == 'M5'
        assert contract['observed_roles'] == sorted(roles)
        assert contract['source_role_specs'] == roles
        counts.update([contract['source_action']])
        pattern = re.compile(contract['pattern_exact'].replace('/^', '/'))
        result = contract['result']
        assert result['Source_case_observations'] == len(contract['cases'])
        assert result['new_Source_cases_this_task'] == 0
        assert all(result[key] is False for key in ('complete_role_matrix', 'Native_coverage_increased', 'Source_TCP_verified'))
        for case in contract['cases']:
            assert case['method'] == 'GET' and case['role'] in roles
            assert case['HTTP_status'] in {200, 401, 403, 404, 406}
            assert pattern.fullmatch(case['path'].split('?', 1)[0].lstrip('/'))
            assert case['source_index'] not in seen
            seen.add(case['source_index'])
            assert case['record_json_pointer'] == f"/cases/{case['source_index']}"
            assert re.fullmatch(r'[0-9a-f]{64}', case['expected_projection_sha256'])
    assert counts == {'list': 3, 'retrieve': 3}
    unmapped = value['unmapped_method_Source_observations']
    assert value['mapped_GET_observations'] == len(seen) == 101
    assert {row['source_index'] for row in unmapped} == {30, 65, 98}
    assert not seen.intersection(row['source_index'] for row in unmapped)
    assert seen.union(row['source_index'] for row in unmapped) == set(range(value['unique_Source_cases']))
    assert len(unmapped) == 3
    for row in unmapped:
        assert row['observed_method'] == row['method'] == 'HEAD' and row['contract_id'] is None
        assert row['HTTP_status'] == 200 and row['role'] == 'admin'
        assert row['Source_TCP_verified'] is row['fully_verified'] is False
        assert row['rendered_representation_bytes'] > 0



def restore_pre_epg(root, rows):
    """Strip only this cohort when verifying an earlier register snapshot."""
    value = checked(root, {'path': INPUT, 'sha256': INPUT_SHA256})
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for contract in value['contracts']:
        operation = operations[contract['contract_id']]
        assert operation.pop('executable_Source_contract') == contract
        operation.update(setup_fixture=None, assertions=[], result=None,
                         coverage_state='scoped_evidence_needs_case_mapping'
                         if operation['related_evidence'] else 'not_yet_mapped')
        for field in ('observed_fixture_definitions', 'complete', 'observed_roles'):
            operation['role_matrix'].pop(field)
        operation['role_matrix']['state'] = 'unmapped'


def verify_private(root):
    """Check the sanitized projection against the original retained credential-bearing rows."""
    root = Path(root)
    binding = checked(root, {'path': INPUT, 'sha256': INPUT_SHA256})
    validate(root, binding)
    authority = binding['fixtures'][FIXTURE]['private_authority']
    path = authority['root']
    result = checked(root, {'path': path + '/result.json', 'sha256': RESULT_SHA256})
    capture = checked(root, {'path': path + '/source-reads.json', 'sha256': CAPTURE_SHA256})
    assert result['fixture_sha256'] == CAPTURE_SHA256 and result['source_cases'] == len(capture['cases']) == 104
    assert result['passed'] is True and result['resources_retained'] is False
    assert result['cleanup'] == 'project removed'
    for relative, expected in authority['frozen_tool_sha256'].items():
        assert result['tool_source_sha256'][relative] == expected
        content = (root / path / 'tool-source' / relative).read_bytes()
        assert hashlib.sha256(content).hexdigest() == expected
    cases = [leaf for definition in binding['contracts'] for leaf in definition['cases']]
    cases += binding['unmapped_method_Source_observations']
    for row in cases:
        capture_row = capture['cases'][row['source_index']]
        assert all(row[key] == capture_row[key] for key in ('path', 'method'))
        assert row['role'] == (capture_row['role'] or 'anonymous')
        assert ('HTTP_AUTHORIZATION' in capture_row['headers']) == (row['role'] != 'anonymous')
        assert row['HTTP_status'] == capture_row['expected']['status']
        assert row['expected_projection_sha256'] == digest(capture_row['expected'])
        if row['method'] == 'HEAD':
            assert row['rendered_representation_bytes'] == capture_row['expected']['rendered_representation_bytes']
    return {'Source_framework_observations_verified': len(cases), 'new_Source_executions': 0}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    options = parser.parse_args()
    print(json.dumps(verify_private(options.root)))
