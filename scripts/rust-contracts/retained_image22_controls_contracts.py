"""Verify retained Original Image22 TS, stream and group assignments without private fixture access.

Original Dispatcharr authors retain application and algorithm credit.
"""
import collections
import re
from urllib.parse import urlsplit

from source_contracts import checked

KIND = 'retained_original_Image22_TS_stream_group_HTTP'
INPUT = 'scripts/rust-contracts/retained-image22-controls/bindings.json'
SHA256 = '3de11379faa601354aab3cf1faaa976724dfefdb54cb6eb58bb29926ee3a8c44'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert load(root) == value
    assert value['binding_kind'] == KIND and value['operation_count'] == len(value['contracts']) == 7
    assert value['new_Source_executions'] == value['fully_verified_operations'] == 0
    assert value['Native_coverage_increased'] is False
    assert sum(definition['method'] is None for definition in value['contracts']) == 3
    identities = set()
    seen = collections.Counter()
    for definition in value['contracts']:
        fixture_id = definition['fixture_id']
        pattern = definition['pattern_exact'].replace('/^', '/')
        pattern = re.sub(r'<str:[^>]+>', r'[^/]+', pattern)
        pattern = re.sub(r'<uuid:[^>]+>', r'[0-9a-f-]{36}', pattern)
        pattern = re.sub(r'<int:[^>]+>', r'[0-9]+', pattern)
        cases = definition['cases']
        assert definition['fully_verified'] is False and fixture_id in value['fixtures']
        assert definition['observed_methods'] == sorted({row['method'] for row in cases})
        assert definition['observed_roles'] == sorted({row['role'] for row in cases})
        assert definition['result']['Source_case_observations'] == len(cases)
        assert definition['result']['new_Source_cases_this_task'] == 0
        assert definition['result']['Native_coverage_increased'] is False
        for row in cases:
            key = (fixture_id, row['source_index'])
            assert key not in identities
            identities.add(key)
            seen[fixture_id] += 1
            assert row['request_driver'] in value['fixtures'][fixture_id]['request_tools_sha256']
            assert re.fullmatch(pattern, urlsplit(row['path']).path.lstrip('/'))
            assert definition['method'] is None or row['method'] == definition['method']
            assert 100 <= row['HTTP_status'] <= 599
            assert re.fullmatch(r'[0-9a-f]{64}', row['response_projection_sha256'])
    assert len(identities) == value['unique_Source_cases'] == 22
    assert seen == {'retained-Image22-TS-stream-group': 22}


def restore_pre_image22_controls(root, rows):
    """Reverse this packet while checking earlier immutable register snapshots."""
    value = load(root)
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for definition in value['contracts']:
        operation = operations[definition['contract_id']]
        assert operation.pop('executable_Source_contract') == definition
        operation.update(setup_fixture=None, assertions=[], result=None,
                         coverage_state='scoped_evidence_needs_case_mapping'
                         if operation['related_evidence'] else 'not_yet_mapped')
        for key in ('observed_fixture_definitions', 'complete', 'observed_roles'):
            operation['role_matrix'].pop(key)
        operation['role_matrix']['state'] = 'unmapped'
