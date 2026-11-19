"""Verify retained Original playback assignments without private fixture access.

Original Dispatcharr authors retain application and algorithm credit.
"""
import collections
import re
from urllib.parse import urlsplit

from source_contracts import checked

KIND = 'retained_original_playback_VOD_XC_TCP'
INPUT = 'scripts/rust-contracts/retained-playback/bindings.json'
SHA256 = '8f1609f176b00a6a41557e944fc690135bb644a35a975c905cd1f4fc213e2655'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert load(root) == value
    assert value['binding_kind'] == KIND and value['operation_count'] == len(value['contracts']) == 7
    assert value['new_Source_executions'] == value['fully_verified_operations'] == 0
    assert value['Native_coverage_increased'] is False
    assert value['fixtures']['vod-retained']['excluded']['profile_follows']['case_count'] == 2
    assert sum(definition['method'] is None for definition in value['contracts']) == 6
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
            key = (fixture_id, row['record_json_pointer'])
            assert key not in identities
            identities.add(key)
            seen[fixture_id] += 1
            assert row['contract_id'] == definition['contract_id']
            assert re.fullmatch(pattern, urlsplit(row['path']).path.lstrip('/'))
            assert definition['method'] is None or row['method'] == definition['method']
            assert 100 <= row['HTTP_status'] <= 599
            assert re.fullmatch(r'[0-9a-f]{64}', row['response_projection_sha256'])
    assert len(identities) == value['unique_Source_cases'] == 54
    assert seen == {'vod-retained': 15, 'xc-retained': 23,
                    'vod-stop-retained': 1, 'vod-category-retained': 15}


def restore_pre_playback(root, rows):
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
