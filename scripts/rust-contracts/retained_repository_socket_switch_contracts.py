"""Verify retained Original repository, socket and switch assignments without private fixture access.

Original Dispatcharr authors retain application and algorithm credit.
"""
import collections
import re
from urllib.parse import urlsplit

from source_contracts import checked

KIND = 'retained_original_repository_socket_switch'
INPUT = 'scripts/rust-contracts/retained-repository-socket-switch/bindings.json'
SHA256 = 'f909413998e013506341c1cee9da908af95af1e6c37a9fab2c0f647353c5e995'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert load(root) == value
    assert value['binding_kind'] == KIND and value['operation_count'] == len(value['contracts']) == 6
    assert value['new_Source_executions'] == value['fully_verified_operations'] == 0
    assert value['Native_coverage_increased'] is False
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
            key = (row['fixture_case_source'], row['record_index'])
            assert key not in identities
            identities.add(key)
            seen[fixture_id] += 1
            transport = row['transport']
            assert transport in ('Original_TCP_HTTP', 'Original_Django_APIClient',
                                 'Original_Daphne_WebSocket_handshake')
            if fixture_id == 'socket':
                assert transport == 'Original_Daphne_WebSocket_handshake'
                assert row['method'] == 'GET' and row['path'] == '/ws/'
                assert row['HTTP_status'] in (101, 403)
            assert re.fullmatch(pattern, urlsplit(row['path']).path.lstrip('/'))
            assert definition['method'] is None or row['method'] == definition['method']
            assert 100 <= row['HTTP_status'] <= 599
            assert re.fullmatch(r'[0-9a-f]{64}', row['response_projection_sha256'])
    assert len(identities) == value['unique_Source_cases'] == 57
    assert seen == {'repository': 37, 'socket': 17, 'switch': 3}
    excluded, = value['excluded_non_HTTP_outcome']
    assert excluded['fixture_case_source'] == 'Source24'
    assert excluded['framework_exception_type'] == 'builtins.AttributeError'
    assert excluded['HTTP_response_returned'] is False
    assert ('Source24', 24) not in identities


def restore_pre_repository_socket_switch(root, rows):
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
