"""Validate exact retained anonymous Original alias outcomes and observer boundaries."""
from collections import Counter
import re
from urllib.parse import urlsplit

from source_contracts import checked

KIND = 'retained_original_alias19_anonymous_HTTP'
INPUT = 'scripts/rust-contracts/retained-alias19/bindings.json'
SHA256 = 'fd613f7113123c01f06f7be29e3c9499f050849eece39e2a16109924e49474f2'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert load(root) == value and value['binding_kind'] == KIND
    assert value['operation_count'] == len(value['contracts']) == value['retained_Source_responses'] == 19
    assert value['fully_verified_operations'] == value['new_Source_executions'] == 0
    assert value['Native_coverage_increased'] is False
    [fixture] = value['fixtures'].values()
    assert fixture['actual_runner_exit'] == 1 and fixture['outer_passed'] is False and fixture['exact_cleanup_passed'] is True
    assert fixture['planned_business_calls'] == 332 and fixture['actual_returned'] == 20 and fixture['unstarted_business_calls'] == 312
    assert fixture['stopping_condition'] == 'StoppedRoleLogRecordBudget after the retired process stop'
    incomplete = fixture['partial_with_incomplete_observers']
    assert incomplete['actual_status'] == 401 and incomplete['state'] == 'incomplete-observers'
    assert incomplete['assigned_in_register'] is False and len(incomplete['missing']) == 8
    assert not any(contract['contract_id'] == incomplete['contract_id'] for contract in value['contracts'])
    methods = Counter()
    seen = set()
    for index, contract in enumerate(value['contracts']):
        method = contract['method']
        assert contract['fully_verified'] is False and contract['observed_methods'] == [method]
        assert contract['observed_roles'] == ['anonymous'] and contract['source_role_specs']['anonymous']['complete'] is False
        [case] = contract['cases']
        assert case['method'] == method and case['role'] == 'anonymous' and case['status'] == 401
        assert case['plan_index'] == index
        pattern = contract['pattern_exact'].replace('/^', '/')
        assert '(?P<format>' in pattern and re.fullmatch(pattern, urlsplit(case['path']).path.lstrip('/'))
        assert re.fullmatch('[0-9a-f]{64}', case['body_sha256']) and case['body_bytes'] > 0
        assert case['scoped_observations'] == {'actual_SQL_tables_and_sequences_unchanged': True,
              'broker_and_owned_provider_unchanged': True, 'actual_task_publications': 0, 'actual_WebSocket_events': 0,
              'only_observed_cache_key_delta': 'Original live worker heartbeat'}
        assert len(case['captured_artifact_sha256']) == 14
        assert contract['contract_id'] not in seen
        seen.add(contract['contract_id'])
        methods[method] += 1
    assert methods == {'GET': 9, 'PATCH': 3, 'POST': 7}


def restore_pre_alias19(root, rows):
    """Recover the preceding register for historical equality assertions."""
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for contract in load(root)['contracts']:
        operation = operations[contract['contract_id']]
        assert operation.pop('executable_Source_contract') == contract
        operation.update(setup_fixture=None, assertions=[], result=None, coverage_state='scoped_evidence_needs_case_mapping'
                         if operation['related_evidence'] else 'not_yet_mapped')
        for key in ('observed_fixture_definitions', 'complete', 'observed_roles'):
            operation['role_matrix'].pop(key)
        operation['role_matrix']['state'] = 'unmapped'
