"""Validate scoped Original format-alias request and observer custody."""
from collections import Counter
import hashlib
import json
import re
from urllib.parse import urlsplit

from source_contracts import checked

KIND = 'retained_original_alias_remainder_HTTP'
INPUT = 'scripts/rust-contracts/retained-alias-remainder/bindings.json'
SHA256 = '08f261fb927efdb917f61d2ba277ab1dcf2f408186d30456722b85daebd1233c'
FIXTURE = 'Original-alias-remainder-a2a-6'
ROLES = ('anonymous', 'streamer', 'standard', 'admin')
ARTIFACTS = {
    'resolver-witness.json', 'wire.json', 'SQL-before.json', 'SQL-after.json',
    'broker-before.json', 'broker-after.json', 'cache-before.json', 'cache-after.json',
    'provider-before.json', 'provider-after.json', 'journal.json', 'actual-task-backends.json',
    'WS.json', 'receiver-end.json',
}


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def is_digest(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def operation_digest(operation):
    return hashlib.sha256(json.dumps(operation, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def validate(root, value):
    assert value == load(root) and value['binding_kind'] == KIND
    assert value['qualified_projection_sha256'] == '5680112ccda733bb7e00410bf91bb664a7850fa7e46f7a428221ec55517b8852'
    assert value['frozen_crosswalk_sha256'] == '4f499e87e80f0b2b8e4cdc3ab870610cff118c85125e723891d7bf5c4e2fff61'
    assert value['frozen_selection_sha256'] == '01e409fe11de239d2e86a97b5d5dc58c5bc9e5827b885ca7980c632f65e9c6ce'
    assert value['new_Source_executions'] == value['fully_verified_operations'] == 0
    assert value['Native_coverage_increased'] is False
    fixture = value['fixtures'][FIXTURE]
    assert set(value['fixtures']) == {FIXTURE}
    assert fixture['Source_revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert fixture['actual_runner_exit'] == fixture['blocked_or_partial'] == fixture['unstarted_calls'] == 0
    assert fixture['outer_passed'] is fixture['exact_cleanup_passed'] is True
    assert fixture['affected_resources_semantically_qualified'] is False
    assert fixture['retained_prior_anonymous_not_replayed'] == 19
    receipt = value['capture_boundary']
    assert receipt['execution_capture_sha256'] == fixture['qualification_input_sha256']['dispatch/execution.json']
    assert receipt['controller_capture_sha256'] == fixture['qualification_input_sha256']['results/report.json']
    assert all(is_digest(value) for value in fixture['qualification_input_sha256'].values())
    assert is_digest(receipt['independent_cleanup_sha256'])
    assert all(receipt[key] is True for key in ('stdout_EOF', 'stderr_EOF', 'host_process_group_settled',
                                                'actual_CID_and_host_generation_absence'))
    assert receipt['blocked_or_partial_cases'] == receipt['unstarted_plan_indices'] == []
    assert receipt['observer_roles_started'] == receipt['observer_roles_retired'] == 481
    assert receipt['pending_retired_role_log_captures'] == 0
    assert receipt['full_M0_accepted'] is receipt['performance_verified'] is False
    assert len(receipt['explicit_incomplete_domains']) == 4

    decision = value['mapping_decisions']
    contracts = value['contracts']
    assert len(decision) == len(contracts) == 83
    assert set(decision) == {contract['contract_id'] for contract in contracts}
    assert Counter(item['binding'] for item in decision.values()) == {'primary': 64, 'supplemental': 19}
    assert all(is_digest(item['original_operation_sha256']) for item in decision.values())
    slots = []
    statuses = Counter()
    unresolved = 0
    target_indices = set()
    for contract in contracts:
        index = contract['cases'][0]['plan_index'] % len(contracts)
        target_indices.add(index)
        method = contract['method']
        roles = ROLES[1:] if index < 19 else ROLES
        kind = decision[contract['contract_id']]['binding']
        assert kind == ('supplemental' if index < 19 else 'primary')
        assert contract['fixture_id'] == FIXTURE and contract['fully_verified'] is False
        assert contract['observed_methods'] == [method or 'GET']
        assert contract['observed_roles'] == list(roles)
        assert len(contract['cases']) == len(roles) == contract['result']['Source_case_observations']
        assert contract['result']['new_Source_cases_this_task'] == 0
        assert contract['result']['Native_coverage_increased'] is False
        unresolved += method is None
        pattern = contract['pattern_exact'].replace('/^', '/')
        if '(?P<format>' not in pattern:
            pattern = re.escape(pattern).replace(re.escape('<int:pk>'), '[0-9]+')
        for role, case in zip(roles, contract['cases'], strict=True):
            assert contract['source_role_specs'][role] == {
                'fixture_role_or_supplied_credential': role, 'authorization_header_present': role != 'anonymous',
                'complete': False,
            }
            assert case['contract_id'] == contract['contract_id'] and case['role'] == role
            assert case['raw_method'] == method and case['observed_method'] == (method or 'GET')
            assert case['source_action'] == contract['source_action']
            assert case['plan_index'] == index + len(contracts)*ROLES.index(role)
            assert re.fullmatch(pattern, urlsplit(case['path']).path.lstrip('/'))
            assert case['partial_route_observation_only'] is True and is_digest(case['body_sha256'])
            assert 0 <= case['body_bytes'] <= 4*1024*1024
            assert set(case['private_artifact_sha256']) == ARTIFACTS
            assert all(is_digest(digest) for digest in case['private_artifact_sha256'].values())
            slots.append(case['plan_index'])
            statuses[case['status']] += 1
    assert target_indices == set(range(len(contracts)))
    assert sorted(slots) == list(range(19, 332)) and unresolved == 3
    assert dict(statuses) == {200: 65, 201: 5, 204: 4, 401: 54, 403: 100, 404: 10, 500: 75}
    assert receipt['observed_HTTP_status_counts'] == {str(key): count for key, count in statuses.items()}


def restore_pre_alias_remainder(root, rows):
    """Remove only this binding and affirm the exact preexisting operation."""
    packet = load(root)
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for contract in packet['contracts']:
        identifier = contract['contract_id']
        operation = operations[identifier]
        decision = packet['mapping_decisions'][identifier]
        if decision['binding'] == 'supplemental':
            captures = operation['supplemental_executable_Source_captures']
            added = [row for row in captures if row['input']['kind'] == KIND]
            assert len(added) == 1 and added[0]['definition'] == contract
            captures.remove(added[0])
            if not captures:
                operation.pop('supplemental_executable_Source_captures')
        else:
            assert operation.pop('executable_Source_contract') == contract
            operation.update(setup_fixture=None, assertions=[], result=None,
                             coverage_state='scoped_evidence_needs_case_mapping'
                             if operation['related_evidence'] else 'not_yet_mapped')
            role_matrix = operation['role_matrix']
            for field in ('observed_fixture_definitions', 'complete', 'observed_roles'):
                role_matrix.pop(field)
            role_matrix['state'] = 'unmapped'
        assert operation_digest(operation) == decision['original_operation_sha256'], identifier
