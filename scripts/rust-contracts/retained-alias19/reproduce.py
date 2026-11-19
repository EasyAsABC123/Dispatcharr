"""Project completed Original anonymous alias responses and exact scoped observers."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import re

SOURCE_DIR = 'artifacts/rust/m0-alias-source-path-successor-a2a-4'
PROJECTION = '9ef74828ecb83c8b67372a9a31f7be7e12d58480697a42261ae8330b0933d30b'
KIND = 'retained_original_alias19_anonymous_HTTP'
MISSING = ['SQL-after.json', 'broker-after.json', 'cache-after.json', 'provider-after.json',
           'journal.json', 'actual-task-backends.json', 'WS.json', 'receiver-end.json']


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_bytes())


def generate(repo, prior):
    assert prior['executable_Source_mapping']['operations'] == 382
    artifact = repo/SOURCE_DIR
    selection, projection_path = artifact/'alias-route-crosswalk.json', artifact/'alias-source-projection.json'
    assert sha(projection_path) == PROJECTION
    plan = load(selection)
    report = load(projection_path)
    run = artifact/'actual-source'
    assert report['Source_revision'] == prior['baseline_ref'] == plan['Source_revision']
    assert report['register_revision'] == plan['register_revision']
    assert report['outer_actual_exit'] == 1 and report['exact_Original_cleanup']
    assert report['outer_selected_workflow_passed'] is False and report['unstarted_calls'] == 312
    assert not report['full_milestone_accepted'] and not report['application_performance_verified']
    for name, digest in report['input_sha256'].items():
        assert sha(run/name) == digest
    terminal = load(run/'dispatch/execution.json')
    controller = load(run/'results/report.json')
    assert terminal['actual_exit'] == 1 and terminal['failure'] is None and not terminal['remaining_group_members']
    assert all(terminal[key] for key in ('process_group_settled', 'stdout_EOF', 'stderr_EOF'))
    assert controller['expected_backend_order'] == ['Original'] and controller['exact_cleanup_passed']
    [cleanup] = controller['cleanup']
    assert all(cleanup[key] for key in ('exact_owned_absence', 'all_original_process_generations_absent', 'host_CLI_groups_settled'))
    assert not cleanup['errors']
    [report_path] = list((run/'fixtures').glob('Original-*/m0-format-and-alias-dispatch/report.json'))
    original = load(report_path)
    assert original['status'] == 'failed' and len(original['calls']) == 20
    assert original['Source_only'] and original['Native_contexts'] == 0
    lookup = {op['contract_id']: (route, op) for route in prior['runtime_routes'] for op in route['operations']}
    contracts = []
    for index, observed in enumerate(report['observations']):
        assert observed['plan_index'] == index
        target = plan['selected'][index]
        route, operation = lookup[observed['contract_id']]
        assert target['contract_id'] == observed['contract_id']
        assert operation['coverage_state'] in ('not_yet_mapped', 'scoped_evidence_needs_case_mapping')
        assert not operation['fully_verified'] and 'executable_Source_contract' not in operation
        assert observed['role'] == 'anonymous' and observed['status'] == 401
        assert observed['observed_method'] == observed['raw_method'] == operation['method']
        assert observed['resolver_ordinal'] == route['resolver_ordinal'] and observed['source_action'] == operation['source_action']
        assert route['pattern_exact'] == target['exact_pattern'] and route['source_handler'] == target['source_handler']
        assert original['calls'][index]['path'] == observed['path']
        case_path = report_path.parent/'private'/f'anonymous-{index:03d}'
        custody = observed['private_artifact_sha256']
        for name, digest in custody.items():
            assert sha(case_path/name) == digest
        witness = load(case_path/'resolver-witness.json')
        assert witness['matched'] and witness['actual'] == witness['request']['expected']
        assert witness['request']['method'] == operation['method'] and witness['request']['path'] == observed['path']
        wire = load(case_path/'wire.json')
        assert 'Authorization' not in wire['request']['headers']
        assert wire['request']['role'] == 'anonymous' and wire['response']['status'] == 401
        assert wire['request']['method'] == operation['method'] and wire['request']['path'] == observed['path']
        body = base64.b64decode(wire['response']['body_base64'], validate=True)
        assert hashlib.sha256(body).hexdigest() == observed['body_sha256'] and len(body) == observed['body_bytes']
        before, after = [load(case_path/f'SQL-{boundary}.json') for boundary in ('before', 'after')]
        assert all(before[key] == after[key] for key in ('tables', 'sequences'))
        assert load(case_path/'broker-before.json') == load(case_path/'broker-after.json')
        assert load(case_path/'provider-before.json') == load(case_path/'provider-after.json')
        assert len(load(case_path/'journal.json')['publications']) == 0
        assert load(case_path/'actual-task-backends.json')['all_published_task_IDs'] == []
        assert load(case_path/'WS.json')['events'] == load(case_path/'WS.json')['raw_events_base64'] == []
        caches = [{row['key']: row for row in load(case_path/f'cache-{boundary}.json')['all_observed_keys']}
                  for boundary in ('before', 'after')]
        changed = [key for key in set.union(*(set(cache) for cache in caches)) if caches[0].get(key) != caches[1].get(key)]
        assert len(changed) == 1 and re.fullmatch(r'live:worker:[^:]+:[0-9]+:heartbeat', changed[0])
        case = {key: observed[key] for key in ('path', 'role', 'status', 'body_sha256', 'body_bytes', 'resolver_ordinal', 'plan_index')}
        case.update(method=operation['method'], captured_artifact_sha256=custody,
                    scoped_observations={'actual_SQL_tables_and_sequences_unchanged': True,
                                         'broker_and_owned_provider_unchanged': True,
                                         'actual_task_publications': 0, 'actual_WebSocket_events': 0,
                                         'only_observed_cache_key_delta': 'Original live worker heartbeat'})
        contract = {key: operation[key] for key in ('contract_id', 'method', 'source_action', 'owning_milestone')}
        contract.update(pattern_exact=route['pattern_exact'], source_handler=route['source_handler'],
                        fixture_id='Original-alias-fourth', observed_roles=['anonymous'], observed_methods=[operation['method']],
                        source_role_specs={'anonymous': {'fixture_role_or_supplied_credential': 'anonymous without credentials', 'complete': False}},
                        assertions=['Match actual Original resolver, declared method and anonymous literal path',
                                    'Preserve actual denial and scoped SQL, receiver and task comparisons'],
                        result={'Source_case_observations': 1, 'new_Source_cases_this_task': 0, 'Native_coverage_increased': False},
                        cases=[case], fully_verified=False)
        contracts.append(contract)
    [excluded] = report['blocked_or_partial']
    idx = excluded['plan_index']
    wire_path = report_path.parent/'private'/f'anonymous-{idx:03d}'/'wire.json'
    excluded_wire = load(wire_path)
    assert excluded['missing'] == MISSING and excluded['state'] == 'incomplete-observers' and idx == 19
    assert excluded_wire['request']['role'] == 'anonymous' and excluded_wire['response']['status'] == 401
    exception = report_path.parent/'private/failure.json'
    assert "RuntimeError: StoppedRoleLogRecordBudget" in load(exception)['traceback']
    assert len(contracts) == 19 == report['qualified_operation_ids']
    fixture = {'actual_runner_exit': 1, 'exact_cleanup_passed': True, 'outer_passed': False,
               'Source_revision': prior['baseline_ref'], 'planned_business_calls': 332,
               'actual_returned': 20, 'unstarted_business_calls': 312,
               'partial_with_incomplete_observers': {**excluded, 'actual_status': 401,
                   'actual_path': excluded_wire['request']['path'], 'wire_sha256': sha(wire_path),
                   'assigned_in_register': False},
               'stopping_condition': 'StoppedRoleLogRecordBudget after the retired process stop',
               'raw_failure_sha256': sha(exception),
               'input_sha256': {str(path.relative_to(repo)): sha(path) for path in (selection, projection_path, run/'dispatch/execution.json', run/'results/report.json', report_path)}}
    return {'binding_kind': KIND, 'operation_count': 19, 'retained_Source_responses': 19, 'fully_verified_operations': 0,
            'Native_coverage_increased': False, 'new_Source_executions': 0, 'fixtures': {'Original-alias-fourth': fixture},
            'contracts': contracts, 'credit': 'Original Dispatcharr and dependency authors retain all application and algorithm credit.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--private-root', type=Path, required=True)
    parser.add_argument('--register', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(generate(args.private_root.resolve(), load(args.register)), indent=2)+'\n')
