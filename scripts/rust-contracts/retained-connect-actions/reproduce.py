"""Project next retained Original Connect routes without inferring generated IDs."""
import hashlib
import json
from pathlib import Path
import re
import argparse

DRIVER = 'scripts/rust-account-contracts/probes.py'
CLIENT = 'scripts/rust-integration/run.py'
META = 'scripts/rust-integration/metadata_fixture.py'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_bytes())


def capture(repo, register):
    ops = {op['contract_id']: (route, op) for route in register['runtime_routes'] for op in route['operations']}


    def authority(name, keys, drivers):
        run = repo/'artifacts/rust'/name
        inputs = [run/file for file in ('receipt.json', 'execution.json', 'independent-cleanup.json', 'evidence/integration.json')]
        receipt, execution, cleanup, report = map(load, inputs)
        assert receipt['source_revision'] == report['source_baseline'] == register['baseline_ref']
        assert receipt['passed'] and report['passed'] and receipt['foundation']['passed']
        assert receipt['tracked_runner_actual_exit'] == execution['actual_exit'] == 0
        assert execution['failure'] is None and not execution['remaining_group_members']
        assert all(execution[key] for key in ('process_group_settled', 'stdout_EOF', 'stderr_EOF'))
        assert receipt['project'] == report['project'] == cleanup['project']
        assert cleanup['passed'] and all(not value for value in cleanup['inventories'].values())
        assert all(receipt[key] for key in keys)
        sources = {}
        for file in drivers:
            path = run/'evidence/tool-source'/file
            assert sha(path) == report['tool_source_sha256'][file]
            inputs.append(path)
            sources[file] = path.read_text()
        return run, inputs, receipt, report, sources


    log_run, log_inputs, log_receipt, logs, log_source = authority('m7-connect-combined-candidate79-1',
                                                                   ['connect_scope_passed'], [DRIVER, CLIENT])
    log_probe = log_source[DRIVER].split('def run_connect_logs(', 1)[1].split('\ndef ', 1)[0]
    assert "path = '/api/connect/logs/'" in log_probe
    assert 'http(base + path + query, token=token,' in log_probe
    assert 'def http(url, body=None, token=None, method=None,' in log_source[CLIENT]
    assert 'data=json.dumps(body).encode() if body is not None else None' in log_source[CLIENT]
    queries = {
        'empty-react-query': '?page=1&page_size=50', 'populated-react-query': '?page=1&page_size=50',
        'first-page': '?page_size=2', 'second-page': '?page=2&page_size=2', 'last-page': '?page=last&page_size=2',
        'type-filter': '?type=webhook', 'disabled-integration-filter': '?integration={fixture_script_id}',
        'combined-filters': '?integration={fixture_webhook_id}&type=webhook',
        'conflicting-filters': '?integration={fixture_script_id}&type=webhook',
        'filtered-page': '?type=webhook&page_size=1&integration={fixture_webhook_id}',
        'invalid-page': '?page=3&page_size=2', 'anonymous': '', 'standard': '?page=1&page_size=50',
        'viewer': '?page=1&page_size=50',
    }
    log_cases = []
    for i, row in enumerate(logs['checks']):
        if not row['name'].startswith('connect-logs:'):
            continue
        case = row['name'].split(':', 1)[1]
        detail = row['details']
        assert row['passed'] and detail['original_contract_verified'] and detail['original_mime'] == 'application/json'
        assert detail['python_status'] == detail['rust_status'] and detail['python_body_sha256'] == detail['rust_body_sha256']
        log_cases.append({'case': case, 'method': 'GET', 'literal_path': '/api/connect/logs/',
                          'query_scope': queries[case], 'generated_query_values_are_symbolic': '{' in queries[case],
                          'role': case if case in ('anonymous', 'standard', 'viewer') else 'admin',
                          'status': detail['python_status'], 'response_projection_sha256': detail['python_body_sha256'],
                          'observation_pointer': f'/checks/{i}'})
    assert [case['case'] for case in log_cases] == list(queries)

    rep_run, rep_inputs, rep_receipt, replace, rep_source = authority('m7-connect-replace-candidate79-source-1',
                                                                    ['original_replace_scope_passed'], [DRIVER, META])
    assert rep_receipt['scope'] == 'original_connect_replace_only'
    rep_probe = rep_source[DRIVER].split('def run_connect_replace_source(', 1)[1].split('\ndef ', 1)[0]
    assert "route = f'/api/connect/integrations/{integration}/subscriptions/set/'" in rep_probe
    assert "issued.append(('PUT', integration, route))" in rep_probe
    assert "method='PUT'" in rep_probe and "IDs = fixture({'action':'connect_replace_seed'})" in rep_probe
    assert "'webhook':first.pk,'script':script.pk,'conflict':conflict.pk" in rep_source[META]
    need = re.compile(r'django\.request (?P<error>Unauthorized|Forbidden|Not Found|Bad Request|Internal Server Error): (?P<path>/api/connect/integrations/\d+/subscriptions/set/)')
    response_codes = {'Unauthorized': 401, 'Forbidden': 403, 'Not Found': 404, 'Bad Request': 400,
                      'Internal Server Error': 500}
    raw_access = rep_run/'evidence/compose.log'
    rep_inputs.append(raw_access)
    access = [(i, match['path'], response_codes[match['error']])
              for i, line in enumerate(raw_access.read_text(errors='replace').splitlines(), 1) if (match := need.search(line))]
    assert len(access) == 13
    access_points = iter(access)
    rep_cases = []
    role_overrides = {'anonymous': 'anonymous', 'standard': 'standard', 'viewer': 'viewer',
                      'inactive-admin': 'inactive admin', 'role-before-parser': 'standard'}
    for i, row in enumerate(replace['checks']):
        if not row['name'].startswith('connect-replace-source:'):
            continue
        case = row['name'].split(':', 1)[1]
        detail = row['details']
        assert row['passed']
        status = detail['actual_status']
        if status == 200:
            assert detail['exact_subscription_log_and_sequence_state']
            label = 'script' if case in ('script-ignores-templates', 'empty-replacement') else 'webhook'
            path = f'/api/connect/integrations/{{fixture_{label}_id}}/subscriptions/set/'
            log_pointer = None
        else:
            log_pointer, path, logged_status = next(access_points)
            assert logged_status == status
            if case == 'lookup-before-parser':
                assert path == '/api/connect/integrations/9223372036854775806/subscriptions/set/'
            if case == 'stored-duplicate-partial-failure':
                assert detail['stale_log_and_subscription_removed_with_earlier_upserts_committed']
            else:
                assert detail['database_and_subscription_sequence_unchanged']
        effects = {key: value for key, value in detail.items() if key not in ('actual_body_sha256', 'actual_status')}
        rep_cases.append({'case': case, 'method': 'PUT', 'path_or_template': path, 'literal_path_verified': log_pointer is not None,
                          'original_log_line': log_pointer, 'role': role_overrides.get(case, 'admin'), 'status': status,
                          'response_projection_sha256': detail['actual_body_sha256'], 'observed_effects': effects,
                          'observation_pointer': f'/checks/{i}'})
    assert next(access_points, None) is None and len(rep_cases) == 17
    [dispatch_guard] = [row for row in replace['checks'] if row['name'] == 'connect-replace-no-dispatch']
    assert dispatch_guard['passed'] and dispatch_guard['details'] == {'only_owned_subscription_put': True, 'put_count': 17,
           'owned_receiver_before': 0, 'owned_receiver_after': 0}


    def route_packet(cid, method, action, status, inputs, cases):
        route, op = ops[cid]
        assert op['method'] == method and op['source_action'] == action
        assert op['coverage_state'] == 'not_yet_mapped' and not op['fully_verified']
        return {'contract_id': cid, 'method': method, 'source_action': action, 'outer_actual_exit': 0,
                'exact_owned_cleanup_passed': True, 'source_handler': route['source_handler'],
                'exact_discovery_pattern': route['pattern_exact'], 'candidate_revision': status['candidate_revision'],
                'observations': cases, 'input_sha256': {str(path.relative_to(repo)): sha(path) for path in inputs}}


    value = {'Source_revision': register['baseline_ref'], 'operations': [
        route_packet('route-de258e62c08a0966ba7f:GET', 'GET', 'list', log_receipt, log_inputs, log_cases),
        route_packet('route-97c801e89fbcca3b37ea:PUT', 'PUT', 'set_subscriptions', rep_receipt, rep_inputs, rep_cases)],
        'replace_owned_receiver_guard': dispatch_guard['details'], 'route_path_literal_observations': 27,
        'additional_symbolic_fixture_path_observations': 4, 'new_executions': 0,
        'original_author_credit': 'Original Dispatcharr authors retain all application and algorithm credit.'}
    return value


def generate(repo, register):
    receipt = capture(repo, register)
    ops = {op['contract_id']: op for route in register['runtime_routes'] for op in route['operations']}
    fixtures = {}
    contracts = []
    for interval in receipt['operations']:
        method = interval['method']
        fixture_id = 'm7-connect-combined-candidate79-1' if method == 'GET' else 'm7-connect-replace-candidate79-source-1'
        cases = []
        symbolic = []
        for observation in interval['observations']:
            case = dict(observation)
            if method == 'GET':
                case['path'] = case.pop('literal_path')
                cases.append(case)
            elif case['literal_path_verified']:
                case['path'] = case.pop('path_or_template')
                cases.append(case)
            else:
                symbolic.append(case)
        roles = sorted({case['role'] for case in cases})
        operation = ops[interval['contract_id']]
        fixtures[fixture_id] = {'source_revision': receipt['Source_revision'], 'actual_runner_exit': interval['outer_actual_exit'],
                                'exact_cleanup_passed': interval['exact_owned_cleanup_passed'],
                                'candidate_revision': interval['candidate_revision'],
                                'input_sha256': interval['input_sha256']}
        if method == 'PUT':
            fixtures[fixture_id].update(native_replace_exercised=False,
                                       owned_receiver_guard=receipt['replace_owned_receiver_guard'],
                                       actual_successes_with_symbolic_ID=symbolic)
        contract = {key: operation[key] for key in ('contract_id', 'method', 'source_action', 'owning_milestone')}
        contract.update(pattern_exact=interval['exact_discovery_pattern'], source_handler=interval['source_handler'],
                        fixture_id=fixture_id, observed_roles=roles, observed_methods=[method],
                        source_role_specs={role: {'fixture_role_or_supplied_credential': role, 'complete': False} for role in roles},
                        assertions=['Match retained Original method, role and literal request path against the stable route declaration',
                                    'Preserve the observed HTTP outcome, scoped SQL assertions and exact process cleanup'],
                        result={'Source_case_observations': len(cases), 'new_Source_cases_this_task': 0, 'Native_coverage_increased': False},
                        cases=cases, fully_verified=False)
        contracts.append(contract)
    assert sum(len(contract['cases']) for contract in contracts) == receipt['route_path_literal_observations'] == 27
    return {'binding_kind': 'retained_original_Connect_log_read_and_subscription_set_HTTP',
            'operation_count': 2, 'retained_Source_responses_with_literal_route_path': 27,
            'additional_symbolic_fixture_path_responses': 4, 'fully_verified_operations': 0,
            'new_Source_executions': 0, 'Native_coverage_increased': False,
            'fixtures': fixtures, 'contracts': contracts,
            'credit': 'Original Dispatcharr and dependency authors retain all application and algorithm credit.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--private-root', type=Path, required=True)
    parser.add_argument('--register', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(generate(args.private_root.resolve(), load(args.register)), indent=2) + '\n')
