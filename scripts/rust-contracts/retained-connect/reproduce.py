"""Project the two retained packaged Original Connect collection observations."""
import argparse
import hashlib
import json
from pathlib import Path

KIND = 'retained_original_Connect_collection_HTTP'
ROUTE = '/api/connect/integrations/'
DRIVER = 'scripts/rust-account-contracts/probes.py'
REQUESTS = 'scripts/rust-integration/run.py'
GET = {
    'empty-collection': ('admin bearer empty collection', 200),
    'admin-seeded-collection': ('admin bearer seeded collection', 200),
    'anonymous': ('anonymous', 401),
    'standard': ('standard bearer', 403),
    'viewer': ('viewer bearer', 403),
    'inactive-admin': ('inactive admin bearer', 401),
    'admin-api-key': ('admin API key', 200),
    'invalid-bearer-before-api-key': ('invalid bearer plus admin API key', 401),
}
POST = {
    'anonymous': ('anonymous', 401),
    'standard': ('standard bearer', 403),
    'viewer': ('viewer bearer', 403),
    'inactive-admin': ('inactive admin bearer', 401),
    'role-before-json-parser': ('standard bearer', 403),
    'admin-malformed-json': ('admin bearer', 400),
    'required-fields': ('admin bearer', 400),
    'invalid-field-aggregation': ('admin bearer', 400),
    'top-level-array': ('admin bearer', 400),
    'missing-webhook-url': ('admin bearer', 400),
    'non-string-webhook-url': ('admin bearer', 400),
    'unsupported-model-type': ('admin bearer', 400),
    'missing-local-script': ('admin bearer', 400),
    'stock-webhook': ('admin bearer', 201),
    'duplicate-name-and-default-enabled': ('admin bearer', 201),
    'existing-directory-script-path': ('admin bearer', 201),
}
REJECT = ('database_rows_and_sequence_unchanged',)
CREATE = ('exactly_one_integration_and_sequence_advance_without_subscriptions_or_deliveries',
          'utc_creation_in_request_window')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_bytes())


def observations(root, run_id, source_baseline, method):
    run = root/'artifacts/rust'/run_id
    paths = [run/name for name in ('receipt.json', 'execution.json', 'independent-cleanup.json', 'evidence/integration.json')]
    receipt, execution, cleanup, report = map(load, paths)
    assert receipt['source_revision'] == report['source_baseline'] == source_baseline
    assert not receipt['passed'] and not report['passed']
    assert receipt['tracked_runner_actual_exit'] == execution['actual_exit'] == 1
    assert execution['failure'] is None and execution['remaining_group_members'] == []
    assert all(execution[key] for key in ('stdout_EOF', 'stderr_EOF', 'process_group_settled'))
    assert receipt['project'] == cleanup['project'] == report['project']
    assert cleanup['passed'] and all(not owned for owned in cleanup['inventories'].values())
    [foundation] = [check for check in report['checks'] if check['name'] == 'status-parity:/proxy/ts/stream/not-a-channel']
    assert not foundation['passed'] and foundation['details']['python_status'] == foundation['details']['rust_status'] == 404
    assert foundation['details']['python_body_sha256'] != foundation['details']['rust_body_sha256']
    driver = run/'evidence/tool-source'/DRIVER
    paths.append(driver)
    assert sha(driver) == report['tool_source_sha256'][DRIVER]
    if method == 'GET':
        assert receipt['connect_scope_passed']
        probe = driver.read_text().split('def run_connect_reads(', 1)[1]
        http = run/'evidence/tool-source'/REQUESTS
        paths.append(http)
        assert sha(http) == report['tool_source_sha256'][REQUESTS]
        assert 'def http(url, body=None, token=None, method=None,' in http.read_text()
        assert 'data=json.dumps(body).encode() if body is not None else None' in http.read_text()
        assert 'return http(base + route, token=token,' in probe
        prefix, roles = 'connect-reads:', GET
    else:
        assert receipt['original_create_scope_passed'] and receipt['scope'] == 'original_connect_create_only'
        probe = driver.read_text().split('def run_connect_create_source(', 1)[1]
        assert "http(baseline+route, body=data, token=token, method='POST'," in probe
        assert "urllib.request.Request(baseline+route, data=b'{', headers=headers, method='POST')" in probe
        assert 'def post(data, token=admin):' in probe and "malformed(accounts['standard']['access'])" in probe
        prefix, roles = 'connect-create-source:', POST
    assert f"route = '{ROUTE}'" in probe
    cases = []
    for index, check in enumerate(report['checks']):
        if not check['name'].startswith(prefix):
            continue
        name = check['name'].removeprefix(prefix)
        role, status = roles[name]
        details = check['details']
        assert check['passed']
        if method == 'GET':
            assert details['original_contract_verified'] and details['python_status'] == details['rust_status'] == status
            assert details['original_mime'] == details['requested_accept'] == 'application/json'
            assert details['python_body_sha256'] == details['rust_body_sha256']
            digest = details['python_body_sha256']
            effects = []
        else:
            assert details['actual_status'] == status
            effects = list(CREATE if status == 201 else REJECT)
            assert all(details[key] for key in effects)
            digest = details['actual_body_sha256']
        cases.append({'case': name, 'role': role, 'method': method, 'path': ROUTE,
                      'status': status, 'response_projection_sha256': digest,
                      'observed_effects': effects, 'observation_pointer': f'/checks/{index}'})
    assert [row['case'] for row in cases] == list(roles)
    fixture = {'original_transport': 'packaged Original TCP HTTP', 'actual_runner_exit': 1,
               'outer_passed': False, 'exact_cleanup_passed': True,
               'other_failed_gate': foundation, 'source_revision': source_baseline,
               'candidate_revision': receipt['candidate_revision'],
               'retained_input_sha256': {str(path.relative_to(root)): sha(path) for path in paths}}
    if method == 'POST':
        [guard] = [(i, case) for i, case in enumerate(report['checks']) if case['name'] == 'connect-create-no-dispatch']
        assert guard[1]['passed'] and guard[1]['details'] == {'issued_only_original_collection_post': True,
            'original_creation_post_count': 16, 'actual_owned_webhook_requests_before': 0,
            'actual_owned_webhook_requests_after': 0}
        fixture['owned_webhook_guard'] = {'pointer': f'/checks/{guard[0]}', **guard[1]['details']}
        fixture['native_create_exercised'] = False
    return fixture, cases


def generate(root, previous):
    baseline = previous['baseline_ref']
    [route] = [row for row in previous['runtime_routes'] if row['route_id'] == 'route-04cdc39881caace6c343']
    assert route['pattern_exact'] == 'api/connect/^integrations/$'
    assert route['source_handler'] == 'apps.connect.api_views.IntegrationViewSet'
    declared = {op['method']: op for op in route['operations']}
    fixtures = {}
    contracts = []
    for method, action, fixture_id in [('GET', 'list', 'm7-connect-list-candidate78-1'),
                                       ('POST', 'create', 'm7-connect-create-candidate78-source-1')]:
        operation = declared[method]
        assert operation['source_action'] == action and operation['coverage_state'] == 'not_yet_mapped'
        assert operation.get('executable_Source_contract') is None and not operation['fully_verified']
        fixtures[fixture_id], cases = observations(root, fixture_id, baseline, method)
        roles = sorted({case['role'] for case in cases})
        contract = {key: operation[key] for key in ('contract_id', 'method', 'source_action', 'owning_milestone')}
        contract.update(pattern_exact=route['pattern_exact'], source_handler=route['source_handler'], fixture_id=fixture_id,
                        observed_methods=[method], observed_roles=roles,
                        source_role_specs={role: {'fixture_role_or_supplied_credential': role, 'complete': False} for role in roles},
                        fully_verified=False,
                        assertions=['Match the literal Original collection path, method, fixture role and retained HTTP response',
                                    'Preserve observed response bodies by projection digest and scoped database observation'],
                        result={'Source_case_observations': len(cases), 'new_Source_cases_this_task': 0, 'Native_coverage_increased': False},
                        cases=cases)
        contracts.append(contract)
    return {'binding_kind': KIND, 'operation_count': 2, 'retained_Source_responses': 24,
            'fully_verified_operations': 0, 'Native_coverage_increased': False, 'new_Source_executions': 0,
            'fixtures': fixtures, 'contracts': contracts,
            'credit': 'Original Dispatcharr authors retain all application and algorithm credit.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--private-root', type=Path, required=True)
    parser.add_argument('--register', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    value = generate(args.private_root, load(args.register))
    args.output.write_text(json.dumps(value, indent=2) + '\n')
