"""Reproduce scoped Original Connect detail evidence from frozen private artifacts."""
import argparse
import hashlib
import json
from pathlib import Path
import re


KIND = 'retained_original_Connect_detail_PUT_DELETE_HTTP'
ROUTE = 'route-a6f08b2b757cba093f6e'
DETAIL = '/api/connect/integrations/'
MISSING = DETAIL + '9223372036854775806/'
DRIVER = 'scripts/rust-account-contracts/probes.py'
CLIENT = 'scripts/rust-integration/run.py'
META = 'scripts/rust-integration/metadata_fixture.py'
ERRORS = {'Unauthorized': 401, 'Forbidden': 403, 'Not Found': 404, 'Bad Request': 400}
ACCESS = re.compile(r'^baseline-1\s+\| .* django\.request (?P<error>Unauthorized|Forbidden|Not Found|Bad Request): (?P<path>/api/connect/integrations/\d+/)$')
ROLE_CASES = (
    ('anonymous', 'anonymous', 401), ('standard', 'standard', 403), ('viewer', 'viewer', 403),
    ('inactive-admin', 'inactive admin', 401),
)
UPDATE_CASES = ROLE_CASES + (
    ('role-before-parser', 'standard', 403), ('lookup-before-parser', 'admin', 404),
    ('admin-malformed-body', 'admin', 400), ('wrong-top-level', 'admin', 400),
    ('required-name-type', 'admin', 400), ('field-error-aggregation', 'admin', 400),
    ('invalid-webhook-config', 'admin', 400), ('unsupported-model-type', 'admin', 400),
    ('script-path-missing', 'admin', 400), ('type-change-revalidates-retained-config', 'admin', 400),
    ('stock-webhook-form', 'admin', 200), ('missing-config-enabled-preserve-stored', 'admin', 200),
    ('stock-enabled-toggle-with-read-only-fields', 'admin', 200),
    ('script-conversion-preserves-existing-children', 'admin', 200),
)
DELETE_CASES = ROLE_CASES + (
    ('permission-before-lookup', 'standard', 403), ('missing-integration', 'admin', 404),
    ('stock-delete-exact-cascade', 'admin', 204), ('repeat-delete', 'admin', 404),
    ('childless-delete-ignores-unused-body', 'admin', 204),
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_bytes())


def fixture_evidence(root, fixture_id, operation, method, source_revision, expected_cases):
    run = root/'artifacts/rust'/fixture_id
    inputs = [run/file for file in ('receipt.json', 'execution.json', 'independent-cleanup.json', 'evidence/integration.json')]
    receipt, execution, cleanup, report = map(load, inputs)
    assert receipt['source_revision'] == report['source_baseline'] == source_revision
    assert receipt['scope'] == f'original_connect_{operation}_only'
    assert receipt['passed'] and report['passed'] and receipt[f'original_{operation}_scope_passed']
    assert receipt['foundation'] == {'eligible_for_connect_scope': True, 'mismatches': [], 'passed': True}
    assert receipt['tracked_runner_actual_exit'] == execution['actual_exit'] == 0
    assert execution['failure'] is None and not execution['remaining_group_members']
    assert all(execution[key] for key in ('process_group_settled', 'stdout_EOF', 'stderr_EOF'))
    assert receipt['project'] == report['project'] == cleanup['project']
    assert cleanup['passed'] and cleanup['inventories'] == {'container': [], 'network': [], 'volume': []}
    assert {row['service'] for row in cleanup['owned_container_generations']} == {'baseline', 'origin', 'candidate', 'data-init'}
    assert all(row['container_absent'] and row['host_generation_absent'] for row in cleanup['owned_container_generations'])
    assert receipt['performance_verified'] is report['application_performance_verified'] is report['resources_retained'] is False

    sources = {}
    for file in (DRIVER, META, CLIENT):
        path = run/'evidence/tool-source'/file
        assert digest(path) == report['tool_source_sha256'][file]
        inputs.append(path)
        sources[file] = path.read_text()
    function = f'run_connect_{operation}_source'
    assert f"('connect_{operation}_source','{function}')" in sources[CLIENT]
    probe = sources[DRIVER].split(f'def {function}(', 1)[1].split('\ndef ', 1)[0]
    assert "route = f'/api/connect/integrations/{integration}/'" in probe
    assert f"method='{method}'" in probe
    assert f"'action':'connect_{operation}_seed'" in probe.replace(' ', '')
    assert f"'action':'connect_{operation}_snapshot'" in probe.replace(' ', '')
    assert 'connect_create_owned_origin_count' in probe
    assert all(name in sources[META] for name in ('Integration.objects.bulk_create', 'EventSubscription.objects.bulk_create',
                                                    'DeliveryLog.objects.bulk_create', 'SELECT last_value'))

    log = run/'evidence/compose.log'
    inputs.append(log)
    warnings = iter((index, matched['path'], ERRORS[matched['error']])
                    for index, line in enumerate(log.read_text().splitlines(), 1)
                    if (matched := ACCESS.fullmatch(line)))
    prefix = f'connect-{operation}-source:'
    observed = [(i, row) for i, row in enumerate(report['checks']) if row['name'].startswith(prefix)]
    literal, symbolic = [], []
    target_path = None
    for (index, check), (label, role, status) in zip(observed, expected_cases, strict=True):
        assert check['name'] == prefix + label and check['passed']
        detail = check['details']
        assert detail['actual_status'] == status and re.fullmatch('[0-9a-f]{64}', detail['actual_body_sha256'])
        byte_count = detail.get('actual_body_bytes')
        case = {'case': label, 'method': method, 'role': role, 'status': status,
                'body_sha256': detail['actual_body_sha256'], 'observation_pointer': f'/checks/{index}'}
        effects = {key: value for key, value in detail.items() if key not in
                   ('actual_body_sha256', 'actual_status', 'actual_body_bytes')}
        case['observed_effects'] = effects
        if method == 'PUT':
            assert byte_count is None
            effect = ('exact_only_target_updated_with_references_and_sequences_preserved' if status == 200
                      else 'database_references_and_sequences_unchanged')
            assert effects == {effect: True}
            case['body_digest_scope'] = 'sorted canonical JSON projection'
        else:
            assert type(byte_count) is int and byte_count >= 0
            case.update(body_bytes=byte_count, body_digest_scope='actual HTTP response bytes')
            assert effects == ({
                'exact_fixture_shape_observed': True,
                'exact_cascade_with_unrelated_rows_and_sequences_preserved': True,
            } if status == 204 else {'all_connect_rows_and_sequences_unchanged': True})
            if status == 204:
                assert byte_count == 0 and case['body_sha256'] == hashlib.sha256(b'').hexdigest()
                if label == 'stock-delete-exact-cascade':
                    assert "removes('stock-delete-exact-cascade', target, subscriptions=2, logs=3)" in probe
                    case['removed_owned_SQL_rows'] = {'integrations': 1, 'subscriptions': 2, 'delivery_logs': 3}
                else:
                    assert "removes('childless-delete-ignores-unused-body', childless, subscriptions=0, logs=0, raw=b'{')" in probe
                    case['removed_owned_SQL_rows'] = {'integrations': 1, 'subscriptions': 0, 'delivery_logs': 0}
        if status in (200, 204):
            owned_label = 'edited' if method == 'PUT' else ('childless' if label.startswith('childless-') else 'target')
            case.update(path_template=DETAIL + '{fixture_' + owned_label + '_id}/',
                        original_log_line=None, literal_path_verified=False)
            symbolic.append(case)
            continue
        line, path, recorded_status = next(warnings)
        assert recorded_status == status
        if label in ('permission-before-lookup', 'missing-integration', 'lookup-before-parser'):
            assert path == MISSING
        else:
            if target_path is None:
                target_path = path
            assert path == target_path and path != MISSING
        case.update(path=path, original_log_line=line, literal_path_verified=True)
        literal.append(case)
    assert next(warnings, None) is None

    guard_name = f'connect-{operation}-no-dispatch'
    [guard] = [row for row in report['checks'] if row['name'] == guard_name]
    assert guard['passed']
    count = len(expected_cases)
    guard_counts = {'put_count': count} if method == 'PUT' else {'delete_attempted': count, 'delete_completed': count}
    assert guard['details'] == {
        f'only_owned_integration_detail_{method.lower()}': True,
        'owned_receiver_before': 0, 'owned_receiver_after': 0, **guard_counts,
    }
    return literal, {
        'source_revision': source_revision, 'actual_runner_exit': 0, 'exact_cleanup_passed': True,
        'candidate_revision': receipt['candidate_revision'], 'candidate_detail_method_exercised': False,
        'input_sha256': {str(path.relative_to(root)): digest(path) for path in inputs},
        'owned_receiver_guard': guard['details'], 'actual_successes_with_symbolic_ID': symbolic,
    }


def generate(root, register):
    source_revision = register['baseline_ref']
    operations = {operation['contract_id']: (route, operation)
                  for route in register['runtime_routes'] for operation in route['operations']}
    fixtures = {}
    contracts = []
    for method, verb, action, expected_cases in (('PUT', 'update', 'update', UPDATE_CASES),
                                                ('DELETE', 'delete', 'destroy', DELETE_CASES)):
        contract_id = f'{ROUTE}:{method}'
        route, operation = operations[contract_id]
        assert (route['resolver_ordinal'], route['name'], route['pattern_exact'], route['source_handler']) == (
            334, 'api:connect:integration-detail', 'api/connect/^integrations/(?P<pk>[^/.]+)/$', 'apps.connect.api_views.IntegrationViewSet'
        )
        assert operation['method'] == method and operation['source_action'] == action and not operation['fully_verified']
        assert operation['coverage_state'] == 'not_yet_mapped' and not operation['related_evidence']
        fixture_id = f'm7-connect-{verb}-candidate80-source-1'
        cases, fixtures[fixture_id] = fixture_evidence(root, fixture_id, verb, method, source_revision, expected_cases)
        roles = sorted({case['role'] for case in cases})
        contracts.append({
            'contract_id': contract_id, 'method': method, 'source_action': action,
            'owning_milestone': operation['owning_milestone'], 'pattern_exact': route['pattern_exact'],
            'source_handler': route['source_handler'], 'fixture_id': fixture_id, 'observed_roles': roles, 'observed_methods': [method],
            'source_role_specs': {role: {'fixture_role_or_supplied_credential': role, 'complete': False} for role in roles},
            'assertions': ['Match retained Original method, role and literal request path against the stable detail route',
                           'Preserve scoped Connect SQL and the owned receiver result with exact process cleanup'],
            'result': {'Source_case_observations': len(cases), 'new_Source_cases_this_task': 0, 'Native_coverage_increased': False},
            'cases': cases, 'fully_verified': False,
        })
    assert sum(len(contract['cases']) for contract in contracts) == 21
    assert sum(len(fixture['actual_successes_with_symbolic_ID']) for fixture in fixtures.values()) == 6
    return {
        'binding_kind': KIND, 'operation_count': 2, 'retained_Source_responses_with_literal_route_path': 21,
        'additional_symbolic_fixture_path_responses': 6, 'fully_verified_operations': 0,
        'new_Source_executions': 0, 'Native_coverage_increased': False, 'fixtures': fixtures, 'contracts': contracts,
        'credit': 'Original Dispatcharr and dependency authors retain all application and algorithm credit.',
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--private-root', type=Path, required=True)
    parser.add_argument('--register', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(generate(args.private_root.resolve(), load(args.register)), indent=2) + '\n')
