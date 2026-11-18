"""Retained SD HTTP assignments with actual four-path resolver custody.
Original Dispatcharr, Django/DRF and dependency authors retain all credit.
No application, resolver, network or database call is executed by this module.
"""
import collections
import copy
import hashlib
import json
from pathlib import Path

KIND = 'retained_original_SD25_resolver_scoped_HTTP'
BASE = 'scripts/rust-contracts/retained-sd/'
INPUT = BASE + 'bindings.json'
INVENTORY_SHA = '17615493dff02341212c50a1c541e67b5d82a42fa6e036d7fb14433ea43baffa'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def checked(root, name, expected):
    path = Path(root) / name
    assert path.is_file() and not path.is_symlink()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, 'exact SD authority bytes'
    return json.loads(path.read_bytes())


def inventory(root):
    return checked(root, BASE + 'inventory.json', INVENTORY_SHA)


def dependencies():
    root = Path(__file__).resolve().parents[2]
    return {INPUT, BASE + 'inventory.json', *(BASE + name for name in inventory(root))}


def generate(root):
    files = {name: checked(root, BASE + name, sha) for name, sha in inventory(root).items()
             if name.endswith('.json')}
    # Non-JSON executed source bytes remain exact dependencies too.
    for name, sha in inventory(root).items():
        assert hashlib.sha256((Path(root) / BASE / name).read_bytes()).hexdigest() == sha
    candidate = files['candidate-input.json']
    q3, q4 = files['Source3-qualification.json'], files['Source10-qualification.json']
    terminal, report, observed = (files['resolver/' + name] for name in ('Root-terminal.json', 'UID-report.json', 'observations.json'))
    assert terminal['passed'] and terminal['actual_exit'] == 0 and terminal['failure'] is None
    assert all(terminal[key] is True for key in ['stdout_EOF', 'stderr_EOF', 'process_group_settled'])
    assert terminal['remaining_group_members'] == [] and terminal['output_bytes_discarded'] == 0
    assert terminal['UID_report_sha256'] == inventory(root)['resolver/UID-report.json']
    assert report['passed'] and report['control_actual_exit'] == report['actual_exit'] == 0
    assert report['exact_CID_absent'] is True and len(report['exact_CID']) == 64
    assert report['observation_sha256'] == inventory(root)['resolver/observations.json']
    assert report['prepared_closure_sha256'] == inventory(root)['resolver/executed-control/closure.json']
    closure = files['resolver/executed-control/closure.json']
    for name in ['input.json', 'plan.json', 'source-files.json', 'qualify_resolver.py', 'run_uid.py']:
        assert closure['files'][name] == inventory(root)['resolver/executed-control/' + name]
    assert len(files['resolver/executed-control/source-files.json']) == 1211
    assert len(files['resolver/executed-control/input.json']['routes']) == 592
    assert observed['passed'] and observed['actual_UID'] == 65532
    assert observed['all592_resolver_identities_order_actions_exact'] is True
    assert observed['Source_HTTP_business_calls'] == report['Source_HTTP_business_calls'] == 0
    counts = observed['counts']
    assert counts == report['actual_guard_counts']
    assert counts == {'SQL_connection_or_cursor_attempts': 0, 'network_attempts': 1,
                      'thread_attempts': 0, 'child_attempts': 0, 'handler_invocations': 0}
    assert report['qualified_blocked_import_probe_count'] == observed['qualified_blocked_import_probe_count'] == 1
    denied, = observed['denied_operations']
    assert denied['stage'] == 'django_setup' and denied['operation'] == 'socket.socket.bind'
    assert denied['caller_file_sha256'] == '2633bbdb69731e5ccb5cf4e4afd65605d86c7979cc5633126f50c92d5ad74a74'
    assert denied['caller']['module'] == 'urllib3.util.connection' and denied['caller']['function'] == '_has_ipv6'
    assert denied['original_operation_executed'] is False and denied['IPv6_loopback_ephemeral_bind'] is True
    assert q3['passed'] and q3['Source_target_HTTP_calls_complete'] == 20
    assert q3['actual_original_controller_exit'] == q4['actual_controller_exit'] == 1
    assert q3['separate_recovered21_actualjournal_complete'] is True
    assert q4['new_SD_captures_qualified'] == 4 and q4['new_timeshift_captures_qualified'] == 0
    resolutions = observed['observations']
    assert len(resolutions) == 4
    paths = {}
    for resolved in resolutions:
        assert resolved['path'] not in paths
        assert resolved['actual_resolver_ordinal'] == 155
        assert resolved['actual_pattern'] == 'api/epg/^sources/(?P<pk>[^/.]+)/sd-lineups/$'
        assert resolved['actual_handler'] == 'apps.epg.api_views.EPGSourceViewSet'
        assert resolved['actual_view_name'] == 'api:epg:epg-source-sd-lineups'
        assert resolved['actual_action_map'] == {'get': 'sd_lineups', 'post': 'sd_lineups', 'delete': 'sd_lineups'}
        assert resolved['actual_kwargs'] == {'pk': resolved['path'].split('/')[4]}
        paths[resolved['path']] = resolved
    fixtures = {}
    contracts = []
    side = []
    all_cases = [case for contract in candidate['contracts'] for case in contract['cases']] + candidate['unmapped_method_observations']
    assert len(all_cases) == 25
    for row in all_cases:
        witness = paths[row['path']]
        match = [x for x in witness['retained_observations'] if x['Source_group'] == row['Source_group'] and x['label'] == row['label']]
        assert len(match) == 1
        assert all(row[k] == match[0][k] for k in ['method', 'role', 'HTTP_status', 'private_wire_sha256', 'private_wire_path'])
        # Qualifiers bind the exact original private wire, without publishing JWTs.
        qualifier = q4 if row['Source_group'] == 'Source10-qualified-SD4' else q3
        refs = qualifier['evidence']
        actual = [v for k, v in refs.items() if k.endswith('/' + row['label'] + '/wire.json')]
        assert len(actual) == 1 and actual[0]['sha256'] == row['private_wire_sha256']
        if row['Source_group'] == 'Source3-qualified20':
            step = q3['steps'][row['label']]
            assert all(row[k] == step[v] for k, v in [('method', 'method'), ('role', 'role'), ('HTTP_status', 'actual_status'), ('body_sha256', 'actual_response_body_sha256')])
        if row['Source_group'] == 'Source10-qualified-SD4':
            leaf, = [x for x in q4['leaves'] if x['label'] == row['label']]
            assert leaf['capture_qualified'] and leaf['actual_HTTP_status'] == row['HTTP_status']
            assert leaf['body_sha256'] == row['body_sha256'] and leaf['actual_body_bytes'] == row['body_bytes']
    for candidate_contract in candidate['contracts']:
        contract = copy.deepcopy(candidate_contract)
        contract.pop('status'); contract.pop('route_binding_scope')
        contract['fixture_id'] = 'retained-sd:' + contract['method']
        contract['observed_methods'] = [contract['method']]
        contract['source_role_specs'] = {role: {'observed_label': role, 'scope': 'Actual retained Source actor label; complete role matrix unqualified'} for role in contract['observed_roles']}
        for case in contract['cases']:
            case['actual_resolver_witness_sha256'] = digest(paths[case['path']])
            case['response_body_sha256'] = case['body_sha256']
        contract['assertions'] = ['Exact qualified response status/body digest and retained raw wire custody', 'Actual full592 resolver/order/actions and successful URLPattern chain bind each actual path to ordinal155', 'Retained qualified SQL/sequences/provider/jobs/WebSocket observations; no Native acceptance']
        contract['result'] = {'state': 'retained_Source_HTTP_observations_with_actual_resolver_addendum', 'Source_observations_qualified': len(contract['cases']), 'new_Source_cases_this_task': 0, 'Native_coverage_increased': False, 'fully_verified': False}
        contract['remaining'] = ['Complete methods/roles/inputs and Native parity remain unqualified', 'Full milestone and performance acceptance remain open']
        fixtures[contract['fixture_id']] = {'baseline_revision': '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430', 'baseline_image': report['image_id'], 'setup': ['Retained owned Source3 SD models and credential-condition actors', 'Retained owned TLS SD Origin/trust and full SQL/cache/job/WS capture', 'Retained Source10 four SD exceptional observations; original controller1 preserved'], 'actual_Source_controller_exits': {'Source3': 1, 'Source10': 1}, 'resolver_addendum_exit': 0, 'Source_HTTP_replayed': False, 'full_executed_Source_HTTP_setup_reconstruction_claimed': False}
        contracts.append(contract)
    for original in candidate['unmapped_method_observations']:
        side.append({'route_id': 'route-7e6941c073eccaa2b6dd', 'contract_id': None, 'pattern_exact': resolutions[0]['actual_pattern'], 'source_handler': resolutions[0]['actual_handler'], 'observed_method': original['method'], 'observation': copy.deepcopy(original), 'declared_method_set_expanded': False, 'fully_verified': False, 'scope': 'Retained OPTIONS/HEAD/PUT observation without a declared operation; no method alias inferred'})
    assert collections.Counter(c['method'] for c in all_cases) == {'GET': 8, 'DELETE': 11, 'OPTIONS': 4, 'HEAD': 1, 'PUT': 1}
    return {'format_version': 1, 'binding_kind': KIND, 'credit': 'Original Dispatcharr and dependency authors retain all application/algorithm credit.', 'contracts': contracts, 'fixtures': fixtures, 'operation_count': 2, 'operation_bound_Source_observations': 19, 'unmapped_method_Source_observations': side, 'qualified_Source_observations': 25, 'actual_resolver_paths': 4, 'new_Source_HTTP_executions': 0, 'new_pure_resolver_execution': 1, 'actual_resolver_network_attempts': 1, 'original_network_operations_executed': 0, 'fully_verified_operations': 0, 'Native_coverage_increased': False, 'formal_milestone_accepted': False}


def validate(root, value, bindings=None):
    expected = generate(root)
    assert value == expected, 'exact retained SD assignments and actual resolver custody'
    return {'new_assignments': 2, 'Source_observations': 25, 'operation_observations': 19, 'method_side_observations': 6, 'fully_verified_added': 0}


def restore_pre_SD(rows):
    """Reverse only reviewed SD assignment fields for immutable older oracles."""
    root = Path(__file__).resolve().parents[2]
    packet = generate(root)
    proof = checked(root, BASE + 'preservation.json', inventory(root)['preservation.json'])
    selected = {c['contract_id']: c for c in packet['contracts']}
    for route in rows:
        for operation in route['operations']:
            identifier = operation['contract_id']
            if identifier not in selected:
                continue
            assert operation['executable_Source_contract'] == selected[identifier]
            # Verify all added fields before restoring the frozen two-row baseline.
            prior = proof['selected'][identifier]
            expected = copy.deepcopy(prior)
            c = selected[identifier]
            expected.update(setup_fixture=packet['fixtures'][c['fixture_id']], assertions=c['assertions'], result=c['result'], executable_Source_contract=c, coverage_state='partial_executable_Source_contract')
            expected['role_matrix'].update(observed_fixture_definitions=c['source_role_specs'], complete=False, state='partial_Original_real_HTTP_capture_assignment', observed_roles=c['observed_roles'])
            assert operation == expected
            operation.clear(); operation.update(copy.deepcopy(prior))
    return rows
