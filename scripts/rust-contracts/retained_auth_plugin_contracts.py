"""Retained auth51/plugin35 partial Source mappings with exact resolver custody.
Original Dispatcharr, Django/DRF and urllib3 authors retain all application and
framework credit and licenses. No Source request or resolver is executed here.
"""
import ast
import collections
import hashlib
import json
from pathlib import Path

KIND = 'retained_original_auth51_plugin35_resolver_scoped_HTTP'
BASE = 'scripts/rust-contracts/retained-auth-plugin/'
INPUT = BASE + 'bindings.json'
RESOLVER_INVENTORY_SHA = 'b3689776b8880c79b72c8b8c83e25b01c41a03bbab6905ebc473ccc00f176468'
AUTH_INDEX = {
    'auth1': ('scripts/rust-integration/auth-route-session-source1/', '79bcb6daf2ab10ea0957c6f18f8e920fb552f1a066eea93fd7663d0336562e2f'),
    'auth2': ('scripts/rust-integration/auth-route-session-source2/', 'd7bbe96b337dc69f2b5945f35dbde9ed9c5dcd124877c41313aecc82dd6eeea3'),
}
PLUGIN = 'scripts/rust-plugin-contracts/source-plugin-http-4.json'
PLUGIN_SHA = '56219aacf7b8844e8ced5724783aa752dec6d006f31039f2c568cd68020112d3'
PLUGIN_VERIFIED = 'scripts/rust-plugin-contracts/verified-plugin-http-4.json'
PLUGIN_VERIFIED_SHA = 'f8ba7630de3c9f86bb30e052da09c1d9574c56ab4581c73fd4c4c1e68d4af467'
CALLER_SHA = '2633bbdb69731e5ccb5cf4e4afd65605d86c7979cc5633126f50c92d5ad74a74'
IMAGE = 'sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def checked(root, name, expected):
    path = Path(root) / name
    assert path.is_file() and not path.is_symlink() and sha(path) == expected, 'exact retained authority bytes'
    return json.loads(path.read_bytes())


def resolver_inventory(root):
    return checked(root, BASE + 'resolver/inventory.json', RESOLVER_INVENTORY_SHA)


def dependencies():
    root = Path(__file__).resolve().parents[2]
    refs = {INPUT, BASE + 'resolver/inventory.json', PLUGIN, PLUGIN_VERIFIED, 'docs/rust-runtime-contracts.json'}
    refs.update(BASE + 'resolver/' + name for name in resolver_inventory(root))
    refs.add(BASE + 'definitions/inventory.json')
    refs.update(BASE + name for name in checked(root, BASE + 'definitions/inventory.json', DEFINITION_INVENTORY_SHA))
    for prefix, expected in AUTH_INDEX.values():
        index = checked(root, prefix + 'index.json', expected)
        refs.add(prefix + 'index.json')
        refs.update(prefix + item['path'] for item in index['files'])
    return refs


def qualify_resolver(root):
    """Offline separate component qualification; UID3 raw exit1 stays exit1."""
    root = Path(root)
    inventory = resolver_inventory(root)
    for name, expected in inventory.items():
        path = root / BASE / 'resolver' / name
        assert path.is_file() and not path.is_symlink() and sha(path) == expected
    base = root / BASE / 'resolver'
    load = lambda name: json.loads((base / name).read_bytes())
    terminal = load('Root-terminal.json')
    assert terminal['owner_exec_session'] == 23480 and terminal['actual_exit'] == 1
    assert terminal['actual_terminal_consumed'] is True and terminal['qualified'] is False
    assert terminal['exact_UID_CID_absent'] is True
    report = load('UID3-report.json')
    assert report['passed'] is False and report['control_actual_exit'] == 1
    assert report['image_id'] == IMAGE and report['exact_CID_absent'] is True
    closure = load('executed-control/closure.json')
    assert sha(base / 'executed-control/closure.json') == report['prepared_closure_sha256']
    for name, expected in closure['files'].items():
        assert sha(base / 'executed-control' / name) == expected
    control = load('executed-control/input.json')
    assert control['Original_revision'] == ORIGINAL and len(control['routes']) == 592
    assert len(load('executed-control/source-files.json')) == 1211
    discovery = control['runtime_discovery']
    checked(root, discovery['path'], discovery['sha256'])
    for reference in control['retained_case_authorities']:
        checked(root, reference['path'], reference['sha256'])
    commands = load('UID3-commands.json')
    runs = [row for row in commands if 'run' in row['argv']]
    assert len(runs) == 1 and runs[0]['actual_exit'] == 1
    argv = runs[0]['argv']
    assert IMAGE in argv and '--read-only' in argv and '--pull=never' in argv
    for option, value in [('--network', 'none'), ('--user', '65532:65532'), ('--entrypoint', '/dispatcharrpy/bin/python')]:
        assert argv[argv.index(option) + 1] == value
    observed = json.loads(runs[0]['stdout'].splitlines()[-1])
    assert observed == report['control_failure'] and observed['passed'] is False
    assert observed['actual_UID'] == 65532 and observed['error_type'] == 'AssertionError'
    assert observed['all592_resolver_identities_order_actions_exact'] is True
    assert observed['Source_HTTP_business_calls'] == 0 and observed['Source_or_Native_acceptance'] is False
    assert observed['counts'] == {'SQL_connection_or_cursor_attempts': 0, 'network_attempts': 1,
                                  'thread_attempts': 0, 'child_attempts': 0, 'handler_invocations': 0}
    assert len(observed['denied_operations']) == 1
    event = observed['denied_operations'][0]
    assert event['kind'] == 'network_attempts' and event['operation'] == 'socket.socket.bind'
    assert event['stage'] == 'django_setup' and event['socket_family'] == 10
    assert event['IPv6_loopback_ephemeral_bind'] is True and event['original_operation_executed'] is False
    caller = event['caller']
    assert caller == {'module': 'urllib3.util.connection', 'function': '_has_ipv6',
                      'filename': '/dispatcharrpy/lib/python3.13/site-packages/urllib3/util/connection.py', 'lineno': 127}
    assert event['caller_file_sha256'] == sha(base / 'urllib3-connection.py') == CALLER_SHA
    tree = ast.parse((base / 'urllib3-connection.py').read_bytes())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_has_ipv6')
    binds = [node for node in ast.walk(function) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute) and node.func.attr == 'bind']
    assert len(binds) == 1 and binds[0].lineno == caller['lineno']
    guarded = [node for node in ast.walk(function) if isinstance(node, ast.Try)
               and any(bind in list(ast.walk(stmt)) for stmt in node.body for bind in binds)]
    assert len(guarded) == 1 and any(isinstance(h.type, ast.Name) and h.type.id == 'Exception' for h in guarded[0].handlers)
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == 'HAS_IPV6' for target in node.targets)]
    assert len(assignments) == 1
    probe = assignments[0]
    assert probe.lineno == event['caller_frames'][1]['lineno'] == 137
    assert isinstance(probe.value, ast.Call) and isinstance(probe.value.func, ast.Name) and probe.value.func.id == '_has_ipv6'
    assert len(probe.value.args) == 1 and isinstance(probe.value.args[0], ast.Constant) and probe.value.args[0].value == '::1'
    cid = (base / 'UID3-probe.cid').read_text().strip()
    assert cid == report['exact_CID'] and len(cid) == 64
    absence = [row for row in commands if 'id=' + cid in row['argv'] and row['actual_exit'] == 0 and row['stdout'].strip() == '']
    assert absence and commands[-1] in absence
    resolutions = observed['completed_resolutions']
    assert len(resolutions) == len(control['cases']) == 86
    for actual, case in zip(resolutions, control['cases']):
        assert all(actual[key] == value for key, value in case.items())
        ordinal = actual['actual_resolver_ordinal']
        route = control['routes'][ordinal]
        assert actual['actual_pattern'] == route['pattern'] and actual['actual_view_name'] == route['name']
        assert actual['actual_handler'] == route['view'] and actual['actual_action_map'] == route['methods']
        assert actual['Source_HTTP_replayed'] is False and actual['method_permission_or_role_acceptance_added'] is False
    return {'qualified_route_binding_component': True, 'qualification_kind': 'offline_scoped_retained_UID3_resolution_under_one_denied_import_probe',
            'actual_original_UID_controller_exit': 1, 'actual_original_raw_passed': False,
            'actual_original_UID_scope_qualified': False, 'actual_guard_counts': observed['counts'],
            'actual_denied_import_probe': event, 'actual_resolver_routes': 592, 'retained_resolutions': 86,
            'underlying_network_operation_executed': False, 'new_Source_or_resolver_executions': 0,
            'runtime_network_initialization_parity_claimed': False, 'Native_or_milestone_acceptance': False}, resolutions, control['routes']


def retained_sources(root):
    root = Path(root)
    sources = {}
    authorities = []
    for group, (prefix, expected) in AUTH_INDEX.items():
        index = checked(root, prefix + 'index.json', expected)
        authorities.append({'path': prefix + 'index.json', 'sha256': expected})
        for item in index['files']:
            assert sha(root / prefix / item['path']) == item['sha256']
        qualified = json.loads((root / prefix / 'qualification.json').read_bytes())
        assert qualified['actual_controller_exit'] == 0 and qualified['Source_observations_qualified'] == (45 if group == 'auth1' else 6)
        assert qualified['Native_acceptance'] is False and qualified['full_milestone_accepted'] is False
        assert len(qualified['observations']) == (45 if group == 'auth1' else 6)
        sources[group] = qualified['observations']
    plugin = checked(root, PLUGIN, PLUGIN_SHA)
    verification = checked(root, PLUGIN_VERIFIED, PLUGIN_VERIFIED_SHA)
    assert verification['passed'] is True and verification['actual_TCP_cases'] == 35
    assert verification['Original_revision'] == ORIGINAL and verification['Original_image'].endswith(IMAGE)
    assert verification['dataset_sha256'] == PLUGIN_SHA and len(plugin['cases']) == 35
    assert plugin['native_verified'] is False and plugin['performance_verified'] is False
    sources['plugin35'] = plugin['cases']
    authorities.extend([{'path': PLUGIN, 'sha256': PLUGIN_SHA}, {'path': PLUGIN_VERIFIED, 'sha256': PLUGIN_VERIFIED_SHA}])
    return sources, authorities

DEFINITION_INVENTORY_SHA = 'bb4e074bcdfe77f30ca81248d639a256084abafe9a6e33eb3e7b7adfb9d3b8dd'


def definitions(root):
    inventory = checked(root, BASE + 'definitions/inventory.json', DEFINITION_INVENTORY_SHA)
    assert len(inventory) == 6
    for name, expected in inventory.items():
        assert sha(Path(root) / BASE / name) == expected
    for group in ['auth1', 'auth2']:
        prefix = 'definitions/' + group + '/'
        closure = json.loads((Path(root) / BASE / prefix / 'driver-closure.json').read_bytes())
        assert closure['files']['cohort/auth_route_session.py'] == inventory[prefix + 'auth_route_session.py']
        assert closure['Source_image'] == IMAGE
    verification = checked(root, PLUGIN_VERIFIED, PLUGIN_VERIFIED_SHA)
    for name in ['source_http.py', 'run_http.py']:
        assert inventory['definitions/plugin35/' + name] == verification['tools']['scripts/rust-plugin-contracts/' + name]
    return inventory


def derive(root):
    root = Path(root)
    qualification, resolved, routes = qualify_resolver(root)
    sources, authorities = retained_sources(root)
    recipe_inventory = definitions(root)
    seen = collections.Counter()
    identifiers = []
    for route in routes:
        identity = {key: route.get(key) for key in ['name', 'pattern', 'view', 'methods']}
        token = digest(identity)
        occurrence = seen[token]
        seen[token] += 1
        identifiers.append('route-' + token[:20] + ('-' + str(occurrence) if occurrence else ''))
    grouped = collections.defaultdict(list)
    side = []
    for actual in resolved:
        group = actual['group']
        index = actual['Source_index']
        source = sources[group][index]
        assert all(source[key] == actual[key] for key in ['role', 'method', 'path'])
        assert source['status'] == actual['observed_HTTP_status']
        route = routes[actual['actual_resolver_ordinal']]
        route_id = identifiers[actual['actual_resolver_ordinal']]
        observed_method = actual['method']
        declared = route.get('methods')
        definition_method = observed_method if declared else None
        row = {'source_group': group, 'Source_index': index,
               'source_authority_path': PLUGIN if group == 'plugin35' else AUTH_INDEX[group][0] + 'qualification.json',
               'source_authority_pointer': ('/cases/' if group == 'plugin35' else '/observations/') + str(index),
               'source_record_sha256': digest(source), 'role': source['role'],
               'method': observed_method, 'path': source['path'], 'status': source['status'],
               'response_body_sha256': source.get('body_sha256'),
               'actual_resolver_ordinal': actual['actual_resolver_ordinal'],
               'actual_resolver_witness_sha256': digest(actual),
               'actual_kwargs': actual['actual_kwargs'],
               'Source_business_replayed': False, 'Native_acceptance': False}
        assert len(row['response_body_sha256']) == 64
        if group == 'plugin35':
            row['response_body_bytes'] = source['body_bytes']
            assert source['actual_original_TCP'] is True
        if declared and observed_method.lower() not in declared:
            assert group.startswith('auth')
            side.append({'route_id': route_id, 'contract_id': None, 'pattern_exact': route['pattern'],
                         'source_handler': route['view'], 'observed_method': observed_method,
                         'observation': row, 'declared_method_set_expanded': False,
                         'fully_verified': False,
                         'scope': 'Actual retained method-refusal observation outside discovery declared operations; no guessed operation ID or method/format alias'})
            continue
        identifier = route_id + ':' + (definition_method or 'METHOD_UNRESOLVED')
        grouped[identifier].append((row, route, definition_method,
                                    declared[observed_method.lower()] if declared else None))
    assert len(grouped) == 13 and len(side) == 14
    assert sum(len(rows) for rows in grouped.values()) == 72
    assert collections.Counter(x['source_group'] for rows in grouped.values() for x, *_ in rows) == {'auth1': 31, 'auth2': 6, 'plugin35': 35}
    fixtures = {}
    contracts = []
    for identifier, entries in sorted(grouped.items()):
        cases = [entry[0] for entry in entries]
        route, method, action = entries[0][1:]
        assert all(entry[1:] == (route, method, action) for entry in entries)
        groups = sorted({case['source_group'] for case in cases})
        roles = sorted({case['role'] for case in cases})
        fixture_id = 'retained-auth-plugin:' + identifier
        contexts = {}
        for group in groups:
            recipe = 'definitions/' + group + '/' + ('source_http.py' if group == 'plugin35' else 'auth_route_session.py')
            contexts[group] = {'executed_recipe_definition': {'path': BASE + recipe, 'sha256': recipe_inventory[recipe]},
                               'actual_Source_controller_exit': 0,
                               'qualified_existing_Original_observations': 35 if group == 'plugin35' else 45 if group == 'auth1' else 6,
                               'source_case_indices': [case['Source_index'] for case in cases if case['source_group'] == group],
                               'scope': ('Actual pinned Source uWSGI/TCP, owned plugin config/users/files/Python/ORM execution; no Native or generic plugin executor acceptance' if group == 'plugin35'
                                         else 'Actual isolated Source TCP/credentials/cookies/CSRF/JWT/session observations; Source1 streamer429 and Source2 fresh six-call streamer companion remain distinct; no replay or single51-controller claim'),
                               'full_executed_controller_reconstruction_from_this_assignment_packet_claimed': False}
        fixtures[fixture_id] = {'id': fixture_id, 'baseline_revision': ORIGINAL, 'baseline_image': IMAGE,
                                'contexts': contexts, 'Source_authorities': authorities,
                                'setup_assertion_definition': 'Exact executed recipe bytes and indexed original qualified Source observations; owned actor/plugin setup, raw credential omission and selected SQL/cache/event/worker cleanup scopes remain those of the original capture. This packet is not a new full-controller replay closure.',
                                'resolver_authority': {'public_component_inventory_sha256': RESOLVER_INVENTORY_SHA,
                                                       'qualified_component': True, 'original_UID_controller_exit': 1,
                                                       'raw_original_UID_passed': False, 'network_attempts': 1,
                                                       'blocked_attempt_scope': 'Measured urllib3 import _has_ipv6 at django_setup; AF_INET6/::1/port0 bind denied before I/O; exact code SHA/AST proof and all592route identities/order/actions checked'},
                                'new_Source_or_resolver_executions': 0}
        milestone = 'M8' if route['view'].startswith(('apps.plugins.', 'django.contrib.admin.')) else 'M5'
        contracts.append({'contract_id': identifier, 'source_handler': route['view'],
                          'pattern_exact': route['pattern'], 'method': method, 'source_action': action,
                          'owning_milestone': milestone, 'fixture_id': fixture_id,
                          'observed_roles': roles,
                          'source_role_specs': {role: {'observed_label': role, 'kind': 'actual retained Source actor/credential-condition label',
                                                      'exact_executed_recipe_and_case_contexts': contexts,
                                                      'complete_role_matrix': False} for role in roles},
                          'observed_methods': sorted({case['method'] for case in cases}),
                          'operation_method_discovery_resolved': method is not None,
                          'cases': cases,
                          'assertions': ['Exact retained Source method/path/role/status/body SHA and indexed effect authority agree with the actual pinned Django successful URLPattern-chain ordinal; no guessed regex/catchall/format alias',
                                         'Original recipe bytes and qualified Source selected SQL/events/credential/session or plugin/ORM/cleanup scope remain attached to their actual independent contexts',
                                         'Separate UID3 resolver component keeps controller exit1/rawpassedfalse/networkattempt1 literal, with one measured blocked urllib3 import probe and no underlying I/O; no resolver or business request replay'],
                          'result': {'state': 'partial_retained_Original_auth_or_plugin_HTTP_observations',
                                     'Source_cases_passed': len(cases), 'new_Source_cases_this_task': 0,
                                     'Native_coverage_increased': False, 'complete_role_matrix': False,
                                     'fully_verified': False, 'performance_verified': False,
                                     'resolver_original_controller_exit': 1, 'resolver_raw_passed': False,
                                     'resolver_blocked_network_attempts': 1},
                          'fully_verified': False,
                          'remaining': ['Only original measured role/input/error domains; complete methods/roles/format aliases, Native, browser/default graph and performance remain open',
                                        'Null method/action stays unresolved; observed methods do not rewrite the declaration',
                                        'Original private credential/cookie/CSRF/JWT bytes remain hash evidence; no new public credential/signature or full SQL/cache redecoding claim',
                                        'UID3 is a separately qualified resolver component under a blocked import probe, not a green original controller or runtime network-init parity']})
    return {'format_version': 1, 'binding_kind': KIND,
            'credit': 'Original Dispatcharr, Django/DRF, SimpleJWT and urllib3 authors retain all application/framework/algorithm credit and licensing.',
            'contracts': contracts, 'fixtures': fixtures,
            'unmapped_method_Source_observations': side,
            'qualified_Source_observations': 86, 'operation_bound_Source_observations': 72,
            'method_refusal_side_observations': 14, 'operation_count': 13,
            'resolver_component_qualification': qualification,
            'new_Source_or_resolver_executions': 0, 'Native_coverage_increased': False,
            'fully_verified_operations': 0, 'formal_milestone_accepted': False}


def validate(root, value, bindings=None):
    assert value == derive(root), 'exact retained auth51/plugin35 scoped Source definitions and fourteen method-refusal observations'
    return {'operation_roots': 13, 'bound_observations': 72, 'method_refusal_side_observations': 14,
            'resolver_original_controller_exit': 1, 'new_Source_or_resolver_executions': 0,
            'Native_coverage_increased': False, 'fully_verified_added': 0}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--qualify-resolver', action='store_true', help='Offline retained UID3 component qualification only; no Docker/Django/business replay')
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if args.qualify_resolver:
        print(json.dumps(qualify_resolver(root)[0], indent=2))
    elif args.verify_only:
        print(json.dumps(validate(root, json.loads((root / INPUT).read_bytes()))))
    else:
        value = derive(root)
        target = root / INPUT
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
        print(json.dumps(validate(root, value)))
