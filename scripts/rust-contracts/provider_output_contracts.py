"""Scoped Original provider/output and synthetic consumer-method definitions.
Original Dispatcharr authors retain application/algorithm credit and licenses.
Historical workflow/source associations are not new requests or live transports.
"""
import ast
import hashlib
import json
import re
from pathlib import Path
from framework_auth_contracts import ast_fingerprint
from account_settings_contracts import user_defaults
from channel_guide_contracts import actor, authentication, trace_requests, level_number
from dvr_import_contracts import fixture_arguments

KIND = 'pinned_original_provider_output_consumer_tests'
PREFIX = 'scripts/rust-contracts/provider-output/'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
CLASSES = {'apps/m3u/tests/test_account_custom_action_permissions.py': ['M3UAccountCustomActionPermissionTests'],
           'apps/m3u/tests/test_account_destroy.py': ['M3UAccountDestroyTests'],
           'apps/output/tests/test_views.py': ['OutputM3UTest', 'GenerateM3URadioAttributeTests', 'OutputEPGXMLEscapingTest'],
           'tests/test_websocket_consumer_filter.py': ['ConsumerUpdateFilteringTests', 'ConsumerM3UProfileTestReceiveTests']}
CONSUMER_CLASSES = {'ConsumerUpdateFilteringTests', 'ConsumerM3UProfileTestReceiveTests'}


IDENTITIES = {'route-811ae7e4cfcd7e6efc16:DELETE': {'pattern_exact': 'api/m3u/^accounts/(?P<pk>[^/.]+)/$', 'source_handler': 'apps.m3u.api_views.M3UAccountViewSet', 'method': 'DELETE', 'source_action': 'destroy', 'owning_milestone': 'M5'}, 'route-811ae7e4cfcd7e6efc16:PATCH': {'pattern_exact': 'api/m3u/^accounts/(?P<pk>[^/.]+)/$', 'source_handler': 'apps.m3u.api_views.M3UAccountViewSet', 'method': 'PATCH', 'source_action': 'partial_update', 'owning_milestone': 'M5'}, 'route-811ae7e4cfcd7e6efc16:PUT': {'pattern_exact': 'api/m3u/^accounts/(?P<pk>[^/.]+)/$', 'source_handler': 'apps.m3u.api_views.M3UAccountViewSet', 'method': 'PUT', 'source_action': 'update', 'owning_milestone': 'M5'}, 'route-3fe6d669be6baca32a21:GET': {'pattern_exact': 'api/m3u/^accounts/(?P<pk>[^/.]+)/auto-created-channels-count/$', 'source_handler': 'apps.m3u.api_views.M3UAccountViewSet', 'method': 'GET', 'source_action': 'auto_created_channels_count', 'owning_milestone': 'M5'}, 'route-a858d1c652a3e524fe09:POST': {'pattern_exact': 'api/m3u/^accounts/(?P<pk>[^/.]+)/refresh-vod/$', 'source_handler': 'apps.m3u.api_views.M3UAccountViewSet', 'method': 'POST', 'source_action': 'refresh_vod', 'owning_milestone': 'M6'}, 'route-a517753f7d24fb020760:POST': {'pattern_exact': 'api/m3u/^accounts/(?P<pk>[^/.]+)/repack-group/$', 'source_handler': 'apps.m3u.api_views.M3UAccountViewSet', 'method': 'POST', 'source_action': 'repack_group', 'owning_milestone': 'M5'}, 'route-9cd96a9bce5c0a14b46d:PATCH': {'pattern_exact': 'api/m3u/^accounts/(?P<pk>[^/.]+)/group-settings/$', 'source_handler': 'apps.m3u.api_views.M3UAccountViewSet', 'method': 'PATCH', 'source_action': 'update_group_settings', 'owning_milestone': 'M5'}, 'route-b4a3f38e76ae962a071f:METHOD_UNRESOLVED': {'pattern_exact': 'output/^m3u(?:/(?P<profile_name>[^/]+))?/?$', 'source_handler': 'apps.output.views.m3u_endpoint', 'method': None, 'source_action': None, 'owning_milestone': 'M7'}, 'route-a41fa134f8e99a70d993:METHOD_UNRESOLVED': {'pattern_exact': 'output/^epg(?:/(?P<profile_name>[^/]+))?/?$', 'source_handler': 'apps.output.views.epg_endpoint', 'method': None, 'source_action': None, 'owning_milestone': 'M7'}}
CONSUMER = {'candidate_id': 'candidate-7c32b3c0e1688b313f5c', 'source': {'kind': 'websocket_consumer_candidate', 'path': 'dispatcharr/consumers.py', 'line': 59, 'name': 'MyWebSocketConsumer'}}
ORIGINAL_FILES = [{'original_path': 'apps/m3u/tests/test_account_custom_action_permissions.py', 'path': 'scripts/rust-contracts/provider-output/original/apps/m3u/tests/test_account_custom_action_permissions.py', 'sha256': '037b41afbb5fe94787c4db90ee424aa19e9b1b57c5454012e2ea50950bdbb93e'}, {'original_path': 'apps/m3u/tests/test_account_destroy.py', 'path': 'scripts/rust-contracts/provider-output/original/apps/m3u/tests/test_account_destroy.py', 'sha256': '0342f648f3ab61c41afa816b90b69094d8102972875b05e1d92b2b63ced159c5'}, {'original_path': 'apps/output/tests/test_views.py', 'path': 'scripts/rust-contracts/provider-output/original/apps/output/tests/test_views.py', 'sha256': 'c17759fcb7ac989c39ec60a1aa338cb9549a807cc08fb4796e230da8f136da6d'}, {'original_path': 'tests/test_websocket_consumer_filter.py', 'path': 'scripts/rust-contracts/provider-output/original/tests/test_websocket_consumer_filter.py', 'sha256': 'd4bdb5fe9642ab02da6331c783ec9ca144029aa684dd4c7617d7bf249aa063fb'}, {'original_path': 'dispatcharr/consumers.py', 'path': 'scripts/rust-contracts/provider-output/original/dispatcharr/consumers.py', 'sha256': '429bb971a13b853c4b87f63d0f350c68f94d5e3bff79455945e1cbb8a1fb1775'}, {'original_path': 'apps/output/urls.py', 'path': 'scripts/rust-contracts/provider-output/original/apps/output/urls.py', 'sha256': '1b9ca342b738e572b22e38541145ed57402f6eb9070b967acefd255a8123c5cd'}, {'original_path': 'dispatcharr/urls.py', 'path': 'scripts/rust-contracts/provider-output/original/dispatcharr/urls.py', 'sha256': '98266b2fc6ce2e495dfc6840589691b1ab67015d49c3596c6ddb764ec593429f'}, {'original_path': 'apps/m3u/api_urls.py', 'path': 'scripts/rust-contracts/provider-output/original/apps/m3u/api_urls.py', 'sha256': '492bbd9c4c8127a55cde251c00bb565aa5c8ef2d6a26999b2e70059781cdc81a'}]

AUTHORITIES = [{'path': 'scripts/rust-contracts/framework-auth/full-suite-reference.json', 'sha256': '494e9945c39e94c2c05301efed9bba3ba89b5da3b60eb55142f2c701729091c6'}, {'path': 'scripts/rust-contracts/provider-output/observations.json', 'sha256': 'e3c7a110a04bbb2c9ab463bdf052586653da8617123def4a49a45ccd237830b6'}, {'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/models.py', 'sha256': '05cab8c590de411e68ff83b72d9d6256ca43007cbbab5d2a7f96a3a8965a9ded'}]
HTTP_ASSERTIONS = ['Execute exact named Original workflows with unchanged actor, setup, inheritance, helpers, teardown and mocks', 'Preserve original whole-workflow assertions and successful3362 blocks; no Source rerun for labels/counts', 'Associate APIClient paths or exact named reverse declarations with stable IDs; observed methods do not resolve null declarations', 'Keep forced auth, output network/cache/log/DB-close, provider task/repack/proxy-stop mocks and noTCP limits explicit']
CONSUMER_ASSERTIONS = ['Execute exact named Original synthetic consumer update/receive workflows with unchanged helpers and assertions', 'Require actual MyWebSocketConsumer construction, synthetic SimpleNamespace scope.user and AsyncMock send; no realWebSocket/ASGI/credential/broker claim', 'Preserve original receive/update invocations, regex-deadline mocks/assertions and successful historical result blocks', 'Keep this partial method-level Source definition separate from HTTP, fullconsumer/lifecycle and Native verification']

def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def segment(text, node):
    return ast.get_source_segment(text, node)


def file_text(root, name):
    reference = next(row for row in ORIGINAL_FILES if row['original_path'] == name)
    return (root / reference['path']).read_text()


def context(text, cls, mixin=None):
    methods = {node.name: node for node in cls.body if isinstance(node, ast.FunctionDef)}
    result = {'class_ast_sha256': ast_fingerprint(cls), 'base_expressions': [segment(text, node) for node in cls.bases],
        'setup': segment(text, methods['setUp']) if 'setUp' in methods else None,
        'teardown': segment(text, methods['tearDown']) if 'tearDown' in methods else None,
        'class_decorators': [segment(text, node) for node in cls.decorator_list],
        'helpers': {name: segment(text, method) for name, method in methods.items() if not name.startswith('test')},
        'module_helpers': {node.name: segment(text, node) for node in ast.parse(text).body if isinstance(node, ast.FunctionDef)}}
    if mixin is not None:
        assert any(isinstance(base, ast.Name) and base.id == 'OutputEndpointTestMixin' for base in cls.bases)
        assert any(isinstance(node, ast.Call) and ast.unparse(node.func) == 'super().setUp' for node in ast.walk(methods['setUp']))
        result['inherited_OutputEndpointTestMixin'] = context(text, mixin)
        result['inherited_mock_scope'] = 'network ACL, XC network ACL, logs, DB-close, EPG teardown and Redis chunk-response machinery patched exactly as Original'
        result['real_ACL_Redis_cache_ownership_proven'] = False
    return result


def named_output_route(root, node, test, methods):
    if isinstance(node, ast.Name):
        assignment = next(statement for statement in ast.walk(test) if isinstance(statement, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == node.id for target in statement.targets))
        node = assignment.value
    assert isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and ast.unparse(node.func.value) == 'self'
    assert node.func.attr in ['_m3u_url', '_epg_url'], 'only exact named Source URL helpers'
    helper = methods[node.func.attr]
    reverse = next(call for call in ast.walk(helper) if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == 'reverse')
    name = ast.literal_eval(reverse.args[0]); assert name in ['output:m3u_endpoint', 'output:epg_endpoint']
    kwargs = next(keyword.value for keyword in reverse.keywords if keyword.arg == 'kwargs')
    assert isinstance(kwargs, ast.Dict) and [ast.literal_eval(key) for key in kwargs.keys] == ['profile_name']
    assert ast.unparse(kwargs.values[0]) == 'self.profile.name'
    urls = ast.parse(file_text(root, 'apps/output/urls.py'))
    assert any(isinstance(statement, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'app_name' for target in statement.targets)
               and ast.literal_eval(statement.value) == 'output' for statement in urls.body)
    route_name = name.split(':')[1]
    declaration = next(call for call in ast.walk(urls) if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == 're_path'
                       and any(keyword.arg == 'name' and ast.literal_eval(keyword.value) == route_name for keyword in call.keywords))
    pattern = 'output/' + ast.literal_eval(declaration.args[0])
    handler = 'apps.output.views.' + declaration.args[1].id
    root_urls = ast.parse(file_text(root, 'dispatcharr/urls.py'))
    mount = next(call for call in ast.walk(root_urls) if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == 'path'
                 and call.args and isinstance(call.args[0], ast.Constant) and call.args[0].value == 'output/'
                 and len(call.args) == 2 and isinstance(call.args[1], ast.Call))
    assert ast.unparse(mount.args[1].func) == 'include' and ast.literal_eval(mount.args[1].args[0]) == ('apps.output.urls', 'output')
    assert any(keyword.arg == 'namespace' and ast.literal_eval(keyword.value) == 'output' for keyword in mount.args[1].keywords)
    identifier = next(identifier for identifier, identity in IDENTITIES.items() if identity['pattern_exact'] == pattern and identity['source_handler'] == handler)
    return identifier, {'reverse_name': name, 'symbolic_profile': 'original fixture self.profile.name; no invented literal slash/path transcript',
        'helper_source_ast_sha256': ast_fingerprint(helper), 'URL_declaration_ast_sha256': ast_fingerprint(declaration),
        'root_namespace_mount_ast_sha256': ast_fingerprint(mount), 'resolver_pattern_exact': pattern}


def case_definition(text, filename, cls, test, requests):
    assertions = [segment(text, node) for node in ast.walk(test) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr.startswith('assert')]
    assert assertions
    return {'test_id': filename[:-3].replace('/', '.') + '.' + cls.name + '.' + test.name, 'source_path': filename,
        'class': cls.name, 'method': test.name, 'fixture_id': filename + ':' + cls.name, 'source_ast_sha256': ast_fingerprint(test),
        'assertion_expressions': assertions, 'decorators': [segment(text, node) for node in test.decorator_list],
        'patch_contexts': [segment(text, node) for node in ast.walk(test) if isinstance(node, ast.Call) and
                           (ast.unparse(node.func) in ['patch', 'override_settings', 'mock.patch', 'patch.object', 'patch.dict'] or ast.unparse(node.func).startswith('mock.patch.'))],
        'sites': requests, 'assertion_scope': 'whole unmodified Original workflow and exact mocks; not isolated per-site wire/effect assertions'}


def consumer_sites(text, test, methods, levels, default):
    construction = next(call for call in ast.walk(test) if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                        and ast.unparse(call.func) == 'self._consumer')
    user = construction.args[0]
    assert isinstance(user, ast.Call) and isinstance(user.func, ast.Name) and user.func.id == '_user'
    user_helper = next(node for node in ast.parse(text).body if isinstance(node, ast.FunctionDef) and node.name == '_user')
    environment = fixture_arguments(user_helper, user, {})
    assert isinstance(environment['authenticated'], ast.Constant) and environment['authenticated'].value is True
    number = level_number(environment['user_level'], levels, default)
    constructor_helper = methods['_consumer']
    assert any(isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == 'MyWebSocketConsumer' for call in ast.walk(constructor_helper))
    assert any(isinstance(statement, ast.Assign) and ast.unparse(statement.targets[0]) == 'consumer.scope' for statement in constructor_helper.body)
    assert any(isinstance(statement, ast.Assign) and ast.unparse(statement.targets[0]) == 'consumer.send' and ast.unparse(statement.value) == 'AsyncMock()' for statement in constructor_helper.body)
    invocations = [call for call in ast.walk(test) if isinstance(call, ast.Call) and isinstance(call.func, ast.Call)
                   and isinstance(call.func.func, ast.Name) and call.func.func.id == 'async_to_sync' and len(call.func.args) == 1
                   and isinstance(call.func.args[0], ast.Attribute) and ast.unparse(call.func.args[0].value) == 'consumer'
                   and call.func.args[0].attr in ['update', 'receive']]
    assert invocations
    return [{'source_expression': segment(text, call), 'source_ast_sha256': ast_fingerprint(call), 'method': call.func.args[0].attr,
             'synthetic_user': {'is_authenticated': True, 'user_level': number, 'constructor': 'Original SimpleNamespace helper, not Django/ASGI credential authentication'},
             'constructor_helper_ast_sha256': ast_fingerprint(constructor_helper), 'send': 'AsyncMock',
             'transport': 'direct consumer method via async_to_sync with synthetic scope; no actual WebSocket/handshake/broker',
             'actual_WebSocket_ASGI_credential_broker_claim': False} for call in invocations]


def derive(root, registry):
    root = Path(root); levels, default = user_defaults(root)
    fixtures, cases, assignments, consumer_cases = {}, {}, {}, []
    for reference in ORIGINAL_FILES:
        filename = reference['original_path']
        if filename not in CLASSES:
            continue
        text = (root / reference['path']).read_text(); tree = ast.parse(text)
        module_functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
        for cls in (node for node in tree.body if isinstance(node, ast.ClassDef) and node.name in CLASSES[filename]):
            methods = {node.name: node for node in cls.body if isinstance(node, ast.FunctionDef)}
            setup = methods.get('setUp'); functions = dict(module_functions, **methods)
            fixture_id = filename + ':' + cls.name
            output = filename == 'apps/output/tests/test_views.py'
            consumer = cls.name in CONSUMER_CLASSES
            mixin = next((node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'OutputEndpointTestMixin'), None) if output else None
            fixtures[fixture_id] = context(text, cls, mixin)
            for test in (node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name.startswith('test')):
                if consumer:
                    sites = consumer_sites(text, test, methods, levels, default)
                elif output:
                    assert any(isinstance(node, ast.Assign) and ast.unparse(node.targets[0]) == 'self.client' and ast.unparse(node.value) == 'Client()' for node in ast.walk(setup))
                    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ['login', 'force_login', 'force_authenticate', 'credentials'] for node in ast.walk(cls))
                    sites = []
                    for call in sorted((node for node in ast.walk(test) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                                        and ast.unparse(node.func.value) == 'self.client' and node.func.attr in ['get', 'post']), key=lambda node: (node.lineno, node.col_offset)):
                        identifier, named = named_output_route(root, call.args[0], test, methods)
                        sites.append({'line': call.lineno, 'source_expression': segment(text, call), 'source_ast_sha256': ast_fingerprint(call),
                            'HTTP_method_observed': call.func.attr.upper(), 'contract_id': identifier, 'named_reverse_resolution': named,
                            'actor': {'kind': 'anonymous Django Client fixture under inherited network-access mocks', 'effective_user_level': None},
                            'transport': 'Django test Client routing and body consumption; no TCP/real ACL/Redis cache ownership claim'})
                    assert sites
                else:
                    urls = {}; constants = {}
                    setup_actor = authentication(setup, {}, 10**9, None, 'self.client') if setup else None
                    sites = trace_requests(test, methods, constants, urls, text, IDENTITIES, registry, setup_actor)
                    for site in sites:
                        node = site.pop('authentication_node')
                        site['actor'] = actor(node, [*(setup.body if setup else []), *test.body], functions, levels, default)
                    assert sites
                case = case_definition(text, filename, cls, test, sites); identifier = case['test_id']; cases[identifier] = case
                if consumer:
                    consumer_cases.append(identifier)
                else:
                    for site in sites:
                        assignments.setdefault(site['contract_id'], [])
                        if identifier not in assignments[site['contract_id']]:
                            assignments[site['contract_id']].append(identifier)
    return {'fixtures': fixtures, 'cases': cases, 'assignments': assignments, 'consumer_cases': consumer_cases,
            'User_levels': levels, 'omitted_User_level_default': default}


def dependencies():
    return [PREFIX + 'bindings.json'] + [reference['path'] for reference in ORIGINAL_FILES + AUTHORITIES]


def generate(root, registry):
    root = Path(root)
    for reference in ORIGINAL_FILES + AUTHORITIES:
        assert sha((root / reference['path']).read_bytes()) == reference['sha256'], 'exact provider/output/consumer Original authority bytes'
    summary = json.loads((root / AUTHORITIES[0]['path']).read_text())
    observation = json.loads((root / AUTHORITIES[1]['path']).read_text())
    assert summary['source_commit'] == ORIGINAL and summary['exit_code'] == 0 and summary['tests_passed'] == 3362
    assert summary['scope'] == 'unchanged-upstream-full-python-test-suite'
    assert observation['full_log_sha256'] == summary['artifact_sha256']['tests.log'] and observation['full_log_bytes'] == 881752
    assert observation['new_Source_executions'] == 0
    index = {operation['contract_id']: (route, operation) for route in registry['runtime_routes'] for operation in route['operations']}
    for identifier, identity in IDENTITIES.items():
        route, operation = index[identifier]
        assert {**{key: route[key] for key in ['pattern_exact', 'source_handler']},
                **{key: operation[key] for key in ['method', 'source_action', 'owning_milestone']}} == identity, 'stable provider/output operation identity'
    candidate = next(row for row in registry['non_HTTP_candidates'] if row['candidate_id'] == CONSUMER['candidate_id'])
    assert candidate['source'] == CONSUMER['source'] and candidate['fully_verified'] is False
    consumer_source = file_text(root, 'dispatcharr/consumers.py')
    definition = next(node for node in ast.parse(consumer_source).body if isinstance(node, ast.ClassDef) and node.name == 'MyWebSocketConsumer')
    assert definition.lineno == CONSUMER['source']['line'] == 59
    derived = derive(root, registry)
    assert set(derived['assignments']) == set(IDENTITIES) and len(derived['cases']) == 34 and len(derived['consumer_cases']) == 10
    assert set(observation['cases']) == set(derived['cases'])
    for identifier, case in derived['cases'].items():
        recorded = observation['cases'][identifier]
        assert recorded['historical_result'] == 'ok' and recorded['framework_log_block'].startswith(case['method'] + ' (' + identifier + ')')
        assert re.search(r'(?:\.\.\. |\n)ok\s*\Z', recorded['framework_log_block'])
        assert sha(recorded['framework_log_block'].encode()) == recorded['framework_log_block_sha256']
        assert recorded['full_log_byte_end'] - recorded['full_log_byte_start'] == len(recorded['framework_log_block'].encode())
        case.update(recorded)
        case['replay_kind'] = 'historical_named_Original_workflow_offline_assertion_replay'
    fixtures, contracts = {}, []
    for identifier, identity in IDENTITIES.items():
        selected = [derived['cases'][test] for test in derived['assignments'][identifier]]
        relevant = [site for case in selected for site in case['sites'] if site['contract_id'] == identifier]
        roles = sorted({'anonymous_under_inherited_network_mock' if site['actor']['effective_user_level'] is None
                        else 'Original_user_level_' + str(site['actor']['effective_user_level']) for site in relevant})
        fixture_id = 'provider-output:' + identifier
        fixtures[fixture_id] = {'id': fixture_id, 'baseline_revision': ORIGINAL, 'baseline_image': summary['image'],
            'execution': {'entrypoint': 'scripts/rust-baseline/run.py', 'argv': ['--output', '<fresh-empty-output>', *[case['test_id'] for case in selected]],
                          'fresh_live_replay_qualified_this_task': False, 'historical_full_suite_authority': AUTHORITIES[0]},
            'class_fixture_definitions': {key: derived['fixtures'][key] for key in sorted({case['fixture_id'] for case in selected})},
            'ownership': 'Existing disposable Original TestCase fixtures; no production or live provider access',
            'omitted_User_level_actual_default': derived['omitted_User_level_default'],
            'output_mixin_network_cache_log_DB_teardown_mocks_retained': True,
            'M3U_admin_delay_repack_and_destroy_proxy_stop_mocks_retained': True}
        contracts.append(dict(identity, contract_id=identifier, fixture_id=fixture_id, observed_roles=roles,
            source_role_specs={role: {'authority': 'exact original fixture actor and mock context', 'complete_role_matrix': False} for role in roles},
            observed_request_methods=sorted({site['HTTP_method_observed'] for site in relevant}), cases=selected,
            assertions=HTTP_ASSERTIONS,
            result={'state': 'historical_Original_provider_output_workflows_passed_not_new_execution', 'historical_test_cases': len(selected),
                    'new_Source_cases_this_task': 0, 'historical_authority': 'scripts/rust-baseline/verified-full-baseline.json'},
            fully_verified=False, remaining=['Forced provider authentication and output inherited network mocks do not establish real credential/ACL behavior',
                'Task/repack/proxy-stop/Redis cache/logging/DB-close mocks keep only original assertion scope',
                'Named reverse association is not a literal URL/slash-alias or TCP transcript',
                'Unobserved roles/grammar/methods/aliases and Native/browser/performance/M0/M1 remain open']))
    consumer_id = 'consumer-methods10'
    consumer_selected = [derived['cases'][identifier] for identifier in derived['consumer_cases']]
    fixtures[consumer_id] = {'id': consumer_id, 'baseline_revision': ORIGINAL, 'baseline_image': summary['image'],
        'execution': {'entrypoint': 'scripts/rust-baseline/run.py', 'argv': ['--output', '<fresh-empty-output>', *derived['consumer_cases']],
                      'fresh_live_replay_qualified_this_task': False},
        'class_fixture_definitions': {key: derived['fixtures'][key] for key in sorted({case['fixture_id'] for case in consumer_selected})},
        'ownership': 'Original SimpleTestCase synthetic scope and AsyncMock send; no real socket/group/broker',
        'actual_WebSocket_ASGI_credential_broker_claim': False}
    non_HTTP = [dict(CONSUMER, fixture_id=consumer_id, cases=consumer_selected,
        definition_ast_sha256=ast_fingerprint(definition), observed_methods=sorted({site['method'] for case in consumer_selected for site in case['sites']}),
        assertions=CONSUMER_ASSERTIONS,
        result={'state': 'historical_Original_synthetic_consumer_method_workflows_passed_not_new_execution', 'historical_test_cases': 10,
                'new_Source_cases_this_task': 0, 'actual_WebSocket_ASGI_credential_broker_claim': False},
        fully_verified=False,
        remaining=['Synthetic scope/AsyncMock send only; no live WebSocket, handshake, authentication backend, ASGI routing, subscriptions or Redis/pubsub',
            'Historical regex deadline assertions are not new timing or resource measurements', 'No Native/full consumer/HTTP/M0/M1 acceptance'])]
    return {'format_version': 1, 'binding_kind': KIND, 'Original_revision': ORIGINAL, 'original_test_files': ORIGINAL_FILES,
            'authorities': AUTHORITIES, 'operation_count': 9, 'non_HTTP_candidate_count': 1,
            'unique_historical_test_cases': 34, 'new_Source_cases_this_task': 0,
            'fixtures': fixtures, 'contracts': contracts, 'non_HTTP_contracts': non_HTTP,
            'Native_coverage_increased': False, 'fully_verified_added': 0}


def validate(root, value, bindings=None):
    root = Path(root)
    registry = json.loads((root / 'docs/rust-contract-register.json').read_text())
    assert value == generate(root, registry), 'exact provider/output/consumer Source fixture, inheritance, method and result definitions'
    return {'passed': True, 'operations': 9, 'non_HTTP_candidates': 1, 'unique_historical_workflows': 34,
            'new_Source_executions': 0, 'fully_verified_added': 0}


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--historical-log', type=Path)
    args = parser.parse_args()
    value = json.loads((args.root / (PREFIX + 'bindings.json')).read_text())
    result = validate(args.root, value)
    if args.historical_log is not None:
        raw = args.historical_log.read_bytes()
        observed = json.loads((args.root / (PREFIX + 'observations.json')).read_text())
        assert sha(raw) == observed['full_log_sha256'] and len(raw) == observed['full_log_bytes']
        for case in observed['cases'].values():
            assert raw[case['full_log_byte_start']:case['full_log_byte_end']] == case['framework_log_block'].encode()
        result['deep_historical_raw_blocks'] = len(observed['cases'])
    print(json.dumps(result))


if __name__ == '__main__':
    main()
