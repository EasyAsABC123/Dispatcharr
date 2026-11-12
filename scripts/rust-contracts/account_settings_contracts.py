"""Exact Original account/client and CoreSettings model test assignments.
Original Dispatcharr authors retain all application/algorithm credit and licenses.
No Original module is imported; request-site analysis is deliberately narrow.
"""
import ast
import hashlib
import json
import re
from pathlib import Path
from framework_auth_contracts import ast_fingerprint

ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
HTTP_CLASSES = {
    'apps/accounts/tests/test_user_me_endpoint.py': ['UserMeAdminOnlyPropsTests'],
    'apps/accounts/tests/test_user_app_data.py': ['UserAppDataEndpointTests'],
    'apps/accounts/tests/test_auth_group_permissions.py': ['AuthGroupPermissionTests'],
    'core/tests/test_system_events_permissions.py': ['SystemEventsPermissionTests'],
    'core/tests/test_locked_profiles.py': ['LockedProfileApiTests'],
}
MODEL_CLASSES = ['CoreSettingsGroupCacheTests', 'DefaultUserAgentCacheTests',
                 'DefaultStreamProfileRedirectCacheTests', 'GetDvrSeriesRulesTest',
                 'SetDvrSeriesRulesTest', 'EpgIgnoreListsTest', 'CoreSettingsSerializerDvrTest']
MODEL_ID = 'candidate-169edf8ffadd457e8928'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def original_path(root, name):
    return root / 'scripts/rust-contracts/accounts-settings/original' / name


def segment(text, node):
    return ast.get_source_segment(text, node)


def user_defaults(root):
    tree = ast.parse(original_path(root, 'apps/accounts/models.py').read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'User')
    levels = next(n for n in cls.body if isinstance(n, ast.ClassDef) and n.name == 'UserLevel')
    values = {n.targets[0].id: ast.literal_eval(n.value)[0]
              for n in levels.body if isinstance(n, ast.Assign)}
    field = next(n for n in cls.body if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == 'user_level' for t in n.targets))
    default = next(k.value for k in field.value.keywords if k.arg == 'default')
    assert isinstance(default, ast.Attribute) and default.value.id == 'UserLevel'
    return values, values[default.attr]


def class_context(text, cls, levels, default_level):
    setup = next((n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'setUp'), None)
    roles = {}
    if setup:
        for n in ast.walk(setup):
            if not isinstance(n, ast.Assign) or not isinstance(n.value, ast.Call):
                continue
            if not isinstance(n.value.func, ast.Attribute) or n.value.func.attr != 'create_user':
                continue
            kw = {k.arg: k.value for k in n.value.keywords}
            level = kw.get('user_level')
            number = default_level if level is None else levels[level.attr] if isinstance(level, ast.Attribute) else ast.literal_eval(level)
            for target in n.targets:
                roles[segment(text, target)] = {
                    'effective_user_level': number,
                    'level_source': 'omitted; pinned User.user_level default' if level is None else segment(text, level),
                    'source_creation': segment(text, n.value),
                    'api_key_fixture_sha256': sha(ast.literal_eval(kw['api_key']).encode()) if 'api_key' in kw else None,
                }
    return {
        'class_ast_sha256': ast_fingerprint(cls),
        'setUp': segment(text, setup) if setup else None,
        'helper_methods': {n.name: segment(text, n) for n in cls.body
                           if isinstance(n, ast.FunctionDef) and not n.name.startswith('test')},
        'created_user_roles': roles,
        'decorators': [segment(text, n) for n in cls.decorator_list],
        'module_helpers': {n.name: segment(text, n) for n in ast.parse(text).body if isinstance(n, ast.FunctionDef)},
    }


def path_expression(node, constants, class_url):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == 'self' and node.attr == 'url':
        return class_url
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        if node.func.id == 'me_key_url':
            return '/api/accounts/users/me/app-data/{KEY}/'
        if node.func.id == 'user_app_data_url':
            keyed = len(node.args) > 1 or any(k.arg == 'key' and not (isinstance(k.value, ast.Constant) and k.value.value is None) for k in node.keywords)
            return '/api/accounts/users/{PK}/app-data/' + ('{KEY}/' if keyed else '')
    if isinstance(node, ast.JoinedStr):
        result = ''
        for value in node.values:
            if isinstance(value, ast.Constant):
                result += value.value
            elif isinstance(value, ast.FormattedValue) and isinstance(value.value, ast.Name) and value.value.id in constants:
                result += constants[value.value.id]
            elif isinstance(value, ast.FormattedValue) and isinstance(value.value, ast.Attribute) and value.value.attr in ['pk', 'id']:
                result += '{PK}'
            else:
                raise ValueError('UnqualifiedURLExpression')
        return result
    raise ValueError('UnqualifiedURLExpression')


def route_for(rows, template, method):
    # Representatives exercise only the immutable resolver-pattern order; these
    # are NOT claimed as observed fixture IDs or wire requests.
    path = template.replace('{PK}', '1').replace('{KEY}', 'com.example.fixture').split('?', 1)[0].lstrip('/')
    for route in rows:
        pattern = route['pattern_exact'].replace('/^', '/')
        pattern = re.sub(r'<int:[^>]+>', '[0-9]+', pattern)
        pattern = re.sub(r'<str:[^>]+>', '[^/]+', pattern)
        try:
            matched = re.fullmatch(pattern, path)
        except re.error:
            continue
        if matched:
            for operation in route['operations']:
                if operation['method'] in [method, None]:
                    return route, operation
            raise ValueError('ObservedMethodNotDeclaredOrPlaceholder')
    raise ValueError('NoExactExistingRoute')


def derive(root, registry):
    root = Path(root)
    levels, default_level = user_defaults(root)
    cases = {}
    fixtures = {}
    assignments = {}
    model_tests = []
    groups = dict(HTTP_CLASSES)
    groups['core/tests/test_core.py'] = MODEL_CLASSES
    for filename, class_names in groups.items():
        text = original_path(root, filename).read_text()
        tree = ast.parse(text)
        constants = {n.targets[0].id: n.value.value for n in tree.body
                     if isinstance(n, ast.Assign) and len(n.targets) == 1
                     and isinstance(n.targets[0], ast.Name) and isinstance(n.value, ast.Constant)
                     and isinstance(n.value.value, str)}
        for cls in [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name in class_names]:
            key = filename + ':' + cls.name
            context = class_context(text, cls, levels, default_level)
            fixtures[key] = context
            setup = next((n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'setUp'), None)
            class_url = None
            if setup:
                for n in ast.walk(setup):
                    if isinstance(n, ast.Assign) and any(isinstance(t, ast.Attribute) and t.attr == 'url' for t in n.targets):
                        if isinstance(n.value, ast.Constant):
                            class_url = n.value.value
            for method in [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name.startswith('test')]:
                identifier = filename[:-3].replace('/', '.') + '.' + cls.name + '.' + method.name
                assertions = [segment(text, n) for n in ast.walk(method) if isinstance(n, ast.Call)
                              and ((isinstance(n.func, ast.Attribute) and n.func.attr.startswith('assert'))
                                   or (isinstance(n.func, ast.Name) and n.func.id == 'assert_error'))]
                assert assertions, identifier
                item = {'test_id': identifier, 'source_path': filename, 'class': cls.name,
                        'method': method.name, 'fixture_id': key, 'source_ast_sha256': ast_fingerprint(method),
                        'assertion_expressions': assertions,
                        'assertion_scope': 'whole unmodified workflow; no unrelated model mutation attributed to an HTTP call',
                        'authentication_statements': [segment(text, n) for n in ast.walk(method)
                            if isinstance(n, ast.Call) and ((isinstance(n.func, ast.Attribute) and n.func.attr == 'force_authenticate')
                            or (isinstance(n.func, ast.Name) and n.func.id == 'force_authenticate'))],
                        'patch_contexts': [segment(text, n) for n in ast.walk(method)
                            if isinstance(n, ast.Call) and ((isinstance(n.func, ast.Name) and n.func.id in ['patch', 'override_settings'])
                            or (isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name) and n.func.value.id in ['patch', 'mock']))],
                        'request_sites': []}
                if filename == 'core/tests/test_core.py':
                    # Full class/helper context binds the CoreSettings-specific
                    # methods; no API request or settings REST route is inferred.
                    assert 'CoreSettings' in segment(text, cls)
                    item['transport'] = 'Original Django TestCase/model/cache/serializer, no HTTP endpoint claim'
                    model_tests.append(identifier)
                else:
                    requests = [n for n in ast.walk(method) if isinstance(n, ast.Call)
                                and isinstance(n.func, ast.Attribute)
                                and isinstance(n.func.value, ast.Attribute)
                                and isinstance(n.func.value.value, ast.Name) and n.func.value.value.id == 'self'
                                and n.func.value.attr in ['client', 'factory']
                                and n.func.attr in ['get', 'post', 'put', 'patch', 'delete', 'generic']]
                    for call in sorted(requests, key=lambda n: (n.lineno, n.col_offset)):
                        generic = call.func.attr == 'generic'
                        http_method = ast.literal_eval(call.args[0]).upper() if generic else call.func.attr.upper()
                        template = path_expression(call.args[1 if generic else 0], constants, class_url)
                        route, operation = route_for(registry['runtime_routes'], template, http_method)
                        if call.func.value.attr == 'factory':
                            if cls.name == 'AuthGroupPermissionTests':
                                expected_view = 'apps.accounts.api_views.GroupViewSet' if '/groups/' in template else 'apps.accounts.api_views.WrappedAPIView'
                            else:
                                expected_view = 'core.api_views.WrappedAPIView'
                            assert route['source_handler'] == expected_view
                        # Source-specific actor context, never inferred from a
                        # target user's ID or every user created by the fixture.
                        auth_calls = [n for n in ast.walk(method) if isinstance(n, ast.Call)
                                      and ((isinstance(n.func, ast.Attribute) and n.func.attr == 'force_authenticate')
                                      or (isinstance(n.func, ast.Name) and n.func.id == 'force_authenticate'))]
                        setup_auth = [n for n in ast.walk(setup) if isinstance(n, ast.Call)
                                      and isinstance(n.func, ast.Attribute) and n.func.attr == 'force_authenticate'] if setup else []
                        selected_auth = next(iter(setup_auth), None)
                        eligible = auth_calls if call.func.value.attr == 'factory' else [n for n in auth_calls if n.lineno < call.lineno]
                        if eligible:
                            selected_auth = max(eligible, key=lambda n: n.lineno)
                        user_node = None
                        if selected_auth:
                            user_node = next((k.value for k in selected_auth.keywords if k.arg == 'user'), None)
                            if user_node is None and selected_auth.args:
                                user_node = selected_auth.args[0]
                        expression = segment(text, user_node) if user_node is not None else 'None'
                        api_key = next((k.value for k in call.keywords if k.arg == 'HTTP_X_API_KEY'), None)
                        if api_key is not None and expression == 'None':
                            key_hash = sha(ast.literal_eval(api_key).encode())
                            expression = next(name for name, definition in context['created_user_roles'].items()
                                              if definition['api_key_fixture_sha256'] == key_hash)
                            actor_kind = 'Original API-key authentication in Django APIClient'
                        else:
                            actor_kind = 'anonymous' if expression == 'None' else 'forced Original test authentication; credential backend bypassed'
                        actor = {'kind': actor_kind, 'expression': expression,
                                 'user_fixture': context['created_user_roles'].get(expression)}
                        if expression != 'None':
                            assert actor['user_fixture'] is not None
                        witness = {'line': call.lineno, 'source_expression': segment(text, call),
                                   'source_ast_sha256': ast_fingerprint(call), 'HTTP_method_observed': http_method,
                                   'symbolic_path': template, 'contract_id': operation['contract_id'], 'actor': actor,
                                   'transport': 'APIRequestFactory direct view/function, no router/TCP assertion' if call.func.value.attr == 'factory' else 'Django APIClient; no TCP transcript claim'}
                        item['request_sites'].append(witness)
                        assignments.setdefault(operation['contract_id'], {'route': route, 'operation': operation, 'cases': []})
                        if identifier not in assignments[operation['contract_id']]['cases']:
                            assignments[operation['contract_id']]['cases'].append(identifier)
                    if not item['request_sites']:
                        assert identifier == 'core.tests.test_locked_profiles.LockedProfileApiTests.test_model_save_rejects_locked_output_profile_changes'
                        continue  # Explicit model-only test, not assigned to any HTTP operation.
                    item['transport'] = item['request_sites'][0]['transport']
                cases[identifier] = item
    return {'cases': cases, 'fixtures': fixtures, 'assignments': assignments,
            'model_tests': model_tests, 'user_level_enums': levels, 'missing_level_actual_default': default_level}


EXPECTED_ORIGINAL_FILES = [{'original_path': 'apps/accounts/api_urls.py', 'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/api_urls.py', 'sha256': 'aa2ddbb0a87e5d68e89d674dd144cf753798115f1c89a6ade15eb4baa3ca8523'}, {'original_path': 'apps/accounts/models.py', 'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/models.py', 'sha256': '05cab8c590de411e68ff83b72d9d6256ca43007cbbab5d2a7f96a3a8965a9ded'}, {'original_path': 'apps/accounts/tests/test_auth_group_permissions.py', 'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/tests/test_auth_group_permissions.py', 'sha256': '68b8323b8dd316d0923efc7a780d0174dd3d4a1966245641c223dbfceff25cf3'}, {'original_path': 'apps/accounts/tests/test_user_app_data.py', 'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/tests/test_user_app_data.py', 'sha256': '0d77a4c955691e55ce7fc24f234343dae21ab6b4d4a2621eac06dbdb4b6c1e3a'}, {'original_path': 'apps/accounts/tests/test_user_me_endpoint.py', 'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/tests/test_user_me_endpoint.py', 'sha256': 'd1ddaed6ee2a491c41dc2bbe2d66c9945bbe545d3235a8742f1ac31853df5923'}, {'original_path': 'core/api_urls.py', 'path': 'scripts/rust-contracts/accounts-settings/original/core/api_urls.py', 'sha256': 'aa780f16f02f7bcffe5fdf26aa3f658a0e91190e84377540c75ec73b1dbce81e'}, {'original_path': 'core/models.py', 'path': 'scripts/rust-contracts/accounts-settings/original/core/models.py', 'sha256': 'a6098d95888fecc5de8f5ce0515a5a290178ed23cfd50839d014b304263d5afb'}, {'original_path': 'core/tests/test_core.py', 'path': 'scripts/rust-contracts/accounts-settings/original/core/tests/test_core.py', 'sha256': 'a2b9baa587280a7b960d176c57878e982cf32f696be808311eccbf802ac8cccc'}, {'original_path': 'core/tests/test_locked_profiles.py', 'path': 'scripts/rust-contracts/accounts-settings/original/core/tests/test_locked_profiles.py', 'sha256': '34b69640fe44406d479f2951fea77e852571b4d857a03b9c6c4948d636aae454'}, {'original_path': 'core/tests/test_system_events_permissions.py', 'path': 'scripts/rust-contracts/accounts-settings/original/core/tests/test_system_events_permissions.py', 'sha256': 'aad78ec7b635a3465019bdcfd4d0638a21f5c70a845255227877267a9d06c680'}]
EXPECTED_AUTHORITIES = [{'path': 'scripts/rust-contracts/framework-auth/full-suite-reference.json', 'sha256': '494e9945c39e94c2c05301efed9bba3ba89b5da3b60eb55142f2c701729091c6'}, {'path': 'scripts/rust-contracts/accounts-settings/observations.json', 'sha256': 'bfe88b7798e786ce84bc4d624b926c1355694d419418a20e864451ea8e44cd8d'}]
EXPECTED_HTTP_IDENTITIES = {'route-e26d7a42abc653fff1d5:PATCH': {'method': 'PATCH', 'source_action': 'me', 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/^users/me/$', 'source_handler': 'apps.accounts.api_views.UserViewSet'}, 'route-0b5cb5ae6ca3b2e0f085:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/users/me/app-data/<str:key>/', 'source_handler': 'apps.accounts.api_views.UserAppDataKeyView'}, 'route-c64b218c4c1ac556d865:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/users/me/app-data/', 'source_handler': 'apps.accounts.api_views.UserAppDataCollectionView'}, 'route-4a50fcc0d000739bc60c:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/users/<int:user_id>/app-data/', 'source_handler': 'apps.accounts.api_views.UserAppDataCollectionView'}, 'route-f7f70739debd2202db5e:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/users/<int:user_id>/app-data/<str:key>/', 'source_handler': 'apps.accounts.api_views.UserAppDataKeyView'}, 'route-e26d7a42abc653fff1d5:GET': {'method': 'GET', 'source_action': 'me', 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/^users/me/$', 'source_handler': 'apps.accounts.api_views.UserViewSet'}, 'route-d89c8561ac609119a031:GET': {'method': 'GET', 'source_action': 'list', 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/^users/$', 'source_handler': 'apps.accounts.api_views.UserViewSet'}, 'route-08a0d591ac8296e5ed6e:PATCH': {'method': 'PATCH', 'source_action': 'partial_update', 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/^users/(?P<pk>[^/.]+)/$', 'source_handler': 'apps.accounts.api_views.UserViewSet'}, 'route-5b71ec12d4fa8414975e:GET': {'method': 'GET', 'source_action': 'list', 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/^groups/$', 'source_handler': 'apps.accounts.api_views.GroupViewSet'}, 'route-74d9c31060dfc7c91063:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/accounts/permissions/', 'source_handler': 'apps.accounts.api_views.WrappedAPIView'}, 'route-11307f60cca29fe0e061:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/core/system-events/', 'source_handler': 'core.api_views.WrappedAPIView'}, 'route-0d2bf3aa82ca7c57fc62:PATCH': {'method': 'PATCH', 'source_action': 'partial_update', 'owning_milestone': 'M5', 'pattern_exact': 'api/core/^streamprofiles/(?P<pk>[^/.]+)/$', 'source_handler': 'core.api_views.StreamProfileViewSet'}, 'route-0d2bf3aa82ca7c57fc62:DELETE': {'method': 'DELETE', 'source_action': 'destroy', 'owning_milestone': 'M5', 'pattern_exact': 'api/core/^streamprofiles/(?P<pk>[^/.]+)/$', 'source_handler': 'core.api_views.StreamProfileViewSet'}, 'route-e07365518a567eba0898:PUT': {'method': 'PUT', 'source_action': 'update', 'owning_milestone': 'M5', 'pattern_exact': 'api/core/^outputprofiles/(?P<pk>[^/.]+)/$', 'source_handler': 'core.api_views.OutputProfileViewSet'}, 'route-e07365518a567eba0898:PATCH': {'method': 'PATCH', 'source_action': 'partial_update', 'owning_milestone': 'M5', 'pattern_exact': 'api/core/^outputprofiles/(?P<pk>[^/.]+)/$', 'source_handler': 'core.api_views.OutputProfileViewSet'}, 'route-e07365518a567eba0898:DELETE': {'method': 'DELETE', 'source_action': 'destroy', 'owning_milestone': 'M5', 'pattern_exact': 'api/core/^outputprofiles/(?P<pk>[^/.]+)/$', 'source_handler': 'core.api_views.OutputProfileViewSet'}}
EXPECTED_MODEL_SOURCE = {'kind': 'model_candidate', 'path': 'core/models.py', 'line': 362, 'name': 'CoreSettings'}


EXPECTED_HTTP_ASSERTIONS = ['Execute exact named Original methods with unchanged setUp/helpers/decorators and in-method authentication/mocks', 'Preserve all source assertion expressions and successful named log blocks; assertions belong to whole original workflows', 'Associate only exact request AST sites/paths/view actions; do not attribute unrelated model mutations to an endpoint', 'Keep APIClient/RequestFactory transport, authentication bypass and observed methods explicit; no TCP/Native/complete role claims']
EXPECTED_MODEL_ASSERTIONS = ['Execute exact selected Original CoreSettings model/cache/DVR/EPG/serializer test methods and helpers', 'Preserve raw Original code, setup/cache teardown, mock scopes and every original assertion', 'Require actual named successful log blocks in qualified3362 run, not module-only association', 'No HTTP endpoint, user role, Native parity or complete model behavior inferred']

def validate(root, value, bindings=None):
    root = Path(root)
    assert value['format_version'] == 1 and value['binding_kind'] == 'pinned_original_account_settings_tests'
    assert value['original_test_files'] == EXPECTED_ORIGINAL_FILES
    assert value['authorities'] == EXPECTED_AUTHORITIES
    loaded = {}
    for reference in EXPECTED_ORIGINAL_FILES + EXPECTED_AUTHORITIES:
        path = root / reference['path']
        assert sha(path.read_bytes()) == reference['sha256'], 'exact Original/observation authority'
        if path.suffix == '.json':
            loaded[reference['path']] = json.loads(path.read_text())
    summary = loaded['scripts/rust-contracts/framework-auth/full-suite-reference.json']
    observation = loaded['scripts/rust-contracts/accounts-settings/observations.json']
    assert summary['source_commit'] == ORIGINAL and summary['tests_passed'] == 3362
    assert summary['exit_code'] == summary['cleanup_exit_code'] == 0
    assert observation['actual_raw_log_sha256'] == summary['artifact_sha256']['tests.log']
    registry = json.loads((root / 'docs/rust-contract-register.json').read_text())
    assert registry['counts']['runtime_routes'] == 592 and registry['counts']['runtime_operation_work_items'] == 798
    assert registry['counts']['static_non_HTTP_candidates'] == 250
    expected = derive(root, registry)
    obs = {c['test_id']: c for c in observation['cases']}
    assert len(obs) == len(observation['cases']) == len(expected['cases']) == 80
    for identifier, case in expected['cases'].items():
        actual = obs[identifier]
        assert {k: actual[k] for k in case} == case, 'exact test AST/requests/assertions/authentication'
        block = actual['framework_log_block']
        assert block.startswith(case['method'] + ' (' + identifier + ')')
        assert re.search(r'(?:\.\.\. |\n)ok\s*\Z', block), 'actual successful per-test terminal'
        assert sha(block.encode()) == actual['framework_log_block_sha256']
        assert actual['historical_result'] == 'ok'
    assert value['missing_user_level_authority']['actual_default'] == 'UserLevel.STREAMER'
    assert value['missing_user_level_authority']['value'] == expected['missing_level_actual_default'] == 0
    assert len(value['contracts']) == value['operation_count'] == len(EXPECTED_HTTP_IDENTITIES) == 16
    seen = set()
    for contract in value['contracts']:
        identifier = contract['contract_id']
        assert identifier not in seen
        seen.add(identifier)
        assert {k: contract[k] for k in EXPECTED_HTTP_IDENTITIES[identifier]} == EXPECTED_HTTP_IDENTITIES[identifier]
        assignment = expected['assignments'][identifier]
        assert [c['test_id'] for c in contract['cases']] == assignment['cases']
        roles = {}
        classes = {}
        methods = set()
        for case in contract['cases']:
            original = obs[case['test_id']]
            assert {k: case[k] for k in original} == original
            selected = [w for w in original['request_sites'] if w['contract_id'] == identifier]
            assert selected and case['selected_request_sites'] == selected
            for site in selected:
                methods.add(site['HTTP_method_observed'])
                actor = site['actor']
                key = case['fixture_id'] + '|' + actor['expression'] + '|' + actor['kind']
                roles[key] = actor
            classes[case['fixture_id']] = expected['fixtures'][case['fixture_id']]
        assert contract['observed_request_methods'] == sorted(methods), 'observations not declarations'
        assert contract['observed_roles'] == sorted(roles) and contract['source_role_specs'] == roles
        fixture = value['fixtures'][contract['fixture_id']]
        assert fixture['roles'] == roles and fixture['source_classes'] == classes
        assert fixture['execution'] == {'entrypoint': 'scripts/rust-baseline/run.py', 'argv': ['--output', '<fresh-empty-output>', *assignment['cases']]}
        assert fixture['baseline_revision'] == ORIGINAL and fixture['baseline_image'] == summary['image']
        assert contract['result'] == {'state': 'historical_Original_framework_tests_passed_not_new_execution',
                                     'historical_test_assignments': len(contract['cases']), 'new_Source_cases_this_task': 0,
                                     'authority': 'scripts/rust-contracts/accounts-settings/observations.json'}
        assert contract['assertions'] == EXPECTED_HTTP_ASSERTIONS and len(contract['remaining']) == 4
        assert contract['fully_verified'] is False
    assert seen == set(EXPECTED_HTTP_IDENTITIES)
    assert len(value['non_HTTP_contracts']) == value['non_HTTP_candidate_count'] == 1
    model = value['non_HTTP_contracts'][0]
    assert model['candidate_id'] == MODEL_ID and model['source'] == EXPECTED_MODEL_SOURCE
    assert model['owning_milestone'] == 'M5' and model['fully_verified'] is False
    assert model['cases'] == [obs[x] for x in expected['model_tests']]
    fixture = value['fixtures'][model['fixture_id']]
    assert fixture['roles'] == {} and 'no HTTP' in fixture['scope']
    assert fixture['source_classes'] == {k: v for k, v in expected['fixtures'].items() if k.startswith('core/tests/test_core.py:')}
    assert fixture['execution'] == {'entrypoint': 'scripts/rust-baseline/run.py', 'argv': ['--output', '<fresh-empty-output>', *expected['model_tests']]}
    assert model['result'] == {'state': 'historical_Original_model_tests_passed_not_new_execution', 'historical_test_cases': 35,
                               'new_Source_cases_this_task': 0, 'authority': 'scripts/rust-contracts/accounts-settings/observations.json'}
    assert model['assertions'] == EXPECTED_MODEL_ASSERTIONS and len(model['remaining']) == 3
    assert value['unique_historical_tests'] == 80 and value['historical_HTTP_tests'] == 45 and value['historical_model_tests'] == 35
    assert value['fresh_Source_case_count'] == value['fully_verified_operations'] == 0 and value['Native_coverage_increased'] is False
    return {'passed': True, 'HTTP_assignments': 16, 'nonHTTP_assignments': 1,
            'historical_unique_tests': 80, 'new_executions': 0, 'Native_coverage_added': False}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    data = json.loads((args.root / 'scripts/rust-contracts/accounts-settings/bindings.json').read_text())
    print(json.dumps(validate(args.root, data)))
