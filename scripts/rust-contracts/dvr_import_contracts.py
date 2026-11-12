"""Scoped historical Original DVR/SeriesRules/import Source definitions.
Original Dispatcharr authors retain all application/algorithm credit and licenses.
APIRequestFactory direct view calls are framework-only, not HTTP/router dispatch.
"""
import ast
import copy
import hashlib
import json
import re
from pathlib import Path
from framework_auth_contracts import ast_fingerprint
from account_settings_contracts import route_for, user_defaults
from channel_guide_contracts import bind, arguments, source_url, level_number, authentication, trace_requests

KIND = 'pinned_original_dvr_import_tests'
PREFIX = 'scripts/rust-contracts/dvr-import/'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
CLASSES = {'apps/channels/tests/test_dvr_access.py': ['DvrAccessApiTests'],
           'apps/channels/tests/test_recording_stop_cancel.py': ['StopEndpointTests'],
           'apps/channels/tests/test_recording_extend.py': ['ExtendEndpointTests'],
           'apps/epg/tests/test_epg_import_api.py': ['EPGImportAPITests'],
           'apps/channels/tests/test_channel_api.py': ['SeriesRuleAPITests']}
FACTORY_CLASSES = {'StopEndpointTests', 'ExtendEndpointTests'}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def segment(text, node):
    return ast.get_source_segment(text, node)


def fixture_arguments(function, call, environment):
    values = arguments(function, call, environment)
    for parameter, default in zip(function.args.kwonlyargs, function.args.kw_defaults):
        supplied = next((keyword.value for keyword in call.keywords if keyword.arg == parameter.arg), default)
        assert supplied is not None, 'required keyword-only Source fixture argument'
        values[parameter.arg] = bind(supplied, environment)
    return values


def fixture_actor(node, statements, functions, levels, default, depth=0):
    assert depth <= 6, 'bounded Source actor helper depth'
    assert node is not None, 'this cohort has explicit forced Source fixture actors'
    expression = ast.unparse(node)
    assigned = [x for statement in statements for x in ast.walk(statement) if isinstance(x, ast.Assign)
                and any(ast.unparse(target) == expression for target in x.targets)]
    if assigned:
        result = fixture_actor(assigned[-1].value, statements, functions, levels, default, depth + 1)
        updates = [x for statement in statements for x in ast.walk(statement) if isinstance(x, ast.Assign)
                   and any(ast.unparse(target) == expression + '.user_level' for target in x.targets)]
        if updates:
            result = dict(result, effective_user_level=level_number(updates[-1].value, levels, default), level_source=ast.unparse(updates[-1]))
        return dict(result, expression=expression)
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Attribute) and node.func.attr == 'create_user':
            level = next((keyword.value for keyword in node.keywords if keyword.arg == 'user_level'), None)
            return {'kind': 'forced Original test authentication; credential backend bypassed', 'expression': expression,
                    'effective_user_level': level_number(level, levels, default),
                    'level_source': 'omitted; pinned User.user_level default' if level is None else ast.unparse(level)}
        name = node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id if isinstance(node.func, ast.Name) else None
        if name in functions:
            helper = functions[name]
            environment = fixture_arguments(helper, node, {})
            body = [bind(statement, environment) for statement in helper.body]
            returned = next(statement for statement in reversed(body) if isinstance(statement, ast.Return))
            return fixture_actor(returned.value, body, functions, levels, default, depth + 1)
    raise ValueError('UnqualifiedDVRActorFixture: ' + expression)


def factory_trace(function, methods, registry, text, trace=None, environment=None, depth=0):
    assert depth <= 6, 'bounded factory helper depth'
    trace = trace or []
    environment = environment or {}
    result = []
    for call in sorted((x for x in ast.walk(function) if isinstance(x, ast.Call)), key=lambda x: (x.lineno, x.col_offset)):
        if not isinstance(call.func, ast.Attribute):
            continue
        if ast.unparse(call.func.value) == 'self.factory' and call.func.attr == 'post':
            template = source_url(call.args[0], {}, {}, methods, environment)
            route, operation = route_for(registry['runtime_routes'], template, 'POST')
            if operation['contract_id'] not in IDENTITIES:
                continue
            request_assignment = next(x for x in ast.walk(function) if isinstance(x, ast.Assign) and x.value is call)
            assert len(request_assignment.targets) == 1 and isinstance(request_assignment.targets[0], ast.Name)
            request_name = request_assignment.targets[0].id
            auth = next(x for x in ast.walk(function) if isinstance(x, ast.Call) and isinstance(x.func, ast.Name)
                        and x.func.id == 'force_authenticate' and x.args and isinstance(x.args[0], ast.Name) and x.args[0].id == request_name)
            user = next(k.value for k in auth.keywords if k.arg == 'user')
            view_assignment = next(x for x in ast.walk(function) if isinstance(x, ast.Assign) and isinstance(x.value, ast.Call)
                                   and isinstance(x.value.func, ast.Attribute) and ast.unparse(x.value.func) == 'RecordingViewSet.as_view')
            assert len(view_assignment.targets) == 1 and isinstance(view_assignment.targets[0], ast.Name)
            mapping = ast.literal_eval(view_assignment.value.args[0])
            assert mapping == {'post': operation['source_action']}, 'exact factory view action'
            invocation = next(x for x in ast.walk(function) if isinstance(x, ast.Call) and isinstance(x.func, ast.Name)
                              and x.func.id == view_assignment.targets[0].id and x.args and isinstance(x.args[0], ast.Name) and x.args[0].id == request_name)
            assert call.lineno < auth.lineno < view_assignment.lineno <= invocation.lineno
            pk = next(k.value for k in invocation.keywords if k.arg == 'pk')
            bound_pk = bind(pk, environment)
            pk_template = str(bound_pk.value) if isinstance(bound_pk, ast.Constant) and type(bound_pk.value) is int else source_url(pk, {}, {}, methods, environment)
            assert template.split('/')[-3] == pk_template, 'factory path ID and explicit view PK agree symbolically'
            assert route['source_handler'] == 'apps.channels.api_views.RecordingViewSet'
            result.append({'line': call.lineno, 'source_expression': segment(text, call), 'source_ast_sha256': ast_fingerprint(call),
                'HTTP_method_observed': 'POST', 'symbolic_path': template, 'contract_id': operation['contract_id'],
                'helper_call_chain': trace, 'authentication_expression': ast.unparse(bind(user, environment)),
                'transport': 'APIRequestFactory direct DRF view invocation only; no HTTP/router dispatch or TCP',
                'HTTP_dispatch_proved': False, 'router_dispatch_proved': False,
                'force_authenticate_expression': segment(text, auth), 'view_action_expression': segment(text, view_assignment.value),
                'view_invocation_expression': segment(text, invocation), 'view_action': mapping,
                'symbolic_view_pk': pk_template, 'site_kind': 'static framework request site in a successful Original workflow, not a wire request count'})
        elif isinstance(call.func.value, ast.Name) and call.func.value.id == 'self' and call.func.attr in methods and not call.func.attr.startswith('test'):
            helper = methods[call.func.attr]
            result += factory_trace(helper, methods, registry, text,
                trace + [{'line': call.lineno, 'source_expression': segment(text, call), 'helper': helper.name, 'helper_ast_sha256': ast_fingerprint(helper)}],
                fixture_arguments(helper, call, environment), depth + 1)
    return result


def conditional_admin_fixture(text, functions):
    helper = functions['_make_admin']
    creation = next(x for x in ast.walk(helper) if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and x.func.attr == 'get_or_create')
    defaults = next(k.value for k in creation.keywords if k.arg == 'defaults')
    assert isinstance(defaults, ast.Dict) and len(defaults.keys) == 1 and ast.literal_eval(defaults.keys[0]) == 'user_level'
    assert ast.unparse(defaults.values[0]) == 'User.UserLevel.ADMIN'
    return {'kind': 'forced Original framework helper fixture; get_or_create default is conditional', 'expression': 'self.user',
            'effective_user_level': None, 'declared_create_default_user_level': 10, 'existing_row_level_asserted': False,
            'get_or_create_source': segment(text, creation), 'source_helper_ast_sha256': ast_fingerprint(helper),
            'scope': 'TestCase helper defaults are preserved, not a universal assertion about an already existing row'}


def dvr_argument(node, statements, functions, values, depth=0):
    assert depth <= 6, 'bounded DVR fixture helper depth'
    expression = ast.unparse(node)
    assigned = [x for statement in statements for x in ast.walk(statement) if isinstance(x, ast.Assign)
                and any(ast.unparse(target) == expression for target in x.targets)]
    if assigned:
        return dvr_argument(assigned[-1].value, statements, functions, values, depth + 1)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == '_user':
        environment = fixture_arguments(functions['_user'], node, {})
        parameter = environment['dvr_access']
        if isinstance(parameter, ast.Constant) and parameter.value is None:
            return {'fixture_dvr_access_argument': None, 'fixture_key_added': False, 'source_argument': 'None (omitted or explicit)', 'effective_policy_not_inferred': True}
        assert isinstance(parameter, ast.Name) and parameter.id in values, 'exact DVR access constant'
        return {'fixture_dvr_access_argument': values[parameter.id], 'fixture_key_added': True, 'source_argument': parameter.id, 'effective_policy_not_inferred': True}
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == '_manager_without':
        helper = functions['_manager_without']
        environment = fixture_arguments(helper, node, {})
        body = [bind(x, environment) for x in helper.body]
        returned = next(x for x in reversed(body) if isinstance(x, ast.Return))
        return dvr_argument(returned.value, body, functions, values, depth + 1)
    raise ValueError('UnqualifiedDVRFixtureArgument: ' + expression)


def derive(root, registry):
    root = Path(root)
    levels, default = user_defaults(root)
    access_reference = next(row for row in ORIGINAL_FILES if row['original_path'] == 'apps/channels/dvr_access.py')
    access_text = (root / access_reference['path']).read_text()
    values = {x.targets[0].id: ast.literal_eval(x.value) for x in ast.parse(access_text).body if isinstance(x, ast.Assign)
              and len(x.targets) == 1 and isinstance(x.targets[0], ast.Name) and x.targets[0].id in ['DVR_ACCESS_NONE', 'DVR_ACCESS_VIEW', 'DVR_ACCESS_MANAGE']}
    assert values == {'DVR_ACCESS_NONE': 'none', 'DVR_ACCESS_VIEW': 'view', 'DVR_ACCESS_MANAGE': 'manage'}
    fixtures, cases, assignments, excluded = {}, {}, {}, []
    for reference in ORIGINAL_FILES:
        filename = reference['original_path']
        if filename not in CLASSES:
            continue
        text = (root / reference['path']).read_text(); tree = ast.parse(text)
        constants = {x.targets[0].id: x.value.value for x in tree.body if isinstance(x, ast.Assign) and len(x.targets) == 1 and isinstance(x.targets[0], ast.Name) and isinstance(x.value, ast.Constant) and isinstance(x.value.value, str)}
        module_functions = {x.name: x for x in tree.body if isinstance(x, ast.FunctionDef)}
        for cls in (x for x in tree.body if isinstance(x, ast.ClassDef) and x.name in CLASSES[filename]):
            methods = {x.name: x for x in cls.body if isinstance(x, ast.FunctionDef)}; setup = methods.get('setUp')
            urls = {ast.unparse(target): x.value.value for x in ast.walk(setup) if isinstance(x, ast.Assign) and isinstance(x.value, ast.Constant) and isinstance(x.value.value, str) for target in x.targets if isinstance(target, ast.Attribute)} if setup else {}
            functions = dict(module_functions, **methods)
            fixture_id = filename + ':' + cls.name
            context = {'class_ast_sha256': ast_fingerprint(cls), 'setup': segment(text, setup) if setup else None,
                'teardown': segment(text, methods['tearDown']) if 'tearDown' in methods else None,
                'class_decorators': [segment(text, x) for x in cls.decorator_list],
                'helpers': {name: segment(text, method) for name, method in methods.items() if not name.startswith('test')},
                'module_helpers': {name: segment(text, helper) for name, helper in module_functions.items()}}
            setup_actor = authentication(setup, {}, 10**9, None, 'self.client') if setup else None
            for name, test in methods.items():
                if not name.startswith('test'):
                    continue
                identifier = filename[:-3].replace('/', '.') + '.' + cls.name + '.' + name
                if cls.name in FACTORY_CLASSES:
                    requests = factory_trace(test, methods, registry, text)
                else:
                    # Only a response-derived ID used by the exact DVR create/delete test needs a new URL symbol.
                    local_urls = dict(urls)
                    rewritten = copy.deepcopy(test)
                    response_ids = [target.id for x in ast.walk(test) if isinstance(x, ast.Assign) and isinstance(x.value, ast.Subscript)
                                    and isinstance(x.value.value, ast.Attribute) and x.value.value.attr == 'data' and isinstance(x.value.slice, ast.Constant)
                                    and x.value.slice.value == 'id' for target in x.targets if isinstance(target, ast.Name)]
                    if response_ids:
                        class ResponseID(ast.NodeTransformer):
                            def visit_FormattedValue(self, value):
                                if isinstance(value.value, ast.Name) and value.value.id in response_ids:
                                    value.value = ast.Constant('{PK}')
                                return self.generic_visit(value)
                        rewritten = ResponseID().visit(rewritten)
                    requests = trace_requests(rewritten, methods, constants, local_urls, text, IDENTITIES, registry, setup_actor)
                    # The response-ID rewrite only controls symbolic URL resolution; original request AST/source remains authoritative.
                    for request in requests:
                        if response_ids:
                            original_call = next((x for x in ast.walk(test) if isinstance(x, ast.Call) and x.lineno == request['line'] and segment(text, x) == request['source_expression']), None)
                            if original_call is not None:
                                request['source_ast_sha256'] = ast_fingerprint(original_call)
                        node = request.pop('authentication_node')
                        request['actor'] = fixture_actor(node, [*(setup.body if setup else []), *test.body], functions, levels, default)
                        if cls.name == 'DvrAccessApiTests':
                            request['DVR_fixture_argument'] = dvr_argument(node, [*(setup.body if setup else []), *test.body], functions, values)
                if not requests:
                    excluded.append(identifier); continue
                if cls.name in FACTORY_CLASSES:
                    assert ast.unparse(next(x for x in ast.walk(setup) if isinstance(x, ast.Assign) and any(ast.unparse(t) == 'self.factory' for t in x.targets)).value) == 'APIRequestFactory()'
                    for request in requests:
                        assert request['authentication_expression'] == 'self.user'
                        request['actor'] = conditional_admin_fixture(text, functions)
                helpers = sorted({step['helper'] for request in requests for step in request['helper_call_chain']})
                assertions = [segment(text, x) for x in ast.walk(test) if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and (x.func.attr.startswith('assert') or x.func.attr in ['assert_called_once_with', 'assert_not_called'])]
                helper_assertions = {helper: [segment(text, x) for x in ast.walk(methods[helper]) if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and x.func.attr.startswith('assert')] for helper in helpers}
                assert assertions or any(helper_assertions.values()), identifier
                fixtures[fixture_id] = context
                cases[identifier] = {'test_id': identifier, 'source_path': filename, 'class': cls.name, 'method': name, 'fixture_id': fixture_id,
                    'source_ast_sha256': ast_fingerprint(test), 'assertion_expressions': assertions, 'helper_assertion_expressions': helper_assertions,
                    'request_sites': requests, 'decorators': [segment(text, x) for x in test.decorator_list],
                    'patch_contexts': [segment(text, x) for x in ast.walk(test) if isinstance(x, ast.Call) and (ast.unparse(x.func) in ['patch', 'override_settings', 'mock.patch', 'patch.object', 'patch.dict'] or ast.unparse(x.func).startswith('mock.patch.'))],
                    'assertion_scope': 'whole unmodified workflow; mocked task/threads/websocket effects remain mocked, not actual background execution'}
                for request in requests:
                    assignments.setdefault(request['contract_id'], [])
                    if identifier not in assignments[request['contract_id']]:
                        assignments[request['contract_id']].append(identifier)
    return {'fixtures': fixtures, 'cases': cases, 'assignments': assignments, 'excluded_methods': excluded,
            'User_levels': levels, 'omitted_user_level_default': default, 'DVR_constants': values}

IDENTITIES = {'route-f11670971b47e9fa9cdc:GET': {'method': 'GET', 'source_action': 'list', 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/^recordings/$', 'source_handler': 'apps.channels.api_views.RecordingViewSet'}, 'route-f11670971b47e9fa9cdc:POST': {'method': 'POST', 'source_action': 'create', 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/^recordings/$', 'source_handler': 'apps.channels.api_views.RecordingViewSet'}, 'route-b406c782172baf90cd51:DELETE': {'method': 'DELETE', 'source_action': 'destroy', 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/^recordings/(?P<pk>[^/.]+)/$', 'source_handler': 'apps.channels.api_views.RecordingViewSet'}, 'route-980f4f82aacd9064eb76:GET': {'method': 'GET', 'source_action': 'file', 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/^recordings/(?P<pk>[^/.]+)/file/$', 'source_handler': 'apps.channels.api_views.RecordingViewSet'}, 'route-506c0f01222feb30791b:GET': {'method': 'GET', 'source_action': 'list', 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/^recurring-rules/$', 'source_handler': 'apps.channels.api_views.RecurringRecordingRuleViewSet'}, 'route-506c0f01222feb30791b:POST': {'method': 'POST', 'source_action': 'create', 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/^recurring-rules/$', 'source_handler': 'apps.channels.api_views.RecurringRecordingRuleViewSet'}, 'route-b0a5c844dd8860509aa9:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/series-rules/', 'source_handler': 'apps.channels.api_views.SeriesRulesAPIView'}, 'route-5fb7bc1498c1f48f786b:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/series-rules/bulk-remove/', 'source_handler': 'apps.channels.api_views.BulkRemoveSeriesRecordingsAPIView'}, 'route-8622fcfadd81428963af:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/recordings/bulk-delete-upcoming/', 'source_handler': 'apps.channels.api_views.BulkDeleteUpcomingRecordingsAPIView'}, 'route-8463af6be513da4deb1c:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/dvr/comskip-config/', 'source_handler': 'apps.channels.api_views.ComskipConfigAPIView'}, 'route-a77300b3a33332d48d91:POST': {'method': 'POST', 'source_action': 'stop', 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/^recordings/(?P<pk>[^/.]+)/stop/$', 'source_handler': 'apps.channels.api_views.RecordingViewSet'}, 'route-5830b98d8dfc94b8da38:POST': {'method': 'POST', 'source_action': 'extend', 'owning_milestone': 'M5', 'pattern_exact': 'api/channels/^recordings/(?P<pk>[^/.]+)/extend/$', 'source_handler': 'apps.channels.api_views.RecordingViewSet'}, 'route-4572aed84f826eed1b79:METHOD_UNRESOLVED': {'method': None, 'source_action': None, 'owning_milestone': 'M5', 'pattern_exact': 'api/epg/import/', 'source_handler': 'apps.epg.api_views.EPGImportAPIView'}}
ORIGINAL_FILES = [{'original_path': 'apps/channels/tests/test_dvr_access.py', 'path': 'scripts/rust-contracts/dvr-import/original/apps/channels/tests/test_dvr_access.py', 'sha256': '6b9f8477c1586b983ed581a121f39dc5057c542a683b2679e5547db84069fb2d'}, {'original_path': 'apps/channels/tests/test_recording_stop_cancel.py', 'path': 'scripts/rust-contracts/dvr-import/original/apps/channels/tests/test_recording_stop_cancel.py', 'sha256': '328ee0042df82b0993648674d4fc0841ed50e6b86b5a2615ceb0bfbabc52332b'}, {'original_path': 'apps/channels/tests/test_recording_extend.py', 'path': 'scripts/rust-contracts/dvr-import/original/apps/channels/tests/test_recording_extend.py', 'sha256': '7acd38eaacc5cdfe5e3e95c877f0f6f8beb262eab22badcac1eb58692ac0edad'}, {'original_path': 'apps/epg/tests/test_epg_import_api.py', 'path': 'scripts/rust-contracts/dvr-import/original/apps/epg/tests/test_epg_import_api.py', 'sha256': '40da579dfb6c21fadb13267f339b1de32db4ff7387f6fff1581b459f63e8898b'}, {'original_path': 'apps/channels/dvr_access.py', 'path': 'scripts/rust-contracts/dvr-import/original/apps/channels/dvr_access.py', 'sha256': 'a8578c6f6f2db031e180203bc6b731992034bd48455f877e711812261faaf058'}, {'original_path': 'apps/channels/tests/test_channel_api.py', 'path': 'scripts/rust-contracts/channels-guide/original/apps/channels/tests/test_channel_api.py', 'sha256': 'a52a70e68428b111b269a8aa6f49457c27a19078a9ce76898c7328c21739c90a'}, {'original_path': 'apps/channels/api_urls.py', 'path': 'scripts/rust-contracts/channels-guide/original/apps/channels/api_urls.py', 'sha256': '51ed630131a4bcc1c88188f181af33945948beb8a4363676b293d654fd9b1dc4'}, {'original_path': 'apps/epg/api_urls.py', 'path': 'scripts/rust-contracts/channels-guide/original/apps/epg/api_urls.py', 'sha256': '1a9ee02a182a595b1864e857ff34d3d15458afde1930befc0d39782d6c13d422'}]

AUTHORITIES = [{'path': 'scripts/rust-contracts/framework-auth/full-suite-reference.json', 'sha256': '494e9945c39e94c2c05301efed9bba3ba89b5da3b60eb55142f2c701729091c6'}, {'path': 'scripts/rust-contracts/dvr-import/observations.json', 'sha256': '2c7ea4ba5b12852b66371a18c00e9ce04ebb7d38655c745e3aa8a875a1bcc9f5'}, {'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/models.py', 'sha256': '05cab8c590de411e68ff83b72d9d6256ca43007cbbab5d2a7f96a3a8965a9ded'}]

ASSERTIONS = ['Execute exact named historical Original methods with their original setup, helpers, forced authentication and mocks',
    'Retain whole-workflow SQL/response/mock assertions and named successful log blocks; no unrelated helper/model tests mapped to REST',
    'Bind only exact APIClient sites or explicit APIRequestFactory force-authenticate/as_view/action/PK invocation; framework-only calls do not prove HTTP/router dispatch',
    'EPG.delay, websocket, thread and revoke mocks remain mocked; no Celery/background execution or Native/completeness claim']


def dependencies():
    return [PREFIX + 'bindings.json'] + [row['path'] for row in ORIGINAL_FILES + AUTHORITIES]


def generate(root, registry):
    root = Path(root)
    for reference in ORIGINAL_FILES + AUTHORITIES:
        assert sha((root / reference['path']).read_bytes()) == reference['sha256'], 'exact DVR/import Original authority bytes'
    summary = json.loads((root / AUTHORITIES[0]['path']).read_text())
    observation = json.loads((root / AUTHORITIES[1]['path']).read_text())
    assert summary['source_commit'] == ORIGINAL and summary['exit_code'] == 0 and summary['tests_passed'] == 3362
    assert summary['scope'] == 'unchanged-upstream-full-python-test-suite'
    assert observation['full_log_sha256'] == summary['artifact_sha256']['tests.log'] and observation['full_log_bytes'] == 881752
    assert observation['fresh_Source_executions'] == 0
    indexed = {op['contract_id']: (route, op) for route in registry['runtime_routes'] for op in route['operations']}
    for identifier, identity in IDENTITIES.items():
        route, op = indexed[identifier]
        assert {**{key: route[key] for key in ['pattern_exact', 'source_handler']},
                **{key: op[key] for key in ['method', 'source_action', 'owning_milestone']}} == identity, 'stable DVR/import operation identity'
    derived = derive(root, registry)
    assert set(derived['assignments']) == set(IDENTITIES) and len(derived['cases']) == 55
    assert set(observation['cases']) == set(derived['cases'])
    assert sum(len(case['request_sites']) for case in derived['cases'].values()) == 90
    for identifier, case in derived['cases'].items():
        block = observation['cases'][identifier]
        assert block['framework_log_block'].startswith(case['method'] + ' (' + identifier + ')')
        assert re.search(r'(?:\.\.\. |\n)ok\s*\Z', block['framework_log_block']) and block['historical_result'] == 'ok'
        assert sha(block['framework_log_block'].encode()) == block['framework_log_block_sha256']
        assert block['full_log_byte_end'] - block['full_log_byte_start'] == len(block['framework_log_block'].encode())
        case.update(block)
        case['replay_kind'] = 'historical_named_Original_workflow_offline_assertion_replay'
    contracts, fixtures = [], {}
    for identifier, identity in IDENTITIES.items():
        selected = [derived['cases'][test] for test in derived['assignments'][identifier]]
        relevant = [site for case in selected for site in case['request_sites'] if site['contract_id'] == identifier]
        def role_name(site):
            return ('Original_conditional_admin_create_default' if site['actor'].get('existing_row_level_asserted') is False
                    else 'Original_user_level_' + str(site['actor']['effective_user_level']))
        roles = sorted({role_name(site) for site in relevant})
        specs = {role: {'authority': 'exact Original fixture/helper and explicit authentication; no credential backend or complete role matrix',
                       'fixture_actors': [site['actor'] for site in relevant if role_name(site) == role], 'complete_role_matrix': False} for role in roles}
        fixture_id = 'dvr-import:' + identifier
        fixtures[fixture_id] = {'id': fixture_id, 'baseline_revision': ORIGINAL, 'baseline_image': summary['image'],
            'ownership': 'Existing disposable Original TestCase database/cache fixtures, no production access',
            'execution': {'entrypoint': 'scripts/rust-baseline/run.py', 'argv': ['--output', '<fresh-empty-output>', *[case['test_id'] for case in selected]],
                          'fresh_live_replay_qualified_this_task': False, 'historical_full_suite_authority': AUTHORITIES[0]},
            'class_fixture_definitions': {key: derived['fixtures'][key] for key in sorted({case['fixture_id'] for case in selected})},
            'Original_User_level_enum': derived['User_levels'], 'omitted_User_field_level_default': derived['omitted_user_level_default'],
            'DVR_fixture_constants': derived['DVR_constants'], 'factory_get_or_create_admin_default_is_conditional': True,
            'EPG_import_delay_is_mocked': True, 'required_environment': 'Existing Original Django/DRF fixture, never imported into SDK'}
        contracts.append(dict(identity, contract_id=identifier, fixture_id=fixture_id, observed_roles=roles, source_role_specs=specs,
            observed_request_methods=sorted({site['HTTP_method_observed'] for site in relevant}), cases=selected, assertions=ASSERTIONS,
            result={'state': 'historical_Original_DVR_import_workflows_passed_not_new_execution', 'historical_test_cases': len(selected),
                    'new_Source_cases_this_task': 0, 'historical_authority': 'scripts/rust-baseline/verified-full-baseline.json'},
            fully_verified=False, remaining=['Only original workflows/assertions and explicit mocks apply; unobserved roles/grammar/aliases remain unverified',
                'Factory direct invocation proves no HTTP/router/TCP dispatch; APIClient also proves no TCP transcript',
                'Conditional get_or_create defaults do not prove arbitrary existing-row roles; forced auth bypasses credentials',
                'Mocked EPG.delay/thread/websocket/revoke effects do not prove real Celery/background execution',
                'No Native/browser/performance/full M0 or M1 claim']))
    return {'format_version': 1, 'binding_kind': KIND, 'original_revision': ORIGINAL, 'original_test_files': ORIGINAL_FILES,
            'authorities': AUTHORITIES, 'operation_count': 13, 'unique_historical_test_cases': 55,
            'static_request_sites': 90, 'fresh_Source_case_count': 0, 'fully_verified_operations': 0,
            'Native_coverage_increased': False, 'fixtures': fixtures, 'contracts': contracts, 'non_HTTP_contracts': [],
            'excluded_test_methods': derived['excluded_methods']}


def validate(root, value, bindings=None):
    root = Path(root)
    registry = json.loads((root / 'docs/rust-contract-register.json').read_text())
    assert value == generate(root, registry), 'exact DVR/import fixtures, framework/action/actor/mock associations and named results'
    return {'passed': True, 'operations': 13, 'unique_historical_tests': 55, 'static_request_sites': 90,
            'fresh_Original_executions': 0, 'fully_verified': 0}


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
        observation = json.loads((args.root / (PREFIX + 'observations.json')).read_text())
        assert sha(raw) == observation['full_log_sha256'] and len(raw) == observation['full_log_bytes']
        for case in observation['cases'].values():
            assert raw[case['full_log_byte_start']:case['full_log_byte_end']] == case['framework_log_block'].encode()
        result['deep_historical_raw_blocks'] = len(observation['cases'])
    print(json.dumps(result))


if __name__ == '__main__':
    main()
