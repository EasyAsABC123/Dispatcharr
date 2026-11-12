"""Exact request/helper associations in retained Original channel/guide tests.
Original Dispatcharr authors retain all application/algorithm credit and licenses.
AST request sites are source associations in named successful workflows, not TCP traces.
"""
import ast
import copy
import hashlib
import json
import re
from pathlib import Path
from framework_auth_contracts import ast_fingerprint
from account_settings_contracts import route_for, user_defaults

KIND = 'pinned_original_channel_guide_tests'
PREFIX = 'scripts/rust-contracts/channels-guide/'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
HTTP = {'get', 'post', 'put', 'patch', 'delete', 'head', 'options'}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def segment(text, node):
    return ast.get_source_segment(text, node)


def bind(node, environment):
    class Substitute(ast.NodeTransformer):
        def visit_Name(self, value):
            return copy.deepcopy(environment.get(value.id, value))
    return Substitute().visit(copy.deepcopy(node))


def arguments(function, call, environment):
    names = [x.arg for x in function.args.args if x.arg not in ['self', 'cls']]
    values = {name: bind(value, environment) for name, value in zip(names, call.args)}
    values.update({k.arg: bind(k.value, environment) for k in call.keywords if k.arg in names})
    defaults = dict(zip(names[-len(function.args.defaults):], function.args.defaults)) if function.args.defaults else {}
    for name in names:
        if name not in values and name in defaults:
            values[name] = bind(defaults[name], environment)
    return values


def source_url(node, constants, urls, methods, environment=None, depth=0):
    assert depth <= 6, 'bounded URL helper depth'
    node = bind(node, environment or {})
    if isinstance(node, ast.Constant) and (node.value is None or isinstance(node.value, str)):
        return node.value
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    if isinstance(node, ast.Attribute):
        if ast.unparse(node) in urls:
            return urls[ast.unparse(node)]
        if node.attr in ['id', 'pk']:
            return '{PK}'
    if isinstance(node, ast.Name) and node.id in ['profile_id', 'channel_id']:
        return '{PK}'
    if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
        for value in node.values:
            resolved = source_url(value, constants, urls, methods, depth=depth + 1)
            if resolved:
                return resolved
        return None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return source_url(node.left, constants, urls, methods, depth=depth + 1) + source_url(node.right, constants, urls, methods, depth=depth + 1)
    if isinstance(node, ast.JoinedStr):
        parts = []
        for part in node.values:
            if isinstance(part, ast.Constant):
                parts.append(part.value)
            elif isinstance(part, ast.FormattedValue):
                parts.append(source_url(part.value, constants, urls, methods, depth=depth + 1))
            else:
                raise ValueError('UnqualifiedURLString')
        return ''.join(parts)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Lambda):
        env = {parameter.arg: value for parameter, value in zip(node.func.args.args, node.args)}
        return source_url(bind(node.func.body, env), constants, urls, methods, depth=depth + 1)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == 'self' and node.func.attr in ['_url', '_streams_path']:
        method = methods[node.func.attr]
        env = arguments(method, node, {})
        # These two Source URL helpers only assign a default channel ID then return an f-string.
        for statement in method.body:
            if isinstance(statement, ast.Assign) and len(statement.targets) == 1 and isinstance(statement.targets[0], ast.Name):
                env[statement.targets[0].id] = bind(statement.value, env)
        returned = next(x for x in method.body if isinstance(x, ast.Return))
        return source_url(returned.value, constants, urls, methods, env, depth + 1)
    raise ValueError('UnqualifiedURLExpression: ' + ast.unparse(node))


def level_number(node, levels, default):
    if node is None:
        return default
    if isinstance(node, ast.Constant) and type(node.value) is int:
        return node.value
    if isinstance(node, ast.Attribute) and node.attr in levels:
        return levels[node.attr]
    raise ValueError('UnqualifiedUserLevel: ' + ast.unparse(node))


def actor(node, statements, functions, levels, default, depth=0):
    assert depth <= 6, 'bounded user fixture helper depth'
    if node is None or isinstance(node, ast.Constant) and node.value is None:
        return {'kind': 'anonymous APIClient fixture', 'expression': 'None', 'effective_user_level': None}
    expression = ast.unparse(node)
    # Exact assignment source, including explicit user_level writes after create_user.
    assigned = [x for s in statements for x in ast.walk(s) if isinstance(x, ast.Assign)
                and any(('self.' + ast.unparse(t)[4:] if ast.unparse(t).startswith('cls.') else ast.unparse(t)) == expression for t in x.targets)]
    if assigned:
        result = actor(assigned[-1].value, statements, functions, levels, default, depth + 1)
        updates = [x for s in statements for x in ast.walk(s) if isinstance(x, ast.Assign)
                   and any(('self.' + ast.unparse(t)[4:] if ast.unparse(t).startswith('cls.') else ast.unparse(t)) == expression + '.user_level' for t in x.targets)]
        if updates:
            result = dict(result, effective_user_level=level_number(updates[-1].value, levels, default), level_source=ast.unparse(updates[-1]))
        return dict(result, expression=expression)
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Attribute) and node.func.attr == 'create_user':
            value = next((k.value for k in node.keywords if k.arg == 'user_level'), None)
            return {'kind': 'forced Original test authentication; credential backend bypassed', 'expression': expression,
                    'effective_user_level': level_number(value, levels, default),
                    'level_source': 'omitted; pinned User.user_level default' if value is None else ast.unparse(value)}
        name = node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id if isinstance(node.func, ast.Name) else None
        if name in functions:
            function = functions[name]
            environment = arguments(function, node, {})
            bound = [bind(x, environment) for x in function.body]
            returned = next(x for x in reversed(bound) if isinstance(x, ast.Return))
            return actor(returned.value, bound, functions, levels, default, depth + 1)
    raise ValueError('UnqualifiedActorFixture: ' + expression)


def authentication(method, environment, before, inherited, receiver):
    calls = [x for x in ast.walk(method) if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute)
             and x.func.attr == 'force_authenticate' and x.lineno < before and ast.unparse(x.func.value) == receiver]
    if calls:
        call = max(calls, key=lambda x: x.lineno)
        value = next((k.value for k in call.keywords if k.arg == 'user'), call.args[0] if call.args else ast.Constant(None))
        return bind(value, environment)
    return inherited


def trace_requests(function, methods, constants, urls, text, selected, registry, parent_auth, trace=None, environment=None, depth=0):
    assert depth <= 6, 'bounded request helper depth'
    trace = trace or []
    environment = dict(environment or {})
    for local in ast.walk(function):
        if isinstance(local, ast.Assign) and len(local.targets) == 1 and isinstance(local.targets[0], ast.Name) and (isinstance(local.value, ast.Lambda) or isinstance(local.value, ast.Constant) and isinstance(local.value.value, str)):
            environment[local.targets[0].id] = bind(local.value, environment)
    result = []
    for call in sorted((x for x in ast.walk(function) if isinstance(x, ast.Call)), key=lambda x: (x.lineno, x.col_offset)):
        if not isinstance(call.func, ast.Attribute):
            continue
        receiver = ast.unparse(call.func.value)
        if call.func.attr in HTTP and receiver in ['self.client', 'client', 'anon_client']:
            if not call.args:
                continue
            template = source_url(call.args[0], constants, urls, methods, environment)
            route, operation = route_for(registry['runtime_routes'], template, call.func.attr.upper())
            if operation['contract_id'] not in selected:
                continue
            authenticated = authentication(function, environment, call.lineno, parent_auth if receiver == 'self.client' else None, receiver)
            result.append({'line': call.lineno, 'source_expression': segment(text, call), 'source_ast_sha256': ast_fingerprint(call),
                'HTTP_method_observed': call.func.attr.upper(), 'symbolic_path': template, 'contract_id': operation['contract_id'],
                'receiver': receiver, 'helper_call_chain': trace, 'authentication_expression': ast.unparse(authenticated) if authenticated is not None else 'None',
                'authentication_node': authenticated,
                'transport': 'Original Django APIClient; no TCP transcript claim',
                'site_kind': 'static request site in the actually successful unmodified workflow; not per-site runtime request count'})
        elif receiver == 'self' and call.func.attr in methods and not call.func.attr.startswith('test') and call.func.attr not in ['setUp', 'tearDown', 'setUpTestData']:
            helper = methods[call.func.attr]
            # Pure URL constructors have no requests and are resolved only by source_url.
            if call.func.attr in ['_url', '_streams_path']:
                continue
            inherited = authentication(function, environment, call.lineno, parent_auth, 'self.client')
            next_trace = trace + [{'line': call.lineno, 'source_expression': segment(text, call), 'helper': helper.name, 'helper_ast_sha256': ast_fingerprint(helper)}]
            result += trace_requests(helper, methods, constants, urls, text, selected, registry, inherited,
                                     next_trace, arguments(helper, call, environment), depth + 1)
    return result


def derive(root, registry):
    root = Path(root)
    levels, default = user_defaults(root)
    fixtures, cases, assignments, excluded = {}, {}, {}, []
    for reference in ORIGINAL_FILES:
        filename = reference['original_path']
        text = (root / reference['path']).read_text()
        tree = ast.parse(text)
        constants = {x.targets[0].id: x.value.value for x in tree.body if isinstance(x, ast.Assign) and len(x.targets) == 1
                     and isinstance(x.targets[0], ast.Name) and isinstance(x.value, ast.Constant) and isinstance(x.value.value, str)}
        module_functions = {x.name: x for x in tree.body if isinstance(x, ast.FunctionDef)}
        for cls in (x for x in tree.body if isinstance(x, ast.ClassDef)):
            methods = {x.name: x for x in cls.body if isinstance(x, ast.FunctionDef)}
            setup = methods.get('setUp')
            urls = {ast.unparse(t): x.value.value for x in ast.walk(setup) if isinstance(x, ast.Assign) and isinstance(x.value, ast.Constant)
                    and isinstance(x.value.value, str) for t in x.targets if isinstance(t, ast.Attribute)} if setup else {}
            functions = dict(module_functions, **methods)
            fixture_id = filename + ':' + cls.name
            context = {'class_ast_sha256': ast_fingerprint(cls), 'setup': segment(text, setup) if setup else None,
                'setUpTestData': segment(text, methods['setUpTestData']) if 'setUpTestData' in methods else None,
                'teardown': segment(text, methods['tearDown']) if 'tearDown' in methods else None,
                'class_decorators': [segment(text, x) for x in cls.decorator_list],
                'helpers': {name: segment(text, method) for name, method in methods.items() if not name.startswith('test')},
                'module_helpers': {name: segment(text, method) for name, method in module_functions.items()}}
            setup_actor = authentication(setup, {}, 10**9, None, 'self.client') if setup else None
            for name, test in methods.items():
                if not name.startswith('test'):
                    continue
                identifier = filename[:-3].replace('/', '.') + '.' + cls.name + '.' + name
                requests = trace_requests(test, methods, constants, urls, text, IDENTITIES, registry, setup_actor)
                if not requests:
                    excluded.append(identifier)
                    continue
                assertions = [segment(text, x) for x in ast.walk(test) if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and x.func.attr.startswith('assert')]
                # Helper assertions are retained alongside test assertions, never duplicated into a pretend per-request transcript.
                helper_names = {step['helper'] for r in requests for step in r['helper_call_chain']}
                helper_assertions = {name: [segment(text, x) for x in ast.walk(methods[name]) if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and x.func.attr.startswith('assert')] for name in sorted(helper_names)}
                assert assertions or any(helper_assertions.values()), identifier
                for request in requests:
                    node = request.pop('authentication_node')
                    try:
                        request['actor'] = actor(node, [*(methods['setUpTestData'].body if 'setUpTestData' in methods else []), *(setup.body if setup else []), *test.body], functions, levels, default)
                    except ValueError as error:
                        raise ValueError(identifier + ': ' + str(error)) from error
                fixtures[fixture_id] = context
                cases[identifier] = {'test_id': identifier, 'source_path': filename, 'class': cls.name, 'method': name,
                    'fixture_id': fixture_id, 'source_ast_sha256': ast_fingerprint(test), 'assertion_expressions': assertions,
                    'helper_assertion_expressions': helper_assertions, 'request_sites': requests,
                    'decorators': [segment(text, x) for x in test.decorator_list],
                    'patch_contexts': [segment(text, x) for x in ast.walk(test) if isinstance(x, ast.Call) and (ast.unparse(x.func) in ['patch', 'override_settings', 'mock.patch', 'patch.object', 'patch.dict'] or ast.unparse(x.func).startswith('mock.patch.'))],
                    'assertion_scope': 'whole original workflow; not unrelated model assertions assigned to an isolated HTTP request'}
                for request in requests:
                    assignments.setdefault(request['contract_id'], [])
                    if identifier not in assignments[request['contract_id']]:
                        assignments[request['contract_id']].append(identifier)
    return {'fixtures': fixtures, 'cases': cases, 'assignments': assignments, 'excluded_methods': excluded,
            'User_levels': levels, 'omitted_user_level_default': default}

IDENTITIES = {'route-568206a919e930cddfea:PATCH': {'method': 'PATCH', 'pattern_exact': 'api/channels/^channels/edit/bulk/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'edit_bulk', 'owning_milestone': 'M5'}, 'route-0a733e108828586bf291:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^channels/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'list', 'owning_milestone': 'M5'}, 'route-0d1599b84a3e14ad8472:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^channels/summary/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'summary', 'owning_milestone': 'M5'}, 'route-92513d7363bfd51d45fc:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^channels/ids/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'get_ids', 'owning_milestone': 'M5'}, 'route-19bdf8661c1cb2d36711:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^channels/(?P<pk>[^/.]+)/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'retrieve', 'owning_milestone': 'M5'}, 'route-19bdf8661c1cb2d36711:PATCH': {'method': 'PATCH', 'pattern_exact': 'api/channels/^channels/(?P<pk>[^/.]+)/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'partial_update', 'owning_milestone': 'M5'}, 'route-974061a96fc0fd45870a:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^channels/(?P<pk>[^/.]+)/reorder/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'reorder', 'owning_milestone': 'M5'}, 'route-51434e1f5fdd0789aea2:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^channels/assign/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'assign', 'owning_milestone': 'M5'}, 'route-db57f015df417f751994:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^channels/from-stream/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'from_stream', 'owning_milestone': 'M5'}, 'route-db3a78dbace6356b715c:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^groups/$', 'source_handler': 'apps.channels.api_views.ChannelGroupViewSet', 'source_action': 'list', 'owning_milestone': 'M5'}, 'route-e0adb9f74703029f9925:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^groups/(?P<pk>[^/.]+)/claim-custom/$', 'source_handler': 'apps.channels.api_views.ChannelGroupViewSet', 'source_action': 'claim_custom', 'owning_milestone': 'M5'}, 'route-43655576925d1457c856:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^groups/(?P<pk>[^/.]+)/unclaim-custom/$', 'source_handler': 'apps.channels.api_views.ChannelGroupViewSet', 'source_action': 'unclaim_custom', 'owning_milestone': 'M5'}, 'route-f140a95f4ea400cc74f0:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^profiles/$', 'source_handler': 'apps.channels.api_views.ChannelProfileViewSet', 'source_action': 'create', 'owning_milestone': 'M5'}, 'route-cd24026f634440d00996:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^profiles/(?P<pk>[^/.]+)/duplicate/$', 'source_handler': 'apps.channels.api_views.ChannelProfileViewSet', 'source_action': 'duplicate', 'owning_milestone': 'M5'}, 'route-63f8a1ec8ecb0eb78339:METHOD_UNRESOLVED': {'method': None, 'pattern_exact': 'api/channels/profiles/<int:profile_id>/channels/<int:channel_id>/', 'source_handler': 'apps.channels.api_views.UpdateChannelMembershipAPIView', 'source_action': None, 'owning_milestone': 'M5'}, 'route-0b72bd315e20d1d33a91:METHOD_UNRESOLVED': {'method': None, 'pattern_exact': 'api/channels/channels/<int:channel_id>/streams/', 'source_handler': 'apps.channels.api_views.GetChannelStreamsAPIView', 'source_action': None, 'owning_milestone': 'M5'}, 'route-c76c9a38c97d5c51b40a:METHOD_UNRESOLVED': {'method': None, 'pattern_exact': 'api/channels/channels/<int:channel_id>/streams/stats/', 'source_handler': 'apps.channels.api_views.GetChannelStreamStatsAPIView', 'source_action': None, 'owning_milestone': 'M5'}, 'route-5695ce1e63d8669af1a0:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^channels/by-uuids/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'get_by_uuids', 'owning_milestone': 'M5'}, 'route-ef627283fb23a277c00e:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^channels/numbers-in-range/$', 'source_handler': 'apps.channels.api_views.ChannelViewSet', 'source_action': 'numbers_in_range', 'owning_milestone': 'M5'}, 'route-482250154227c0dc1a8b:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^streams/ids/$', 'source_handler': 'apps.channels.api_views.StreamViewSet', 'source_action': 'get_ids', 'owning_milestone': 'M5'}, 'route-ef81b677a046911214d9:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^streams/groups/$', 'source_handler': 'apps.channels.api_views.StreamViewSet', 'source_action': 'get_groups', 'owning_milestone': 'M5'}, 'route-5e6cfaf56a7f8e7fba4e:GET': {'method': 'GET', 'pattern_exact': 'api/channels/^streams/filter-options/$', 'source_handler': 'apps.channels.api_views.StreamViewSet', 'source_action': 'get_filter_options', 'owning_milestone': 'M5'}, 'route-1aa1901d7715c5d2682b:POST': {'method': 'POST', 'pattern_exact': 'api/channels/^streams/by-ids/$', 'source_handler': 'apps.channels.api_views.StreamViewSet', 'source_action': 'get_by_ids', 'owning_milestone': 'M5'}, 'route-3efd3bc586265b0ce5da:METHOD_UNRESOLVED': {'method': None, 'pattern_exact': 'api/epg/current-programs/', 'source_handler': 'apps.epg.api_views.CurrentProgramsAPIView', 'source_action': None, 'owning_milestone': 'M5'}, 'route-f513447db1b88aa96c34:METHOD_UNRESOLVED': {'method': None, 'pattern_exact': 'api/epg/grid/', 'source_handler': 'apps.epg.api_grid.EPGGridAPIView', 'source_action': None, 'owning_milestone': 'M5'}, 'route-1b7eb13541b7f6b9b797:GET': {'method': 'GET', 'pattern_exact': 'api/epg/^programs/search/$', 'source_handler': 'apps.epg.api_views.ProgramViewSet', 'source_action': 'search', 'owning_milestone': 'M5'}}
ORIGINAL_FILES = [{'path': 'scripts/rust-contracts/channels-guide/original/apps/channels/tests/test_channel_api.py', 'original_path': 'apps/channels/tests/test_channel_api.py', 'sha256': 'a52a70e68428b111b269a8aa6f49457c27a19078a9ce76898c7328c21739c90a'}, {'path': 'scripts/rust-contracts/channels-guide/original/apps/channels/tests/test_channel_membership_auth.py', 'original_path': 'apps/channels/tests/test_channel_membership_auth.py', 'sha256': '0d0e49689ab93724293eec8421533ceaecfcf980ff16694fb31b1503105abe96'}, {'path': 'scripts/rust-contracts/channels-guide/original/apps/channels/tests/test_channel_profile_create.py', 'original_path': 'apps/channels/tests/test_channel_profile_create.py', 'sha256': 'cb9a969c185a233382246aac6ab6f10cad7535c22970a92bd2711b7c3b6fbb99'}, {'path': 'scripts/rust-contracts/channels-guide/original/apps/channels/tests/test_channel_streams_visibility.py', 'original_path': 'apps/channels/tests/test_channel_streams_visibility.py', 'sha256': 'a25adecc72609da758642b6a954202ec8981df792e0892081eb18d47a8793907'}, {'path': 'scripts/rust-contracts/channels-guide/original/apps/channels/tests/test_stream_custom_action_permissions.py', 'original_path': 'apps/channels/tests/test_stream_custom_action_permissions.py', 'sha256': '7e75644fa159e73cc3eca49ffc4000535d2f6bab6f886aafa44afa63ad85d184'}, {'path': 'scripts/rust-contracts/channels-guide/original/apps/epg/tests/test_current_programs_api.py', 'original_path': 'apps/epg/tests/test_current_programs_api.py', 'sha256': '2aba252f83e0c7d184e57ca65499b58f1ffd031b0d8e41693f71097e448629e8'}, {'path': 'scripts/rust-contracts/channels-guide/original/apps/epg/tests/test_epg_grid_api.py', 'original_path': 'apps/epg/tests/test_epg_grid_api.py', 'sha256': 'ab8f5fea8e0a4fc835ca58032385acfb9e5ab4d5bab9aea72b5b796dfd1053d4'}, {'path': 'scripts/rust-contracts/channels-guide/original/apps/epg/tests/test_epg_search_api.py', 'original_path': 'apps/epg/tests/test_epg_search_api.py', 'sha256': '8925eaf045b00ea9755804706cd67bd611f2af5d48dc1db190cd7aafa823c158'}, {'path': 'scripts/rust-contracts/channels-guide/original/apps/epg/tests/test_program_search_permissions.py', 'original_path': 'apps/epg/tests/test_program_search_permissions.py', 'sha256': '35d9a2e63e5da9e799c4da913fd33244e473c28f221f0732194dacbd1c403775'}]

# Exact URL definitions supplement runtime discovery identity.
URL_FILES = [{'original_path': 'apps/channels/api_urls.py', 'path': 'scripts/rust-contracts/channels-guide/original/apps/channels/api_urls.py', 'sha256': '51ed630131a4bcc1c88188f181af33945948beb8a4363676b293d654fd9b1dc4'}, {'original_path': 'apps/epg/api_urls.py', 'path': 'scripts/rust-contracts/channels-guide/original/apps/epg/api_urls.py', 'sha256': '1a9ee02a182a595b1864e857ff34d3d15458afde1930befc0d39782d6c13d422'}]


def dependencies():
    return [PREFIX + 'bindings.json'] + [r['path'] for r in ORIGINAL_FILES + URL_FILES + AUTHORITIES]


def checked_authorities(root):
    for reference in ORIGINAL_FILES + URL_FILES + AUTHORITIES:
        assert sha((root / reference['path']).read_bytes()) == reference['sha256'], 'exact channel/guide Original authority bytes'
    summary = json.loads((root / AUTHORITIES[0]['path']).read_text())
    observation = json.loads((root / AUTHORITIES[1]['path']).read_text())
    assert summary['source_commit'] == ORIGINAL and summary['exit_code'] == 0 and summary['tests_passed'] == 3362
    assert summary['scope'] == 'unchanged-upstream-full-python-test-suite'
    assert observation['full_log_sha256'] == summary['artifact_sha256']['tests.log']
    assert observation['full_log_bytes'] == 881752 and observation['new_Source_executions'] == 0
    return summary, observation


def generate(root, registry):
    root = Path(root)
    summary, observations = checked_authorities(root)
    index = {operation['contract_id']: (route, operation) for route in registry['runtime_routes'] for operation in route['operations']}
    for identifier, identity in IDENTITIES.items():
        route, operation = index[identifier]
        assert {**{key: route[key] for key in ['pattern_exact', 'source_handler']},
                **{key: operation[key] for key in ['method', 'source_action', 'owning_milestone']}} == identity, 'stable channel/guide operation identity'
    derived = derive(root, registry)
    assert set(derived['assignments']) == set(IDENTITIES) and len(derived['cases']) == 167
    assert set(observations['cases']) == set(derived['cases']), 'exact selected successful workflows'
    assert sum(len(case['request_sites']) for case in derived['cases'].values()) == 206
    for identifier, case in derived['cases'].items():
        block = observations['cases'][identifier]
        assert block['historical_result'] == 'ok'
        assert block['framework_log_block'].startswith(case['method'] + ' (' + identifier + ')')
        assert re.search(r'(?:\.\.\. |\n)ok\s*\Z', block['framework_log_block']), 'actual successful named test terminal'
        assert sha(block['framework_log_block'].encode()) == block['framework_log_block_sha256']
        assert block['full_log_byte_end'] - block['full_log_byte_start'] == len(block['framework_log_block'].encode())
        case.update(block)
        case['transport'] = 'Original Django APIClient; no TCP transcript claim'
        case['replay_kind'] = 'historical_named_Original_test_workflow_offline_assertion_replay'
    fixtures, contracts = {}, []
    for identifier, identity in IDENTITIES.items():
        selected_cases = [derived['cases'][name] for name in derived['assignments'][identifier]]
        relevant = [site for case in selected_cases for site in case['request_sites'] if site['contract_id'] == identifier]
        roles = sorted({'anonymous' if site['actor']['effective_user_level'] is None else 'Original_user_level_' + str(site['actor']['effective_user_level']) for site in relevant})
        specs = {role: {'effective_user_level': None if role == 'anonymous' else int(role.rsplit('_', 1)[1]),
                       'authority': 'exact Original setup/helper/in-method authentication and role assignments',
                       'credential_backend': 'not exercised by force_authenticate; anonymous case has no forced actor',
                       'complete_role_matrix': False} for role in roles}
        fixture_id = 'channels-guide:' + identifier
        fixture_keys = sorted({case['fixture_id'] for case in selected_cases})
        fixtures[fixture_id] = {'id': fixture_id, 'baseline_revision': ORIGINAL, 'baseline_image': summary['image'],
            'ownership': 'Existing disposable Original TestCase database/cache fixtures; no production or paid-provider access',
            'execution': {'entrypoint': 'scripts/rust-baseline/run.py', 'argv': ['--output', '<fresh-empty-output>', *[case['test_id'] for case in selected_cases]],
                          'fresh_live_replay_qualified_this_task': False, 'historical_full_suite_authority': AUTHORITIES[0]},
            'class_fixture_definitions': {key: derived['fixtures'][key] for key in fixture_keys},
            'Original_User_level_enum': derived['User_levels'], 'omitted_user_level_actual_default': derived['omitted_user_level_default'],
            'required_environment': 'Existing Original Django/DRF baseline environment; not the dependency-free SDK'}
        contracts.append(dict(identity, contract_id=identifier, fixture_id=fixture_id,
            observed_roles=roles, source_role_specs=specs, observed_request_methods=sorted({site['HTTP_method_observed'] for site in relevant}),
            cases=selected_cases, assertions=ASSERTIONS,
            result={'state': 'historical_Original_channel_guide_tests_passed_not_new_execution', 'historical_test_cases': len(selected_cases),
                    'new_Source_cases_this_task': 0, 'historical_authority': 'scripts/rust-baseline/verified-full-baseline.json'},
            fully_verified=False, remaining=['Only the original workflow assertions and explicit mocks apply; no complete endpoint/role/grammar claim',
                'APIClient bypasses TCP and force_authenticate bypasses credential verification',
                'Static helper/request sites are not per-site live wire counts or method declarations',
                'Native/browser/performance and unobserved format aliases remain unverified']))
    return {'format_version': 1, 'binding_kind': KIND, 'original_revision': ORIGINAL, 'original_test_files': ORIGINAL_FILES,
            'original_URL_files': URL_FILES, 'authorities': AUTHORITIES, 'operation_count': 26,
            'unique_historical_test_cases': 167, 'static_request_sites': 206, 'fresh_Source_case_count': 0,
            'fully_verified_operations': 0, 'Native_coverage_increased': False, 'fixtures': fixtures, 'contracts': contracts,
            'non_HTTP_contracts': [], 'excluded_test_methods': derived['excluded_methods']}


def validate(root, value, bindings=None):
    root = Path(root)
    registry = json.loads((root / 'docs/rust-contract-register.json').read_text())
    assert value == generate(root, registry), 'exact channel/guide Source fixtures, actors, helper/request associations and named results'
    return {'passed': True, 'operations': 26, 'unique_historical_tests': 167, 'static_request_sites': 206,
            'fresh_Original_executions': 0, 'fully_verified': 0}

AUTHORITIES = [{'path': 'scripts/rust-contracts/framework-auth/full-suite-reference.json', 'sha256': '494e9945c39e94c2c05301efed9bba3ba89b5da3b60eb55142f2c701729091c6'}, {'path': 'scripts/rust-contracts/channels-guide/observations.json', 'sha256': '106ddff65b9ff6b3d78969dfd732f816d2223a9137991a0841969098b56e2827'}, {'path': 'scripts/rust-contracts/accounts-settings/original/apps/accounts/models.py', 'sha256': '05cab8c590de411e68ff83b72d9d6256ca43007cbbab5d2a7f96a3a8965a9ded'}]
ASSERTIONS = ['Execute exact named Original test methods with their original setup/helpers, authentication and mocks', 'Preserve whole-workflow assertions and successful historical log blocks; no unrelated model effects attributed to an endpoint', 'Bind only exact declared request/helper AST sites to immutable resolver operation identities; observed methods never resolve null declarations', 'Retain APIClient/forced-authentication limits and actual explicit/default user levels; no TCP/Native/complete role claim']


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--historical-log', type=Path, help='Optional exact retained full3362 log; no new Source execution')
    args = parser.parse_args()
    value = json.loads((args.root / (PREFIX + 'bindings.json')).read_text())
    result = validate(args.root, value)
    if args.historical_log is not None:
        raw = args.historical_log.read_bytes()
        observation = json.loads((args.root / (PREFIX + 'observations.json')).read_text())
        assert sha(raw) == observation['full_log_sha256'] and len(raw) == observation['full_log_bytes']
        for case in observation['cases'].values():
            assert raw[case['full_log_byte_start']:case['full_log_byte_end']] == case['framework_log_block'].encode(), 'exact full-log named result block'
        result['deep_historical_raw_blocks'] = len(observation['cases'])
    print(json.dumps(result))


if __name__ == '__main__':
    main()
