"""Pure pinned Django URL resolution. No HTTP/view/SQL/network/child execution.
Original Dispatcharr and Django/DRF authors retain their full credit and licenses.
"""
import hashlib
import ast
import inspect
import json
import os
from pathlib import Path
import socket
import secrets
import subprocess
import sys
import threading

ROOT = Path('/resolver')
SOURCE = Path('/app')
load = lambda name: json.loads((ROOT / name).read_bytes())
expected = load('source-files.json')
actual = {str(p.relative_to(SOURCE)): hashlib.sha256(p.read_bytes()).hexdigest()
          for p in SOURCE.rglob('*') if p.is_file()}
assert actual == expected and len(actual) == 1211, 'exact complete pinned Source tree'
assert os.getuid() == 65532
request = load('input.json')
assert len(request['cases']) == 4 and len(request['routes']) == 592
counts = {'SQL_connection_or_cursor_attempts': 0, 'network_attempts': 0,
          'thread_attempts': 0, 'child_attempts': 0, 'handler_invocations': 0}


# Actual denied attempts are retained, including caught library import probes.
# No original network/SQL/child function is ever called by this wrapper.
denied_operations = []
stage = 'before_django_setup'
observations = []
forest_proof = False


def deny(kind, operation):
    def forbidden(*args, **kwargs):
        counts[kind] += 1
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        path = Path(filename)
        frames = []
        current = frame
        for _ in range(12):
            if current is None:
                break
            frames.append({'module': current.f_globals.get('__name__'),
                           'function': current.f_code.co_name,
                           'filename': current.f_code.co_filename,
                           'lineno': current.f_lineno})
            current = current.f_back
        event = {'kind': kind, 'operation': operation, 'stage': stage,
                 'caller': frames[0], 'caller_frames': frames,
                 'caller_file_sha256': hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None,
                 'original_operation_executed': False}
        if operation == 'socket.socket.bind':
            event['socket_family'] = int(args[0].family)
            address = args[1]
            event['IPv6_loopback_ephemeral_bind'] = (isinstance(address, tuple)
                and len(address) >= 2 and address[0] == '::1' and address[1] == 0)
        if len(denied_operations) < 16:
            denied_operations.append(event)
        raise RuntimeError('PureResolverDenied_' + kind + '_' + operation)
    return forbidden


def qualified_blocked_ipv6_import_probe(event):
    # This is a named blocked-attempt scope, not a network permission or zero.
    # All predicates use the measured caller. Any other caller remains failure.
    if not (event['kind'] == 'network_attempts'
            and event['operation'] == 'socket.socket.bind'
            and event['stage'] == 'django_setup'
            and event['caller']['module'] == 'urllib3.util.connection'
            and event['caller']['function'] == '_has_ipv6'
            and event['caller']['filename'] == '/dispatcharrpy/lib/python3.13/site-packages/urllib3/util/connection.py'
            and event.get('socket_family') == int(socket.AF_INET6)
            and event.get('IPv6_loopback_ephemeral_bind') is True
            and event['original_operation_executed'] is False):
        return False
    # Verify the pinned image library actually implements a best-effort bind
    # probe at import, rather than accepting a function name alone.
    raw = Path(event['caller']['filename']).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == event['caller_file_sha256']
    if event['caller_file_sha256'] != '2633bbdb69731e5ccb5cf4e4afd65605d86c7979cc5633126f50c92d5ad74a74':
        return False
    tree = ast.parse(raw)
    funcs = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_has_ipv6']
    if len(funcs) != 1:
        return False
    bound_try = any(isinstance(node, ast.Try)
        and any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == 'bind'
                for stmt in node.body for call in ast.walk(stmt))
        and any(isinstance(handler.type, ast.Name) and handler.type.id == 'Exception' for handler in node.handlers)
        for node in ast.walk(funcs[0]))
    import_probe = any(isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'HAS_IPV6' for target in node.targets)
        and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
        and node.value.func.id == '_has_ipv6' and len(node.value.args) == 1
        and isinstance(node.value.args[0], ast.Constant) and node.value.args[0].value == '::1'
        for node in tree.body)
    return bound_try and import_probe


for name in ['connect', 'connect_ex', 'bind', 'listen', 'sendto', 'sendmsg']:
    if hasattr(socket.socket, name):
        setattr(socket.socket, name, deny('network_attempts', 'socket.socket.' + name))
for name in ['getaddrinfo', 'gethostbyname', 'gethostbyname_ex', 'gethostbyaddr', 'create_connection']:
    setattr(socket, name, deny('network_attempts', 'socket.' + name))
threading.Thread.start = deny('thread_attempts', 'threading.Thread.start')
subprocess.Popen = deny('child_attempts', 'subprocess.Popen')
for name in ['fork', 'forkpty', 'posix_spawn', 'posix_spawnp', 'system']:
    if hasattr(os, name):
        setattr(os, name, deny('child_attempts', 'os.' + name))
stage = 'before_django_setup'
callback_codes = set()

def main():
    global stage, callback_codes, observations, forest_proof
    sys.path.insert(0, str(SOURCE))
    sys.argv = [str(ROOT / 'qualify_resolver.py'), 'shell']
    # Required by actual pinned SimpleJWT import; stays in this process memory.
    # No token or authentication method is called by URL resolution.
    os.environ['DJANGO_SECRET_KEY'] = secrets.token_urlsafe(48)
    os.environ['DJANGO_SETTINGS_MODULE'] = 'dispatcharr.settings'
    os.environ['DISABLE_ML_DOWNLOADS'] = 'true'
    os.environ['DISPATCHARR_PLUGINS_DIR'] = '/tmp/empty-owned-plugins'
    Path('/tmp/empty-owned-plugins').mkdir(exist_ok=False)
    from django.db.backends.base.base import BaseDatabaseWrapper
    BaseDatabaseWrapper.ensure_connection = deny('SQL_connection_or_cursor_attempts', 'BaseDatabaseWrapper.ensure_connection')
    BaseDatabaseWrapper.cursor = deny('SQL_connection_or_cursor_attempts', 'BaseDatabaseWrapper.cursor')
    callback_codes = set()


    def no_handlers(frame, event, arg):
        if event == 'call' and frame.f_code in callback_codes:
            counts['handler_invocations'] += 1
            raise RuntimeError('PureResolverDenied_HTTPHandler')


    sys.setprofile(no_handlers)
    import django
    stage = 'django_setup'
    django.setup()
    from django.urls import URLPattern, URLResolver, get_resolver, resolve
    from dispatcharr.app_initialization import should_skip_initialization
    assert should_skip_initialization() is True, 'actual pinned shell initialization guard'


    def symbol(value):
        return str(getattr(value, '__module__', '')) + '.' + str(getattr(value, '__qualname__', type(value).__qualname__))


    forest = []
    chains = {}


    def walk(patterns, prefix='', namespaces=(), chain=()):
        for pattern in patterns:
            path = prefix + str(pattern.pattern)
            branch = chain + (pattern,)
            if isinstance(pattern, URLResolver):
                walk(pattern.url_patterns, path,
                     namespaces + ((pattern.namespace,) if pattern.namespace else ()), branch)
            elif isinstance(pattern, URLPattern):
                callback = pattern.callback
                view = getattr(callback, 'cls', getattr(callback, 'view_class', callback))
                record = {'pattern': path, 'name': ':'.join((*namespaces, pattern.name or '')),
                          'view': symbol(view), 'methods': getattr(callback, 'actions', None)}
                index = len(forest)
                forest.append(record)
                chains[tuple(id(p) for p in branch)] = index
                code = getattr(callback, '__code__', None)
                if code is not None:
                    callback_codes.add(code)


    stage = 'resolver_forest_load'
    walk(get_resolver().url_patterns)
    identity = lambda row: {key: row.get(key) for key in ['pattern', 'name', 'view', 'methods']}
    stage = 'resolver_identity_comparison'
    assert [identity(row) for row in forest] == [identity(row) for row in request['routes']], 'all592 actual resolver entries/order/actions must match pinned discovery'
    forest_proof = True
    observations = []
    stage = 'resolve_retained_paths'
    for case in request['cases']:
        path = case['path'].split('?', 1)[0]
        match = resolve(path)
        # ResolverMatch.tried retains the actual successful URLPattern chain last.
        # This binds the exact leaf object/ordinal, including catchalls and duplicate
        # callbacks, without guessing a regex winner or propagating format aliases.
        branch = tuple(id(p) for p in match.tried[-1])
        assert branch in chains, 'actual successful resolver chain belongs to frozen forest'
        ordinal = chains[branch]
        route = forest[ordinal]
        view = getattr(match.func, 'cls', getattr(match.func, 'view_class', match.func))
        assert symbol(view) == route['view']
        assert match.view_name == route['name']
        observations.append({**case, 'actual_resolver_ordinal': ordinal,
                             'actual_pattern': route['pattern'], 'actual_view_name': match.view_name,
                             'actual_handler': route['view'], 'actual_action_map': route['methods'],
                             'actual_kwargs': match.kwargs,
                             'Source_HTTP_replayed': False, 'method_permission_or_role_acceptance_added': False})
    assert len(observations) == 4 and forest_proof
    assert all(x['actual_resolver_ordinal'] == 155 and x['actual_handler'] == 'apps.epg.api_views.EPGSourceViewSet' and x['actual_view_name'] == 'api:epg:epg-source-sd-lineups' and x['actual_action_map'] == {'get':'sd_lineups','post':'sd_lineups','delete':'sd_lineups'} for x in observations), 'exact declared Source SD resolver root and action map'
    allowed_blocked_probe = (counts['network_attempts'] == 1 and len(denied_operations) == 1
                            and qualified_blocked_ipv6_import_probe(denied_operations[0]))
    assert all(value == 0 for kind, value in counts.items() if kind != 'network_attempts'), 'SQL/thread/child/handler denial remains a failure'
    assert counts['network_attempts'] == 0 or allowed_blocked_probe, 'unexpected denied network attempt; inspect measured caller receipt'
    assert len(denied_operations) == counts['network_attempts'], 'complete nonzero blocked-attempt accounting'
    sys.setprofile(None)
    print(json.dumps({'passed': True, 'actual_UID': os.getuid(), 'Django_version': django.get_version(),
                      'actual_pinned_Django_app_registry_loaded': True,
                      'required_import_signing_key_generated_only_in_memory': True,
                      'token_issuance_or_verification_invoked': False, 'actual_shell_initialization_skipped': True,
                      'all592_resolver_identities_order_actions_exact': True, 'Source_files': 1211,
                      'Source_HTTP_business_calls': 0, 'Source_or_Native_acceptance': False,
                      'counts': counts, 'observations': observations,
                      'denied_operations': denied_operations,
                      'qualified_blocked_import_probe_count': 1 if allowed_blocked_probe else 0,
                      'network_scope': 'All network operations denied; one measured urllib3 import-only IPv6 loopback bind capability probe may be separately qualified, preserving network_attempts=1',
                      'network_initialization_or_runtime_parity_claimed': False,
                      'credit': 'Original Dispatcharr and Django/DRF authors retain all application/framework credit.'}, separators=(',', ':')))


try:
    main()
except Exception as error:
    sys.setprofile(None)
    print(json.dumps({'passed': False, 'actual_UID': os.getuid(),
                      'failure_stage': stage, 'error_type': type(error).__name__,
                      'counts': counts, 'callback_code_objects_loaded': len(callback_codes),
                      'denied_operations': denied_operations,
                      'all592_resolver_identities_order_actions_exact': forest_proof,
                      'completed_resolutions': observations,
                      'Source_HTTP_business_calls': 0,
                      'Source_or_Native_acceptance': False,
                      'required_import_signing_key_value_published': False}, separators=(',', ':')), flush=True)
    raise
