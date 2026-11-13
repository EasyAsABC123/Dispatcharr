"""Draft real-resolver Source32 collector. Never imports the app at module import."""
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from capture_common import (audit, cache_snapshot, connected_database, exception_record,
                            literal, require, runtime_snapshot, save)

ROOT = Path('/settings-root')
REPORT = Path('/evidence/core-settings-source-private.json')


def rows(model):
    data = literal(list(model.objects.order_by('id').values()[:10001]))
    require(len(data) <= 10000 and len(json.dumps(data).encode()) <= 8388608, 'BoundedRows')
    return data


def target(spec, aliases, plan):
    name = spec['target']
    if name == 'collection':
        return plan['routes']['collection']
    if name == 'check':
        return plan['routes']['check']
    key = plan['target_policy']['missing'] if name == 'missing' else aliases[name]
    require(type(key) is int and key > 0, 'ActualOrDeclaredMissingID')
    return plan['routes']['detail'].format(bound_id=key)


def main():
    require(os.environ.get('SETTINGS32_AUTHORITY') == 'owned-source-settings-only', 'Authority')
    stage = os.environ['SETTINGS32_STAGE']
    database = os.environ['SETTINGS32_DATABASE']
    require(stage in ('audit', 'exercise'), 'Stage')
    require(database == 'dispatcharr' if stage == 'audit' else
            re.fullmatch(r'rust_probe_settings32_[a-f0-9]{10}', database) is not None, 'ExactDatabase')
    require('shell' in sys.argv, 'ManagementShellMarkerBeforeSetup')
    # Host invokes through the real management shell; no manual django.setup.
    from dispatcharr.app_initialization import should_skip_initialization
    from django.db import connection, connections, transaction
    from django.contrib.auth import get_user_model
    from django.urls import resolve
    from django.utils import timezone
    from django.core.cache import cache
    from rest_framework.test import APIClient
    from core.api_views import CoreSettingsViewSet
    from core.models import CoreSettings, SystemEvent
    from public_manifest import manifest
    from redis_channels import Probe
    sys.path.insert(0, str(ROOT / 'definition'))
    from validate_definition import validate
    from effects import validate_seed

    plan = json.loads((ROOT / 'definition/plan.json').read_text())
    validate(plan, ROOT / 'definition')
    schema = json.loads((ROOT / 'inputs/schema62-authority.json').read_text())
    report = {'capture_valid': False, 'actual_request_count': 0, 'completed_capture_count': 0,
              'stage': stage, 'database': database, 'Original_git': plan['Original_git'],
              'force_authenticate_used': False, 'Source_TCP_or_Native': False,
              'APIClient_raise_request_exception': False, 'requests': [], 'aliases': {},
              'phase': 'startup', 'passed': False, 'recorder_is_capture_only': True}
    probe = None

    def flush():
        if stage == 'exercise':
            save(REPORT, report)

    def guard():
        require(should_skip_initialization() is True and connected_database(connection) == database,
                'ActualStartupSkipAndDB')

    def snapshot():
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            return {'audit': audit(connection), 'public_manifest': manifest(connection, schema),
                    'settings': rows(CoreSettings), 'users': rows(get_user_model()),
                    'events': rows(SystemEvent), 'cache': cache_snapshot(),
                    'named_cache': {key: literal(cache.get(key)) for name in
                        ('owned.settings.new','owned.settings.renamed','owned.settings.foreign','epg_settings')
                        for key in (CoreSettings.group_cache_key(name), CoreSettings.group_cache_ver_key(name))},
                    'raw_Redis': probe.raw(), 'Channels': probe.snapshot()}

    try:
        guard()
        report['startup'] = {'real_management_shell_marker': 'shell' in sys.argv,
                             'should_skip_initialization': should_skip_initialization(),
                             'default_graph_or_web_startup_acceptance': False}
        if stage == 'audit':
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                report['audit'] = audit(connection)
                report['public_manifest'] = manifest(connection, schema)
            report['capture_valid'] = True
            report['passed'] = True
            return
        nonce = database.removeprefix('rust_probe_settings32_')
        probe = Probe(nonce)
        report['initial_Redis_before_receiver'] = probe.raw()
        report['receiver_fixture_context'] = probe.register()
        for item in json.loads((ROOT / 'inputs/actual-dependencies.json').read_text()):
            require(hashlib.sha256(Path(item['runtime_path']).read_bytes()).hexdigest() == item['sha256'],
                    'PinnedActualRuntimeDependency')
        report['before_owned_isolation'] = snapshot()
        report['runtime_before_seed'] = runtime_snapshot()
        report['phase'] = 'seed'
        flush()
        User = get_user_model()
        actors = {}
        with transaction.atomic():
            guard()
            require(User.objects.count() == 0, 'FreshOwnedUsers')
            CoreSettings.objects.all().delete()
            for item in plan['seed']['settings']:
                CoreSettings.objects.create(**item)
            for alias, item in plan['auth']['roles'].items():
                if item is None:
                    continue
                custom = {'UI': item['custom_ui']} if 'custom_ui' in item else {}
                # Pinned network_access_allowed reads custom_properties.allowed_networks.
                actors[alias] = User.objects.create(
                    id=item['id'], username='rust-owned-settings32-' + nonce + '-' + alias,
                    password='!', api_key='settings32-' + nonce + '-' + alias,
                    user_level=item['user_level'], is_active=True,
                    custom_properties={'allowed_networks': custom} if custom else {})
            with connection.cursor() as cursor:
                cursor.execute("SELECT setval('core_coresettings_id_seq',%s,%s)", [1002, True])
                cursor.execute("SELECT setval('accounts_user_id_seq',%s,%s)", [3, True])
        # These are explicit owned cache sentinels, not imported system/auth state.
        for name in ('owned.settings.new', 'owned.settings.renamed'):
            cache.set(CoreSettings.group_cache_key(name), {'owned_sentinel': name}, timeout=300)
        report['seed'] = snapshot()
        report['seed_binding'] = validate_seed(report['seed'], plan)
        report['phase'] = 'requests'
        flush()
        actions = {'POST': 'create', 'GET': 'retrieve', 'PATCH': 'partial_update',
                   'PUT': 'update', 'DELETE': 'destroy'}
        for spec in plan['cases']:
            guard()
            path = target(spec, report['aliases'], plan)
            match = resolve(path)
            action = 'check' if spec['target'] == 'check' else actions[spec['method']]
            require(match.func.cls is CoreSettingsViewSet and
                    match.func.actions.get(spec['method'].lower()) == action, 'CorrectResolvedClassAndAction')
            setup = None
            if 'pre_request_setup' in spec:
                require(spec['id'] == 12, 'OnlyDeclaredRenameWarm')
                setup = {'before':snapshot(), 'declared':spec['pre_request_setup']}
                for item in spec['pre_request_setup']['cache_set']:
                    cache.set(item['key'], item['value'], timeout=item['timeout'])
                setup['after'] = snapshot()
                report.setdefault('between_request_fixture_setup',[]).append({'before_case':spec['id'],**setup})
                flush()
            body = (ROOT / 'definition' / spec['body']).read_bytes()
            require(len(body) == spec['body_bytes'] and hashlib.sha256(body).hexdigest() == spec['body_sha256'], 'LiteralBody')
            client = APIClient(enforce_csrf_checks=True)
            client.raise_request_exception = False
            headers = {} if spec['role'] == 'anonymous' else {'HTTP_X_API_KEY': actors[spec['role']].api_key}
            client.credentials(**headers)
            row = {'id': spec['id'], 'name': spec['name'], 'role': spec['role'], 'method': spec['method'],
                   'target_alias': spec['target'], 'target_id': match.kwargs.get('pk'), 'path': path,
                   'body_sha256': spec['body_sha256'], 'body_bytes': len(body),
                   'raw_headers_private': {key: value.encode().hex() for key, value in headers.items()},
                   'resolver': {'class': match.func.cls.__module__ + '.' + match.func.cls.__qualname__,
                                'action': action, 'kwargs': match.kwargs, 'url_name': match.url_name},
                   'before': snapshot(), 'began_utc': timezone.now().isoformat(), 'actual_request_invoked': True}
            report['requests'].append(row)
            report['actual_request_count'] += 1
            flush()
            try:
                response = client.generic(spec['method'], path, data=body, content_type=spec['content_type'],
                                          HTTP_ACCEPT='application/json', REMOTE_ADDR=plan['auth']['REMOTE_ADDR'])
            except BaseException:
                row.update(transport='framework_exception', framework_exception=exception_record(sys.exc_info()))
            else:
                raw = bytes(response.content)
                require(len(raw) <= 65536, 'ResponseBodyCapacity')
                reply_headers = dict(response.items())
                require(sum(len(k.encode())+len(v.encode()) for k,v in reply_headers.items()) <= 65536, 'ResponseHeaderCapacity')
                try:
                    parsed = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    parsed = None
                request = (getattr(response, 'renderer_context', None) or {}).get('request')
                actor = getattr(request, 'user', None)
                backend = getattr(request, 'successful_authenticator', None)
                row.update(transport='http_response', status=response.status_code, headers=reply_headers,
                           json=parsed, response_body_hex=raw.hex(), response_body_bytes=len(raw),
                           response_body_sha256=hashlib.sha256(raw).hexdigest(),
                           framework_exception=exception_record(getattr(response, 'exc_info', None)),
                           actual_auth={'user_id': getattr(actor,'pk',None),
                                        'is_authenticated':bool(getattr(actor,'is_authenticated',False)),
                                        'backend_class':None if backend is None else type(backend).__module__+'.'+type(backend).__qualname__})
            finally:
                row['completed_utc'] = timezone.now().isoformat()
            row['after'] = snapshot()
            row['capture_complete'] = row.get('framework_exception') is None or row['framework_exception']['complete']
            require(row['capture_complete'], 'CompleteLiteralExceptionCapacity')
            report['completed_capture_count'] += 1
            flush()
            if 'bind_created_alias' in spec:
                value = row.get('json')
                require(isinstance(value, dict) and type(value.get('id')) is int and value['id'] > 0,
                        'ActualCreateIdentityRequiredNoInventedFallback')
                report['aliases'][spec['bind_created_alias']] = value['id']
                flush()
        report.update(capture_valid=True, phase='complete')
    except BaseException:
        report['outer_fixture_catch'] = {'phase': report['phase'], 'exception': exception_record(sys.exc_info())}
    finally:
        if probe is not None:
            try:
                report['final_before_receiver_cleanup'] = snapshot()
                report['receiver_cleanup'] = probe.discard()
                report['final_Redis_after_receiver_cleanup'] = probe.raw()
                probe.close_clients()
            except BaseException:
                report['receiver_cleanup_exception'] = exception_record(sys.exc_info())
                report['capture_valid'] = False
        connections.close_all()
        report['connections_close_called'] = True
        save(REPORT if stage == 'exercise' else Path('/evidence/default-' + os.environ['SETTINGS32_AUDIT_LABEL'] + '-private.json'), report)
    require(report['capture_valid'] is True and (stage == 'audit' or report['actual_request_count'] == 32), 'CompleteSource32NoRetry')


if __name__ == '__main__':
    main()
