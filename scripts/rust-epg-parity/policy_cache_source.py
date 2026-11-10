"""Original cached EPG history policy oracle; disposable DB/Redis only.
All application and algorithm credit belongs to original Dispatcharr authors.
"""
import json
import os
import sys
from unittest.mock import patch
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture only')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.core.cache import cache
from core.models import CoreSettings
from core.utils import RedisClient
GROUP = CoreSettings.group_cache_key('epg_settings')
VERSION = CoreSettings.group_cache_ver_key('epg_settings')
PHYSICAL_GROUP = ':1:' + GROUP
PHYSICAL_VERSION = ':1:' + VERSION
ABSENT = object()
DB = {'epg_history_min_days': 2, 'epg_history_max_days': 10, 'epg_default_prev_days': 3, 'epg_default_days': 4}


def snapshot(client):
    result = {}
    for name, key in [('group', PHYSICAL_GROUP), ('version', PHYSICAL_VERSION)]:
        kind = client.type(key)
        if isinstance(kind, bytes):
            kind = kind.decode()
        result[name + '_type'] = kind
        if kind == 'string':
            try:
                result[name] = cache.get(GROUP if name == 'group' else VERSION)
            except Exception as error:
                result[name + '_error'] = type(error).__name__
        result[name + '_ttl'] = client.ttl(key)
    return result


def main():
    client = RedisClient.get_client()
    definitions = [
        ('cold-db', DB, ABSENT, ABSENT, ABSENT, None),
        ('warm-stale-cache', DB, {'epg_history_min_days': 3, 'epg_history_max_days': 9}, ABSENT, ABSENT, None),
        ('warm-empty-map', DB, {}, ABSENT, ABSENT, None),
        ('missing-db', ABSENT, ABSENT, ABSENT, ABSENT, None),
        ('empty-db', {}, ABSENT, ABSENT, ABSENT, None),
        ('nonobject-db', ['not a settings map'], ABSENT, ABSENT, ABSENT, None),
        ('scalar-cache', DB, 'not a map', ABSENT, ABSENT, None),
        ('null-cache', DB, None, ABSENT, ABSENT, None),
        ('bool-cache', DB, False, ABSENT, ABSENT, None),
        ('version-unchanged', DB, ABSENT, 'same', 'same', None),
        ('version-python-equal', DB, ABSENT, True, 1, None),
        ('version-changed', DB, ABSENT, 'before', 'after', None),
        ('wrong-group-type', DB, ABSENT, ABSENT, ABSENT, 'group-hash'),
        ('wrong-version-type', DB, ABSENT, ABSENT, ABSENT, 'version-hash'),
        ('malformed-pickle', DB, ABSENT, ABSENT, ABSENT, 'malformed'),
        ('skip-cache', DB, {'epg_history_min_days': 3, 'epg_history_max_days': 9}, 'keep', ABSENT, 'skip'),
        ('wrong-auth', DB, ABSENT, ABSENT, ABSENT, 'auth'),
        ('backend-down', DB, ABSENT, ABSENT, ABSENT, 'backend'),
    ]
    cases = []
    for name, value, cached, before, after, fault in definitions:
        if value is ABSENT:
            CoreSettings.objects.filter(key='epg_settings').delete()
        else:
            CoreSettings.objects.update_or_create(key='epg_settings', defaults={'name': 'Isolated EPG policy fixture', 'value': value})
        client.delete(PHYSICAL_GROUP, PHYSICAL_VERSION)
        if cached is not ABSENT:
            cache.set(GROUP, cached, timeout=123)
        if before is not ABSENT:
            cache.set(VERSION, before, timeout=300)
        if fault == 'group-hash':
            client.hset(PHYSICAL_GROUP, 'field', 'value')
        elif fault == 'version-hash':
            client.hset(PHYSICAL_VERSION, 'field', 'value')
        elif fault == 'malformed':
            client.set(PHYSICAL_GROUP, b'not-a-pickle')
        initial = snapshot(client)
        original = CoreSettings._load_group_value
        loads = []
        def load(key, defaults):
            loads.append(key)
            result = original(key, defaults)
            if after is not ABSENT:
                cache.set(VERSION, after, timeout=300)
            return result
        result = None
        error = None
        import contextlib
        import redis
        fault_client = None
        if fault in ('auth', 'backend'):
            fault_client = redis.Redis(host='127.0.0.1', port=6379 if fault == 'auth' else 1,
                password='fixture-wrong' if fault == 'auth' else 'playback-fixture-only',
                socket_connect_timeout=.2, socket_timeout=.2)
        connection_override = patch.object(cache.client, 'get_client', return_value=fault_client) if fault_client else contextlib.nullcontext()
        with connection_override, patch.object(CoreSettings, '_load_group_value', side_effect=load), \
             patch.dict(os.environ, {'DISPATCHARR_SKIP_REDIS_CACHE': '1' if fault == 'skip' else '0'}):
            try:
                policy = CoreSettings.get_epg_history_policy()
                result = {'min_days': policy.min_days, 'max_days': policy.max_days,
                          'default_prev_days': policy.default_prev_days, 'default_days': policy.default_days}
            except Exception as caught:
                error = type(caught).__name__
        if fault_client:
            fault_client.connection_pool.disconnect()
        cases.append({'name': name, 'db_present': value is not ABSENT, 'db': None if value is ABSENT else value,
            'cached_present': cached is not ABSENT, 'cached': None if cached is ABSENT else cached,
            'version_before_present': before is not ABSENT, 'version_before': None if before is ABSENT else before,
            'version_after_present': after is not ABSENT, 'version_after': None if after is ABSENT else after,
            'fault': fault, 'initial': initial, 'expected_policy': result, 'expected_error': error,
            'expected_loads': len(loads), 'expected_cache': snapshot(client)})
    print(json.dumps({'format_version': 1, 'scope': 'Actual original cached EPG history policy and version-fence components',
                      'cases': cases, 'application_jobs_verified': False, 'performance_verified': False}))


if __name__ == '__main__':
    main()
