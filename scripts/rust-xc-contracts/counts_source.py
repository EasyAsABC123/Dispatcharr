#!/usr/bin/env python3
"""Actual original Redis connection-count helper corpus, disposable fixture only.

Original Dispatcharr authors and contributors retain full application credit.
Run inside the pinned original container after fixture setup. No native claim.
"""
import json
import hashlib
import os
import sys
from pathlib import Path

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('disposable fixture required')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from core.utils import RedisClient
from apps.proxy.utils import get_user_active_connections


def wire(value):
    return {'bytes_hex': value.hex()} if isinstance(value, bytes) else value


def main():
    redis = RedisClient.get_client()
    keys = [
        'live:channel:xc-counter-oracle.p5:clients:owned',
        'timeshift:channel:xc-counter-oracle:clients:owned',
        'vod_persistent_connection:xc-counter-oracle-owned',
    ]
    # Never delete somebody else's fixture data or accept a polluted witness.
    assert not any(redis.exists(key) for key in keys)
    assert not get_user_active_connections(None), 'requires no active fixture media'
    variants = [
        ('ordinary', '424242', '100.5'),
        ('missing-user', None, '100.5'),
        ('empty-user', '', '100.5'),
        ('other-user', '7', '100.5'),
        ('signed-user', '+424242', '100.5'),
        ('underscore-user', '424_242', '100.5'),
        ('unicode-user', '٤٢٤٢٤٢', '100.5'),
        ('spaced-user', '\u2003424242\u2003', '100.5'),
        ('invalid-user', 'invalid', '100.5'),
        ('huge-user', '1' * 4301, '100.5'),
        ('missing-time', '424242', None),
        ('empty-time', '424242', ''),
        ('underscore-time', '424242', '1_000.5'),
        ('unicode-time', '424242', '١٠٠.٥'),
        ('spaced-time', '424242', '\u2003100.5\u2003'),
        ('invalid-time', '424242', 'invalid'),
        ('nan-time', '424242', 'NaN'),
        ('infinite-time', '424242', '+Infinity'),
        ('overflow-time', '424242', '1e9999'),
        ('invalid-utf8-user', b'\xff', '100.5'),
        ('invalid-utf8-time', '424242', b'\xff'),
    ]
    rows = []
    try:
        for namespace, key in enumerate(keys):
            timestamp = 'created_at' if namespace == 2 else 'connected_at'
            for name, user, value in variants:
                redis.delete(*keys)
                fields = {'content_uuid': 'owned-content'}
                if user is not None:
                    fields['user_id'] = user
                if value is not None:
                    fields[timestamp] = value
                redis.hset(key, mapping=fields)
                rows.append({
                    'namespace': ['live', 'timeshift', 'vod'][namespace],
                    'case': name,
                    'user': wire(user), 'timestamp': wire(value),
                    'all_count': len(get_user_active_connections(None)),
                    'filtered_count': len(get_user_active_connections(424242)),
                })
            redis.delete(*keys)
            redis.set(key, 'wrong-type')
            rows.append({'namespace': ['live', 'timeshift', 'vod'][namespace], 'case': 'wrong-type', 'all_count': len(get_user_active_connections(None)), 'filtered_count': len(get_user_active_connections(424242))})
        # A failure after an accepted earlier row must discard the earlier count.
        redis.delete(*keys)
        redis.hset(keys[0], mapping={'user_id': '424242', 'connected_at': '1'})
        redis.hset(keys[1], mapping={'user_id': 'invalid', 'connected_at': '2'})
        rows.append({'case': 'late-invalid-user-discards-early-row', 'all_count': len(get_user_active_connections(None)), 'filtered_count': len(get_user_active_connections(424242))})
    finally:
        redis.delete(*keys)
    source = Path(get_user_active_connections.__code__.co_filename)
    print(json.dumps({'scope': 'Actual original get_user_active_connections with real Redis', 'source_path': 'apps/proxy/utils.py', 'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'native_verified': False, 'cases': rows, 'owned_keys_removed': not any(redis.exists(key) for key in keys)}, ensure_ascii=True, allow_nan=False))


if __name__ == '__main__':
    main()
