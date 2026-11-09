"""Bounded scalar cleanup checks on disposable Redis; never expose session IDs."""
import json
import os
import sys
import redis
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only' or os.environ.get('REDIS_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture only')
profile = int(sys.argv[1])
client = redis.Redis(host='127.0.0.1', port=6379, password=os.environ['REDIS_PASSWORD'],
                     decode_responses=True, socket_timeout=2)
counts = {}
for name, pattern in [('vod_metadata', 'vod_persistent_connection:*'),
                      ('timeshift_pool', 'timeshift:pool:*'), ('timeshift_api', 'timeshift:session:*')]:
    count = 0
    scanned = 0
    for key in client.scan_iter(match=pattern, count=100):
        scanned += 1
        if scanned > 4096:
            raise ValueError('Experiment Redis cleanup inspection exceeds4096keys')
        if client.type(key) == 'hash':
            count += 1
    counts[name] = count
counts['provider_connections'] = int(client.get('profile_connections:' + str(profile)) or 0)
print(json.dumps(counts))
