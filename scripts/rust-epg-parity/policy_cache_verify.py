"""Decode native-filled group cache with the actual original Django client."""
import base64
import json
import os
from pathlib import Path
import sys
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture only')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.core.cache import cache
rows = json.loads(Path('/evidence/epg-policy-native-cache.json').read_text())
count = 0
for row in rows:
    if row.get('encoded_base64') is None:
        continue
    value = cache.client.decode(base64.b64decode(row['encoded_base64']))
    if value != row['expected_value']:
        raise RuntimeError('Original Django reader rejected native policy cache value')
    count += 1
print(json.dumps({'original_django_decoded_native_cache_values': count, 'passed': True}))
