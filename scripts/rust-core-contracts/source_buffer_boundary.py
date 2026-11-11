"""Actual pinned StreamBuffer/Redis observations; no benchmark or native claim.
Original Dispatcharr authors retain full application and algorithm credit.
"""
import copy
import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path

if os.environ.get('M1_CORE_DISPOSABLE_FIXTURE') != 'playback-fixture-only':
    raise RuntimeError('Owned disposable core fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.db import connection
from core.models import CoreSettings
from apps.proxy.config import TSConfig
from apps.proxy.live_proxy.input.buffer import StreamBuffer
import redis

client = redis.Redis(host='127.0.0.1', port=6379, password='playback-fixture-only')
assert client.ping()
with connection.cursor() as cursor:
    cursor.execute('SELECT current_database()')
    database = cursor.fetchone()[0]
assert database.startswith('rust_probe_backup_jobs_')
channel = 'core-source-' + uuid.uuid4().hex
owned_channels=[channel]
setting = CoreSettings.objects.filter(key='proxy_settings').first()
setting_existed = setting is not None
old_value = copy.deepcopy(setting.value) if setting_existed else None
old_chunk = TSConfig.BUFFER_CHUNK_SIZE
buffer = None
records = []


def packet(seed, pid=256):
    value = bytearray([seed % 251] * 188)
    value[:6] = bytes([0x47, pid >> 8, pid & 255, 0x30 | (seed % 16), 1, 0])
    return bytes(value)


def state(label):
    rows = []
    for index in range(1, buffer.index + 1):
        data = client.get(buffer.buffer_prefix + str(index))
        if data is not None:
            rows.append({'sequence': index, 'bytes': len(data), 'hex': data.hex(),
                         'sha256': hashlib.sha256(data).hexdigest()})
    records.append({'label': label, 'index': buffer.index,
                    'write_buffer_bytes': len(buffer._write_buffer),
                    'partial_packet_bytes': len(getattr(buffer, '_partial_packet', b'')),
                    'chunks': rows,
                    'discontinuities': sorted(buffer.discontinuities_in_range(0, buffer.index + 1)),
                    'oldest_last_consumed': buffer.find_oldest_available_chunk(0),
                    'age0_last_consumed': buffer.find_chunk_index_by_time(0),
                    'age60_last_consumed': buffer.find_chunk_index_by_time(60)})



try:
    defaults = {'effective_chunk_bytes': TSConfig.BUFFER_CHUNK_SIZE,'effective_ttl_seconds': TSConfig.get_redis_chunk_ttl()}
    configured = CoreSettings.get_proxy_settings();configured['redis_chunk_ttl']=2
    if setting is None:setting=CoreSettings.objects.create(key='proxy_settings',name='Owned boundary fixture',value=configured)
    else:setting.value=configured;setting.save(update_fields=['value'])
    TSConfig.clear_proxy_settings_cache();TSConfig.BUFFER_CHUNK_SIZE=376
    buffer=StreamBuffer(channel_id=channel,redis_client=client)
    for number in [1,2]:
        result=buffer.mark_discontinuity();state('mark'+str(number));records[-1]['mark_result']=result;records[-1]['payload_bytes']=sum(x['bytes']for x in records[-1]['chunks'])
        assert buffer.add_chunk(packet(number*2-1)+packet(number*2))
        state('publish'+str(number));records[-1]['payload_bytes']=sum(x['bytes']for x in records[-1]['chunks'])
    result=buffer.mark_discontinuity();state('pending_mark3_at_full_payload_cap');records[-1]['mark_result']=result;records[-1]['payload_bytes']=sum(x['bytes']for x in records[-1]['chunks'])
    assert records[-1]['index']==2 and records[-1]['payload_bytes']==752 and records[-1]['discontinuities']==[1,2,3] and result==3
    assert buffer.add_chunk(packet(5)+packet(6))
    state('third_publication_exceeds_declared_native_payload_budget');over_budget=records.pop()

finally:
    if buffer is not None:
        buffer.stop()
    for channel in owned_channels:
        keys = list(client.scan_iter(match='live:channel:' + channel + ':*', count=100))
        assert len(keys) <= 64
        if keys: client.delete(*keys)
        assert not list(client.scan_iter(match='live:channel:' + channel + ':*', count=100))
    TSConfig.BUFFER_CHUNK_SIZE = old_chunk
    if setting is not None:
        if not setting_existed:
            setting.delete()
        else:
            setting.value = old_value
            setting.save(update_fields=['value'])
    TSConfig.clear_proxy_settings_cache()

print(json.dumps({'passed': True, 'scope': 'Actual Original buffer helpers and real Redis; Source-only, no native/performance acceptance',
                  'defaults': defaults, 'configured_chunk_bytes': 376, 'configured_ttl_seconds': 2,
                  'records': records, 'source_only_over_budget_third_publication':over_budget, 'owned_channel_keys_removed': True,
                  'proxy_setting_value_restored': True,
                  'database_scope': 'Owned disposable database; a newly created setting may advance its sequence',
                  'source_buffer_sha256': hashlib.sha256(Path('/app/apps/proxy/live_proxy/input/buffer.py').read_bytes()).hexdigest(),
                  'credit': 'Original Dispatcharr authors and contributors retain application/algorithm credit; AGPL preserved.'}, separators=(',', ':')))
