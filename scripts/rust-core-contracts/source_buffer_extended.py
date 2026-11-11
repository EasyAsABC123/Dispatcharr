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
    defaults = {'effective_chunk_bytes': TSConfig.BUFFER_CHUNK_SIZE,
                'effective_ttl_seconds': TSConfig.get_redis_chunk_ttl()}
    configured = CoreSettings.get_proxy_settings()
    configured['redis_chunk_ttl'] = 2
    if setting is None:
        setting = CoreSettings.objects.create(key='proxy_settings', name='Core buffer fixture', value=configured)
    else:
        setting.value = configured
        setting.save(update_fields=['value'])
    TSConfig.clear_proxy_settings_cache()
    TSConfig.BUFFER_CHUNK_SIZE = 376
    buffer = StreamBuffer(channel_id=channel, redis_client=client)
    assert buffer.chunk_ttl == 2 and buffer.target_chunk_size == 376
    state('empty')
    data = packet(1) + packet(2, 257)
    assert buffer.add_chunk(data[:100])
    state('partial100')
    assert buffer.add_chunk(data[100:])
    state('published_two_packets')
    assert buffer.get_chunks_exact(0, 1) == [data]
    chunks, last_consumed = buffer.get_optimized_client_data(0)
    assert chunks == [data] and last_consumed == 1
    records.append({'label': 'cursor_adapter', 'Source_last_consumed_before': 0,
                    'Source_last_consumed_after': last_consumed,
                    'Native_next_before': 1, 'Native_next_after': last_consumed + 1,
                    'full_bytes_verified': True})
    assert buffer.add_chunk(packet(3) + packet(4)[:17])
    state('pending_complete_and_partial')
    buffer.mark_discontinuity()
    assert buffer.index == 2 and len(buffer._partial_packet) == 0
    state('mark_flushes_complete_drops_partial')
    assert buffer.add_chunk(packet(5) + packet(6) + packet(7, 258))
    state('post_mark_stamping_first_public_chunk')
    buffer.mark_discontinuity()
    state('second_mark_flushes_previously_stamped_pending_PID')
    assert bytes.fromhex(records[-1]['chunks'][-1]['hex'])[5] == 128
    buffer.reset_buffer_position()
    state('reset_discards_local_leftovers')
    time.sleep(2.2)
    state('real_ttl_expired')
    assert records[-1]['chunks'] == []
    assert buffer.add_chunk(packet(8) + packet(9))
    state('fresh_after_expiry')
    buffer.stop()
    state('stop_preserves_redis_reads')
    assert buffer.add_chunk(packet(10)) is False
    records.append({'label': 'stopped_write_rejected', 'passed': True})
    # Independent actual Original construction/stop/reset cases, not fake flags.
    buffer = StreamBuffer(channel_id=channel, redis_client=client)
    state('new_writer_reads_existing_index')
    assert buffer.add_chunk(packet(36) + packet(37))
    state('new_writer_publishes_next_existing_index')
    buffer.stop()
    time.sleep(2.2)
    buffer = StreamBuffer(channel_id=channel, redis_client=client)
    state('new_writer_reads_persistent_index_after_ttl')
    assert buffer.add_chunk(packet(38) + packet(39))
    state('new_writer_continues_after_ttl')
    buffer.stop()
    for mode in ['initial-mark', 'partial-stop', 'complete-partial-stop', 'reset-partial', 'delayed-marker']:
        channel='core-source-'+uuid.uuid4().hex
        owned_channels.append(channel)
        buffer=StreamBuffer(channel_id=channel,redis_client=client)
        if mode=='initial-mark':
            assert buffer.mark_discontinuity()==1
            state('initial_mark_before_first_publication')
            assert buffer.add_chunk(packet(31)+packet(32))
            state('initial_mark_first_publication')
            buffer.stop()
        elif mode=='partial-stop':
            assert buffer.add_chunk(packet(33)[:100])
            buffer.stop()
            state('stop_retains_only_partial_packet_source_boundary')
        elif mode=='complete-partial-stop':
            assert buffer.add_chunk(packet(34)+packet(35)[:17])
            buffer.stop()
            state('stop_discards_complete_and_partial')
        elif mode=='delayed-marker':
            assert buffer.mark_discontinuity()==1
            time.sleep(1.2)
            assert buffer.add_chunk(packet(50)+packet(51))
            time.sleep(1.05)
            state('marker_expires_while_media_retained')
            records[-1]['reader_discontinuity']=1 in buffer.discontinuities_in_range(0,1)
            buffer.stop()
        else:
            assert buffer.add_chunk(packet(40)[:100])
            buffer.reset_buffer_position()
            state('reset_discards_only_partial_packet')
            buffer.stop()

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
                  'records': records, 'owned_channel_keys_removed': True,
                  'proxy_setting_value_restored': True,
                  'database_scope': 'Owned disposable database; a newly created setting may advance its sequence',
                  'source_buffer_sha256': hashlib.sha256(Path('/app/apps/proxy/live_proxy/input/buffer.py').read_bytes()).hexdigest(),
                  'credit': 'Original Dispatcharr authors and contributors retain application/algorithm credit; AGPL preserved.'}, separators=(',', ':')))
