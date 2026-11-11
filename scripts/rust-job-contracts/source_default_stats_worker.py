"""Pinned default-task envelopes and actual fetch-stats worker effects.
Original Dispatcharr/Celery authors retain credit/licenses. Copied fixture only.
Other default families are inspected/published but never executed by this tool.
"""
import base64
import hashlib
import importlib
import inspect
import json
import os
from pathlib import Path
import signal
import subprocess
import time

import source_scheduler_publication as f
assert f.report['passed']
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from apps.proxy.tasks import fetch_channel_stats

report = {'passed': False, 'checks': list(f.report['checks']), 'defaults': [], 'cases': [],
          'scope': 'Pinned six default task envelopes/attributes/source inventory; actual fetch_channel_stats prefork worker, ChannelsRedis events and results only. Other families unexecuted.'}
worker = None
cache = f.redis.Redis(host='localhost', password='playback-fixture-only', db=0)
layer = get_channel_layer()
channel = 'specific.rust_owned_default_stats!receiver'
channel_key = layer.prefix + channel.split('!')[0] + '!'
group_key = layer._group_key('updates')
owned = ['live:channel:rust-owned-stats:metadata', 'live:channel:rust-owned-stats:clients',
         'live:channel:rust-owned-stats:input:buffer:index',
         'live:channel:rust-owned-stats:clients:owned-client',
         channel_key, group_key]


def check(name, value):
    report['checks'].append({'name': name, 'passed': bool(value)})
    if not value:
        raise AssertionError(name)


def rows(kind):
    path = Path('/evidence/periodic-worker-' + kind + '.jsonl')
    return [json.loads(v) for v in path.read_text().splitlines()] if path.exists() else []


def wait(fn):
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        if worker is not None and worker.poll() is not None:
            raise RuntimeError('Owned stats worker exited')
        time.sleep(.02)
    raise TimeoutError('Owned stats worker result deadline')


def cleanup_case():
    cache.delete(*owned)
    async_to_sync(layer.group_add)('updates', channel)


try:
    f.broker.flushdb()
    f.backend.flushdb()
    for key in ['dvr:recover_dispatch_lock', 'core:version_check_dispatch_lock']:
        cache.set(key, '1', ex=3600)
    f.app.loader.import_default_modules()
    defaults = [
        'celery.backend_cleanup', 'apps.proxy.tasks.fetch_channel_stats',
        'core.tasks.scan_and_process_files', 'apps.channels.tasks.maintain_recurring_recordings',
        'core.tasks.check_for_version_update', 'apps.m3u.tasks.check_account_expirations',
    ]
    for name in defaults:
        task = f.app.tasks[name]
        sent = task.apply_async(args=[], kwargs={})
        envelope = f.queue()[0]
        check('default-' + name + '-exact-task-and-noarg-body', envelope['headers']['task'] == name and json.loads(base64.b64decode(envelope['body']))[:2] == [[], {}])
        check('default-' + name + '-publication-no-execution', sent.state == 'PENDING' and f.backend.dbsize() == 0)
        function = task.run
        data = inspect.getsource(function).encode()
        filename = 'default-task-' + name.replace('.', '-') + '.py'
        Path('/evidence/' + filename).write_bytes(data)
        report['defaults'].append({'task': name, 'envelope': envelope, 'source_file': filename,
                                   'source_sha256': hashlib.sha256(data).hexdigest(),
                                   'attributes': {key: getattr(task, key) for key in ['acks_late', 'ignore_result', 'track_started', 'reject_on_worker_lost', 'max_retries', 'time_limit', 'soft_time_limit']}})
        f.broker.flushdb()
    for name in ['apps.proxy.tasks', 'apps.proxy.live_proxy.channel_status', 'core.tasks', 'apps.channels.tasks', 'apps.m3u.tasks', 'core.utils']:
        path = Path(inspect.getsourcefile(importlib.import_module(name)))
        data = path.read_bytes()
        Path('/evidence/framework-default-' + name.replace('.', '-') + '.py').write_bytes(data)
    env = dict(os.environ, C_FORCE_ROOT='true', PYTHONPATH='/job-tools:/app')
    with Path('/evidence/default-stats-worker-private.log').open('ab') as log:
        worker = subprocess.Popen(['/dispatcharrpy/bin/celery', '-A', 'periodic_worker_observer:app', 'worker', '--pool=prefork', '--concurrency=1', '--without-gossip', '--without-mingle', '--without-heartbeat', '--loglevel=WARNING', '--hostname=rust-default-stats@fixture'], env=env, stdout=log, stderr=log, start_new_session=True)
    for label in ['empty', 'static-live-client', 'ghost-client', 'wrongtype-metadata', 'invalid-buffer-index', 'group-send-wrongtype']:
        cleanup_case()
        if label in ['static-live-client', 'ghost-client', 'invalid-buffer-index']:
            cache.hset(owned[0], mapping={'state': 'active', 'url': 'http://owned.invalid/fixture', 'stream_profile': 'owned', 'owner': 'owned-fixture', 'channel_name': 'Owned fixture', 'total_bytes': '188', 'init_time': '0'})
            cache.sadd(owned[1], 'owned-client')
            if label != 'ghost-client':
                cache.hset(owned[3], mapping={'user_agent': 'Owned fixture', 'ip_address': '192.0.2.1', 'user_id': '0', 'output_format': 'mpegts', 'output_profile_id': '0'})
        if label == 'wrongtype-metadata':
            cache.set(owned[0], 'owned-wrongtype')
        if label == 'invalid-buffer-index':
            cache.set(owned[2], 'not-an-integer')
        if label == 'group-send-wrongtype':
            cache.delete(group_key)
            cache.set(group_key, 'owned-wrongtype')
        result = fetch_channel_stats.apply_async(args=[], kwargs={})
        post = wait(lambda: next((v for v in rows('postrun') if v['id'] == result.id), None))
        key = f.app.backend.get_key_for_task(result.id)
        metadata = json.loads(f.backend.get(key))
        check(label + '-normal-null-result', post['state'] == 'SUCCESS' and metadata['status'] == 'SUCCESS' and metadata['result'] is None and result.get(timeout=2) is None)
        check(label + '-result-shape-and-expiry', set(metadata) == {'status', 'result', 'traceback', 'children', 'date_done', 'task_id'} and metadata['traceback'] is None and metadata['children'] == [] and 3580 <= f.backend.ttl(key) <= 3600)
        values = cache.zrange(channel_key, 0, 8)
        if len(values) > 8 or any(len(v) > 1024 * 1024 for v in values):
            raise RuntimeError('Owned event byte/count bound')
        events = [layer.deserialize(v) for v in values]
        check(label + '-event-count', len(events) == (0 if label == 'group-send-wrongtype' else 1))
        stats = json.loads(events[0]['data']['stats']) if events else None
        if events:
            check(label + '-exact-envelope', events[0]['type'] == 'update' and events[0]['data']['success'] is True and events[0]['data']['type'] == 'channel_stats')
            check(label + '-channel-count', stats['count'] == (1 if label in ['static-live-client', 'ghost-client'] else 0))
        if label == 'static-live-client':
            check(label + '-actual-live-client-retained', cache.scard(owned[1]) == 1 and stats['channels'][0]['client_count'] == 1 and stats['channels'][0]['clients'][0]['client_id'] == 'owned-client')
        if label == 'ghost-client':
            check(label + '-actual-ghost-removal', cache.scard(owned[1]) == 0 and stats['channels'][0]['client_count'] == 0)
        report['cases'].append({'name': label, 'metadata': metadata, 'events': events, 'stats': stats, 'queue_count': f.broker.llen('celery'), 'unacked_count': f.broker.hlen('unacked')})
    check('only-owned-stats-tasks-executed', len(rows('prerun')) == 6 and len(rows('postrun')) == 6 and all(v['task'] == fetch_channel_stats.name for v in rows('prerun')))
    check('all-owned-tasks-earlyack-drained', f.broker.llen('celery') == 0 and f.broker.hlen('unacked') == 0)
    report['passed'] = True
finally:
    if worker is not None and worker.poll() is None:
        os.killpg(worker.pid, signal.SIGTERM)
        try:
            worker.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait(timeout=5)
            report['forced_cleanup'] = True
    cache.delete(*owned)
    f.broker.flushdb()
    f.backend.flushdb()
    report['queue_and_results_after_cleanup'] = [f.broker.dbsize(), f.backend.dbsize()]
    Path('/evidence/source-default-stats-worker-private.json').write_text(json.dumps(report, indent=2, default=str) + '\n')
