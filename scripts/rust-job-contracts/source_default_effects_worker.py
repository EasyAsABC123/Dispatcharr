"""Actual six default family baseline effects, with bounded owned dependencies.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
Stats is exercised first by its independent controller. Other task bodies remain
unchanged; version HTTP503 is an explicitly labeled loopback dependency control.
"""
import http.server
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import uuid

import source_default_stats_worker as stats
assert stats.report['passed']
f = stats.f
from apps.channels.models import Recording, RecurringRecordingRule
from apps.m3u.models import M3UAccountProfile
from core.models import SystemNotification
from celery.backends.redis import RedisBackend
import inspect

report = {'passed': False, 'checks': list(stats.report['checks']), 'cases': [],
          'scope': 'Actual default-family Original baseline worker effects: Redis backend cleanup, empty watch scan, disabled recurring rules, version ownedHTTP503, inactive expiration profiles plus stale notification deletion. Broader file/recording/version/profile branches remain open.'}
worker = None


def check(name, value):
    report['checks'].append({'name': name, 'passed': bool(value)})
    if not value:
        raise AssertionError(name)


class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        assert self.path == '/owned-version503'
        self.send_response(503)
        self.end_headers()
        self.wfile.write(b'owned version dependency unavailable')

    def log_message(self, *args):
        pass


server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Origin)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    f.broker.flushdb()
    f.backend.flushdb()
    f.EPGSource.objects.all().update(is_active=False)
    RecurringRecordingRule.objects.all().update(enabled=False)
    M3UAccountProfile.objects.all().update(is_active=False)
    f.PeriodicTask.objects.all().update(enabled=False)
    f.PeriodicTasks.update_changed()
    for name in ['epgs', 'm3us', 'logos']:
        path = Path('/data') / name
        path.mkdir(exist_ok=True)
        check('empty-owned-watch-directory-' + name, list(path.iterdir()) == [])
    recordings = list(Recording.objects.order_by('id').values())
    stale = SystemNotification.objects.create(notification_key='xc-exp-warning-rust-owned-default',
                notification_type='account_expiration', title='Owned stale fixture', message='Owned stale fixture', is_active=True)
    sentinel = str(uuid.uuid4())
    f.app.backend.store_result(sentinel, None, 'SUCCESS')
    sentinel_key = f.app.backend.get_key_for_task(sentinel)
    sentinel_bytes = f.backend.get(sentinel_key)
    Path('/evidence/default-redis-backend-cleanup-source.py').write_text(inspect.getsource(RedisBackend.cleanup))
    env = dict(os.environ, C_FORCE_ROOT='true', PYTHONPATH='/job-tools:/app',
               DEFAULT_TASK_HTTP_ORIGIN='http://127.0.0.1:' + str(server.server_port))
    with Path('/evidence/default-effects-worker-private.log').open('ab') as log:
        worker = subprocess.Popen(['/dispatcharrpy/bin/celery', '-A', 'default_tasks_worker_observer:app', 'worker', '--pool=prefork', '--concurrency=1', '--without-gossip', '--without-mingle', '--without-heartbeat', '--loglevel=WARNING', '--hostname=rust-default-effects@fixture'], env=env, stdout=log, stderr=log, start_new_session=True)
    stats.worker = worker
    tasks = [('backend-cleanup', 'celery.backend_cleanup', None),
             ('scan-empty', 'core.tasks.scan_and_process_files', None),
             ('recurring-disabled', 'apps.channels.tasks.maintain_recurring_recordings', 0),
             ('version-http503', 'core.tasks.check_for_version_update', None),
             ('expiration-stale', 'apps.m3u.tasks.check_account_expirations', None)]
    for label, name, expected in tasks:
        stats.cleanup_case()
        result = f.app.tasks[name].apply_async(args=[], kwargs={})
        post = stats.wait(lambda: next((v for v in stats.rows('postrun') if v['id'] == result.id), None))
        metadata = json.loads(f.backend.get(f.app.backend.get_key_for_task(result.id)))
        check(label + '-actual-normal-result', post['state'] == 'SUCCESS' and metadata['status'] == 'SUCCESS' and metadata['result'] == expected and type(metadata['result']) is type(expected) and result.get(timeout=2) == expected)
        check(label + '-result-metadata-and-expiry', metadata['traceback'] is None and metadata['children'] == [] and 3580 <= f.backend.ttl(f.app.backend.get_key_for_task(result.id)) <= 3600)
        events = [stats.layer.deserialize(v) for v in stats.cache.zrange(stats.channel_key, 0, 8)]
        if label != 'expiration-stale':
            check(label + '-no-events', events == [])
        if label == 'backend-cleanup':
            check(label + '-Redis-result-backend-noop', f.backend.get(sentinel_key) == sentinel_bytes and f.backend.ttl(sentinel_key) > 3500)
        if label == 'version-http503':
            rows = Path('/evidence/default-version-http-route-private.jsonl').read_text().splitlines()
            check(label + '-exact-one-ownedHTTP-attempt', len(rows) == 1)
        if label == 'expiration-stale':
            check(label + '-actual-stale-row-deleted', not SystemNotification.objects.filter(pk=stale.pk).exists())
            check(label + '-actual-dismissal-event', any(v['type'] == 'update' and v['data'].get('notification_key') == stale.notification_key for v in events))
        check(label + '-no-descendant-or-reservation', f.broker.llen('celery') == 0 and f.broker.hlen('unacked') == 0)
        report['cases'].append({'name': label, 'task': name, 'metadata': metadata, 'events': events})
    check('no-recording-effects', list(Recording.objects.order_by('id').values()) == recordings)
    check('exact-eleven-owned-task-executions', len(stats.rows('postrun')) == 11 and len(stats.rows('prerun')) == 11)
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
    server.shutdown()
    server.server_close()
    stats.cache.delete(*stats.owned)
    f.broker.flushdb()
    f.backend.flushdb()
    report['queue_and_results_after_cleanup'] = [f.broker.dbsize(), f.backend.dbsize()]
    Path('/evidence/source-default-effects-worker-private.json').write_text(json.dumps(report, indent=2, default=str) + '\n')
