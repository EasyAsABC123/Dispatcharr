"""Actual Original/native fetch-stats tasks and exact raw event string parity.
Original Dispatcharr/Celery authors retain application/protocol credit/licenses.
Native dependency helper invokes existing StreamEngine/ClientStore/RealtimeBus,
not Python business callbacks. All data belongs to the copied Compose fixture.
"""
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import uuid
import source_default_stats_worker as reference
assert reference.report['passed']
f = reference.f
report = {'passed': False, 'checks': list(reference.report['checks']), 'cases': [],
          'scope': 'Fixture-only native publication→Original worker and Original publication→native worker using actual existing native scanner/ChannelsRedis bus→Original AsyncResult, six observed stats cases. Other default families unported.'}
worker = None


def check(name, value):
    report['checks'].append({'name': name, 'passed': bool(value)})
    if not value:
        raise AssertionError(name)


def native(action, ident):
    result = subprocess.run(['/evidence/native-stats-worker', '--exact', 'stats_peer::native_stats_peer', '--ignored', '--nocapture'],
                input=json.dumps({'action': action, 'task_id': ident,
                    'broker_url': os.environ['CELERY_BROKER_URL'], 'result_url': os.environ['CELERY_RESULT_BACKEND']}),
                text=True, capture_output=True, timeout=40)
    with Path('/evidence/native-default-stats-private.log').open('a') as output:
        output.write(result.stdout + result.stderr)
    result.check_returncode()
    line = next(v.split('=', 1)[1] for v in result.stdout.splitlines() if v.startswith('NATIVE_STATS_TASK_JSON='))
    return json.loads(line)


try:
    f.broker.flushdb()
    f.backend.flushdb()
    reference.cleanup_case()
    ident = str(uuid.uuid4())
    published = native('publish', ident)
    check('native-publication-owned-id-and-drain', published['result']['published'] == ident and published['owner_released'])
    envelope = f.queue()[0]
    check('native-publication-actual-stats-family', envelope['headers']['task'] == reference.fetch_channel_stats.name)
    env = dict(os.environ, C_FORCE_ROOT='true', PYTHONPATH='/job-tools:/app')
    with Path('/evidence/native-published-original-stats-worker-private.log').open('ab') as log:
        worker = subprocess.Popen(['/dispatcharrpy/bin/celery', '-A', 'periodic_worker_observer:app', 'worker', '--pool=prefork', '--concurrency=1', '--without-gossip', '--without-mingle', '--without-heartbeat', '--loglevel=WARNING', '--hostname=rust-native-pub-stats@fixture'], env=env, stdout=log, stderr=log, start_new_session=True)
    reference.worker = worker
    post = reference.wait(lambda: next((v for v in reference.rows('postrun') if v['id'] == ident), None))
    check('native-publication-actual-Original-worker-null', post['state'] == 'SUCCESS' and f.app.AsyncResult(ident).get(timeout=2) is None)
    events = [reference.layer.deserialize(v) for v in reference.cache.zrange(reference.channel_key, 0, 8)]
    check('native-publication-original-worker-exact-empty-event', events == reference.report['cases'][0]['events'])
    os.killpg(worker.pid, signal.SIGTERM)
    worker.wait(timeout=15)
    check('native-publication-original-worker-graceful', worker.returncode == 0)
    worker = None
    for expected in reference.report['cases']:
        label = expected['name']
        reference.cleanup_case()
        if label in ['static-live-client', 'ghost-client', 'invalid-buffer-index']:
            reference.cache.hset(reference.owned[0], mapping={'state': 'active', 'url': 'http://owned.invalid/fixture', 'stream_profile': 'owned', 'owner': 'owned-fixture', 'channel_name': 'Owned fixture', 'total_bytes': '188', 'init_time': '0'})
            reference.cache.sadd(reference.owned[1], 'owned-client')
            if label != 'ghost-client':
                reference.cache.hset(reference.owned[3], mapping={'user_agent': 'Owned fixture', 'ip_address': '192.0.2.1', 'user_id': '0', 'output_format': 'mpegts', 'output_profile_id': '0'})
        if label == 'wrongtype-metadata':
            reference.cache.set(reference.owned[0], 'owned-wrongtype')
        if label == 'invalid-buffer-index':
            reference.cache.set(reference.owned[2], 'not-an-integer')
        if label == 'group-send-wrongtype':
            reference.cache.delete(reference.group_key)
            reference.cache.set(reference.group_key, 'owned-wrongtype')
        result = reference.fetch_channel_stats.apply_async(args=[], kwargs={})
        actual = native('run', result.id)
        check(label + '-native-worker-completed-own-task', actual['result']['outcome'] == 'Ok(Completed(' + result.id + '))' and actual['owner_released'])
        metadata = json.loads(f.backend.get(f.app.backend.get_key_for_task(result.id)))
        check(label + '-actual-Original-AsyncResult-normalnull', result.get(timeout=2) is None and metadata['status'] == 'SUCCESS' and metadata['result'] is None)
        check(label + '-exact-result-fields-and-TTL', set(metadata) == set(expected['metadata']) and metadata['traceback'] is None and metadata['children'] == [] and 3580 <= f.backend.ttl(f.app.backend.get_key_for_task(result.id)) <= 3600)
        events = [reference.layer.deserialize(v) for v in reference.cache.zrange(reference.channel_key, 0, 8)]
        report['active_comparison'] = {'name': label, 'expected_events': expected['events'], 'actual_events': events}
        check(label + '-exact-raw-string-and-full-event', events == expected['events'])
        check(label + '-no-queue-or-reservation', f.broker.llen('celery') == 0 and f.broker.hlen('unacked') == 0)
        report['cases'].append({'name': label, 'events': events, 'metadata': metadata, 'audit': actual})
    if os.environ.get('SCHEDULER_PHYSICAL_GATES') == '1':
        report['physical_tests'] = {}
        for binary, selector, expected in [('native-stats-worker', 'stats_resources::', 3), ('native-stats-clients', 'statistics_catalog::tests::', 3)]:
            result = subprocess.run(['/evidence/' + binary, selector, '--ignored', '--nocapture', '--test-threads=1'], env=dict(os.environ, REDIS_TEST_URL='redis://:playback-fixture-only@localhost:6379/0'), capture_output=True, text=True, timeout=90)
            Path('/evidence/' + binary + '-physical.log').write_text(result.stdout + result.stderr)
            result.check_returncode()
            matched = re.search(r'test result: ok\. (\d+) passed; 0 failed', result.stdout)
            check(binary + '-actual-physical-tests', matched and int(matched.group(1)) == expected)
            report['physical_tests'][binary] = expected
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
    reference.cache.delete(*reference.owned)
    f.broker.flushdb()
    f.backend.flushdb()
    report['queue_and_results_after_cleanup'] = [f.broker.dbsize(), f.backend.dbsize()]
    Path('/evidence/source-native-default-stats-worker-private.json').write_text(json.dumps(report, indent=2, default=str) + '\n')
