"""Actual Source producer/consumer around controlled native terminal executors.
This does not infer native IndexService error classes from the supplied payloads.
Original Dispatcharr/Celery/Kombu authors retain application/framework credit.
"""
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

if os.environ.get('JOB_DISPOSABLE_FIXTURE') != 'playback-fixture-only' or os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Owned disposable fixture required')
database = os.environ.get('POSTGRES_DB', '')
if not database.startswith('rust_probe_backup_jobs_') or len(database) > 63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in database):
    raise RuntimeError('Copied database required')
sys.path[:0] = ['/app', '/job-tools']
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
import redis
from celery.result import AsyncResult
from dispatcharr.celery import app
from apps.epg.tasks import build_programme_index_task

broker = redis.Redis.from_url(os.environ['CELERY_BROKER_URL'])
backend = redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
assert broker.connection_pool.connection_kwargs['db'] == 8
assert backend.connection_pool.connection_kwargs['db'] == 9
queued = broker.lrange('celery', 0, 32)
if len(queued) > 32 or any(len(row) > 65536 or json.loads(row).get('headers', {}).get('task') != 'apps.plugins.tasks.refresh_plugin_repos' for row in queued):
    raise RuntimeError('Foreign startup queue refused')
broker.flushdb()
backend.flushdb()
cases = []
for filename in ['failure-corpus.json', 'initialization-corpus.json']:
    cases.extend(row for row in json.loads((Path('/evidence') / filename).read_text())['cases'] if row['result']['status'] == 'FAILURE')
assert len(cases) == 8
kind_by_type = {'IsADirectoryError': 'directory', 'ProgrammingError': 'programming',
                'ConnectionError': 'cache_connection', 'AuthenticationError': 'cache_authentication',
                'AttributeError': 'uninitialized_cache', 'OperationalError': 'child_publication'}
checks = []
report = {'passed': False, 'scope': 'Actual Source task producer/result consumer around controlled native immediate/ETA terminal executors; no IndexService classification claim',
          'checks': checks, 'native_execution_classification_verified': False,
          'traceback_policy': 'Truthful bounded native diagnostic; no executed Python frame/content equivalence claim'}


def check(name, condition):
    checks.append({'name': name, 'passed': bool(condition)})
    assert condition, name


def publish(action, task_id, source_id=1):
    parent = str(uuid.uuid4()) if action == 'scheduled' else None
    options = {'task_id': task_id}
    if parent:
        options.update(eta=dt.datetime.now(dt.UTC) + dt.timedelta(milliseconds=100), parent_id=parent, root_id=parent)
        build_programme_index_task.apply_async(args=[source_id], kwargs={'_defer_retry': 1}, **options)
    else:
        build_programme_index_task.apply_async(args=(source_id,), **options)
    return parent


def native(action, task_id, kind, arguments):
    request = {'action': action, 'task_id': task_id, 'kind': kind, 'arguments': arguments,
               'broker_url': os.environ['CELERY_BROKER_URL'], 'result_url': os.environ['CELERY_RESULT_BACKEND']}
    result = subprocess.run(['/evidence/native-index-terminal'], input=json.dumps(request), text=True,
                            capture_output=True, timeout=30, check=True)
    value = json.loads(result.stdout.splitlines()[-1])
    check('controlled-executor-invoked-' + action + '-' + kind, value['calls'] == 1 and value['task_id'] == task_id)
    return value


def actual_service(action, task_id, source_id):
    request = {'action': action, 'task_id': task_id, 'source_id': source_id,
               'broker_url': os.environ['CELERY_BROKER_URL'], 'result_url': os.environ['CELERY_RESULT_BACKEND'],
               'database_url': 'postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/' + database,
               'locks_url': 'redis://:playback-fixture-only@127.0.0.1:6379/0',
               'source_cwd': '/app', 'media_root': '/app/media'}
    result = subprocess.run(['/evidence/native-index-classifier', '--ignored', '--exact',
                             'index_classifier::tests::native_classified_service_peer', '--nocapture'],
                            input=json.dumps(request), text=True, capture_output=True, timeout=30)
    if result.returncode:
        Path('/evidence/classified-peer-failure-private.log').write_text(result.stdout + result.stderr)
    result.check_returncode()
    marker = 'NATIVE_INDEX_TERMINAL_JSON='
    rows = [line.split(marker, 1)[1] for line in result.stdout.splitlines() if marker in line]
    assert len(rows) == 1
    value = json.loads(rows[0])
    check('actual-service-drains-' + action + '-' + str(source_id), value['calls'] == 1
          and value['owner_released'] and value['physical_jobs'] == 0
          and value['file_lease_slots'] == 1 and value['worker_drained']
          and value['service_drained'] and value['shared_transport_drained'])
    return value


def subscription(task_id):
    key = app.backend.get_key_for_task(task_id)
    value = backend.pubsub()
    value.subscribe(key)
    value.get_message(timeout=1)
    return key, value


try:
    for action in ['immediate', 'scheduled']:
        for case in cases:
            task_id = str(uuid.uuid4())
            expected = case['result']['result']
            kind = kind_by_type[expected['exc_type']]
            key, events = subscription(task_id)
            parent = publish(action, task_id)
            value = native(action, task_id, kind, expected['exc_message'])
            metadata = json.loads(backend.get(key))
            check('recorded-terminal-' + action + '-' + case['name'], value['outcome'].startswith('Ok(Completed('))
            check('typed-original-exception-' + action + '-' + case['name'], metadata['status'] == 'FAILURE' and metadata['result'] == expected and metadata['children'] == [] and metadata.get('parent_id') == parent)
            decoded = AsyncResult(task_id, app=app)
            error = decoded.get(timeout=2, propagate=False)
            check('actual-source-reader-' + action + '-' + case['name'], decoded.ready() and decoded.failed() and type(error).__module__ == expected['exc_module'] and type(error).__name__ == expected['exc_type'] and list(error.args) == expected['exc_message'])
            check('native-diagnostic-and-date-' + action + '-' + case['name'], decoded.traceback.startswith('Native Dispatcharr index task diagnostic [controlled_executor]:') and dt.datetime.fromisoformat(metadata['date_done']).utcoffset() == dt.timedelta(0))
            check('expiry-and-event-' + action + '-' + case['name'], 3590 <= backend.ttl(key) <= 3600 and events.get_message(ignore_subscribe_messages=True, timeout=1) is not None)
            events.close()

        # A processed raised outcome must not overwrite earlier SUCCESS, and
        # execution still occurs for the same ID. No artificial FAILURE→SUCCESS
        # raw replacement is used with a previously observed terminal result.
        task_id = str(uuid.uuid4())
        app.backend.store_result(task_id, None, 'SUCCESS')
        key = app.backend.get_key_for_task(task_id)
        previous = backend.get(key)
        backend.expire(key, 300)
        ttl = backend.ttl(key)
        _, events = subscription(task_id)
        publish(action, task_id)
        value = native(action, task_id, 'directory', [21, 'Is a directory'])
        check('existing-success-unchanged-' + action, 'AlreadySucceeded(' in value['outcome'] and backend.get(key) == previous and 0 < backend.ttl(key) <= ttl and events.get_message(ignore_subscribe_messages=True, timeout=.1) is None)
        events.close()

        task_id = str(uuid.uuid4())
        _, events = subscription(task_id)
        publish(action, task_id)
        value = native(action, task_id, 'operational', [])
        check('operational-is-not-fabricated-failure-' + action, value['outcome'] == 'Err(Capacity)' and backend.get(app.backend.get_key_for_task(task_id)) is None and AsyncResult(task_id, app=app).state == 'PENDING' and events.get_message(ignore_subscribe_messages=True, timeout=.1) is None)
        events.close()

        task_id = str(uuid.uuid4())
        publish(action, task_id)
        value = native(action, task_id, 'normal', [])
        result = AsyncResult(task_id, app=app)
        check('legacy-null-success-' + action, value['outcome'].startswith('Ok(Completed(') and result.get(timeout=2) is None and result.successful() and result.traceback is None)
    if os.environ.get('INDEX_TERMINAL_CLASSIFIED') == '1':
        from apps.epg.models import EPGSource, EPGSourceIndex
        from django.db import connection
        root = Path('/app/media') / ('rust-index-terminal-' + uuid.uuid4().hex)
        root.mkdir()
        definitions = [('malformed-xml', b'<tv><programme channel="broken"><title>unterminated'),
                       ('invalid-encoding', b'<tv><programme channel="bad\xff&amp;one"></tv>'),
                       ('empty-file', b''), ('directory', None),
                       ('sql-write', b'<tv><programme channel="owned"><title>Owned</title></programme></tv>')]
        sources = {}
        for name, content in definitions:
            path = root if content is None else root / (name + '.xml')
            if content is not None:
                path.write_bytes(content)
            sources[name] = EPGSource.objects.create(name='rust-index-terminal-' + name,
                source_type='xmltv', is_active=False, file_path=str(path)).pk
        with connection.cursor() as cursor:
            cursor.execute('CREATE FUNCTION owned_index_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_id = '
                           + str(sources['sql-write']) + " THEN RAISE EXCEPTION 'owned-index-sql-write-failure'; END IF; RETURN NEW; END $$")
            cursor.execute('CREATE TRIGGER owned_index_failure BEFORE INSERT OR UPDATE ON epg_epgsourceindex FOR EACH ROW EXECUTE FUNCTION owned_index_failure()')
        observed = {case['name']: case for case in json.loads(Path('/evidence/failure-corpus.json').read_text())['cases']}
        observed_effects = {case['name']: case for case in json.loads(Path('/evidence/failure-effects.json').read_text())['cases']}
        for action in ['immediate', 'scheduled']:
            for name, _ in definitions:
                source_id = sources[name]
                EPGSourceIndex.objects.filter(source_id=source_id).delete()
                task_id = str(uuid.uuid4())
                parent = publish(action, task_id, source_id)
                value = actual_service(action, task_id, source_id)
                check('actual-service-recorded-' + action + '-' + name, value['outcome'].startswith('Ok(Completed('))
                result = AsyncResult(task_id, app=app)
                expected = observed[name]['result']
                data = json.loads(backend.get(app.backend.get_key_for_task(task_id)))
                check('actual-service-original-terminal-' + action + '-' + name,
                      data['status'] == expected['status'] and data['result'] == expected['result']
                      and data['children'] == [] and data.get('parent_id') == parent)
                if expected['status'] == 'FAILURE':
                    error = result.get(timeout=2, propagate=False)
                    check('actual-service-original-exception-' + action + '-' + name,
                          result.failed() and type(error).__module__ == expected['result']['exc_module']
                          and type(error).__name__ == expected['result']['exc_type']
                          and list(error.args) == expected['result']['exc_message']
                          and result.traceback.startswith('Native Dispatcharr index task diagnostic'))
                else:
                    check('actual-service-null-and-exact-index-' + action + '-' + name,
                          result.get(timeout=2) is None and result.successful()
                          and EPGSourceIndex.objects.get(source_id=source_id).data == observed_effects[name]['index'])
                check('actual-service-no-file-lock-' + action + '-' + name,
                      redis.Redis.from_url('redis://:playback-fixture-only@127.0.0.1:6379/0').get('task_lock_epg_source_file_' + str(source_id)) is None)
        report['native_execution_classification_verified'] = True
        report['native_execution_classification_scope'] = 'Only actual FileOpen directory errno21 and Write P0001/no-position with settled release; partial/lossy/empty remain written nullSUCCESS. Other causes and traceback content equivalence unaccepted.'
    check('all-inputs-acked-no-reservations', broker.llen('celery') == broker.hlen('unacked') == broker.zcard('unacked_index') == 0)
    report['passed'] = all(row['passed'] for row in checks)
finally:
    Path('/evidence/source-index-terminal.json').write_text(json.dumps(report, indent=2) + '\n')
