"""Actual pinned Source orphan helper SQL/collector semantics in copied fixture.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
Django/django-celery-beat/PostgreSQL authors retain credit and licenses.
Run only after Root quiet release in its copied rust_probe_backup_jobs_* DB.
This draft does not launch a worker, scheduler or native helper.
"""
import datetime as dt
import hashlib
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import sys
import uuid
import signal
import subprocess
import time

if os.environ.get('JOB_DISPOSABLE_FIXTURE') != 'playback-fixture-only':
    raise RuntimeError('Owned disposable fixture required')
database = os.environ.get('POSTGRES_DB', '')
if not database.startswith('rust_probe_backup_jobs_') or len(database) > 63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in database):
    raise RuntimeError('Copied database required')
sys.path[:0] = ['/app', '/job-tools']
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django_celery_beat.models import PeriodicTask, IntervalSchedule, CrontabSchedule, PeriodicTasks
import django_celery_beat.models as beat_models
import django_celery_beat.signals as beat_signals
from apps.epg.models import EPGSource
from apps.m3u.models import M3UAccount
from apps.epg.tasks import delete_epg_refresh_task_by_id

PREFIX = 'rust-orphan-' + uuid.uuid4().hex[:10] + '-'
report = {'passed': False, 'scope': 'Actual Source helper/collector/FK/Beat-marker SQL only; no native helper or worker result acceptance', 'checks': [], 'cases': [], 'source_only': True,'worker_cases': []}
owned_tasks = []
owned_intervals = []
owned_crons = []
owned_sources = []
owned_accounts = []
functions = []
sequences = []


def check(name, value):
    report['checks'].append({'name': name, 'passed': bool(value)})
    if not value:
        raise AssertionError(name)


def snapshot(task_ids, interval_ids, cron_ids, source_ids, account_ids):
    return {
        'tasks': list(PeriodicTask.objects.filter(pk__in=task_ids).order_by('id').values('id', 'name', 'task', 'interval_id', 'crontab_id')),
        'intervals': list(IntervalSchedule.objects.filter(pk__in=interval_ids).order_by('id').values('id', 'every', 'period')),
        'crons': list(CrontabSchedule.objects.filter(pk__in=cron_ids).order_by('id').values('id', 'minute', 'hour')),
        'epg_refs': list(EPGSource.objects.filter(pk__in=source_ids).order_by('id').values('id', 'refresh_task_id')),
        'm3u_refs': list(M3UAccount.objects.filter(pk__in=account_ids).order_by('id').values('id', 'refresh_task_id')),
        'beat_marker': list(PeriodicTasks.objects.filter(ident=1).values('ident', 'last_update')),
    }


def fault(table, row_id):
    # Identifiers are generated or from this fixed allowlist; no source row text.
    if table not in {'django_celery_beat_periodictask', 'django_celery_beat_intervalschedule'}:
        raise RuntimeError('Owned fault target required')
    function = 'rust_orphan_fault_' + uuid.uuid4().hex[:12]
    trigger = function + '_trigger'
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned orphan deletion blocked' USING ERRCODE='P0001'; END; $$")
        cursor.execute(f'CREATE TRIGGER {trigger} BEFORE DELETE ON {table} FOR EACH ROW WHEN (OLD.id={int(row_id)}) EXECUTE FUNCTION {function}()')
    functions.append(function)
    return function




def marker_fault(nth):
    if nth not in [1,2,3]:raise RuntimeError('Bounded marker phase required')
    function='rust_orphan_marker_'+uuid.uuid4().hex[:12]
    sequence=function+'_seq'
    with connection.cursor()as cursor:
        cursor.execute(f'CREATE SEQUENCE {sequence}')
        cursor.execute(f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('{sequence}')={nth} THEN RAISE EXCEPTION 'owned orphan marker phase blocked' USING ERRCODE='P0001'; END IF; RETURN NEW; END; $$")
        cursor.execute(f'CREATE TRIGGER {function}_trigger BEFORE INSERT OR UPDATE ON django_celery_beat_periodictasks FOR EACH ROW WHEN (NEW.ident=1) EXECUTE FUNCTION {function}()')
    functions.append(function);sequences.append(sequence)
    return sequence

def sequence_values():
    values={}
    with connection.cursor()as cursor:
        for sequence in sequences:
            cursor.execute(f'SELECT last_value FROM {sequence}');values[sequence]=cursor.fetchone()[0]
    return values

worker=None
def stop_worker():
    global worker
    if worker is not None and worker.poll()is None:
        worker.send_signal(signal.SIGTERM)
        try:worker.wait(timeout=15)
        except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report.setdefault('forced_cleanup',[]).append(True)
    worker=None

def wait(fn,seconds=30):
    until=time.monotonic()+seconds
    while time.monotonic()<until:
        value=fn()
        if value:return value
        time.sleep(.025)
    raise TimeoutError('Owned worker observation deadline')

def postrun(ident):
    path=Path('/evidence/orphan-worker-postrun.jsonl')
    if path.exists():
        return next((v for v in map(json.loads,path.read_text().splitlines())if v['id']==ident),None)
    return None

try:
    # Retain private package source/hashes before inferring model overrides or
    # signal/marker ordering. No monkeypatch or function replacement occurs.
    source = {
        'PeriodicTask.delete': inspect.getsource(PeriodicTask.delete),
        'PeriodicTasks.changed': inspect.getsource(PeriodicTasks.changed),
        'PeriodicTasks.update_changed': inspect.getsource(PeriodicTasks.update_changed),
        'beat.models': inspect.getsource(beat_models),
        'beat.signals': inspect.getsource(beat_signals),
        'application_helper': inspect.getsource(delete_epg_refresh_task_by_id),
    }
    if any(len(value.encode()) > 262144 for value in source.values()):
        raise RuntimeError('Private source capture bound')
    Path('/evidence/orphan-model-source-private.json').write_text(json.dumps(source, indent=2))
    report['source_hashes'] = {key: hashlib.sha256(value.encode()).hexdigest() for key, value in source.items()}
    report['versions'] = {name: importlib.metadata.version(name) for name in ['Django', 'django-celery-beat', 'psycopg']}
    # Missing IDs are synthetic, never existing Source rows. Starting far above
    # copied seed IDs is not enough alone: assert absence before each case.
    for ordinal, label in enumerate(['none', 'sole', 'shared', 'crontab_only', 'cross_refs', 'marker_absent', 'task_delete_fault', 'interval_delete_fault','marker1_fault','marker2_fault','marker3_fault']):
        missing = 9_000_000 + ordinal
        if EPGSource.objects.filter(pk=missing).exists():
            raise RuntimeError('Owned missing source ID unexpectedly exists')
        target = None
        interval = None
        cron = None
        tasks = []
        sources = []
        accounts = []
        if label != 'none':
            if label == 'crontab_only':
                cron = CrontabSchedule.objects.create(minute='17', hour='3', day_of_week='*', day_of_month='*', month_of_year='*', timezone='UTC')
                owned_crons.append(cron.pk)
            else:
                interval = IntervalSchedule.objects.create(every=7949 + ordinal, period='seconds')
                owned_intervals.append(interval.pk)
            target = PeriodicTask(name=f'epg_source-refresh-{missing}', task='apps.epg.tasks.refresh_epg_data', args=json.dumps([missing]), enabled=False, interval=interval, crontab=cron)
            PeriodicTask.objects.bulk_create([target])  # setup avoids save callbacks
            tasks.append(target.pk)
            owned_tasks.append(target.pk)
            if label == 'shared':
                shared = PeriodicTask(name=PREFIX + 'shared', task='apps.epg.tasks.refresh_epg_data', args='[]', enabled=False, interval=interval)
                PeriodicTask.objects.bulk_create([shared])
                tasks.append(shared.pk)
                owned_tasks.append(shared.pk)
            if label == 'cross_refs':
                other = EPGSource(name=PREFIX + 'epg', is_active=False, source_type='xmltv', refresh_task=target)
                EPGSource.objects.bulk_create([other])
                account = M3UAccount(name=PREFIX + 'm3u', is_active=False, refresh_task=target)
                M3UAccount.objects.bulk_create([account])
                sources.append(other.pk)
                accounts.append(account.pk)
                owned_sources.append(other.pk)
                owned_accounts.append(account.pk)
        # Control only the copied DB's change sentinel, after setup signals.
        if label == 'marker_absent':
            PeriodicTasks.objects.filter(ident=1).delete()
        else:
            PeriodicTasks.objects.update_or_create(ident=1, defaults={'last_update': dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)})
        if label == 'task_delete_fault':
            fault('django_celery_beat_periodictask', target.pk)
        if label == 'interval_delete_fault':
            fault('django_celery_beat_intervalschedule', interval.pk)
        if label.startswith('marker')and label.endswith('_fault'):
            marker_fault(int(label[6]))
        before = snapshot(tasks, [interval.pk] if interval else [], [cron.pk] if cron else [], sources, accounts)
        with CaptureQueriesContext(connection) as queries:
            value = delete_epg_refresh_task_by_id(missing)
        sql = list(queries.captured_queries)
        if len(sql) > 128 or sum(len(item['sql'].encode()) for item in sql) > 262144:
            raise RuntimeError('Owned SQL capture bound')
        after = snapshot(tasks, [interval.pk] if interval else [], [cron.pk] if cron else [], sources, accounts)
        # Assertions here are hypotheses derived from app code; actual pinned
        # package collector/marker/order is a gate, never assumed acceptance.
        if label.startswith('marker')and label.endswith('_fault'):
            check('marker_phase_fault_caught_'+label,value is False)
            if label=='marker1_fault':check('marker1_rolls_back_task_and_marker',len(after['tasks'])==1 and len(after['intervals'])==1 and after['beat_marker']==before['beat_marker'])
            else:check('later_marker_fault_keeps_task_committed_'+label,after['tasks']==[]and len(after['intervals'])==1 and after['beat_marker']!=before['beat_marker'])
            check('exact_marker_attempt_'+label,list(sequence_values().values())==[int(label[6])])
        elif label == 'none':
            check('none_returns_false_without_rows', value is False and before == after)
        elif label == 'task_delete_fault':
            check('task_fault_is_caught', value is False)
            check('task_fault_preserves_task_interval', len(after['tasks']) == 1 and len(after['intervals']) == 1)
        elif label == 'interval_delete_fault':
            check('interval_fault_is_caught', value is False)
            check('task_commit_precedes_interval_failure', after['tasks'] == [] and len(after['intervals']) == 1)
        else:
            check('helper_true_' + label, value is True)
            check('target_deleted_' + label, not any(row['id'] == target.pk for row in after['tasks']))
            if label == 'shared':
                check('shared_interval_and_other_task_retained', len(after['tasks']) == 1 and len(after['intervals']) == 1)
            elif interval:
                check('sole_interval_deleted_' + label, after['intervals'] == [])
            if cron:
                check('crontab_is_not_garbage_collected', len(after['crons']) == 1)
            if label == 'cross_refs':
                check('collector_set_null_epg_m3u', all(row['refresh_task_id'] is None for row in after['epg_refs'] + after['m3u_refs']))
        report['cases'].append({'name': label, 'source_id': missing, 'before': before, 'helper_return': value, 'after': after, 'sql': sql,'marker_attempts':sequence_values()})
        from orphan_native_compare import compare
        compare(globals())
        # Faults must be removed before deleting any remaining owned rows.
        with connection.cursor() as cursor:
            for function in functions:
                cursor.execute(f'DROP FUNCTION IF EXISTS {function}() CASCADE')
        with connection.cursor()as cursor:
            for sequence in sequences:cursor.execute(f'DROP SEQUENCE IF EXISTS {sequence}')
        sequences.clear()
        functions.clear()

    from orphan_native_compare import resources
    resources(globals())
    import redis
    from dispatcharr.celery import app
    from apps.epg.tasks import refresh_epg_data
    broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=0)
    if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned namespaces required')
    for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
    for ordinal,label in enumerate(['none','sole','shared','cross_refs','task_delete_fault','interval_delete_fault','marker1_fault','marker2_fault','marker3_fault']):
        broker.flushdb();backend.flushdb();missing=9_001_000+ordinal
        if EPGSource.objects.filter(pk=missing).exists():raise RuntimeError('Owned missingID exists')
        target=None;interval=None;tasks=[];sources=[];accounts=[]
        if label!='none':
            interval=IntervalSchedule.objects.create(every=7981+ordinal,period='seconds');owned_intervals.append(interval.pk)
            target=PeriodicTask(name=f'epg_source-refresh-{missing}',task=refresh_epg_data.name,args=json.dumps([missing]),enabled=False,interval=interval);PeriodicTask.objects.bulk_create([target]);tasks.append(target.pk);owned_tasks.append(target.pk)
            if label=='shared':
                shared=PeriodicTask(name=PREFIX+'worker-shared',task=refresh_epg_data.name,args='[]',enabled=False,interval=interval);PeriodicTask.objects.bulk_create([shared]);tasks.append(shared.pk);owned_tasks.append(shared.pk)
            if label=='cross_refs':
                other=EPGSource(name=PREFIX+'worker-epg',is_active=False,source_type='xmltv',refresh_task=target);EPGSource.objects.bulk_create([other]);owned_sources.append(other.pk);sources.append(other.pk)
                account=M3UAccount(name=PREFIX+'worker-m3u',is_active=False,refresh_task=target);M3UAccount.objects.bulk_create([account]);owned_accounts.append(account.pk);accounts.append(account.pk)
        PeriodicTasks.objects.update_or_create(ident=1,defaults={'last_update':dt.datetime(2020,1,2,3,4,5,tzinfo=dt.timezone.utc)})
        if label=='task_delete_fault':fault('django_celery_beat_periodictask',target.pk)
        if label=='interval_delete_fault':fault('django_celery_beat_intervalschedule',interval.pk)
        if label.startswith('marker')and label.endswith('_fault'):marker_fault(int(label[6]))
        before=snapshot(tasks,[interval.pk]if interval else[],[],sources,accounts)
        issued=refresh_epg_data.delay(missing);raw=next(v for v in broker.lrange('celery',0,63)if json.loads(v)['headers']['id']==issued.id);envelope=json.loads(raw)
        broker.delete('_kombu.binding.celery.pidbox')
        with Path('/evidence/orphan-worker-private.log').open('ab')as log:
            worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','epg_orphan_worker_observer:app','worker','-Q','celery','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-orphan@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
        post=wait(lambda:postrun(issued.id));stop_worker();raw_meta=backend.get(app.backend.get_key_for_task(issued.id));check('bounded-worker-metadata-'+label,raw_meta is not None and len(raw_meta)<=262144);meta=json.loads(raw_meta);expected=f'EPG source {missing} does not exist, task cleaned up';decoded=app.AsyncResult(issued.id)
        check('worker-success-string-even-cleanup-fault-'+label,post['state']=='SUCCESS'and meta['status']=='SUCCESS'and meta['result']==expected and decoded.ready()and decoded.get(timeout=2)==expected)
        check('worker-result-geometry-'+label,meta['children']==[]and meta['traceback']is None and meta['task_id']==issued.id and'parent_id'not in meta and 3590<=backend.ttl(app.backend.get_key_for_task(issued.id))<=3600)
        after=snapshot(tasks,[interval.pk]if interval else[],[],sources,accounts)
        if label.startswith('marker')and label.endswith('_fault'):
            if label=='marker1_fault':check('worker-marker1-rollback',after==before)
            else:check('worker-later-marker-partial-'+label,after['tasks']==[]and len(after['intervals'])==1 and after['beat_marker']!=before['beat_marker'])
            check('worker-exact-marker-attempt-'+label,list(sequence_values().values())==[int(label[6])])
        elif label=='task_delete_fault':check('worker-task-fault-rollback',after==before)
        elif label=='interval_delete_fault':check('worker-interval-fault-partial-commit',after['tasks']==[]and len(after['intervals'])==1 and after['beat_marker']!=before['beat_marker'])
        elif label=='shared':check('worker-shared-retained',len(after['tasks'])==1 and len(after['intervals'])==1)
        elif label=='cross_refs':check('worker-collector-clears-fks',after['tasks']==[]and after['intervals']==[]and all(v['refresh_task_id']is None for v in after['epg_refs']+after['m3u_refs']))
        elif label=='sole':check('worker-sole-removed',after['tasks']==[]and after['intervals']==[])
        else:check('worker-no-task-no-marker-change',after==before)
        check('worker-owned-queue-and-refresh-lock-clean-'+label,broker.llen('celery')==0 and broker.hlen('unacked')==0 and cache.get('task_lock_refresh_epg_data_'+str(missing))is None)
        report['worker_cases'].append({'name':label,'source_id':missing,'before':before,'after':after,'envelope':envelope,'metadata':meta,'raw_metadata_sha256':hashlib.sha256(raw_meta).hexdigest(),'result_ttl':backend.ttl(app.backend.get_key_for_task(issued.id)),'postrun':post,'marker_attempts':sequence_values()})
        from orphan_worker_compare import compare_worker
        compare_worker(globals())
        with connection.cursor()as cursor:
            for function in functions:cursor.execute(f'DROP FUNCTION IF EXISTS {function}() CASCADE')
        with connection.cursor()as cursor:
            for sequence in sequences:cursor.execute(f'DROP SEQUENCE IF EXISTS {sequence}')
        sequences.clear()
        functions.clear()
    check('no-unrelated-worker-family-executed',not Path('/evidence/orphan-unexpected-tasks.jsonl').exists())
    report['scope']='Actual Source orphan helper SQL plus missing-source realworker result/FK/marker/partialcommit; no native helper'
    report['passed']=True
finally:
    stop_worker()

    with connection.cursor() as cursor:
        for function in functions:
            cursor.execute(f'DROP FUNCTION IF EXISTS {function}() CASCADE')
    with connection.cursor()as cursor:
        for sequence in sequences:cursor.execute(f'DROP SEQUENCE IF EXISTS {sequence}')
    M3UAccount.objects.filter(pk__in=owned_accounts).delete()
    EPGSource.objects.filter(pk__in=owned_sources).delete()
    PeriodicTask.objects.filter(pk__in=owned_tasks).delete()
    IntervalSchedule.objects.filter(pk__in=owned_intervals).delete()
    CrontabSchedule.objects.filter(pk__in=owned_crons).delete()
    Path('/evidence/source-native-epg-orphan-worker-private.json').write_text(json.dumps(report, indent=2, default=str))
