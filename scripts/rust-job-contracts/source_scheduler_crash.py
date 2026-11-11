"""Actual Original scheduler hard-loss windows on owned copied data.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
The only substituted instance method is a timing barrier around real _do_sync;
publication, database sync, task code and scheduler tick stay Original.
"""
import json, os, signal, subprocess, sys, time
from pathlib import Path

if '--scheduler-child' in sys.argv:
    if os.environ.get('JOB_DISPOSABLE_FIXTURE') != 'playback-fixture-only':
        raise RuntimeError('Owned fixture required')
    database = os.environ.get('POSTGRES_DB', '')
    assert database.startswith('rust_probe_backup_jobs_') and len(database) <= 63
    sys.path.insert(0, '/app')
    sys.argv[:] = [sys.argv[0], 'shell']
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
    import django
    django.setup()
    from django_celery_beat.schedulers import DatabaseScheduler
    from dispatcharr.celery import app
    scheduler = DatabaseScheduler(app=app, lazy=False, max_interval=5)
    mode = os.environ['SCHEDULER_CRASH_MODE']
    if mode != 'restart':
        original = scheduler._do_sync
        def barrier():
            if mode == 'after-sync':
                original()
            Path(os.environ['SCHEDULER_CRASH_GATE']).write_text(mode)
            while True:
                time.sleep(.05)
        scheduler._do_sync = barrier
    delay = scheduler.tick()
    scheduler.sync()
    print('ORIGINAL_SCHEDULER_CRASH_JSON=' + json.dumps({'delay': delay}), flush=True)
    scheduler.close()
    raise SystemExit(0)

import datetime as dt
import source_scheduler_publication as reference
assert reference.report['passed']
f = reference
report = {'passed': False, 'checks': list(reference.report['checks']), 'cases': [],
          'scope': 'Actual Original process loss immediately before/after real first scheduler SQL sync, real publication and restarted tick; controlled timing barrier only, no worker/native timer acceptance'}
process = None
sources = []

def check(name, value):
    report['checks'].append({'name': name, 'passed': bool(value)})
    if not value:
        raise AssertionError(name)

def start(mode, gate, log):
    env = dict(os.environ, SCHEDULER_CRASH_MODE=mode, SCHEDULER_CRASH_GATE=str(gate))
    return subprocess.Popen(['/dispatcharrpy/bin/python', '/job-tools/source_scheduler_crash.py', '--scheduler-child'], env=env,
                            stdout=subprocess.PIPE, stderr=log, start_new_session=True)

def state(row):
    row.refresh_from_db()
    return {'enabled': row.enabled, 'count': row.total_run_count, 'last_run_at': row.last_run_at.isoformat() if row.last_run_at else None}

try:
    f.broker.flushdb(); f.backend.flushdb()
    source = f.EPGSource(name='rust-owned-scheduler-crash-inactive', source_type='xmltv', is_active=False, custom_properties={})
    f.EPGSource.objects.bulk_create([source]); sources.append(source.pk)
    interval = f.IntervalSchedule.objects.create(every=3600, period=f.IntervalSchedule.SECONDS)
    for mode in ['before-sync', 'after-sync']:
        f.broker.flushdb(); f.backend.flushdb()
        f.PeriodicTask.objects.all().update(enabled=False); f.PeriodicTasks.update_changed()
        row = f.create('rust-owned-crash-' + mode, f.refresh_epg_data.name, [source.pk], {}, interval)
        before = state(row)
        gate = Path('/evidence/scheduler-' + mode + '.gate')
        assert not gate.exists()
        with Path('/evidence/scheduler-crash-private.log').open('ab') as log:
            process = start(mode, gate, log)
            deadline = time.monotonic() + 20
            while not gate.exists():
                if process.poll() is not None:
                    raise RuntimeError('Original scheduler exited before barrier')
                if time.monotonic() > deadline:
                    raise TimeoutError('Original scheduler timing barrier')
                time.sleep(.02)
            queued = f.queue()
            stopped = state(row)
            check(mode + '-publication-before-hardloss', len(queued) == 1)
            check(mode + '-exact-sync-boundary', stopped['count'] == (1 if mode == 'after-sync' else 0))
            os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=5); process = None
            check(mode + '-hardloss-did-not-sync', state(row) == stopped)
            process = start('restart', gate, log)
            stdout, _ = process.communicate(timeout=20)
            check(mode + '-restarted-scheduler-exit0', process.returncode == 0); process = None
            marker = b'ORIGINAL_SCHEDULER_CRASH_JSON='
            actual = json.loads(next(line[len(marker):] for line in stdout.splitlines() if line.startswith(marker)))
            after = state(row); envelopes = f.queue()
            expected = 1 if mode == 'after-sync' else 2
            check(mode + '-restart-publication-count', len(envelopes) == expected and after['count'] == 1)
            check(mode + '-restart-delay', actual['delay'] == (5 if mode == 'after-sync' else 0))
            identities = [item['headers']['id'] for item in envelopes]
            check(mode + '-distinct-task-identities', len(set(identities)) == len(identities))
            check(mode + '-no-worker-result', all(f.app.AsyncResult(value).state == 'PENDING' for value in identities) and f.backend.dbsize() == 0)
            report['cases'].append({'name': mode, 'before': before, 'at_hardloss': stopped, 'after_restart': after,
                                    'delay': actual['delay'], 'envelopes': envelopes,
                                    'fixture_control': 'Instance _do_sync timing barrier before real call or after real call; process group SIGKILL, fresh Original scheduler restart, no handler replacement.'})
    report['passed'] = True
finally:
    if process is not None and process.poll() is None:
        os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=5)
    f.EPGSource.objects.filter(pk__in=sources).delete()
    f.broker.flushdb(); f.backend.flushdb()
    report['queue_and_results_after_cleanup'] = [f.broker.dbsize(), f.backend.dbsize()]
    Path('/evidence/source-scheduler-crash-private.json').write_text(json.dumps(report, indent=2, default=str) + '\n')
