"""Actual Original database scheduler startup and reload observations.
Original Dispatcharr, Celery and django-celery-beat authors retain credit/licenses.
Only a disposable copied database and isolated broker/result namespaces are allowed.
"""
import hashlib, importlib.metadata, json, os, sys
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE') != 'playback-fixture-only':
    raise RuntimeError('Explicit owned fixture required')
database = os.environ.get('POSTGRES_DB', '')
if not database.startswith('rust_probe_backup_jobs_') or len(database) > 63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in database):
    raise RuntimeError('Disposable copied database required')
sys.path[:0] = ['/app']; sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
import redis
from dispatcharr.celery import app
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask, PeriodicTasks
from django_celery_beat.schedulers import DatabaseScheduler
broker = redis.Redis.from_url(os.environ['CELERY_BROKER_URL'])
backend = redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
if broker.connection_pool.connection_kwargs['db'] != 8 or backend.connection_pool.connection_kwargs['db'] != 9:
    raise RuntimeError('Owned namespaces required')
broker.flushdb(); backend.flushdb()
report = {'passed': False, 'checks': [], 'versions': {key: importlib.metadata.version(key) for key in ['celery', 'django-celery-beat', 'Django']}, 'scope': 'Actual eager DatabaseScheduler setup/default installation and in-process changed-sentinel reload; no tick, worker, native scheduler, process restart or performance acceptance'}
def check(name, value):
    report['checks'].append({'name': name, 'passed': bool(value)})
    if not value:
        raise AssertionError(name)
def rows():
    return list(PeriodicTask.objects.order_by('name').values('name', 'task', 'enabled', 'interval_id', 'crontab_id', 'args', 'kwargs', 'expire_seconds', 'one_off'))
scheduler = rebuilt = None
try:
    # Deliberate copied-database setup. The complete project/database is removed
    # by the controller; no assertion that the copied rows were restored.
    PeriodicTask.objects.all().delete()
    PeriodicTasks.update_changed()
    report['configured_defaults'] = sorted(app.conf.beat_schedule)
    report['result_expires_enabled'] = bool(app.conf.result_expires)
    scheduler = DatabaseScheduler(app=app, lazy=False, max_interval=5)
    report['installed_rows'] = rows()
    installed = {row['name']: row for row in report['installed_rows']}
    check('configured-default-rows-installed', set(app.conf.beat_schedule) <= set(installed))
    check('backend-cleanup-installation-matches-result-expiry', ('celery.backend_cleanup' in installed) == bool(app.conf.result_expires))
    if 'celery.backend_cleanup' in installed:
        check('backend-cleanup-task-name', installed['celery.backend_cleanup']['task'] == 'celery.backend_cleanup')
    check('eager-construction-has-not-published', broker.dbsize() == 0)
    check('eager-construction-has-no-results', backend.dbsize() == 0)
    # Exclude real application defaults before exercising one owned entry.
    PeriodicTask.objects.all().update(enabled=False)
    PeriodicTasks.update_changed()
    scheduler.schedule; scheduler.schedule
    check('signalled-disabled-defaults-absent', not scheduler.schedule)
    interval = IntervalSchedule.objects.create(every=3600, period=IntervalSchedule.SECONDS)
    row = PeriodicTask.objects.create(name='rust-owned-startup-reload', task='apps.backups.tasks.scheduled_backup_task', interval=interval, args='[]', kwargs='{}', last_run_at=timezone.now())
    check('created-entry-save-reloads', row.name in scheduler.schedule)
    scheduler.schedule
    PeriodicTask.objects.filter(pk=row.pk).update(enabled=False)
    check('bulk-update-without-sentinel-retains-cached-entry', row.name in scheduler.schedule)
    check('cached-entry-retains-prior-enabled-model', scheduler.schedule[row.name].model.enabled is True)
    PeriodicTasks.update_changed()
    check('explicit-sentinel-removes-disabled-entry', row.name not in scheduler.schedule)
    row.enabled = True
    row.save(update_fields=['enabled'])
    check('model-save-reloads-enabled-entry', row.name in scheduler.schedule)
    row.delete()
    check('model-delete-removes-entry', 'rust-owned-startup-reload' not in scheduler.schedule)
    before = rows()
    rebuilt = DatabaseScheduler(app=app, lazy=False, max_interval=5)
    after = rows()
    report['reconstructed_rows'] = after
    check('eager-reconstruction-has-no-duplicate-names', len(after) == len({item['name'] for item in after}))
    check('eager-reconstruction-preserves-default-name-set', {item['name'] for item in before} == {item['name'] for item in after})
    check('reconstruction-has-not-published', broker.dbsize() == 0)
    check('reconstruction-has-no-results', backend.dbsize() == 0)
    source = Path(sys.modules[DatabaseScheduler.__module__].__file__)
    report['scheduler_source_sha256'] = hashlib.sha256(source.read_bytes()).hexdigest()
    report['scheduler_source_private'] = '/evidence/database-scheduler-source-private.py'
    Path(report['scheduler_source_private']).write_bytes(source.read_bytes())
    report['passed'] = True
finally:
    for value in [scheduler, rebuilt]:
        if value is not None:
            value.close()
    Path('/evidence/source-scheduler-startup-private.json').write_text(json.dumps(report, indent=2, default=str) + '\n')
print(json.dumps({'passed': report['passed'], 'checks': len(report['checks'])}))
