"""Actual Original database scheduler startup and reload observations.
Original Dispatcharr, Celery and django-celery-beat authors retain credit/licenses.
Only a disposable copied database and isolated broker/result namespaces are allowed.
"""
import datetime as dt,hashlib, importlib.metadata, inspect,json, os, sys
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
from django_celery_beat.models import CrontabSchedule,IntervalSchedule, PeriodicTask, PeriodicTasks
from django_celery_beat.schedulers import ModelEntry
from django.db import connection
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
    result=[]
    for row in PeriodicTask.objects.order_by('name'):
        item={field.attname:getattr(row,field.attname)for field in row._meta.concrete_fields}
        item['interval']=None if row.interval_id is None else {'every':row.interval.every,'period':row.interval.period}
        item['crontab']=None if row.crontab_id is None else {name:str(getattr(row.crontab,name))for name in ['minute','hour','day_of_week','day_of_month','month_of_year','timezone']}
        result.append(item)
    return result
def marker():return str(PeriodicTasks.last_change())
def cached(value):
    return {name:{'enabled':entry.model.enabled,'args':getattr(entry,'args',None),'kwargs':getattr(entry,'kwargs',None),'count':entry.total_run_count,'last_run_at':str(entry.last_run_at)}for name,entry in sorted(value.schedule.items())}

scheduler = rebuilt = None
try:
    # Deliberate copied-database setup. The complete project/database is removed
    # by the controller; no assertion that the copied rows were restored.
    PeriodicTask.objects.all().delete()
    PeriodicTasks.update_changed()
    report['configured_defaults'] = sorted(app.conf.beat_schedule)
    report['result_expires_enabled'] = bool(app.conf.result_expires)
    scheduler = DatabaseScheduler(app=app, lazy=False, max_interval=5)
    report['installed_rows'] = rows();report['startup_cache']=cached(scheduler);report['startup_marker']=marker();report['app_timezone']=str(app.timezone)
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
    check('created-entry-save-reloads', row.name in scheduler.schedule);report['created_cache']=cached(scheduler);report['created_marker']=marker()
    scheduler.schedule
    PeriodicTask.objects.filter(pk=row.pk).update(enabled=False)
    check('bulk-update-without-sentinel-retains-cached-entry', row.name in scheduler.schedule)
    check('cached-entry-retains-prior-enabled-model', scheduler.schedule[row.name].model.enabled is True);report['unsignalled_cache']=cached(scheduler);report['unsignalled_marker']=marker()
    PeriodicTasks.update_changed()
    check('explicit-sentinel-removes-disabled-entry', row.name not in scheduler.schedule)
    row.enabled = True
    row.save(update_fields=['enabled'])
    check('model-save-reloads-enabled-entry', row.name in scheduler.schedule)
    row.delete()
    check('model-delete-removes-entry', 'rust-owned-startup-reload' not in scheduler.schedule)
    # Ordinary SQL batch updates cannot simulate ORM signal behavior. Capture
    # the marker boundary and the stale cached model explicitly.
    row=PeriodicTask.objects.create(name='rust-owned-nochanges',task='apps.epg.tasks.refresh_epg_data',interval=interval,args='[1]',kwargs='{}')
    scheduler.schedule;scheduler.schedule;before_marker=marker();row.no_changes=True;row.enabled=False;row.save(update_fields=['enabled']);check('nochanges-save-marker-unchanged',marker()==before_marker);check('nochanges-save-retains-cached-enabled',scheduler.schedule[row.name].model.enabled is True)
    PeriodicTasks.update_changed();check('explicit-marker-reloads-nochanges-disabled',row.name not in scheduler.schedule);row.delete()
    # update_from_dict resets only fields supplied by from_entry/_unpack_fields;
    # existing operational state is not reset by reconstructing defaults.
    altered=PeriodicTask.objects.get(name='scan-files');altered.task='owned.wrong';altered.enabled=False;altered.args='[99]';altered.kwargs='{"owned":true}';altered.queue='owned';altered.headers='{"old":true}';altered.one_off=True;altered.total_run_count=7;altered.description='owned persistent description';altered.last_run_at=timezone.now()-dt.timedelta(days=1);altered.save();report['altered_default_before']=rows()

    before = rows()
    rebuilt = DatabaseScheduler(app=app, lazy=False, max_interval=5)
    after = rows()
    report['reconstructed_rows'] = after
    check('eager-reconstruction-has-no-duplicate-names', len(after) == len({item['name'] for item in after}))
    check('eager-reconstruction-preserves-default-name-set', {item['name'] for item in before} == {item['name'] for item in after})
    altered.refresh_from_db();check('default-upsert-resets-task-call-and-options',altered.task=='core.tasks.scan_and_process_files'and altered.args=='[]'and altered.kwargs=='{}'and altered.queue is None and altered.headers=='{}')
    check('default-upsert-preserves-disabled-count-and-oneoff',not altered.enabled and altered.total_run_count==7 and altered.one_off and altered.description=='owned persistent description')
    report['model_source']={name:inspect.getsource(getattr(PeriodicTask,name))for name in ['save','validate_unique']};report['changes_source']={name:inspect.getsource(getattr(PeriodicTasks,name))for name in ['changed','update_changed','last_change']};report['interval_source']=inspect.getsource(IntervalSchedule.from_schedule);report['crontab_source']=inspect.getsource(CrontabSchedule.from_schedule)
    report['argument_load_cases']=[]
    for label,args,kwargs,headers in [('bad-args','broken','{}','{}'),('bad-kwargs','[]','broken','{}'),('bad-headers','[]','{}','broken'),('null-headers','[]','{}','null'),('array-headers','[]','{}','[]'),('empty-fields','','','{}')]:
        model=PeriodicTask.objects.create(name='rust-owned-json-'+label,task='apps.epg.tasks.refresh_epg_data',interval=interval,args=args,kwargs=kwargs,headers=headers)
        before_marker=marker();failure=None;entry=None
        try:entry=ModelEntry(model,app=app)
        except Exception as error:failure={'module':type(error).__module__,'class':type(error).__name__,'args':[str(v)for v in error.args]}
        model.refresh_from_db();report['argument_load_cases'].append({'name':label,'error':failure,'enabled_after':model.enabled,'marker_unchanged':marker()==before_marker,'loaded_args':getattr(entry,'args',None),'loaded_kwargs':getattr(entry,'kwargs',None)})
        check('argument-boundary-'+label,(not model.enabled and failure is None)if label in ['bad-args','bad-kwargs']else(model.enabled and(failure is not None if label in ['bad-headers','null-headers','array-headers']else failure is None)))
        model.no_changes=False;model.delete()
    # A row-level startup write error is caught per-entry; earlier/default
    # schedule/FK changes are not rolled back as a whole startup transaction.
    with connection.cursor()as cursor:
        cursor.execute("CREATE FUNCTION rust_owned_scheduler_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='scan-files' THEN RAISE EXCEPTION 'owned scheduler P0001'; END IF; RETURN NEW; END $$")
        cursor.execute('CREATE TRIGGER rust_owned_scheduler_failure BEFORE INSERT OR UPDATE ON django_celery_beat_periodictask FOR EACH ROW EXECUTE FUNCTION rust_owned_scheduler_fail()')
    try:
        failed=DatabaseScheduler(app=app,lazy=False,max_interval=5);report['caught_startup_cache']=cached(failed);failed.close();check('default-row-error-does-not-abort-startup',PeriodicTask.objects.filter(name='maintain-recurring-recordings').exists());report['caught_startup_rows']=rows()
    finally:
        with connection.cursor()as cursor:cursor.execute('DROP TRIGGER rust_owned_scheduler_failure ON django_celery_beat_periodictask');cursor.execute('DROP FUNCTION rust_owned_scheduler_fail()')

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
    Path('/evidence/source-scheduler-database-private.json').write_text(json.dumps(report, indent=2, default=str) + '\n')
print(json.dumps({'passed': report['passed'], 'checks': len(report['checks'])}))
