"""Controlled-clock observations of unmodified Original ModelEntry interval logic.
Original Dispatcharr/Celery/django-celery-beat credit and licenses retained.
"""
import datetime as dt, hashlib, json, os, sys
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE') != 'playback-fixture-only':
    raise RuntimeError('Explicit owned fixture required')
database = os.environ.get('POSTGRES_DB', '')
if not database.startswith('rust_probe_backup_jobs_') or len(database) > 63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in database):
    raise RuntimeError('Disposable copied database required')
sys.path[:0] = ['/app'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from dispatcharr.celery import app
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from django_celery_beat.schedulers import ModelEntry
fixed = dt.datetime(2026, 11, 10, 12, 0, 0, 123456, tzinfo=dt.timezone.utc)
specs = [
    {'name':'hour_due','last_offset_us':-3_600_000_000},
    {'name':'hour_before','last_offset_us':-3_599_999_999},
    {'name':'hour_late','last_offset_us':-7_200_000_000},
    {'name':'hour_fresh','last_offset_us':0},
    {'name':'future_last','last_offset_us':5_000_000},
    {'name':'disabled','enabled':False},
    {'name':'disabled_expired','enabled':False,'expires_offset_us':-1},
    {'name':'start_future_fraction','start_offset_us':1_250_000},
    {'name':'start_one_microsecond','start_offset_us':1},
    {'name':'start_boundary','start_offset_us':0},
    {'name':'future_start_precedes_expiry','start_offset_us':1_250_000,'expires_offset_us':-1},
    {'name':'expires_boundary','expires_offset_us':0},
    {'name':'expired','expires_offset_us':-1},
    {'name':'expiry_future','expires_offset_us':1},
    {'name':'one_off_first','one_off':True},
    {'name':'one_off_done','one_off':True,'total_run_count':1},
    {'name':'expired_one_off','one_off':True,'total_run_count':2,'expires_offset_us':-1},
    {'name':'disabled_one_off','one_off':True,'total_run_count':2,'enabled':False},
    {'name':'future_start_one_off','one_off':True,'total_run_count':2,'start_offset_us':1},
    {'name':'zero_interval','every':0},
    {'name':'one_day_due','every':1,'period':'days','last_offset_us':-86_400_000_000},
    {'name':'two_minutes_before','every':2,'period':'minutes','last_offset_us':-119_999_999},
    {'name':'three_seconds_due','every':3,'period':'seconds','last_offset_us':-3_000_000},
    {'name':'microsecond_before','every':2,'period':'microseconds','last_offset_us':-1},
    {'name':'microsecond_due','every':2,'period':'microseconds','last_offset_us':-2},
]
report={'passed':False,'scope':'Actual Original ModelEntry.is_due with explicit per-entry _default_now and schedule.nowfun clock dependencies; no elapsed-time, calendar, transport or native application acceptance','fixed_now':fixed.isoformat(),'cases':[]}
owned=[];intervals=[]
try:
    for spec in specs:
        interval=IntervalSchedule.objects.create(every=spec.get('every',3600),period=spec.get('period','seconds'));intervals.append(interval.pk)
        values={'name':'rust-owned-interval-'+spec['name'],'task':'apps.backups.tasks.scheduled_backup_task','interval':interval,'last_run_at':fixed+dt.timedelta(microseconds=spec.get('last_offset_us',-3_600_000_000)),'enabled':spec.get('enabled',True),'one_off':spec.get('one_off',False),'total_run_count':spec.get('total_run_count',0),'args':'[]','kwargs':'{}'}
        for source,target in [('start_offset_us','start_time'),('expires_offset_us','expires')]:
            if source in spec:values[target]=fixed+dt.timedelta(microseconds=spec[source])
        row=PeriodicTask.objects.create(**values);owned.append(row.pk)
        entry=ModelEntry(row,app=app);entry._default_now=lambda:fixed;entry.schedule.nowfun=lambda:fixed
        state=entry.is_due();row.refresh_from_db()
        report['cases'].append({'input':spec,'due':state.is_due,'next_seconds':state.next,'persisted_enabled':row.enabled,'persisted_total_run_count':row.total_run_count,'entry_no_changes':getattr(entry.model,'no_changes',False)})
    report['model_entry_source_sha256']=hashlib.sha256(Path(sys.modules[ModelEntry.__module__].__file__).read_bytes()).hexdigest();report['passed']=True
finally:
    PeriodicTask.objects.filter(pk__in=owned).delete();IntervalSchedule.objects.filter(pk__in=intervals).delete()
    report['owned_rows_removed']=not PeriodicTask.objects.filter(pk__in=owned).exists() and not IntervalSchedule.objects.filter(pk__in=intervals).exists()
    Path('/evidence/source-scheduler-interval-private.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['cases'])}))
