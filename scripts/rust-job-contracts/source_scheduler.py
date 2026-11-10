"""Pinned Original beat scheduler observations; copied DB and broker8/result9 only.
Original Dispatcharr/Celery/django-celery-beat authors retain code credit/licenses.
No task execution, restore, external providers, or native scheduler.
"""
import base64, datetime as dt, hashlib, importlib.metadata, json, os, sys
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Explicit owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_') or len(database)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in database):raise RuntimeError('Copied database required')
sys.path[:0]=['/app'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from django_celery_beat.schedulers import DatabaseScheduler, ModelEntry
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned isolated namespaces required')
broker.flushdb();backend.flushdb()
report={'passed':False,'scope':'Actual Original ModelEntry and lazy DatabaseScheduler tick/sync/reconstruction; no workers/native scheduler/default schedule installation or crash guarantee','checks':[],'cases':[],'versions':{k:importlib.metadata.version(k)for k in ['celery','django-celery-beat','Django']}}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
prior=list(PeriodicTask.objects.values_list('id','enabled'));owned=[];scheduler=None;restart=None
interval=IntervalSchedule.objects.create(every=3600,period=IntervalSchedule.SECONDS)
def create(label,**changes):
 values={'name':'rust-owned-scheduler-'+label,'task':'apps.backups.tasks.scheduled_backup_task','interval':interval,'last_run_at':timezone.now()-dt.timedelta(hours=2),'args':'[]','kwargs':'{"retention_count":2}','headers':'{"owned_scope":"scheduler_probe"}'};values.update(changes)
 row=PeriodicTask.objects.create(**values);owned.append(row.pk);return row
try:
 PeriodicTask.objects.filter(pk__in=[p for p,_ in prior]).update(enabled=False)
 specs=[('due',{},True),('not_due',{'last_run_at':timezone.now()},False),('disabled',{'enabled':False},False),('future_start',{'start_time':timezone.now()+dt.timedelta(hours=1)},False),('expired',{'expires':timezone.now()-dt.timedelta(seconds=1)},False),('one_off_done',{'one_off':True,'total_run_count':1},False),('one_off_first',{'one_off':True},True),('bad_args',{'args':'broken'},False),('bad_kwargs',{'kwargs':'broken'},False)]
 for label,changes,due in specs:
  row=create(label,**changes);entry=ModelEntry(row,app=app);state=entry.is_due();row.refresh_from_db()
  check('model-entry-due-'+label,state.is_due==due)
  if label in ['expired','one_off_done','bad_args','bad_kwargs']:check('model-entry-disables-'+label,not row.enabled)
  if label=='one_off_done':check('one-off-resets-count',row.total_run_count==0)
  report['cases'].append({'name':label,'due':state.is_due,'next_seconds':state.next,'enabled_after':row.enabled,'total_run_count_after':row.total_run_count,'last_run_at':row.last_run_at.isoformat() if row.last_run_at else None})
  row.enabled=False;row.save(update_fields=['enabled'])
 row=create('actual_publication');original_last=row.last_run_at
 # Lazy construction deliberately omits setup_schedule/default-row installation.
 # Tick, reservation, actual broker publication and sync are unmodified Source.
 scheduler=DatabaseScheduler(app=app,lazy=True,max_interval=5)
 check('only-owned-due-entry-loaded',set(scheduler.schedule)=={row.name})
 delay=scheduler.tick();scheduler.sync();row.refresh_from_db()
 queued=broker.lrange('celery',0,64);check('exactly-one-actual-publication',len(queued)==1)
 envelope=json.loads(queued[0]);payload=json.loads(base64.b64decode(envelope['body']))
 check('actual-scheduled-backup-task',envelope['headers']['task']==row.task)
 check('actual-scheduled-backup-kwargs',payload[0]==[] and payload[1]=={'retention_count':2})
 check('periodic-header-preserved',envelope['headers']['periodic_task_name']==row.name and envelope['headers']['owned_scope']=='scheduler_probe')
 check('database-sync-records-one-run',row.total_run_count==1 and row.last_run_at>original_last)
 scheduler.tick();scheduler.sync();check('second-immediate-tick-no-publication',broker.llen('celery')==1)
 restart=DatabaseScheduler(app=app,lazy=True,max_interval=5);check('reconstructed-scheduler-sees-committed-run',restart.schedule[row.name].total_run_count==1 and not restart.schedule[row.name].is_due().is_due)
 restart.tick();restart.sync();check('reconstructed-scheduler-no-duplicate-before-due',broker.llen('celery')==1)
 check('no-worker-results',backend.dbsize()==0)
 report['publication']={'envelope':envelope,'payload':payload,'raw_sha256':hashlib.sha256(queued[0]).hexdigest(),'tick_delay':delay,'last_run_at':row.last_run_at.isoformat(),'total_run_count':row.total_run_count}
 row.enabled=False;row.save(update_fields=['enabled'])
 # Two actual schedulers preload the same due row before either publishes.
 # This observes concurrency behavior; it does not invent a distributed lease.
 overlap=create('overlap');a=DatabaseScheduler(app=app,lazy=True,max_interval=5);b=DatabaseScheduler(app=app,lazy=True,max_interval=5)
 try:
  check('overlap-preloads-same-due-row',set(a.schedule)=={overlap.name} and set(b.schedule)=={overlap.name})
  a.populate_heap();b.populate_heap();before=broker.llen('celery')
  a.tick();a.sync();b.tick();b.sync();overlap.refresh_from_db()
  extra=[json.loads(raw) for raw in broker.lrange('celery',0,64) if json.loads(raw)['headers'].get('periodic_task_name')==overlap.name]
  check('two-preloaded-schedulers-publish-twice',len(extra)==2 and broker.llen('celery')==before+2)
  check('overlap-row-count-is-stale-last-writer',overlap.total_run_count==1)
  report['overlap']={'publications':len(extra),'distinct_task_ids':len({v['headers']['id']for v in extra}),'total_run_count':overlap.total_run_count,'scope':'Controlled preloaded scheduler overlap, not an OS process/crash/soak test'}
 finally:a.close();b.close()
 overlap.enabled=False;overlap.save(update_fields=['enabled'])
 # Use a real AUTH rejection on the owned loopback broker. No worker starts.
 rejected=create('publish_rejected');failed=DatabaseScheduler(app=app,lazy=True,max_interval=5)
 try:
  entry=failed.schedule[rejected.name];before=broker.llen('celery')
  denied=app.connection_for_write('redis://:owned-intentionally-invalid@127.0.0.1:6379/8',connect_timeout=1)
  try:
   producer=app.amqp.Producer(denied)
   try:failed.apply_async(entry,producer=producer)
   except Exception as error:report['publication_failure']={'module':type(error).__module__,'class':type(error).__name__,'args':[str(v) for v in error.args]}
   else:raise AssertionError('Expected actual broker authentication rejection')
  finally:denied.release()
  failed.sync();rejected.refresh_from_db()
  check('rejected-publication-does-not-enqueue',broker.llen('celery')==before)
  check('rejected-publication-still-persists-reservation',rejected.total_run_count==1)
  check('rejected-publication-not-immediately-due',not ModelEntry(rejected,app=app).is_due().is_due)
  report['publication_failure'].update(total_run_count=rejected.total_run_count,last_run_at=rejected.last_run_at.isoformat())
 finally:failed.close()
 report['scheduler_source_sha256']=hashlib.sha256(Path(sys.modules[DatabaseScheduler.__module__].__file__).read_bytes()).hexdigest()
 report['passed']=True
finally:
 if scheduler is not None:scheduler.close()
 if restart is not None:restart.close()
 PeriodicTask.objects.filter(pk__in=owned).delete();interval.delete()
 for pk,enabled in prior:PeriodicTask.objects.filter(pk=pk).update(enabled=enabled)
 report['owned_rows_removed']=not PeriodicTask.objects.filter(pk__in=owned).exists()
 Path('/evidence/source-scheduler-private.json').write_text(json.dumps(report,indent=2)+'\n')
