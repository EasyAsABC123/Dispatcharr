"""Actual Original periodic refresh/Series envelopes and SQL sync boundaries.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
No worker consumes any task. Only copied DB and broker8/result9 are allowed.
"""
import base64,datetime as dt,hashlib,importlib,inspect,json,os,sys
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_')or len(database)>63 or not all(v in 'abcdefghijklmnopqrstuvwxyz0123456789_'for v in database):raise RuntimeError('Copied DB required')
sys.path[:0]=['/app'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.db import connection
from django.utils import timezone
from django_celery_beat.models import PeriodicTask,PeriodicTasks,IntervalSchedule
from django_celery_beat.schedulers import DatabaseScheduler
from apps.epg.models import EPGSource
from apps.epg.tasks import refresh_epg_data
from apps.channels.tasks import evaluate_series_rules
from dispatcharr.celery import app
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
report={'passed':False,'checks':[],'cases':[],'scope':'Actual eager Original scheduler→known periodic envelopes/tick/sync; no worker/native scheduler/main/performance acceptance'};schedulers=[];sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def queue():
 rows=broker.lrange('celery',0,15)
 if len(rows)>8 or any(len(v)>65536 for v in rows):raise RuntimeError('Publication bound')
 return [json.loads(v)for v in rows]
def new_scheduler():
 value=DatabaseScheduler(app=app,lazy=False,max_interval=5);schedulers.append(value);PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();value.schedule;value.schedule;return value
def create(name,task,args,kwargs,interval):
 return PeriodicTask.objects.create(name=name,task=task,interval=interval,args=json.dumps(args),kwargs=json.dumps(kwargs),headers='{}',last_run_at=timezone.now()-dt.timedelta(hours=2))
try:
 broker.flushdb();backend.flushdb();source=EPGSource(name='rust-owned-scheduler-inactive',source_type='xmltv',is_active=False,custom_properties={});EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 interval=IntervalSchedule.objects.create(every=3600,period=IntervalSchedule.SECONDS)
 for label,task,args,kwargs in [('refresh',refresh_epg_data,[source.pk],{'force':True}),('series',evaluate_series_rules,[],{})]:
  broker.flushdb();scheduler=new_scheduler();row=create('rust-owned-periodic-'+label,task.name,args,kwargs,interval);old=row.last_run_at
  check(label+'-one-owned-entry-loaded',set(scheduler.schedule)=={row.name});delay=scheduler.tick();scheduler.sync();row.refresh_from_db();envelopes=queue();check(label+'-one-real-publication',len(envelopes)==1)
  envelope=envelopes[0];payload=json.loads(base64.b64decode(envelope['body'],validate=True));ident=envelope['headers']['id']
  check(label+'-task-body-and-periodic-header',envelope['headers']['task']==task.name and payload[:2]==[args,kwargs]and envelope['headers']['periodic_task_name']==row.name)
  check(label+'-exact-extra-header-only-periodic',set(envelope['headers'])=={'lang','task','id','shadow','eta','expires','group','group_index','retries','timelimit','root_id','parent_id','argsrepr','kwargsrepr','origin','ignore_result','replaced_task_nesting','stamped_headers','stamps','periodic_task_name'})
  check(label+'-initial-identity-routing',envelope['headers']['root_id']==ident and envelope['headers']['parent_id']is None and envelope['properties']['correlation_id']==ident and envelope['properties']['delivery_info']=={'exchange':'','routing_key':'celery'})
  check(label+'-registered-task-timelimit',envelope['headers']['timelimit']==[task.time_limit,None]);check(label+'-tick-zero-and-sync-count',delay==0 and row.total_run_count==1 and row.last_run_at>old)
  scheduler.tick();scheduler.sync();check(label+'-second-tick-no-duplicate',broker.llen('celery')==1);check(label+'-pending-not-a-worker-result',app.AsyncResult(ident).state=='PENDING'and backend.dbsize()==0)
  report['cases'].append({'name':label,'envelope':envelope,'payload':payload,'row_after':{'total_run_count':row.total_run_count,'last_run_at':row.last_run_at.isoformat()},'tick_delay':delay})
  row.enabled=False;row.save(update_fields=['enabled'])
 # The real scheduler pops dirty before save; measure the database-error path.
 broker.flushdb();scheduler=new_scheduler();row=create('rust-owned-sync-failure',refresh_epg_data.name,[source.pk],{},interval);scheduler.schedule;scheduler.schedule
 # A warmup sync establishes public scheduler timing without replacing methods.
 # Reserve/publish directly with advance=True and explicitly sync under fault.
 scheduler.apply_async(scheduler.schedule[row.name]);scheduler.sync();row.refresh_from_db();first_count=row.total_run_count
 with connection.cursor()as cursor:
  cursor.execute("CREATE FUNCTION rust_owned_sync_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='rust-owned-sync-failure' THEN RAISE EXCEPTION 'owned sync P0001'; END IF; RETURN NEW; END $$")
  cursor.execute('CREATE TRIGGER rust_owned_sync_failure BEFORE UPDATE ON django_celery_beat_periodictask FOR EACH ROW EXECUTE FUNCTION rust_owned_sync_fail()')
 try:
  scheduler.apply_async(scheduler.schedule[row.name]);dirty_before=sorted(scheduler._dirty);scheduler.sync();row.refresh_from_db();count_after=row.total_run_count;dirty_after=sorted(scheduler._dirty);scheduler.sync();row.refresh_from_db()
  check('sync-error-keeps-two-real-publications',broker.llen('celery')==2);check('sync-error-does-not-save-new-count',count_after==first_count and row.total_run_count==first_count);check('sync-error-popped-name-not-readded',row.name in dirty_before and row.name not in dirty_after)
  report['sync_server_failure']={'dirty_before':dirty_before,'dirty_after':dirty_after,'persisted_count':count_after,'cached_count':scheduler.schedule[row.name].total_run_count,'queue_count':broker.llen('celery')}
 finally:
  with connection.cursor()as cursor:cursor.execute('DROP TRIGGER rust_owned_sync_failure ON django_celery_beat_periodictask');cursor.execute('DROP FUNCTION rust_owned_sync_fail()')
 report['framework_sources']={}
 for name in ['celery.beat','celery.schedules','django_celery_beat.schedulers','django_celery_beat.models','django_celery_beat.signals','django_celery_beat.tzcrontab']:
  module=importlib.import_module(name);path=Path(inspect.getsourcefile(module));data=path.read_bytes();dest=Path('/evidence')/('framework-'+name.replace('.','-')+'.py');dest.write_bytes(data);report['framework_sources'][name]={'sha256':hashlib.sha256(data).hexdigest(),'private_path':str(dest)}
 report['passed']=True
finally:
 for scheduler in schedulers:scheduler.close()
 EPGSource.objects.filter(pk__in=sources).delete();broker.flushdb();backend.flushdb();report['queue_and_results_after_cleanup']=[broker.dbsize(),backend.dbsize()];Path('/evidence/source-scheduler-publication-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
