"""Actual Original Beat timer→publication→real worker→results→graceful sync.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Only inactive owned refresh and Series0; no task replacement or descendants.
"""
import json,os,signal,subprocess,time
from pathlib import Path
import source_scheduler_worker as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'scope':'Actual Original Celery Beat process default setup→elapsed one-second interval→real worker/results→graceful SQLsync; known refresh/Series0 only, not performance or native application acceptance'}
worker=None;beat=None;sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def read_publications():
 path=Path('/evidence/periodic-timer-publications.jsonl')
 return [json.loads(v)for v in path.read_text().splitlines()]if path.exists()else[]
def stop(process):
 if process is None or process.poll()is not None:return
 os.killpg(process.pid,signal.SIGTERM)
 try:process.wait(timeout=15)
 except subprocess.TimeoutExpired:
  os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5);raise RuntimeError('Forced timer/worker cleanup')
try:
 source=f.EPGSource(name='rust-owned-timer-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 interval=f.IntervalSchedule.objects.create(every=1,period=f.IntervalSchedule.SECONDS)
 env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app')
 with Path('/evidence/periodic-timer-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-timer@fixture'],env=env,stdout=log,stderr=log,start_new_session=True)
 for label,task,args,kwargs in [('refresh',f.refresh_epg_data,[source.pk],{}),('series',f.evaluate_series_rules,[],{})]:
  f.broker.flushdb();f.backend.flushdb();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();row=f.create('rust-owned-real-timer-'+label,task.name,args,kwargs,interval)
  with Path('/evidence/periodic-timer-beat-private.log').open('ab')as log:
   beat=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_timer_observer:app','beat','--scheduler','django_celery_beat.schedulers:DatabaseScheduler','--max-interval','1','--pidfile=','--loglevel=WARNING'],env=env,stdout=log,stderr=log,start_new_session=True)
  started=time.monotonic();deadline=started+20
  while True:
   publications=[v for v in read_publications()if v['headers'].get('periodic_task_name')==row.name]
   if len(publications)>=3:break
   if beat.poll()is not None:raise RuntimeError('Actual Beat exited early')
   if time.monotonic()>deadline:raise TimeoutError('Actual Beat bounded three publications')
   time.sleep(.01)
  stop(beat);check(label+'-actual-Beat-graceful-exit',beat.returncode==0);beat=None
  publications=[v for v in read_publications()if v['headers'].get('periodic_task_name')==row.name]
  check(label+'-exact-three-real-timer-publications',len(publications)==3)
  identifiers=[v['headers']['id']for v in publications];check(label+'-three-distinct-identities',len(set(identifiers))==3)
  for ident in identifiers:
   post=reference.wait(lambda:next((v for v in reference.rows('postrun')if v['id']==ident),None));metadata=json.loads(f.backend.get(f.app.backend.get_key_for_task(ident)));expected=None if label=='refresh'else{'scheduled':0,'details':[]}
   check(label+'-actual-worker-result',post['state']=='SUCCESS'and metadata['result']==expected and f.app.AsyncResult(ident).get(timeout=2)==expected)
  row.refresh_from_db();check(label+'-graceful-close-persists-all-three-runs',row.total_run_count==3 and row.last_run_at is not None)
  check(label+'-no-queue-or-reservation',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0)
  report['cases'].append({'name':label,'elapsed_seconds':time.monotonic()-started,'publications':publications,'count_after_close':row.total_run_count,'last_run_at':row.last_run_at.isoformat(),'results':[json.loads(f.backend.get(f.app.backend.get_key_for_task(v)))for v in identifiers]})
 report['passed']=True
finally:
 stop(beat);stop(worker);f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-scheduler-timer-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
