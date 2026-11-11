"""Actual Original two-entry Beat→real workers→four results→SQL close.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Inactive refresh and Series0 only; no descendants removed or handlers replaced.
"""
import json,os,signal,subprocess,time
from pathlib import Path
import source_scheduler_worker as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'scope':'Actual Original two-known-interval elapsed Beat→real inactive Refresh/Series0 workers→AsyncResult→graceful SQLsync; no native/main/general families acceptance'}
worker=None;beat=None;sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def stop(process):
 if process is not None and process.poll()is None:
  os.killpg(process.pid,signal.SIGTERM)
  try:process.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5);raise RuntimeError('Forced multiworker cleanup')
def publications():
 path=Path('/evidence/periodic-timer-publications.jsonl');return [json.loads(v)for v in path.read_text().splitlines()]if path.exists()else[]
try:
 f.broker.flushdb();f.backend.flushdb();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();recordings=list(reference.Recording.objects.order_by('id').values());source=f.EPGSource(name='rust-owned-original-multi-worker-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk);interval=f.IntervalSchedule.objects.create(every=1,period=f.IntervalSchedule.SECONDS)
 owned=[f.create('rust-owned-multi-workers-z-refresh',f.refresh_epg_data.name,[source.pk],{},interval),f.create('rust-owned-multi-workers-a-series',f.evaluate_series_rules.name,[],{},interval)];names=[v.name for v in owned];env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app')
 with Path('/evidence/multi-real-worker-private.log').open('ab')as log:worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-multi-worker@fixture'],env=env,stdout=log,stderr=log,start_new_session=True)
 with Path('/evidence/multi-real-beat-private.log').open('ab')as log:beat=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_timer_observer:app','beat','--scheduler','django_celery_beat.schedulers:DatabaseScheduler','--max-interval','1','--pidfile=','--loglevel=WARNING'],env=env,stdout=log,stderr=log,start_new_session=True)
 started=time.monotonic();deadline=started+20
 while True:
  observed=[v for v in reference.rows('prerun')if v['headers']and v['headers'].get('periodic_task_name')in names];done={v['id']for v in reference.rows('postrun')}
  if len(observed)>=4 and all(v['id']in done for v in observed):break
  if time.monotonic()>deadline:raise TimeoutError('Original two-entry four actual results')
  if beat.poll()is not None:raise RuntimeError('Original multi Beat exited early')
  time.sleep(.01)
 stop(beat);check('multiworker-actual-Beat-exit0',beat.returncode==0);beat=None;ordered=[v for v in publications()if v['headers'].get('periodic_task_name')in names];check('multiworker-exact-four-ordered-publications',[v['headers']['periodic_task_name']for v in ordered]==[names[0],names[1],names[0],names[1]]);check('multiworker-exact-four-distinct-task-ids',len(observed)==4 and len({v['id']for v in observed})==4)
 results=[]
 for item in observed:
  ident=item['id'];post=next(v for v in reference.rows('postrun')if v['id']==ident);metadata=json.loads(f.backend.get(f.app.backend.get_key_for_task(ident)));expected=None if item['task']==f.refresh_epg_data.name else{'scheduled':0,'details':[]};check('multiworker-real-typed-AsyncResult-'+str(len(results)),post['state']=='SUCCESS'and metadata['status']=='SUCCESS'and metadata['result']==expected and metadata['children']==[]and metadata['traceback']is None and f.app.AsyncResult(ident).get(timeout=2)==expected);results.append(metadata)
 stop(worker);check('multiworker-real-worker-exit0',worker.returncode==0);worker=None;check('multiworker-both-SQL-count2',all(f.PeriodicTask.objects.get(pk=v.pk).total_run_count==2 for v in owned));check('multiworker-no-queue-reservation-recording-effects',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0 and recordings==list(reference.Recording.objects.order_by('id').values()));report.update(passed=True,source_id=source.pk,elapsed_seconds=time.monotonic()-started,publications=ordered,prerun=observed,results=results,periodic_names=names,persisted_counts=[f.PeriodicTask.objects.get(pk=v.pk).total_run_count for v in owned])
finally:
 stop(beat);stop(worker);f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-scheduler-multi-worker-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
