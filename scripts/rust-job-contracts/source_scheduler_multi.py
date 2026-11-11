"""Pinned Original bounded multi-known-interval heap/order reference.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Only inactive refresh and Series0 are published; no worker/task is executed.
Raw load, heap and delivery orders are preserved, never sorted after a diff.
"""
import datetime as dt,heapq,json,os,signal,subprocess,time
from pathlib import Path
from celery.signals import before_task_publish
import source_scheduler_publication as f
assert f.report['passed']
report={'passed':False,'checks':list(f.report['checks']),'cases':[],'scope':'Actual Original eager multi-known-interval load/heap/tick/reload/oneoff/real broker denial and elapsed Beat/graceful+hardloss windows; no native/general calendar/unknown task authority'}
PREFIX='rust-owned-multi-';schedulers=[];sources=[];process=None;attempts=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def publication(sender=None,headers=None,**kwargs):
 if headers and str(headers.get('periodic_task_name','')).startswith(PREFIX):
  if len(attempts)>=64:raise RuntimeError('Owned publication observation bound')
  attempts.append(json.loads(json.dumps({'task':sender,'headers':headers},default=str)))
before_task_publish.connect(publication,weak=False)
def rows():
 return list(f.PeriodicTask.objects.filter(name__startswith=PREFIX).values('id','name','enabled','total_run_count','last_run_at','one_off'))
def delivery():
 values=f.queue();return [v['headers']['periodic_task_name']for v in reversed(values)]
def heap_rows(heap):
 return [{'when':v[0],'priority':v[1],'name':v[2].name,'identity':id(v[2]),'count':v[2].model.total_run_count,'enabled':v[2].model.enabled}for v in heap]
def tick(scheduler):
 pops=[]
 def observe(heap):
  before=heap_rows(heap);value=heapq.heappop(heap);pops.append({'heap_before_pop':before,'selected':value[2].name,'selected_identity':id(value[2])});return value
 marker=f.PeriodicTasks.last_change();delay=scheduler.tick(heappop=observe)
 return {'delay':delay,'pops':pops,'heap_after':heap_rows(scheduler._heap),'delivery_names':delivery(),'rows':rows(),'dirty':list(scheduler._dirty),'marker_changed':f.PeriodicTasks.last_change()!=marker}
def begin(label,definitions):
 f.broker.flushdb();f.backend.flushdb();scheduler=f.new_scheduler();schedulers.append(scheduler);interval=f.IntervalSchedule.objects.create(every=3600,period=f.IntervalSchedule.SECONDS);last=f.timezone.now()-dt.timedelta(hours=2);owned=[]
 for name,family,changes in definitions:
  task=f.refresh_epg_data if family=='refresh'else f.evaluate_series_rules
  fields={'name':PREFIX+label+'-'+name,'task':task.name,'args':json.dumps([source.pk]if family=='refresh'else[]),'kwargs':'{}','headers':'{}','interval':interval,'last_run_at':last};fields.update(changes)
  owned.append(f.PeriodicTask.objects.create(**fields))
 scheduler.schedule;scheduler.schedule
 return scheduler,owned
try:
 source=f.EPGSource(name='rust-owned-multi-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 # Reversed lexical names make accidental name sorting observable.
 for label,families in [('created-refresh-first',['refresh','series']),('created-series-first',['series','refresh'])]:
  scheduler,owned=begin(label,[('z-created-first',families[0],{}),('a-created-second',families[1],{})]);loaded=list(scheduler.schedule);check(label+'-predeclared-actual-load-order',loaded==[v.name for v in owned]);samples=[tick(scheduler)for _ in range(3)];scheduler.sync();check(label+'-exact-ordered-two-publications',delivery()==[v.name for v in owned]);check(label+'-one-publication-per-due-tick',len(samples[0]['delivery_names'])==1 and len(samples[1]['delivery_names'])==2 and len(samples[2]['delivery_names'])==2);check(label+'-both-counts-persist1',all(f.PeriodicTask.objects.get(pk=v.pk).total_run_count==1 for v in owned));report['cases'].append({'name':label,'loaded':loaded,'query':str(scheduler.enabled_models_qs().query),'samples':samples,'after_sync':rows(),'envelopes':f.queue()});scheduler.close()
 scheduler,owned=begin('notdue-first',[('z-future-refresh','refresh',{'last_run_at':f.timezone.now()+dt.timedelta(hours=1)}),('a-due-series','series',{})]);sample=tick(scheduler);scheduler.sync();check('notdue-first-selects-due-series',delivery()==[owned[1].name]);report['cases'].append({'name':'notdue-first','loaded':list(scheduler.schedule),'samples':[sample],'after_sync':rows(),'envelopes':f.queue()});scheduler.close()
 scheduler,owned=begin('oneoff',[('z-first-oneoff','refresh',{'one_off':True}),('a-second-regular','series',{})]);samples=[tick(scheduler)for _ in range(4)];scheduler.sync();check('oneoff-both-families-once',delivery()==[v.name for v in owned]);owned[0].refresh_from_db();check('oneoff-eventually-disabled-reset',not owned[0].enabled and owned[0].total_run_count==0 and owned[0].last_run_at is None);report['cases'].append({'name':'oneoff','samples':samples,'after_sync':rows(),'envelopes':f.queue()});scheduler.close()
 for label,mutation in [('disable-sentinel','disable'),('delete-sentinel','delete')]:
  scheduler,owned=begin(label,[('z-first-refresh','refresh',{}),('a-second-series','series',{})]);samples=[tick(scheduler)]
  if mutation=='disable':owned[0].enabled=False;owned[0].save(update_fields=['enabled'])
  else:owned[0].delete()
  samples.append(tick(scheduler));scheduler.sync();check(label+'-remaining-series-next',delivery()==[owned[0].name,owned[1].name]);check(label+'-removed-entry-not-in-cache',owned[0].name not in scheduler.schedule);report['cases'].append({'name':label,'samples':samples,'after_sync':rows(),'envelopes':f.queue()});scheduler.close()
 # True equal heap times are separately observable identity ties, not names.
 scheduler,owned=begin('identity-tie',[('z-first-refresh','refresh',{}),('a-second-series','series',{})]);fixed=f.timezone.now();identities={}
 for name,entry in scheduler.schedule.items():entry.schedule.nowfun=lambda value=fixed:value;identities[name]=id(entry)
 samples=[tick(scheduler),tick(scheduler)];first=samples[0]['pops'][0];events=first['heap_before_pop'];check('identity-tie-actual-equal-time-and-priority',len({(v['when'],v['priority'])for v in events})==1);check('identity-tie-follows-Source-id-comparator',first['selected_identity']==min(identities.values()));scheduler.sync();check('identity-tie-two-distinct-known-publications',len(delivery())==2 and set(delivery())=={v.name for v in owned});report['cases'].append({'name':'identity-tie','identities':identities,'samples':samples,'after_sync':rows(),'envelopes':f.queue()});scheduler.close()
 # Real producer ACL failure, observing attempts without replacing publication.
 scheduler,owned=begin('lpush-denied',[('z-first-refresh','refresh',{}),('a-second-series','series',{})]);scheduler.close();cache=f.redis.Redis(host='localhost',password='playback-fixture-only',db=0);cache.execute_command('ACL','SETUSER','rust_multi_denied','reset','on','>playback-fixture-only','~*','&*','+@all','-lpush');connection=f.app.connection_for_write('redis://rust_multi_denied:playback-fixture-only@localhost:6379/8',connect_timeout=1)
 def producer(*args,**kwargs):return f.app.amqp.Producer(connection,**kwargs)
 scheduler=f.DatabaseScheduler(app=f.app,lazy=False,max_interval=5,Producer=producer);schedulers.append(scheduler);before=len(attempts);samples=[tick(scheduler),tick(scheduler)];scheduler.sync();observed=attempts[before:];check('lpush-denied-exact-attempt-order',[v['headers']['periodic_task_name']for v in observed]==[v.name for v in owned]);check('lpush-denied-reserves-both-without-durable-frame',not f.queue()and all(f.PeriodicTask.objects.get(pk=v.pk).total_run_count==1 for v in owned));report['cases'].append({'name':'lpush-denied','attempts':observed,'samples':samples,'after_sync':rows()});scheduler.close();connection.release();cache.execute_command('ACL','DELUSER','rust_multi_denied')
 # Real elapsed Beat with two known rows; no worker consumes these frames.
 scheduler,owned=begin('elapsed',[('z-first-refresh','refresh',{}),('a-second-series','series',{})]);scheduler.close();interval=f.IntervalSchedule.objects.create(every=1,period=f.IntervalSchedule.SECONDS)
 for row in owned:row.interval=interval;row.save(update_fields=['interval'])
 env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app')
 with Path('/evidence/multi-beat-private.log').open('ab')as log:process=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_timer_observer:app','beat','--scheduler','django_celery_beat.schedulers:DatabaseScheduler','--max-interval','1','--pidfile=','--loglevel=WARNING'],env=env,stdout=log,stderr=log,start_new_session=True)
 deadline=time.monotonic()+20
 while f.broker.llen('celery')<4:
  if process.poll()is not None:raise RuntimeError('Multi Beat exited early')
  if time.monotonic()>deadline:raise TimeoutError('Multi Beat four publications')
  time.sleep(.01)
 os.killpg(process.pid,signal.SIGTERM);process.wait(timeout=15);check('elapsed-graceful-Beat-exit0',process.returncode==0);process=None;check('elapsed-exact-four-delivery-order',delivery()==[owned[0].name,owned[1].name,owned[0].name,owned[1].name]);check('elapsed-graceful-both-count2',all(f.PeriodicTask.objects.get(pk=v.pk).total_run_count==2 for v in owned));check('elapsed-no-unrelated-family',all(v['headers']['task']in[f.refresh_epg_data.name,f.evaluate_series_rules.name]for v in f.queue()));report['cases'].append({'name':'elapsed','after_sync':rows(),'envelopes':f.queue()})
 check('no-worker-result-or-reservation',f.backend.dbsize()==0 and f.broker.hlen('unacked')==0);report['passed']=True
finally:
 before_task_publish.disconnect(publication)
 if process is not None and process.poll()is None:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 for scheduler in schedulers:scheduler.close()
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-scheduler-multi-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
