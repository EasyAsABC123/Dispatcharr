"""Actual Original eager single-interval tick/mutation/rejection observations.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
No worker consumes these tick publications; copied DB and broker8/result9 only.
"""
import datetime as dt,json
from pathlib import Path
import source_scheduler_worker as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'scope':'Actual eager Original interval ticks/disable/oneoff/cache/sync and real broker LPUSH denial; no native timer/main authority'};sources=[];schedulers=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def state(row):
 row.refresh_from_db();return {'enabled':row.enabled,'count':row.total_run_count,'last_run_present':row.last_run_at is not None}
try:
 f.broker.flushdb();f.backend.flushdb();sid=reference.report['cases'][0]['prerun']['args'][0];source=f.EPGSource(id=sid,name='rust-owned-tick-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(sid);interval=f.IntervalSchedule.objects.create(every=3600,period=f.IntervalSchedule.SECONDS)
 for label,changes,ticks,bulk in [('refresh-due',{},2,False),('series-due',{},2,False),('oneoff-first',{'one_off':True},2,False),('oneoff-completed',{'one_off':True,'total_run_count':1},1,False),('expired',{'expires':f.timezone.now()-dt.timedelta(seconds=1)},1,False),('future-start',{'start_time':f.timezone.now()+dt.timedelta(hours=1)},1,False),('cached-bulk-disabled',{},1,True)]:
  f.broker.flushdb();scheduler=f.new_scheduler();schedulers.append(scheduler);task=f.evaluate_series_rules if label=='series-due'else f.refresh_epg_data;args=[]if label=='series-due'else[sid];kwargs={}if label=='series-due'else{'force':True};row=f.PeriodicTask.objects.create(name='rust-owned-tick-'+label,task=task.name,interval=interval,args=json.dumps(args),kwargs=json.dumps(kwargs),headers='{}',last_run_at=f.timezone.now()-dt.timedelta(hours=2),**changes)
  scheduler.schedule;scheduler.schedule
  if bulk:f.PeriodicTask.objects.filter(pk=row.pk).update(enabled=False)
  samples=[]
  for i in range(ticks):
   marker=f.PeriodicTasks.last_change();delay=scheduler.tick();scheduler.sync();samples.append({'delay':delay,'state':state(row),'queue_count':f.broker.llen('celery'),'marker_changed':f.PeriodicTasks.last_change()!=marker})
  check(label+'-bounded-delay',all(isinstance(v['delay'],(float,int))and 0<=v['delay']<=5 for v in samples))
  if label in ['refresh-due','series-due']:check(label+'-one-publication-count1',samples[0]['queue_count']==1 and samples[0]['state']['count']==1 and samples[-1]['queue_count']==1)
  if label=='oneoff-first':check(label+'-next-tick-disables-and-resets',samples[0]['queue_count']==1 and samples[1]['state']=={'enabled':False,'count':0,'last_run_present':False}and samples[1]['marker_changed'])
  if label in ['oneoff-completed','expired']:check(label+'-no-publication-disabled',samples[0]['queue_count']==0 and not samples[0]['state']['enabled'])
  if label=='future-start':check(label+'-no-publication',samples[0]['queue_count']==0 and samples[0]['state']['enabled'])
  if label=='cached-bulk-disabled':check(label+'-stale-enabled-publishes-but-save-clears-last-run',samples[0]['queue_count']==1 and samples[0]['state']=={'enabled':False,'count':1,'last_run_present':False})
  report['cases'].append({'name':label,'changes':{k:v.isoformat()if hasattr(v,'isoformat')else v for k,v in changes.items()},'samples':samples,'envelopes':f.queue()});scheduler.close()
 report['scheduler_config']={'max_interval':scheduler.max_interval,'sync_every':scheduler.sync_every,'sync_every_tasks':scheduler.sync_every_tasks}
 # This is a real producer dependency, not a replaced error/task implementation.
 cache=f.redis.Redis(host='localhost',password='playback-fixture-only',db=0);cache.execute_command('ACL','SETUSER','rust_scheduler_denied','reset','on','>playback-fixture-only','~*','&*','+@all','-lpush')
 connection=f.app.connection_for_write('redis://rust_scheduler_denied:playback-fixture-only@localhost:6379/8',connect_timeout=1)
 def producer(*args,**kwargs):return f.app.amqp.Producer(connection,**kwargs)
 scheduler=f.DatabaseScheduler(app=f.app,lazy=False,max_interval=5,Producer=producer);schedulers.append(scheduler);f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();scheduler.schedule;scheduler.schedule;row=f.create('rust-owned-tick-lpush-denied',f.refresh_epg_data.name,[sid],{'force':True},interval);f.broker.flushdb();delay=scheduler.tick();scheduler.sync();check('lpush-denied-tick-still-zero-count1-no-queue',delay==0 and state(row)['count']==1 and not f.queue());report['rejected_publication']={'delay':delay,'state':state(row),'queue_count':f.broker.llen('celery'),'fixture_control':'Constructor Producer factory returns a real Kombu producer connected as the owned LPUSH-denied ACL user; no method/task replacement.'};connection.release();cache.execute_command('ACL','DELUSER','rust_scheduler_denied');report['passed']=True
finally:
 for scheduler in schedulers:scheduler.close()
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-scheduler-tick-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
