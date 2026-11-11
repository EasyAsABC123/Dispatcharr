"""Real Original periodic producer/worker/result compatibility on copied data.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
No backup/recording/remote provider execution; only inactive refresh and Series0.
"""
import json,os,signal,subprocess,time
from pathlib import Path
import source_scheduler_publication as f
assert f.report['passed']
from core.models import CoreSettings
from django.core.cache import cache as dc
from apps.channels.models import Recording
report={'passed':False,'checks':list(f.report['checks']),'cases':[],'scope':'Actual Original eager periodic tick→real inactive-refresh/Series0 worker→Redis result metadata; no native worker or main authority'};worker=None;sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def wait(fn):
 end=time.monotonic()+35
 while time.monotonic()<end:
  value=fn()
  if value:return value
  time.sleep(.02)
 raise TimeoutError('Owned periodic worker deadline')
def rows(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
try:
 f.broker.flushdb();f.backend.flushdb();cache=f.redis.Redis(host='localhost',password='playback-fixture-only',db=0)
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 CoreSettings.objects.update_or_create(key='dvr_settings',defaults={'name':'Owned periodic Series0','value':{'series_rules':[]}});dc.delete(CoreSettings.group_cache_key('dvr_settings'));dc.delete(CoreSettings.group_cache_ver_key('dvr_settings'))
 recordings=list(Recording.objects.order_by('id').values())
 source=f.EPGSource(name='rust-owned-periodic-worker-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 interval=f.IntervalSchedule.objects.create(every=3600,period=f.IntervalSchedule.SECONDS)
 expected=[]
 for label,task,args,kwargs in [('refresh',f.refresh_epg_data,[source.pk],{'force':True}),('series',f.evaluate_series_rules,[],{})]:
  scheduler=f.new_scheduler();row=f.create('rust-owned-periodic-worker-'+label,task.name,args,kwargs,interval);scheduler.tick();scheduler.sync();row.enabled=False;row.save(update_fields=['enabled']);envelope=next(v for v in f.queue()if v['headers'].get('periodic_task_name')==row.name);expected.append((label,task,envelope))
 check('exact-two-owned-periodic-families',len(f.queue())==2 and {v['headers']['task']for v in f.queue()}=={f.refresh_epg_data.name,f.evaluate_series_rules.name})
 with Path('/evidence/periodic-source-worker-private.log').open('ab')as log:worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-periodic@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 for label,task,envelope in expected:
  ident=envelope['headers']['id'];post=wait(lambda:next((v for v in rows('postrun')if v['id']==ident),None));prerun=next(v for v in rows('prerun')if v['id']==ident);metadata=json.loads(f.backend.get(f.app.backend.get_key_for_task(ident)))
  check(label+'-periodic-marker-visible-to-actual-handler-request',prerun['headers']=={'periodic_task_name':envelope['headers']['periodic_task_name']});check(label+'-actual-normal-result',post['state']=='SUCCESS'and metadata['status']=='SUCCESS'and metadata['result']==(None if label=='refresh'else{'scheduled':0,'details':[]}));check(label+'-ordinary-result-fields-no-periodic-extension',set(metadata)=={'status','result','traceback','children','date_done','task_id'}and metadata['traceback']is None and metadata['children']==[])
  check(label+'-actual-AsyncResult-decoding',f.app.AsyncResult(ident).get(timeout=2)==metadata['result']);ttl=f.backend.ttl(f.app.backend.get_key_for_task(ident));check(label+'-actual-result-expiry',3580<=ttl<=3600)
  report['cases'].append({'name':label,'envelope':envelope,'prerun':prerun,'postrun':post,'metadata':metadata,'ttl':ttl})
 check('only-owned-families-executed',len(rows('prerun'))==2 and len(rows('postrun'))==2);check('no-recording-or-queue-residue',recordings==list(Recording.objects.order_by('id').values())and f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0);report['passed']=True
finally:
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 for scheduler in f.schedulers:scheduler.close()
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-scheduler-worker-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
