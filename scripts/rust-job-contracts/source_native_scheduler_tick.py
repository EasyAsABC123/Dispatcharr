"""Actual native SQL startup/tick/sync/publication with Original worker/results.
Original Dispatcharr/Celery/framework authors retain credit/licenses.
Known single intervals only; no default timer/calendar/unported task authority.
"""
import base64,datetime as dt,json,os,select,signal,subprocess,time
from pathlib import Path
import source_scheduler_tick as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'results':[],'scope':'Actual native default SQL startup→single-known-interval tick→SQLsync→real periodic publication→Original worker results; no calendar/general heap/timer/main authority'};process=None;worker=None;buffer=b'';sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def request(action):
 global buffer
 process.stdin.write((json.dumps({'action':action})+'\n').encode());process.stdin.flush();end=time.monotonic()+30
 while time.monotonic()<end:
  while b'\n'in buffer:
   line,buffer=buffer.split(b'\n',1)
   if line.startswith(b'NATIVE_DATABASE_SCHEDULER_JSON='):return json.loads(line.split(b'=',1)[1])
  ready,_,_=select.select([process.stdout],[],[],.2)
  if ready:
   value=os.read(process.stdout.fileno(),65536)
   if not value:raise RuntimeError('Native tick peer exited')
   buffer+=value
   if len(buffer)>2*1024*1024:raise RuntimeError('Peer record bound')
 raise TimeoutError('Native tick command observation deadline')
def open_peer(broker=None):
 global process,buffer
 buffer=b'';env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database)
 if broker:env['CELERY_BROKER_URL']=broker
 with Path('/evidence/native-scheduler-tick-stderr-private.log').open('ab')as log:process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,bufsize=0)
 value=request('setup');check('native-eager-six-defaults-setup',len(value['installed'])==6 and not value['failures'])
def close_peer():
 global process
 if process is None:return
 result=request('shutdown');check('native-scheduler-and-producers-drained',result['drained']);process.stdin.close();process.wait(timeout=10);check('native-tick-peer-exit0',process.returncode==0);process=None
def projection(envelope):
 v=json.loads(json.dumps(envelope));v['headers']['id']=v['headers']['root_id']=v['properties']['correlation_id']='<owned-task-id>';v['headers']['origin']='<runtime-origin>';v['properties']['reply_to']='<random-reply>';v['properties']['delivery_tag']='<random-delivery>';v['body']=json.loads(base64.b64decode(v['body']));return v
def consume(label,envelopes):
 global worker
 if not envelopes:return
 from source_scheduler_worker import wait,rows
 with Path('/evidence/periodic-source-worker-private.log').open('ab')as log:worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-native-tick@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 for envelope in envelopes:
  ident=envelope['headers']['id'];post=wait(lambda:next((v for v in rows('postrun')if v['id']==ident),None));metadata=json.loads(f.backend.get(f.app.backend.get_key_for_task(ident)));check('native-tick-'+label+'-actual-Original-worker-result',post['state']=='SUCCESS'and metadata['result']==({'scheduled':0,'details':[]}if envelope['headers']['task']==f.evaluate_series_rules.name else None));check('native-tick-'+label+'-actual-AsyncResult',f.app.AsyncResult(ident).get(timeout=2)==metadata['result']);report['results'].append({'name':label,'metadata':metadata})
 os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15);worker=None;check('native-tick-'+label+'-consumed-direct-native-queue',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0)
try:
 sid=reference.reference.report['cases'][0]['prerun']['args'][0];source=f.EPGSource(id=sid,name='rust-owned-tick-native-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(sid);interval=f.IntervalSchedule.objects.create(every=3600,period=f.IntervalSchedule.SECONDS)
 expected=[]
 for observed in reference.report['cases']:
  f.broker.flushdb();f.backend.flushdb();label=observed['name'];f.PeriodicTask.objects.filter(name='rust-owned-tick-'+label).delete();open_peer();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed()
  changes={k:dt.datetime.fromisoformat(v)if k in ['expires','start_time']else v for k,v in observed['changes'].items()};task=f.evaluate_series_rules if label=='series-due'else f.refresh_epg_data;args=[]if label=='series-due'else[sid];kwargs={}if label=='series-due'else{'force':True};row=f.PeriodicTask.objects.create(name='rust-owned-tick-'+label,task=task.name,interval=interval,args=json.dumps(args),kwargs=json.dumps(kwargs),headers='{}',last_run_at=f.timezone.now()-dt.timedelta(hours=2),**changes)
  request('refresh');request('refresh')
  if label=='cached-bulk-disabled':f.PeriodicTask.objects.filter(pk=row.pk).update(enabled=False)
  samples=[]
  for original in observed['samples']:
   marker=f.PeriodicTasks.last_change();value=request('tick');request('sync');sample={'delay':value['delay_seconds'],'state':reference.state(row),'queue_count':f.broker.llen('celery'),'marker_changed':f.PeriodicTasks.last_change()!=marker};check('native-'+label+'-exact-tick-sql-queue-marker',sample==original);samples.append(sample)
  envelopes=f.queue();check('native-'+label+'-same-publication-count',len(envelopes)==len(observed['envelopes']))
  if envelopes:
   check('native-'+label+'-same-semantic-periodic-frame',[projection(v)for v in envelopes]==[projection(v)for v in observed['envelopes']])
   expected.extend((label,v)for v in envelopes)
  report['cases'].append({'name':label,'samples':samples,'envelopes':envelopes});close_peer();consume(label,envelopes)
 f.broker.flushdb();f.backend.flushdb();cache=f.redis.Redis(host='localhost',password='playback-fixture-only',db=0);cache.execute_command('ACL','SETUSER','rust_scheduler_denied','reset','on','>playback-fixture-only','~*','&*','+@all','-lpush')
 f.PeriodicTask.objects.filter(name='rust-owned-tick-lpush-denied').delete();open_peer('redis://rust_scheduler_denied:playback-fixture-only@localhost:6379/8');f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();row=f.create('rust-owned-tick-lpush-denied',f.refresh_epg_data.name,[sid],{'force':True},interval);value=request('tick');request('sync');observed={'delay':value['delay_seconds'],'state':reference.state(row),'queue_count':f.broker.llen('celery')};check('native-real-LPUSH-denial-keeps-Source-reservation',observed=={k:v for k,v in reference.report['rejected_publication'].items()if k!='fixture_control'}and value['publication']['accepted']is False);report['rejected_publication']=observed;close_peer();cache.execute_command('ACL','DELUSER','rust_scheduler_denied');check('native-no-unexpected-reservations',f.broker.hlen('unacked')==0);report['passed']=True
finally:
 if process is not None and process.poll()is None:process.terminate();process.wait(timeout=10)
 if worker is not None and worker.poll()is None:os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15)
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-native-scheduler-tick-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
