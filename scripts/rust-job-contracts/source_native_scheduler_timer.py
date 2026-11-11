"""Actual native owned timer compared with actual Original Beat/worker results.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Known inactive refresh and Series0; no default application/calendar authority.
"""
import json,os,select,signal,subprocess,time
from pathlib import Path
import source_scheduler_timer as reference
assert reference.report['passed'];f=reference.f;observer=reference.reference
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'scope':'Actual native owned scheduler default setup→elapsed interval→real Original worker/results→retained final SQLsync compared with actual Original Beat; known single interval only, no main/calendar/general heap authority'}
process=None;worker=None;buffer=b'';sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def request(action):
 global buffer
 process.stdin.write((json.dumps({'action':action})+'\n').encode());process.stdin.flush();deadline=time.monotonic()+20
 while time.monotonic()<deadline:
  while b'\n'in buffer:
   line,buffer=buffer.split(b'\n',1)
   if line.startswith(b'NATIVE_DATABASE_SCHEDULER_JSON='):return json.loads(line.split(b'=',1)[1])
  ready,_,_=select.select([process.stdout],[],[],.1)
  if ready:
   data=os.read(process.stdout.fileno(),65536)
   if not data:raise RuntimeError('Native timer exited')
   buffer+=data
   if len(buffer)>2*1024*1024:raise RuntimeError('Response bound')
 raise TimeoutError('Native timer command')
def stop_worker():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15)
 worker=None
try:
 source=f.EPGSource(name='rust-owned-native-timer-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 interval=f.IntervalSchedule.objects.create(every=1,period=f.IntervalSchedule.SECONDS)
 env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app')
 with Path('/evidence/native-timer-original-worker-private.log').open('ab')as log:worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-native-timer@fixture'],env=env,stdout=log,stderr=log,start_new_session=True)
 for expected in reference.report['cases']:
  label=expected['name'];f.broker.flushdb();f.backend.flushdb();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed()
  name='rust-owned-native-real-timer-'+label
  with Path('/evidence/native-scheduler-timer-private.log').open('ab')as log:process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,start_new_session=True)
  buffer=b'';setup=request('setup');check(label+'-native-real-timer-six-defaults',len(setup['installed'])==6 and not setup['failures'])
  task=f.refresh_epg_data if label=='refresh'else f.evaluate_series_rules;row=f.create(name,task.name,[source.pk]if label=='refresh'else[],{},interval);old_count=len(observer.rows('prerun'))
  request('refresh');request('refresh');started=time.monotonic();check(label+'-native-explicit-driver-start',request('driver-start')['started'])
  deadline=started+20
  while True:
   actual=[v for v in observer.rows('prerun')[old_count:]if v['headers']=={'periodic_task_name':name}]
   if len(actual)>=3:break
   if process.poll()is not None:raise RuntimeError('Native timer peer exited')
   if time.monotonic()>deadline:raise TimeoutError('Native three elapsed tasks')
   time.sleep(.01)
  drained=request('shutdown');check(label+'-native-driver-and-SQL-drained',drained['driver_drained']and drained['drained']);process.stdin.close();process.wait(timeout=10);check(label+'-native-timer-exit0',process.returncode==0);process=None
  actual=[v for v in observer.rows('prerun')[old_count:]if v['headers']=={'periodic_task_name':name}];check(label+'-native-exact-three-distinct-publications',len(actual)==3 and len({v['id']for v in actual})==3)
  results=[]
  for item in actual:
   ident=item['id'];post=observer.wait(lambda:next((v for v in observer.rows('postrun')if v['id']==ident),None));metadata=json.loads(f.backend.get(f.app.backend.get_key_for_task(ident)));normal=None if label=='refresh'else{'scheduled':0,'details':[]}
   check(label+'-native-actual-Original-result',post['state']=='SUCCESS'and metadata['status']=='SUCCESS'and metadata['result']==normal and f.app.AsyncResult(ident).get(timeout=2)==normal and metadata['children']==[]);results.append(metadata)
  row.refresh_from_db();check(label+'-native-exact-SQL-count-after-close',row.total_run_count==expected['count_after_close']and row.last_run_at is not None);check(label+'-native-no-queue-or-reservations',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0)
  report['cases'].append({'name':label,'elapsed_seconds':time.monotonic()-started,'prerun':actual,'count_after_close':row.total_run_count,'results':results})
 stop_worker();report['passed']=True
finally:
 if process is not None and process.poll()is None:
  os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 stop_worker();f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-native-scheduler-timer-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
