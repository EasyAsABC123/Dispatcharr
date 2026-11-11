"""Actual native owned timer compared with actual Original Beat/worker results.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Known inactive refresh and Series0; no default application/calendar authority.
"""
import json,os,select,signal,subprocess,time
from pathlib import Path
import source_scheduler_timer as reference
assert reference.report['passed'];f=reference.f;observer=reference.reference
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'source_backend_task_key_pattern':f.app.backend.get_key_for_task('*').decode(),'scope':'Actual native owned scheduler→native Refresh/Series applications→real Original AsyncResult→retained shutdown/SQLsync compared with actual Original Beat; known single interval only, no main/calendar/general heap authority'}
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
def completed():
 keys=list(f.backend.scan_iter(match=f.app.backend.get_key_for_task('*'),count=16))
 if len(keys)>8:raise RuntimeError('Owned result count bound')
 values=[]
 for key in keys:
  raw=f.backend.get(key)
  if len(raw)>65536:raise RuntimeError('Owned result byte bound')
  value=json.loads(raw);values.append({'id':value['task_id'],'metadata':value})
 return values

def stop_worker():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15)
 worker=None
try:
 source=f.EPGSource(name='rust-owned-native-timer-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 interval=f.IntervalSchedule.objects.create(every=1,period=f.IntervalSchedule.SECONDS)
 env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app')
 for expected in reference.report['cases']:
  label=expected['name'];f.broker.flushdb();f.backend.flushdb();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed()
  name='rust-owned-native-real-timer-'+label
  with Path('/evidence/native-scheduler-timer-private.log').open('ab')as log:process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,start_new_session=True)
  buffer=b'';setup=request('setup');check(label+'-native-real-timer-six-defaults',len(setup['installed'])==6 and not setup['failures'])
  task=f.refresh_epg_data if label=='refresh'else f.evaluate_series_rules;row=f.create(name,task.name,[source.pk]if label=='refresh'else[],{},interval)
  check(label+'-native-consumers-prepared-before-intake',request('consumers-start')['consumers_ready']);request('refresh');request('refresh');started=time.monotonic();check(label+'-native-explicit-driver-start',request('driver-start')['started'])
  deadline=started+20
  while True:
   actual=completed()
   if len(actual)>=3:break
   if process.poll()is not None:raise RuntimeError('Native timer peer exited')
   if time.monotonic()>deadline:raise TimeoutError('Native three elapsed tasks')
   time.sleep(.01)
  drained=request('shutdown');check(label+'-native-driver-and-SQL-drained',drained['driver_drained']and drained['drained']);check(label+'-native-both-consumers-drained',drained['refresh_drained']and drained['series_drained']);process.stdin.close();process.wait(timeout=10);check(label+'-native-timer-exit0',process.returncode==0);process=None
  actual=completed();check(label+'-native-exact-three-distinct-publications',len(actual)==3 and len({v['id']for v in actual})==3)
  results=[]
  for item in actual:
   ident=item['id'];metadata=item['metadata'];normal=None if label=='refresh'else{'scheduled':0,'details':[]}
   check(label+'-native-actual-Original-result',metadata['status']=='SUCCESS'and metadata['result']==normal and f.app.AsyncResult(ident).get(timeout=2)==normal and metadata['children']==[]);results.append(metadata)
  row.refresh_from_db();check(label+'-native-exact-SQL-count-after-close',row.total_run_count==expected['count_after_close']and row.last_run_at is not None);check(label+'-native-no-queue-or-reservations',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0)
  report['cases'].append({'name':label,'elapsed_seconds':time.monotonic()-started,'results_observed':actual,'count_after_close':row.total_run_count,'results':results})
 stop_worker();report['passed']=True
finally:
 if not report['passed']:
  head=f.broker.lindex('celery',-1)
  keys=list(f.backend.scan_iter(match=f.app.backend.get_key_for_task('*'),count=16))
  if len(keys)>64:raise RuntimeError('Diagnostic backend key count bound')
  report['failure_diagnostics']={'queue_count':f.broker.llen('celery'),'oldest_frame':json.loads(head)if head is not None and len(head)<=65536 else {'oversized':head is not None},'unacked_count':f.broker.hlen('unacked'),'unacked_scores':f.broker.zcard('unacked_index'),'backend_result_count':len(keys),'result_scan_pattern':f.app.backend.get_key_for_task('*').decode(),'source_rows':list(f.EPGSource.objects.filter(pk__in=sources).values('id','is_active','status','file_path','custom_properties')),'periodic_rows':list(f.PeriodicTask.objects.filter(name__startswith='rust-owned-native-real-timer-').values('name','enabled','total_run_count','last_run_at'))}
  from django.db import connection
  with connection.cursor()as cursor:
   cursor.execute("SELECT application_name,state,wait_event_type,wait_event,left(query,160) FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid LIMIT 16")
   report['failure_diagnostics']['postgresql_activity']=[dict(zip(['application_name','state','wait_event_type','wait_event','query'],row))for row in cursor.fetchall()]
  if process is not None and process.poll()is None:
   try:report['failure_diagnostics']['peer_state']=request('inspect')
   except(Exception,)as error:report['failure_diagnostics']['inspect_error_type']=type(error).__name__
   try:report['failure_diagnostics']['shutdown']=request('shutdown')
   except(Exception,)as error:report['failure_diagnostics']['shutdown_error_type']=type(error).__name__
 if process is not None and process.poll()is None:
  os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 stop_worker();f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-native-scheduler-consumers-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
