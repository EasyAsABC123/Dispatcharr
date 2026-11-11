"""Actual native multi-entry timer+native consumers→Original AsyncResult.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
No queue replay/descendant quarantine; inactive refresh and Series0 only.
"""
import json,os,select,signal,subprocess,time
from pathlib import Path
import source_scheduler_multi_worker as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'scope':'Actual Original/native two-known-interval timer→real native Refresh/Series applications→actual Original AsyncResult→SQL+retained shutdown; no main/general task/calendar authority'}
process=None;buffer=b'';sources=[]
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
   if not data:raise RuntimeError('Native multiworker peer exited')
   buffer+=data
   if len(buffer)>2*1024*1024:raise RuntimeError('Response bound')
 raise TimeoutError('Native multiworker command')
def completed():
 keys=list(f.backend.scan_iter(match=f.app.backend.get_key_for_task('*'),count=16))
 if len(keys)>8:raise RuntimeError('Owned result count bound')
 values=[]
 for key in keys:
  raw=f.backend.get(key)
  if len(raw)>65536:raise RuntimeError('Owned result byte bound')
  values.append(json.loads(raw))
 return values
try:
 f.broker.flushdb();f.backend.flushdb();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();sid=reference.report['source_id'];check('native-multiworker-source-id-vacant',not f.EPGSource.objects.filter(pk=sid).exists());f.EPGSource.objects.bulk_create([f.EPGSource(id=sid,name='rust-owned-native-multi-worker-inactive',source_type='xmltv',is_active=False,custom_properties={})]);sources.append(sid);recordings=list(reference.reference.Recording.objects.order_by('id').values());interval=f.IntervalSchedule.objects.create(every=1,period=f.IntervalSchedule.SECONDS);names=reference.report['periodic_names'];owned=[]
 for name,task,args in [(names[0],f.refresh_epg_data,[sid]),(names[1],f.evaluate_series_rules,[])]:f.PeriodicTask.objects.filter(name=name).delete();owned.append(f.create(name,task.name,args,{},interval))
 with Path('/evidence/native-multiworker-stderr-private.log').open('ab')as log:process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,start_new_session=True)
 setup=request('setup');check('native-multiworker-defaults-prepared',len(setup['installed'])==6 and not setup['failures']);check('native-multiworker-both-consumers-prepared-before-intake',request('consumers-start')['consumers_ready']);request('refresh');request('refresh');check('native-multiworker-exact-parent-scan-order',request('inspect')['load_order']==names);started=time.monotonic();request('driver-start');deadline=started+20
 while len(completed())<4:
  if process.poll()is not None:raise RuntimeError('Native multiworker peer exited early')
  if time.monotonic()>deadline:raise TimeoutError('Native multiworker four actual results')
  time.sleep(.01)
 drained=request('shutdown');check('native-multiworker-all-actual-drains',all(drained[k]for k in ['driver_drained','drained','refresh_drained','series_drained']));process.stdin.close();process.wait(timeout=10);check('native-multiworker-peer-exit0',process.returncode==0);process=None;results=completed();check('native-multiworker-exact-four-distinct-results',len(results)==4 and len({v['task_id']for v in results})==4)
 for index,value in enumerate(results):check('native-multiworker-actual-Original-AsyncResult-'+str(index),value['status']=='SUCCESS'and value['result']in[None,{'scheduled':0,'details':[]}]and value['children']==[]and value['traceback']is None and f.app.AsyncResult(value['task_id']).get(timeout=2)==value['result'])
 check('native-multiworker-exact-two-normal-returns-per-family',sum(v['result']is None for v in results)==2 and sum(v['result']=={'scheduled':0,'details':[]}for v in results)==2);counts=[f.PeriodicTask.objects.get(pk=v.pk).total_run_count for v in owned];check('native-multiworker-exact-SQL-counts-after-close',counts==reference.report['persisted_counts']);check('native-multiworker-no-queue-reservations-recording-effects',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0 and recordings==list(reference.reference.Recording.objects.order_by('id').values()));report.update(passed=True,results=results,counts=counts,elapsed_seconds=time.monotonic()-started,source_backend_task_key_pattern=f.app.backend.get_key_for_task('*').decode())
finally:
 if process is not None and process.poll()is None:
  try:report['failure_state']=request('inspect')
  except(Exception,)as error:report['failure_state_error']=type(error).__name__
  os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-native-scheduler-multi-worker-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
