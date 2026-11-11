"""Actual native bounded known-interval heap compared with Original68.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Raw frame order stays exact; true equal-time identity ties have the separately
predeclared own-runtime identity comparator witness, never post-diff sorting.
"""
import base64,datetime as dt,json,os,select,signal,subprocess,time
from pathlib import Path
import source_scheduler_multi_crash as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'scope':'Actual Original/native known multi-interval SQL load/heap/tick/publication/sentinel/oneoff/ACL/elapsed timer and graceful/hardloss restart; no worker/general calendar/unknown task/main authority'}
from capture_scheduler_framework_licenses import capture
report['framework_licenses']=capture()
process=None;buffer=b'';sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def request(action,**kwargs):
 global buffer
 process.stdin.write((json.dumps({'action':action,**kwargs})+'\n').encode());process.stdin.flush();deadline=time.monotonic()+20
 while time.monotonic()<deadline:
  while b'\n'in buffer:
   line,buffer=buffer.split(b'\n',1)
   if line.startswith(b'NATIVE_DATABASE_SCHEDULER_JSON='):return json.loads(line.split(b'=',1)[1])
  ready,_,_=select.select([process.stdout],[],[],.1)
  if ready:
   data=os.read(process.stdout.fileno(),65536)
   if not data:raise RuntimeError('Native multi peer exited')
   buffer+=data
   if len(buffer)>2*1024*1024:raise RuntimeError('Response bound')
 raise TimeoutError('Native multi command')
def open_peer(broker=None,mode=None,marker=None):
 global process,buffer
 buffer=b'';env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database)
 if broker:env['CELERY_BROKER_URL']=broker
 if mode:env.update(SCHEDULER_CRASH_MODE=mode,SCHEDULER_CRASH_GATE=str(marker))
 with Path('/evidence/native-multi-stderr-private.log').open('ab')as log:process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,start_new_session=True)
 value=request('setup');check('native-multi-six-defaults-prepared',len(value['installed'])==6 and not value['failures'])
def close_peer():
 global process
 if process is None:return
 value=request('shutdown');check('native-multi-actual-drain',value['drained']and value.get('driver_drained',True));process.stdin.close();process.wait(timeout=10);check('native-multi-peer-exit0',process.returncode==0);process=None

def delivery():return [v['headers']['periodic_task_name']for v in reversed(f.queue())]
def projection(envelope):
 value=json.loads(json.dumps(envelope));value['headers']['id']=value['headers']['root_id']=value['properties']['correlation_id']='<owned-task-id>';value['headers']['origin']='<runtime-origin>';value['properties']['reply_to']='<random-reply>';value['properties']['delivery_tag']='<random-delivery>';value['body']=json.loads(base64.b64decode(value['body']));return value

def signature(names,raw=None):
 if raw is None:raw=list(f.PeriodicTask.objects.filter(name__in=names).values('name','enabled','total_run_count','last_run_at','one_off'))
 return {v['name']:{'enabled':v['enabled'],'count':v['total_run_count'],'last':v['last_run_at']is not None,'one_off':v['one_off']}for v in raw if v['name']in names}
def create(label,definitions,sid,seconds=3600):
 f.broker.flushdb();f.backend.flushdb();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();interval=f.IntervalSchedule.objects.create(every=seconds,period=f.IntervalSchedule.SECONDS);last=f.timezone.now()-dt.timedelta(hours=2);owned=[]
 for suffix,family,changes in definitions:
  name='rust-owned-multi-'+label+'-'+suffix;f.PeriodicTask.objects.filter(name=name).delete();task=f.refresh_epg_data if family=='refresh'else f.evaluate_series_rules
  args=[sid]if family=='refresh'else[];fields={'name':name,'task':task.name,'args':json.dumps(args),'kwargs':'{}','headers':'{}','interval':interval,'last_run_at':last};fields.update(changes);owned.append(f.PeriodicTask.objects.create(**fields))
 return owned

def sample(owned,**kwargs):
 names=[v.name for v in owned];marker=f.PeriodicTasks.last_change();value=request('tick',**kwargs);return {'delay':value['delay_seconds'],'rows':signature(names),'delivery_names':delivery(),'marker_changed':f.PeriodicTasks.last_change()!=marker,'publication_names':value['publication_names'],'publication':value['publication']}
try:
 # Reuse the actual Source input IDs explicitly; bodies are not normalized away.
 ids=set()
 for case in reference.report['cases']:
  for envelope in case.get('envelopes',[]):
   if envelope['headers']['task']==f.refresh_epg_data.name:ids.add(json.loads(base64.b64decode(envelope['body']))[0][0])
 for sid in ids:
  check('native-source-id-owned-and-vacant-'+str(sid),not f.EPGSource.objects.filter(pk=sid).exists());f.EPGSource.objects.bulk_create([f.EPGSource(id=sid,name='rust-owned-native-multi-inactive-'+str(sid),source_type='xmltv',is_active=False,custom_properties={})]);sources.append(sid)
 source_case={v['name']:v for v in reference.report['cases']};sid=json.loads(base64.b64decode(source_case['created-refresh-first']['envelopes'][1]['body']))[0][0]
 definitions={
  'created-refresh-first':[('z-created-first','refresh',{}),('a-created-second','series',{})],
  'created-series-first':[('z-created-first','series',{}),('a-created-second','refresh',{})],
  'notdue-first':[('z-future-refresh','refresh',{'last_run_at':f.timezone.now()+dt.timedelta(hours=1)}),('a-due-series','series',{})],
  'oneoff':[('z-first-oneoff','refresh',{'one_off':True}),('a-second-regular','series',{})],
  'disable-sentinel':[('z-first-refresh','refresh',{}),('a-second-series','series',{})],
  'delete-sentinel':[('z-first-refresh','refresh',{}),('a-second-series','series',{})],
  'identity-tie':[('z-first-refresh','refresh',{}),('a-second-series','series',{})],
  'lpush-denied':[('z-first-refresh','refresh',{}),('a-second-series','series',{})],
 }
 for label,rows in definitions.items():
  expected=source_case[label];owned=create(label,rows,sid);broker=None
  if label=='lpush-denied':
   cache=f.redis.Redis(host='localhost',password='playback-fixture-only',db=0);cache.execute_command('ACL','SETUSER','rust_native_multi_denied','reset','on','>playback-fixture-only','~*','&*','+@all','-lpush');broker='redis://rust_native_multi_denied:playback-fixture-only@localhost:6379/8'
  open_peer(broker);request('refresh');request('refresh');loaded=request('inspect')['load_order']
  report['active_case']={'name':label,'expected_loaded':expected.get('loaded'),'actual_loaded':loaded,'owned_names':[v.name for v in owned]}
  if 'loaded'in expected:check('native-'+label+'-exact-raw-load-order',loaded==expected['loaded'])
  names=[v.name for v in owned];samples=[];kwargs={};before_heap=None
  if label=='identity-tie':
   clock=dt.datetime.fromtimestamp(expected['samples'][0]['pops'][0]['heap_before_pop'][0]['when'],dt.timezone.utc).isoformat();request('heap-clock',now=clock);request('populate-heap',now=clock);before_heap=request('inspect')['heap'];kwargs={'now':clock};check('native-identity-tie-exact-equal-time-priority',len({(v['when'],v['priority'])for v in before_heap})==1)
  for index,original in enumerate(expected['samples']):
   if index==1 and label=='disable-sentinel':owned[0].enabled=False;owned[0].save(update_fields=['enabled'])
   if index==1 and label=='delete-sentinel':owned[0].delete()
   actual=sample(owned,**kwargs);samples.append(actual)
   if label=='identity-tie':
    if index==0:check('native-identity-tie-own-Source-comparator',actual['publication_names']==[min(before_heap,key=lambda v:v['identity'])['name']])
   else:
    check('native-'+label+'-tick'+str(index)+'-exact-delay-marker-SQL',actual['delay']==original['delay']and actual['marker_changed']==original['marker_changed']and actual['rows']==signature(names,original['rows']))
    check('native-'+label+'-tick'+str(index)+'-exact-delivery-order',actual['delivery_names']==original['delivery_names'])
  request('sync');check('native-'+label+'-exact-final-row-effects',signature(names)==signature(names,expected['after_sync']))
  if label=='identity-tie':
   # Declared before execution: arbitrary object-identity tie order is checked
   # against each runtime's own identities. Frames remain exact by task name.
   actual=f.queue();original={v['headers']['periodic_task_name']:v for v in expected['envelopes']};check('native-identity-tie-two-exact-frames',len(actual)==2 and len({v['headers']['periodic_task_name']for v in actual})==2 and all(projection(v)==projection(original[v['headers']['periodic_task_name']])for v in actual))
  elif label=='lpush-denied':check('native-lpush-denied-real-attempt-order-and-no-queue',[n for value in samples for n in value['publication_names']]==[v['headers']['periodic_task_name']for v in expected['attempts']]and not f.queue()and all(value['publication']['accepted']is False for value in samples))
  else:check('native-'+label+'-exact-ordered-semantic-frames',[projection(v)for v in f.queue()]==[projection(v)for v in expected['envelopes']])
  report['cases'].append({'name':label,'loaded':loaded,'samples':samples,'heap_before_tie':before_heap,'after_sync':signature(names),'envelopes':f.queue()});close_peer()
  if broker:cache.execute_command('ACL','DELUSER','rust_native_multi_denied')
 # Actual elapsed two-entry timer, no worker or queue replay.
 expected=source_case['elapsed'];owned=create('elapsed',[('z-first-refresh','refresh',{}),('a-second-series','series',{})],sid,seconds=1);open_peer();request('refresh');request('refresh');request('driver-start');deadline=time.monotonic()+20
 while f.broker.llen('celery')<4:
  if time.monotonic()>deadline:raise TimeoutError('Native multi elapsed four frames')
  time.sleep(.01)
 close_peer();check('native-elapsed-exact-ordered-four-frames',[projection(v)for v in f.queue()]==[projection(v)for v in expected['envelopes']]);check('native-elapsed-both-count2',all(f.PeriodicTask.objects.get(pk=v.pk).total_run_count==2 for v in owned));report['cases'].append({'name':'elapsed','envelopes':f.queue()})
 # Same actual process-loss boundaries with native before/after sync barriers.
 for mode in ['before-sync','after-sync']:
  label='hardloss-'+mode;expected=source_case['multi-'+label];envelopes=expected['envelopes'];crash_sid=next(json.loads(base64.b64decode(v['body']))[0][0]for v in envelopes if v['headers']['task']==f.refresh_epg_data.name);owned=create(label,[('z-created-first-refresh','refresh',{}),('a-created-second-series','series',{})],crash_sid);marker=Path('/evidence/native-scheduler-crash-multi-'+mode+'.gate');assert not marker.exists();open_peer(mode=mode,marker=marker);process.stdin.write(b'{"action":"tick"}\n');process.stdin.flush();deadline=time.monotonic()+20
  while not marker.exists():
   if process.poll()is not None:raise RuntimeError('Native multi exited before barrier')
   if time.monotonic()>deadline:raise TimeoutError('Native multi crash barrier')
   time.sleep(.02)
  check('native-'+mode+'-multi-exact-loss-rows',signature([v.name for v in owned])=={v['name']:{'enabled':v['enabled'],'count':v['count'],'last':v['last_run_at']is not None,'one_off':False}for v in expected['at_hardloss']});os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5);process=None;restarts=[]
  for _ in range(2):open_peer();restarts.append(request('tick')['delay_seconds']);close_peer()
  check('native-'+mode+'-multi-exact-restart-delays',restarts==[expected['first_restart']['delay'],expected['after_second_restart']['delay']]);check('native-'+mode+'-multi-exact-restart-frames',[projection(v)for v in f.queue()]==[projection(v)for v in expected['envelopes']]);check('native-'+mode+'-multi-final-counts',all(f.PeriodicTask.objects.get(pk=v.pk).total_run_count==1 for v in owned));report['cases'].append({'name':'multi-'+label,'restart_delays':restarts,'envelopes':f.queue()})
 expected=source_case['multi-graceful'];crash_sid=next(json.loads(base64.b64decode(v['body']))[0][0]for v in expected['envelopes']if v['headers']['task']==f.refresh_epg_data.name);owned=create('graceful',[('z-created-first-refresh','refresh',{}),('a-created-second-series','series',{})],crash_sid);open_peer();request('tick');close_peer();open_peer();request('tick');close_peer();check('native-multi-graceful-exact-other-due-frame',[projection(v)for v in f.queue()]==[projection(v)for v in expected['envelopes']]);check('native-multi-no-results-or-unacked',f.backend.dbsize()==0 and f.broker.hlen('unacked')==0);report['passed']=True
finally:
 if not report['passed']:
  from django.db import connection
  with connection.cursor()as cursor:
   sql,params=f.DatabaseScheduler(app=f.app,lazy=True,max_interval=5).enabled_models_qs().query.sql_with_params()
   cursor.execute('EXPLAIN (FORMAT JSON) '+sql,params)
   report['source_queryset_plan']=cursor.fetchone()[0]
   code=Path('/evidence/frozen/rust/crates/job-transport/src/database_schedule.rs').read_text()
   native_sql=code.split('let rows=sqlx::query("',1)[1].split('").fetch_all',1)[0]
   cursor.execute('EXPLAIN (FORMAT JSON) '+native_sql)
   report['native_loader_plan']=cursor.fetchone()[0]
  if process is not None and process.poll()is None:
   try:report['native_failure_state']=request('inspect')
   except(Exception,)as error:report['native_failure_state_error']=type(error).__name__
 if process is not None and process.poll()is None:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-native-scheduler-multi-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
