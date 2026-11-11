"""Actual bounded native known-crontab DB/tick/publication vs Original74.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Future expiry headers remain explicitly unsupported, not silently stripped.
"""
import base64,datetime as dt,json,os,select,signal,subprocess,time
from pathlib import Path
import source_scheduler_calendar as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'scope':'Actual Original/native known bounded crontab/TZif2026e DBquery/tick/sync/publication and real-clock native timer; future expiry option explicitunsupported, no workers/main/default families authority'}
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
   raw=os.read(process.stdout.fileno(),65536)
   if not raw:raise RuntimeError('Native calendar peer exited')
   buffer+=raw
   if len(buffer)>2*1024*1024:raise RuntimeError('Response bound')
 raise TimeoutError('Native calendar command')
def open_peer(clock=None):
 global process,buffer
 buffer=b''
 with Path('/evidence/native-calendar-db-stderr-private.log').open('ab')as log:process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,start_new_session=True)
 setup=request('setup');check('native-calendar-six-defaults-prepared',len(setup['installed'])==6 and not setup['failures'])
 if clock:request('heap-clock',now=clock)
def close_peer():
 global process
 value=request('shutdown');check('native-calendar-actual-drain',value['drained']and value.get('driver_drained',True));process.stdin.close();process.wait(timeout=10);check('native-calendar-exit0',process.returncode==0);process=None

def projection(envelope):
 value=json.loads(json.dumps(envelope));value['headers']['id']=value['headers']['root_id']=value['properties']['correlation_id']='<owned-task-id>';value['headers']['origin']='<runtime-origin>';value['properties']['reply_to']='<random-reply>';value['properties']['delivery_tag']='<random-delivery>';value['body']=json.loads(base64.b64decode(value['body']));return value

def sample(row,clock):
 marker=f.PeriodicTasks.last_change();value=request('tick',now=clock);request('sync');return {'delay':value['delay_seconds'],'state':reference.state(row),'queue_count':f.broker.llen('celery'),'marker_changed':f.PeriodicTasks.last_change()!=marker,'cached_names':request('inspect')['load_order']}
def reset():f.broker.flushdb();f.backend.flushdb();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed()
def make(label,spec,changes):
 cron=reference.makecron(spec);name='rust-owned-calendar-'+label;f.PeriodicTask.objects.filter(name=name).delete();row=f.PeriodicTask.objects.create(name=name,task=f.refresh_epg_data.name,args=json.dumps([sid]),kwargs='{}',headers='{}',crontab=cron,last_run_at=dt.datetime.fromisoformat(spec['last_run_at']),**{k:dt.datetime.fromisoformat(v)if k in['start_time','expires']else v for k,v in changes.items()});return row,cron
try:
 sid=json.loads(base64.b64decode(reference.report['cases'][0]['envelopes'][0]['body']))[0][0];check('native-calendar-source-ID-owned-vacant',not f.EPGSource.objects.filter(pk=sid).exists());f.EPGSource.objects.bulk_create([f.EPGSource(id=sid,name='rust-owned-native-calendar-inactive',source_type='xmltv',is_active=False,custom_properties={})]);sources.append(sid)
 for case in reference.report['cases']:
  label=case['name'];reset();spec=case['input'];open_peer(spec['now']);row,cron=make(label,spec,case.get('changes',{}));request('refresh',now=spec['now']);request('refresh',now=spec['now']);samples=[]
  if label=='future-expiry-header':
   before=reference.state(row);value=request('tick-observe',now=spec['now']);check('future-expiry-header-explicitUnsupported-not-stripped',value.get('error')=='Unsupported'and not f.queue()and reference.state(row)==before);check('source-future-expiry-header-actually-nonnull',case['envelopes'][0]['headers']['expires']is not None);report['cases'].append({'name':label,'native':value,'authority':'Unsupported; expiry protocol+consumer revocation separate gate'});close_peer();continue
  if case.get('start_gate'):
   actual=request('calendar-start',start_time=case['changes']['start_time'],now=spec['now'],**{k:spec.get(k,default)for k,default in [('minute','0'),('hour','4'),('timezone','UTC'),('day_of_week','*'),('day_of_month','*'),('month_of_year','*')]});check('native-calendar-'+label+'-exact-aligned-start-gate',{'aligned_start':actual['aligned_start'],'next_seconds':actual['next_seconds']}=={k:case['start_gate'][k]for k in['aligned_start','next_seconds']})
  for index,expected in enumerate(case['samples']):
   clock=spec['now']
   if label=='ny-spring'and index==2:clock='2026-03-08T07:30:00+00:00'
   if label=='ny-fall'and index==1:clock='2026-11-01T06:30:00+00:00'
   if label=='reload-zone'and index>=1:
    clock='2026-11-11T04:01:00+00:00'
    if index==1:cron.timezone='America/New_York';cron.save()
    else:cron.hour='*';cron.save()
   actual=sample(row,clock);samples.append(actual);report['active_comparison']={'name':label,'step':index,'expected':expected,'actual':actual};check('native-calendar-'+label+'-step'+str(index)+'-exact-SQL-delay-marker-cache',actual==expected)
  check('native-calendar-'+label+'-exact-ordered-frames',[projection(v)for v in f.queue()]==[projection(v)for v in case['envelopes']]);report['cases'].append({'name':label,'samples':samples,'envelopes':f.queue()});close_peer()
 # Actual real-clock native timer for both known crontab families, no worker.
 reset();expected=reference.report['real_beat'];spec={'timezone':'UTC','minute':expected['minute'],'hour':'*'};open_peer();cron=reference.makecron(spec);names=[v['name']for v in expected['states']];owned=[]
 for name,task,args in[(names[0],f.refresh_epg_data,[sid]),(names[1],f.evaluate_series_rules,[])]:f.PeriodicTask.objects.filter(name=name).delete();owned.append(f.PeriodicTask.objects.create(name=name,task=task.name,args=json.dumps(args),kwargs='{}',headers='{}',crontab=cron,last_run_at=dt.datetime.fromisoformat(expected['initial_last_run'])))
 request('refresh');request('refresh');request('driver-start');deadline=time.monotonic()+20
 while f.broker.llen('celery')<2:
  if time.monotonic()>deadline:raise TimeoutError('Native real-clock crontab two frames')
  time.sleep(.01)
 close_peer();check('native-calendar-real-clock-exact-two-frames',[projection(v)for v in f.queue()]==[projection(v)for v in expected['envelopes']]);check('native-calendar-real-clock-both-count1',all(reference.state(v)['count']==1 for v in owned));report['real_beat']={'states':[reference.state(v)for v in owned],'envelopes':f.queue()};report['passed']=True
finally:
 if process is not None and process.poll()is None:
  try:report['failure_state']=request('inspect')
  except(Exception,)as error:report['failure_state_error']=type(error).__name__
  os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];report['tzdb']=reference.report.get('tzdb');Path('/evidence/source-native-scheduler-calendar-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
