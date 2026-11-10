"""Actual HTTP→Original three-family worker→second import→guide workflow.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
Only copied database and isolated broker8/result9; no descendants quarantined.
"""
import asyncio,datetime as dt,hashlib,json,os,signal,subprocess,sys,time,threading
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_')or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in database):raise RuntimeError('Copied database required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis,requests
from core.models import CoreSettings
from django.core.cache import cache as dc
from apps.accounts.models import User
from apps.channels.models import Channel,Recording
from apps.epg.models import EPGSource,EPGData,ProgramData,EPGSourceIndex
from rest_framework_simplejwt.tokens import RefreshToken
from dispatcharr.celery import app
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
report={'passed':False,'checks':[],'imports':[],'scope':'Actual Original HTTP→refresh+Index+Series0→second import→Current/programme APIs; no descendant route override'};processes=[];owned=[];users=[];channel=None
target=os.environ.get('WORKFLOW_TARGET_URL','http://localhost:5657');native=bool(os.environ.get('WORKFLOW_TARGET_URL'));events=[];event_errors=[];event_ready=threading.Event();event_stop=threading.Event()
def collect_events():
 async def collect():
  from channels.layers import get_channel_layer
  layer=get_channel_layer();name=await layer.new_channel('rust_workflow.');await layer.group_add('updates',name);event_ready.set()
  try:
   while not event_stop.is_set():
    try:value=await asyncio.wait_for(layer.receive(name),timeout=.25)
    except asyncio.TimeoutError:continue
    if len(events)>=256 or len(json.dumps(value,default=str))>65536:raise RuntimeError('Event bound')
    events.append(value)
  finally:await layer.group_discard('updates',name);await layer.close_pools()
 try:asyncio.run(collect())
 except BaseException as error:event_errors.append(type(error).__name__);event_ready.set()
event_thread=threading.Thread(target=collect_events,daemon=True)

def check(name,ok):
 report['checks'].append({'name':name,'passed':bool(ok)})
 if not ok:raise AssertionError(name)
def wait(fn,seconds=50):
 until=time.monotonic()+seconds
 while time.monotonic()<until:
  value=fn()
  if value:return value
  time.sleep(.03)
 raise TimeoutError('Owned complete workflow deadline')
def rows(kind):
 p=Path('/evidence/workflow-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def start(args,name):
 with Path('/evidence/'+name+'-private.log').open('ab')as log:p=subprocess.Popen(args,env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 processes.append(p);return p
try:
 broker.flushdb();backend.flushdb()
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 CoreSettings.objects.update_or_create(key='dvr_settings',defaults={'name':'Owned no rules','value':{'series_rules':[]}})
 dc.delete(CoreSettings.group_cache_key('dvr_settings'));dc.delete(CoreSettings.group_cache_ver_key('dvr_settings'))
 recordings=list(Recording.objects.order_by('id').values())
 root=Path('/evidence/workflow-files');root.mkdir(exist_ok=True);root.chmod(0o755);guide=root/'guide.xml';now=dt.datetime.fromisoformat(os.environ['WORKFLOW_CLOCK'])if 'WORKFLOW_CLOCK'in os.environ else dt.datetime.now(dt.timezone.utc).replace(microsecond=0);report['fixture_clock']=now.isoformat();begin=now-dt.timedelta(minutes=15);end=now+dt.timedelta(hours=2)
 def xml(title):guide.write_text('<tv><channel id="workflow-owned"><display-name>Workflow owned</display-name></channel><programme channel="workflow-owned" start="'+begin.strftime('%Y%m%d%H%M%S +0000')+'" stop="'+end.strftime('%Y%m%d%H%M%S +0000')+'"><title>'+title+'</title></programme></tv>')
 xml('Owned first import');guide.chmod(0o644)
 source=EPGSource(name='rust-complete-workflow',source_type='xmltv',is_active=True,file_path=str(guide),custom_properties={});EPGSource.objects.bulk_create([source]);owned.append(source.pk)
 epg=EPGData.objects.create(epg_source=source,tvg_id='workflow-owned',name='Workflow owned');channel=Channel(name='rust-complete-workflow',channel_number=1987666,epg_data=epg,uuid='117e9aa2-802c-4adb-985c-404017923012');Channel.objects.bulk_create([channel])
 user=User.objects.create(username='rust-complete-workflow-admin',password='!',user_level=10,is_active=True,custom_properties={});users.append(user.pk);token=str(RefreshToken.for_user(user).access_token);headers={'Authorization':'Bearer '+token,'Accept':'application/json'}
 # Remove only known startup plugin publication before intake. No descendants are removed.
 startup=broker.lrange('celery',0,63);check('startup-only-known-plugin',all(json.loads(v)['headers']['task']=='apps.plugins.tasks.refresh_plugin_repos'for v in startup));report['startup_removed']=[hashlib.sha256(v).hexdigest()for v in startup];broker.delete('celery')
 if not native:start(['/dispatcharrpy/bin/uwsgi','--http','127.0.0.1:5657','--module','dispatcharr.wsgi:application','--master','--workers','1','--threads','1','--die-on-term','--need-app'],'workflow-web')
 def ready():
  try:return requests.get(target+'/api/core/version/',timeout=1).status_code==200
  except requests.RequestException:return False
 wait(ready)
 # Startup hooks in this owned copied web may publish only plugin work; quarantine before worker.
 startup=broker.lrange('celery',0,63);check('web-startup-only-known-plugin',all(json.loads(v)['headers']['task']=='apps.plugins.tasks.refresh_plugin_repos'for v in startup));broker.delete('celery')
 if not native:start(['/dispatcharrpy/bin/celery','-A','refresh_workflow_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-workflow@fixture'],'workflow-worker')
 if not native:wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
 event_thread.start();check('event-subscriber-ready',event_ready.wait(5)and not event_errors)
 for number,title in [(1,'Owned first import'),(2,'Owned second import')]:
  xml(title);before=len(rows('postrun'));before_keys=set(backend.keys('celery-tasks:celery-task-meta-*'));event_before=len(events);r=requests.post(target+'/api/epg/import/',headers=headers,json={'id':source.pk,'force':True},timeout=10)
  check('import'+str(number)+'-http202',r.status_code==202 and r.json()=={'success':True,'message':'EPG data refresh initiated.'})
  if native:
   def terminals():
    keys=set(backend.keys('celery-tasks:celery-task-meta-*'))-before_keys
    if len(keys)>3:raise RuntimeError('Unexpected task count')
    return [json.loads(backend.get(k))for k in sorted(keys)]if len(keys)==3 else None
   metas=wait(terminals);parent=next(v for v in metas if 'parent_id'not in v);child_ids=[v[0][0]for v in parent['children']];names=['apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules'];by_id={v['task_id']:v for v in metas};completed=[{'task':'apps.epg.tasks.refresh_epg_data','id':parent['task_id'],'state':parent['status'],'result':parent['result']}]+[{'task':name,'id':ident,'state':by_id[ident]['status'],'result':by_id[ident]['result']}for name,ident in zip(names,child_ids)]
  else:completed=wait(lambda:rows('postrun')[before:]if len(rows('postrun'))>=before+3 else None)
  check('import'+str(number)+'-all-three-normal',len(completed)==3 and all(v['state']=='SUCCESS'for v in completed)and {v['task']for v in completed}=={'apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules'})
  refresh=next(v for v in completed if v['task']=='apps.epg.tasks.refresh_epg_data');meta=json.loads(backend.get(app.backend.get_key_for_task(refresh['id'])));check('import'+str(number)+'-two-result-children',len(meta['children'])==2)
  for row in completed:
   result=app.AsyncResult(row['id']);value=result.get(timeout=2);ttl=backend.ttl(app.backend.get_key_for_task(row['id']));check('resultTTL'+str(number)+'-'+row['task'],3580<=ttl<=3600);check('result'+str(number)+'-'+row['task'],result.successful()and(value is None if row['task']!='apps.channels.tasks.evaluate_series_rules'else value=={'scheduled':0,'details':[]}))
  index=EPGSourceIndex.objects.get(source=source);check('import'+str(number)+'-stored-index',bool(index.data));source.refresh_from_db();check('import'+str(number)+'-source-success',source.status==EPGSource.STATUS_SUCCESS);check('import'+str(number)+'-programme',ProgramData.objects.filter(epg=epg,title=title).exists())
  current=requests.post(target+'/api/epg/current-programs/',headers=headers,json={'channel_uuids':[str(channel.uuid)]},timeout=10)
  programmes=requests.get(target+'/api/epg/programs/',headers=headers,params={'title':title},timeout=10)
  check('import'+str(number)+'-current-and-guide',current.status_code==200 and title in current.text and programmes.status_code==200 and title in programmes.text)
  check('import'+str(number)+'-queue-and-locks-drained',broker.llen('celery')==0 and broker.hlen('unacked')==0 and broker.zcard('unacked_index')==0 and all(cache.get(k)is None for k in ['task_lock_refresh_epg_data_'+str(source.pk),'task_lock_epg_source_file_'+str(source.pk),'task_lock_evaluate_series_rules_all']))
  for child in completed:
   if child['id']!=refresh['id']:
    child_meta=json.loads(backend.get(app.backend.get_key_for_task(child['id'])));check('import'+str(number)+'-child-parent-'+child['task'],child_meta.get('parent_id')==refresh['id'])
  wait(lambda:len(events)>event_before);check('import'+str(number)+'-realtime-refresh',any(isinstance(v.get('data'),dict)and v['data'].get('type')=='epg_refresh'for v in events[event_before:]))
  report['imports'].append({'iteration':number,'http':{'status':r.status_code,'body':r.json()},'completed':completed,'metadata':meta,'source_status':source.status,'index':index.data,'current':current.json(),'programmes':programmes.json(),'events':events[event_before:],'file_sha256':hashlib.sha256(guide.read_bytes()).hexdigest(),'file_mode':oct(guide.stat().st_mode&0o777)})
 check('only-three-owned-families-executed',all(v['task']in {'apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules'}for v in rows('prerun')))
 check('recordings-unchanged',recordings==list(Recording.objects.order_by('id').values()));check('event-reader-no-errors',not event_errors);report['native_candidate']=native;report['path_context']={'file':str(guide),'uid':os.getuid(),'cwd':os.getcwd(),'workers':rows('prerun')};report['passed']=True
finally:
 report['observed_events']=events
 report['precleanup_sources']=list(EPGSource.objects.filter(pk__in=owned).values('id','status','last_message','file_path','custom_properties'))
 report['precleanup_results']=[]
 for key in backend.keys('celery-tasks:celery-task-meta-*')[:16]:
  value=backend.get(key)
  if value is not None and len(value)<=262144:report['precleanup_results'].append(json.loads(value))
 report['precleanup_queue']=[json.loads(v)for v in broker.lrange('celery',0,15)if len(v)<=65536]
 report['precleanup_unacked_count']=broker.hlen('unacked')
 report['precleanup_owned_locks']={key:cache.get(key).decode(errors='replace')if cache.get(key)is not None else None for sid in owned for key in ['task_lock_refresh_epg_data_'+str(sid),'task_lock_epg_source_file_'+str(sid)]}
 if 'guide'in globals()and guide.exists():report['precleanup_file']={'path':str(guide),'bytes':guide.stat().st_size,'mode':oct(guide.stat().st_mode&0o777),'parent_mode':oct(guide.parent.stat().st_mode&0o777),'sha256':hashlib.sha256(guide.read_bytes()).hexdigest()}
 event_stop.set()
 if event_thread.is_alive():event_thread.join(5)
 report['event_reader_errors']=event_errors
 for process in reversed(processes):
  if process.poll()is None:
   os.killpg(process.pid,signal.SIGTERM)
   try:process.wait(timeout=15)
   except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5);report.setdefault('forced_cleanup',[]).append(True)
 if channel is not None:Channel.objects.filter(pk=channel.pk).delete()
 EPGSource.objects.filter(pk__in=owned).delete();User.objects.filter(pk__in=users).delete();broker.flushdb();backend.flushdb();report['queue_and_results_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-refresh-workflow-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
