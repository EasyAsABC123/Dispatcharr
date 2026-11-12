"""Owned actual Original scanner -> unmodified default worker descendant capture.
No task routing overrides/quarantine. Synthetic local M3U/XML only, no providers.
Original Dispatcharr/Django/Celery/Redis/Channels authors retain credit/licenses.
"""
import base64,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid,select
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from dispatcharr.celery import app
from django.db import connection
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.epg.models import EPGSource,EPGSourceIndex,EPGData,ProgramData
from core.models import CoreSettings,SYSTEM_SETTINGS_KEY
from django_celery_beat.models import PeriodicTask
import core.tasks as scanner
from celery.signals import before_task_publish
from owned_reply_proxy import Proxy
TASK='core.tasks.scan_and_process_files';NOW=1700000000.125
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
assert broker.dbsize()==backend.dbsize()==0
app.loader.import_default_modules()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
layer=get_channel_layer();channel='specific.rust_owned_scanner_children!receiver';channel_key=layer.prefix+channel.split('!')[0]+'!';group_key=layer._group_key('updates')
report={'passed':False,'native_verified':True,'checks':[],'cases':[],'predeclared_cases':['epg-empty-active','epg-programme-active','physical-parent','physical-claim','physical-result'],'scope':'Actual Original scanner producer -> scoped native Refresh -> real Original Index/Series consumers and AsyncResult; synthetic local EPG only, no Main/poller/fullscanner claim'}
worker=None;made=[];publications={}
@before_task_publish.connect
def publication(sender=None,headers=None,body=None,properties=None,exchange=None,routing_key=None,**extra):
 if sender==TASK:publications[headers['id']]={'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def stop():
 global worker
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 worker=None
def start(queue='celery'):
 log=open('/evidence/scanner-worker-private.log','ab');p=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q',queue,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-scanner-children@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();return p
def snapshot(m,e):
 a=list(M3UAccount.objects.filter(file_path=str(m)).values())if m else[];s=list(EPGSource.objects.filter(file_path=str(e)).values())if e else[]
 aids=[x['id']for x in a];sids=[x['id']for x in s]
 from apps.channels.models import Stream,ChannelGroup
 return {'accounts':a,'profiles':list(M3UAccountProfile.objects.filter(m3u_account_id__in=aids).values()),'sources':s,'indices':list(EPGSourceIndex.objects.filter(source_id__in=sids).values()),'epg_data':list(EPGData.objects.filter(epg_source_id__in=sids).values()),'programmes':list(ProgramData.objects.filter(epg__epg_source_id__in=sids).values()),'streams':list(Stream.objects.filter(m3u_account_id__in=aids).values()),'groups':list(ChannelGroup.objects.values()),'events':[layer.deserialize(v)for v in cache.zrange(channel_key,0,255)],'locks':{k.decode():{'raw_base64':base64.b64encode(cache.get(k)).decode(),'ttl':cache.ttl(k)}for k in cache.scan_iter('task_lock_*')},'files':[{'path':str(p),'size':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in made if p.exists()],'cache_files':[{'path':str(p),'size':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'raw_base64':base64.b64encode(p.read_bytes()).decode()if p.stat().st_size<=65536 else None}for root in ['/app/media/cached_m3u','/app/media/cached_epg','/data/epgs']for p in Path(root).rglob('*')if p.is_file()and p not in made]}
def case(label,active,mtext=None,etext=None):
 global worker,made
 stop();check(label+'-prior-default-queue-empty',broker.llen('celery')==broker.hlen('unacked')==0)
 for p in made:
  if p.exists():p.unlink()
 made=[];M3UAccount.objects.all().update(is_active=False);EPGSource.objects.all().update(is_active=False)
 CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':active})
 cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel)
 m=e=None
 for root,text,ext in [('/data/m3us',mtext,'.m3u'),('/data/epgs',etext,'.xml')]:
  if text is not None:
   p=Path(root)/('rust-owned-children-'+label+ext);p.write_text(text);os.utime(p,(NOW-7.125,NOW-7.125));made.append(p)
   if ext=='.m3u':m=p
   else:e=p
 before=snapshot(m,e);prior_ids={v['id']for v in records('postrun')};tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':NOW}));worker=start('rust-owned-scanner-parents');result=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=tid,queue='rust-owned-scanner-parents')
 entry={'name':label,'parent_id':tid,'before':before,'parent_publication':publications[tid]};report['cases'].append(entry)
 end=time.monotonic()+30
 while not any(v['id']==tid for v in records('postrun')):
  assert time.monotonic()<end;time.sleep(.025)
 stop()
 sourceid=EPGSource.objects.get(file_path=str(e)).id
 mode=label.removeprefix('physical-')if label.startswith('physical-')else None
 head=json.loads(broker.lindex('celery',-1));proxy=Proxy(mode,tid,head['headers']['id'])if mode else None
 log=open('/evidence/native-child-'+label+'.log','wb')
 native=subprocess.Popen(['/native-child-peer','scanner_refresh_peer::native_scanner_refresh_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,bufsize=1)
 def receive():
  end=time.monotonic()+35
  while time.monotonic()<end:
   assert native.poll()is None,'Native peer exited'
   if select.select([native.stdout],[],[],.1)[0]:
    line=native.stdout.readline();log.write(line.encode());log.flush()
    if 'NATIVE_SCANNER_REFRESH_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_REFRESH_JSON=',1)[1])
  raise TimeoutError('Native child response')
 def command(v):native.stdin.write(json.dumps(v)+'\n');native.stdin.flush();return receive()
 outcomes=[];nativeids=[]
 try:
  config={'parent_id':tid,'source_id':sourceid}
  if proxy:config['broker_url'if mode=='claim'else'result_url']='redis://:playback-fixture-only@127.0.0.1:'+str(proxy.server.server_address[1])+('/8'if mode=='claim'else'/9')
  prepared=command(config);check(label+'-native-prepared',prepared=={'prepared':True})
  initial=broker.lindex('celery',-1)
  for action in ['legacy','foreign']:
   refused=command({'action':action});check(label+'-'+action+'-refused-before-ack',refused['run']=='Err(Transport(UnsupportedTask))'and broker.lindex('celery',-1)==initial)
  if not proxy:
   key=app.backend.get_key_for_task(tid);prior=backend.get(key);modified=json.loads(prior);modified['children']=[];backend.set(key,json.dumps(modified),keepttl=True)
   refused=command({'action':'run'});check(label+'-unlisted-child-retained',refused['run']=='Err(Transport(UnsupportedTask))'and broker.lindex('celery',-1)==initial);backend.set(key,prior,keepttl=True)
  for number in range(1 if mode else 2):
   frame=json.loads(broker.lindex('celery',-1));check(label+f'-head{number}-actual-parented-refresh',frame['headers']['task']=='apps.epg.tasks.refresh_epg_data'and frame['headers']['root_id']==frame['headers']['parent_id']==tid)
   nativeids.append(frame['headers']['id'])
   if proxy:
    check(label+'-native-owned-work-started',command({'action':'start'})=={'started':True});check(label+'-actual-physical-reply-held',proxy.held.wait(15))
    if mode=='parent':check(label+'-parent-receipt-read-before-ack',broker.lindex('celery',-1)==initial and backend.get(app.backend.get_key_for_task(nativeids[-1]))is None)
    if mode=='claim':check(label+'-real-pop-before-delayed-ack-reply',broker.lindex('celery',-1)!=initial and backend.get(app.backend.get_key_for_task(nativeids[-1]))is None)
    if mode=='result':check(label+'-real-result-committed-before-delayed-reply',backend.get(app.backend.get_key_for_task(nativeids[-1]))is not None)
    answer=command({'action':'abort'});outcomes.append(answer);check(label+'-aborted-caller-admission-shutdown-retained',answer=={'caller_retained':True,'slots':0,'admission':'Err(Capacity)','drain':'Err(DrainTimeout)'})
    proxy.release.set();answer=command({'action':'settled'});check(label+'-actual-completion-releases-owner',answer=={'caller_released':True,'slots':1})
   else:
    answer=command({'action':'run'});outcomes.append(answer);check(label+f'-native{number}-completed',answer['run'].startswith('Ok(Completed(')and answer['available_slots']==1)
   value=app.AsyncResult(nativeids[-1]);check(label+f'-Source-decodes-native{number}-null',value.ready()and value.get(timeout=2)is None)
  check(label+'-native-worker-drained',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(label+'-native-exit0',native.returncode==0)
 finally:
  if native.poll()is None:native.kill();native.wait(timeout=5)
  log.close()
  if proxy:proxy.close();entry['proxy']={'mode':mode,'commands':proxy.count,'errors':proxy.errors};check(label+'-proxy-clean',not proxy.errors)
 frames=[json.loads(v)for v in reversed(broker.lrange('celery',0,63))]
 entry['native_outcomes']=outcomes;entry['native_ids']=nativeids;entry['remaining_frames']=frames
 expected_extra={v['headers']['id']for v in frames}
 worker=start()

 end=time.monotonic()+65;stable=None
 while time.monotonic()<end:
  posts=[v for v in records('postrun')if v['id']not in prior_ids];pubs=[v for v in records('children')if v['headers'].get('root_id')==tid];expected={tid}|set(nativeids)|expected_extra|{v['headers']['id']for v in pubs};done={v['id']for v in posts}|set(nativeids)
  idle=expected<=done and broker.llen('celery')==broker.hlen('unacked')==0
  if idle:
   if stable is None:stable=time.monotonic()
   if time.monotonic()-stable>=.5:break
  else:stable=None
  if worker.poll()is not None:raise RuntimeError('Owned original worker exited')
  time.sleep(.025)
 else:raise TimeoutError('Actual descendants did not drain in fixed65s')
 entry.update(fileposts=[v for v in records('filepost')if v['id']in expected],after=snapshot(m,e),publications=pubs,postruns=posts,preruns=[v for v in records('prerun')if v['id']in expected],received=[v for v in records('received')if v['id']in expected],sql=[v for v in records('sql')if v['id']in expected],results={})
 for taskid in expected:
  key=app.backend.get_key_for_task(taskid);raw=backend.get(key)
  entry['results'][taskid]={'metadata':None if raw is None else json.loads(raw),'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'ttl':backend.ttl(key)}
 check(label+'-parent-null-success',entry['results'][tid]['metadata']['status']=='SUCCESS'and result.get(timeout=2)is None)
 check(label+'-all-real-children-completed',expected==done and len(entry['preruns'])+len(nativeids)==len(expected))
 check(label+'-all-metadata-retained',all(v['metadata']is not None and 3500<=v['ttl']<=3600 for v in entry['results'].values()))
 check(label+'-lineage-root-parent-preserved',all(v['headers']['root_id']==tid and v['headers']['parent_id']in expected for v in pubs))
 check(label+'-all-default-route',all(v['exchange']=='' and v['routing_key']=='celery'for v in pubs) and all(v['delivery_info'].get('routing_key')=='celery'for v in entry['received']if v['id']!=tid))
 check(label+'-full-input-bytes-unchanged',before['files']==entry['after']['files'])
 check(label+'-prerun-reservation-observations-recorded',all(isinstance(v.get('unacked_task_ids'),list)and isinstance(v['id'],str)for v in entry['preruns']))
 entry['instant_prerun_own_reservations']=[v['id']for v in entry['preruns']if v['id']in v['unacked_task_ids']]
 check(label+'-known-task-attributes',all(v['attrs']['acks_late']is False and v['attrs']['ignore_result']is False and v['attrs']['track_started']is False for v in entry['preruns']))
 check(label+'-all-normal-success',all(v['state']=='SUCCESS'for v in posts))
 check(label+'-actual-parent-and-descendant-metadata',all(v['metadata']['task_id']==k and v['metadata']['status']=='SUCCESS'and v['metadata']['traceback']is None and set(v['metadata'])>=set(['status','result','traceback','children','date_done','task_id'])for k,v in entry['results'].items()))
 check(label+'-all-owned-task-locks-released',not entry['after']['locks'])
 reference=next(v for v in json.loads(Path('/pair-tools/reference.json').read_text())['cases']if v['name']==('epg-empty-active'if mode else label))
 project_events=lambda values:[{k:v for k,v in event.items()if k!='__asgi_channel__'}for event in values]
 if not mode:check(label+'-exact-Source-event-payload-order',project_events(entry['after']['events'])==project_events(reference['after']['events']))
 check(label+'-exact-Source-final-index-data',[v['data']for v in entry['after']['indices']]==[v['data']for v in reference['after']['indices']])
 source_fields=['name','file_path','extracted_file_path','source_type','status','last_message','is_active']
 project_source=lambda values:[{k:v[k]for k in source_fields}for v in values]
 if not mode:check(label+'-exact-Source-final-source-fields',project_source(entry['after']['sources'])==project_source(reference['after']['sources']))
 if e:
  check(label+'-duplicate-refresh-and-index-series-preserved',[v['task']for v in posts if v['task']!='core.tasks.scan_and_process_files'] == (['apps.epg.tasks.refresh_epg_data']if mode else[])+['apps.epg.tasks.build_programme_index_task','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules'])
  check(label+'-actual-index-and-channel-effects',len(entry['after']['indices'])==1 and len(entry['after']['epg_data'])==(1 if label=='epg-programme-active'else 0) and not entry['after']['programmes'])
 if m:
  account=entry['after']['accounts'][0];g=next(v for v in posts if v['task']=='apps.m3u.tasks.refresh_m3u_groups')
  if not active:check(label+'-inactive-tuple-and-no-state-effects',g['result']==[f"M3UAccount with ID={account['id']} not found or inactive.",None] and account['status']=='idle'and account['last_message']is None and not entry['after']['events'])
  else:
   one=label=='m3u-one-stream-active';single=next(v for v in posts if v['task']=='apps.m3u.tasks.refresh_single_m3u_account')
   check(label+'-typed-group-data',isinstance(g['result'],list)and len(g['result'])==2 and len(g['result'][0])==(1 if one else 0)and g['result'][1]==({'Default Group':{},'Owned Group':{}}if one else{'Default Group':{}}))
   check(label+'-single-exact-normal-return',single['result']==('Dispatched jobs complete.'if one else'Failed to update m3u account, no streams found'))
   check(label+'-single-actual-stream-and-state',len(entry['after']['streams'])==(1 if one else 0)and account['status']==('success'if one else'error')and (one or account['last_message']=='No streams found in M3U source'))
   files=next(v['files']for v in entry['fileposts']if v['id']==g['id']);cache_json=next(v for v in files if v['path'].endswith('/'+str(account['id'])+'.json'))
   check(label+'-actual-group-cache-bytes',json.loads(base64.b64decode(cache_json['raw_base64']))=={'extinf_data':g['result'][0],'groups':g['result'][1]})
 check(label+'-no-unrelated-families',all(v['task']in [TASK,'apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules','apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account']for v in posts))
 stop()
try:
 for root in ['/data/logos','/data/m3us','/data/epgs']:Path(root).mkdir(exist_ok=True);check('fresh-empty-'+root,not list(Path(root).iterdir()))
 import apps.m3u.tasks as mt,apps.epg.tasks as et
 for name,obj in [('scanner',scanner.scan_and_process_files.run),('groups',mt.refresh_m3u_groups.run),('single',mt.refresh_single_m3u_account.run),('epg',et.refresh_epg_data.run)]:Path('/evidence/original-'+name+'.py').write_text(inspect.getsource(obj))
 case('epg-empty-active',True,etext='<tv></tv>\n')
 from datetime import datetime,timedelta,timezone
 now=datetime.now(timezone.utc);fmt=lambda x:x.strftime('%Y%m%d%H%M%S +0000')
 xml='<tv><channel id="owned.channel"><display-name>Owned channel</display-name></channel><programme channel="owned.channel" start="'+fmt(now-timedelta(minutes=10))+'" stop="'+fmt(now+timedelta(hours=2))+'"><title>Owned programme</title></programme></tv>\n'
 case('epg-programme-active',True,etext=xml)
 # M3U child consumers remain unsupported; Source-only six-case corpus retained.
 # case('m3u-inactive',False,mtext='#EXTM3U\n')
 # excluded M3U consumer domain
 # excluded M3U consumer domain
 # excluded M3U consumer domain
 for mode in ['parent','claim','result']:case('physical-'+mode,True,etext='<tv></tv>\n')
 report['passed']=True
finally:
 stop();Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
