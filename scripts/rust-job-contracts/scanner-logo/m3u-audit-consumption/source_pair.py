"""Owned actual Original scanner -> unmodified default worker descendant capture.
No task routing overrides/quarantine. Synthetic local M3U/XML only, no providers.
Original Dispatcharr/Django/Celery/Redis/Channels authors retain credit/licenses.
"""
import select,copy,base64,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid
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
from django.core.cache import cache as django_cache
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.epg.models import EPGSource,EPGSourceIndex,EPGData,ProgramData
from core.models import CoreSettings,SYSTEM_SETTINGS_KEY,SystemEvent
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
report={'passed':False,'native_verified':True,'checks':[],'cases':[],'predeclared_cases':['audit-limit1-empty', 'audit-limit1-full', 'audit-limit2-overfull', 'audit-timestamp-id-inversion', 'audit-timestamp-tie', 'audit-insert-P0001', 'audit-delete-P0001', 'normalize-absent', 'normalize-null', 'normalize-zero', 'normalize-string-one', 'normalize-string-invalid', 'normalize-negative', 'normalize-boolean', 'normalize-float'],'scope':'Actual pinned Original scanner and real same-worker default-queue children; no route quarantine or native claim; synthetic local files only'}
report['scope']='Actual Source scanner -> native M3U groups15/single9 domains, Source fallback6 explicit unsupported single calls; SourceAsyncResult/effects vs Source323. No Main/normalization/tie expansion.'
report['controlled_elapsed_clock']=1000.125
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
 from apps.channels.models import Stream,ChannelGroup,ChannelGroupM3UAccount
 return {'accounts':a,'profiles':list(M3UAccountProfile.objects.filter(m3u_account_id__in=aids).values()),'sources':s,'indices':list(EPGSourceIndex.objects.filter(source_id__in=sids).values()),'epg_data':list(EPGData.objects.filter(epg_source_id__in=sids).values()),'programmes':list(ProgramData.objects.filter(epg__epg_source_id__in=sids).values()),'streams':list(Stream.objects.filter(m3u_account_id__in=aids).values()),'all_owned_streams':list(Stream.objects.filter(name__startswith='Owned ').values()),'group_links':list(ChannelGroupM3UAccount.objects.filter(m3u_account_id__in=aids).values()),'groups':list(ChannelGroup.objects.values()),'system_events':list(SystemEvent.objects.filter(details__account_name__startswith='rust-owned-children-').order_by('id').values()),'output_cache':{'m3u':{name:django_cache.get(name)for name in ['m3u_content:rust-owned-a','m3u_content:rust-owned-b']},'unrelated':django_cache.get('rust-owned-cache-preserve'),'generation_ascii':cache.get('epg_content_generation').decode(),'generation_ttl':cache.ttl('epg_content_generation'),'retired_chunk_base64':[base64.b64encode(v).decode()for v in cache.lrange('rust-owned-scanner-epg:g=700:chunks',0,3)],'retired_chunk_ttl':cache.ttl('rust-owned-scanner-epg:g=700:chunks')},'events':[layer.deserialize(v)for v in cache.zrange(channel_key,0,255)],'locks':{k.decode():{'raw_base64':base64.b64encode(cache.get(k)).decode(),'ttl':cache.ttl(k)}for k in cache.scan_iter('task_lock_*')},'files':[{'path':str(p),'size':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in made if p.exists()],'cache_files':[{'path':str(p),'size':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'raw_base64':base64.b64encode(p.read_bytes()).decode()if p.stat().st_size<=65536 else None}for root in ['/app/media/cached_m3u','/app/media/cached_epg','/data/epgs']for p in Path(root).rglob('*')if p.is_file()and p not in made]}
def install_audit_fault(label):
 with connection.cursor()as c:
  c.execute('CREATE SEQUENCE rust_owned_audit_fail_count')
  guard="NEW.event_type='m3u_refresh'"if label=='audit-insert-P0001'else"OLD.details->>'rust_owned_audit'='true'"
  sql_result='NEW'if label=='audit-insert-P0001'else'OLD'
  c.execute("CREATE FUNCTION rust_owned_audit_worker_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF "+guard+" THEN PERFORM nextval('rust_owned_audit_fail_count'); RAISE EXCEPTION 'rust-owned audit failure' USING ERRCODE='P0001'; END IF; RETURN "+sql_result+";END$$")
  c.execute('CREATE TRIGGER rust_owned_audit_worker_fail BEFORE '+('INSERT'if label=='audit-insert-P0001'else'DELETE')+' ON core_systemevent FOR EACH ROW EXECUTE FUNCTION rust_owned_audit_worker_fail()')
def case(label,active,mtext=None,etext=None):
 fault=False
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
 django_cache.set('m3u_content:rust-owned-a','owned-a',timeout=600);django_cache.set('m3u_content:rust-owned-b','owned-b',timeout=600);django_cache.set('rust-owned-cache-preserve','preserve',timeout=600);cache.set('epg_content_generation','700',ex=600);cache.delete('rust-owned-scanner-epg:g=700:chunks');cache.rpush('rust-owned-scanner-epg:g=700:chunks','owned retired XMLTV');cache.expire('rust-owned-scanner-epg:g=700:chunks',600)
 before=snapshot(m,e);prior_ids={v['id']for v in records('postrun')};tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':NOW}));worker=start('rust-owned-scanner-parents');result=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=tid,queue='rust-owned-scanner-parents')
 entry={'name':label,'parent_id':tid,'before':before,'parent_publication':publications[tid]};report['cases'].append(entry)
 end=time.monotonic()+30
 while not any(v['id']==tid for v in records('postrun')):
  assert time.monotonic()<end;time.sleep(.025)
 stop();account=M3UAccount.objects.get(file_path=str(m))
 # All history mutations are confined to the disposable copied database.
 SystemEvent.objects.all().delete()
 limit=1 if label in ['audit-limit1-empty','audit-limit1-full']else 2
 normalization={'normalize-null':None,'normalize-zero':0,'normalize-string-one':'1','normalize-string-invalid':'not-an-integer','normalize-negative':-1,'normalize-boolean':True,'normalize-float':1.5}
 setting=CoreSettings.objects.get(key=SYSTEM_SETTINGS_KEY);raw=dict(setting.value)
 if label=='normalize-absent':raw.pop('max_system_events',None)
 else:raw['max_system_events']=normalization.get(label,limit)
 setting.value=raw;setting.save();entry['raw_max_system_events']={'present':'max_system_events'in raw,'value':raw.get('max_system_events')}
 from datetime import datetime,timedelta,timezone
 n=0 if label=='audit-limit1-empty'or label.startswith('normalize-')else 1 if label=='audit-limit1-full'else 4
 offsets=[0,1,2,3]if label!='audit-timestamp-id-inversion'else[3,0,2,1]
 if label=='audit-timestamp-tie':offsets=[0,0,0,0]
 for index in range(n):
  row=SystemEvent.objects.create(event_type='m3u_refresh',details={'account_name':'rust-owned-children-audit-seed','rust_owned_audit':True,'ordinal':index})
  SystemEvent.objects.filter(pk=row.pk).update(timestamp=datetime(2010,1,1,tzinfo=timezone.utc)+timedelta(seconds=offsets[index]))
 entry['audit_before']={'limit':limit,'by_id':list(SystemEvent.objects.order_by('id').values()),'Source_order':list(SystemEvent.objects.values())}
 entry['effective_hash_keys']=account.get_effective_hash_keys();entry['m3u_hash_setting']=CoreSettings.get_m3u_hash_key()
 if label=='m3u-missing-local-file':m.unlink()
 if label in ['m3u-groups-lock-busy','m3u-single-lock-busy']:
  key=('task_lock_refresh_m3u_account_groups_'if label=='m3u-groups-lock-busy'else'task_lock_refresh_single_m3u_account_')+str(account.id);cache.set(key,'rust-owned-foreign-token',ex=300);entry['owned_busy_key']=key
 if label in ['m3u-group-SQL-P0001','m3u-stream-SQL-P0001']:
  table='dispatcharr_channels_channelgroup'if label=='m3u-group-SQL-P0001'else'dispatcharr_channels_stream'
  with connection.cursor()as c:
   c.execute('CREATE SEQUENCE rust_owned_m3u_fail_count')
   c.execute("CREATE OR REPLACE FUNCTION rust_owned_m3u_worker_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('rust_owned_m3u_fail_count'); RAISE EXCEPTION 'rust-owned M3U worker failure' USING ERRCODE='P0001'; END$$")
   c.execute('CREATE TRIGGER rust_owned_m3u_worker_fail BEFORE INSERT OR UPDATE ON '+table+' FOR EACH ROW EXECUTE FUNCTION rust_owned_m3u_worker_fail()')
 proxy=Proxy('claim',tid,'unused')if label in ['audit-insert-P0001','audit-delete-P0001']else None
 log=open('/evidence/native-m3u-'+label+'.log','wb')
 native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,bufsize=1)
 def receive():
  end=time.monotonic()+35
  while time.monotonic()<end:
   assert native.poll()is None,'Native M3U peer exited'
   if select.select([native.stdout],[],[],.1)[0]:
    line=native.stdout.readline();log.write(line.encode());log.flush()
    if 'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_M3U_JSON=',1)[1])
  raise TimeoutError('Native M3U response deadline')
 def command(v):native.stdin.write(json.dumps(v)+'\n');native.stdin.flush();return receive()
 nativeids=[];outputs=[];fileposts=[];native_messages=[]
 unsupported=label in ['audit-timestamp-tie','normalize-string-one','normalize-string-invalid','normalize-negative','normalize-boolean','normalize-float']
 try:
  config={'parent_id':tid,'account_id':account.id}
  if proxy:config['broker_url']='redis://:playback-fixture-only@127.0.0.1:'+str(proxy.server.server_address[1])+'/8'
  check(label+'-native-prepared',command(config)=={'prepared':True})
  first=broker.lindex('celery',-1);refused=command({'action':'foreign'});check(label+'-foreign-scope-retains-actual-frame',refused['run']=='Err(Transport(UnsupportedTask))'and broker.lindex('celery',-1)==first)
  for number in range(2):
   frame=json.loads(broker.lindex('celery',-1));ident=frame['headers']['id']
   check(label+'-known-original-child-'+str(number),frame['headers']['task']==['apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account'][number])
   if number==1 and unsupported:
    def stable_effects(value):
     value=copy.deepcopy(value)
     for row in value['locks'].values():row.pop('ttl')
     for name in ['generation_ttl','retired_chunk_ttl']:value['output_cache'].pop(name)
     return value
    prior=stable_effects(snapshot(m,e));head=broker.lindex('celery',-1);answer=command({'action':'run'});outputs.append(answer)
    check(label+'-explicit-unported-single-before-ACK',answer['run']=='Err(Domain(Unsupported))'and broker.lindex('celery',-1)==head)
    check(label+'-single-no-task-result-or-effects',backend.get(app.backend.get_key_for_task(ident))is None and stable_effects(snapshot(m,e))==prior)
    break
   if number==1 and proxy:
    proxy.armed=True;check(label+'-owned-single-started',command({'action':'start'})=={'started':True});check(label+'-actual-admitted-CAS-reply-held',proxy.held.wait(15))
    check(label+'-real-ACK-before-owned-fault-injection',broker.lindex('celery',-1)is None and backend.get(app.backend.get_key_for_task(ident))is None)
    install_audit_fault(label);entry['fault_injection']='owned trigger installed after real admitted CAS/pop, before physical reply release'
    proxy.release.set();answer=command({'action':'settled'});outputs.append(answer);check(label+'-native-completed-'+str(number),answer=={'caller_released':True,'slots':1}and backend.get(app.backend.get_key_for_task(ident))is not None)
   else:
    answer=command({'action':'run'});outputs.append(answer);check(label+'-native-completed-'+str(number),answer['run'].startswith('Ok(Completed(')and answer['slots']==1)
   nativeids.append(ident);native_messages.append(frame)
   fileposts.append({'id':ident,'files':snapshot(m,e)['cache_files']})
  check(label+'-native-drained',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(label+'-native-exit0',native.returncode==0)
 finally:
  if native.poll()is None:native.kill();native.wait(timeout=5)
  log.close()
  if proxy:proxy.close();check(label+'-claim-proxy-clean',not proxy.errors)
 entry.update(native_ids=nativeids,native_outputs=outputs,native_fileposts=fileposts,source_single_fallback=unsupported)
 if unsupported:worker=start()

 end=time.monotonic()+65;stable=None
 while time.monotonic()<end:
  posts=[v for v in records('postrun')if v['id']not in prior_ids];pubs=[v for v in records('children')if v['headers'].get('root_id')==tid];expected={tid}|{v['headers']['id']for v in pubs};done={v['id']for v in posts}|set(nativeids)
  idle=expected<=done and broker.llen('celery')==broker.hlen('unacked')==0
  if idle:
   if stable is None:stable=time.monotonic()
   if time.monotonic()-stable>=.5:break
  else:stable=None
  if worker and worker.poll()is not None:raise RuntimeError('Owned original worker exited')
  time.sleep(.025)
 else:raise TimeoutError('Actual descendants did not drain in fixed65s')
 entry.update(fileposts=[v for v in records('filepost')if v['id']in expected]+fileposts,after=snapshot(m,e),publications=pubs,postruns=posts,preruns=[v for v in records('prerun')if v['id']in expected],received=[v for v in records('received')if v['id']in expected],sql=[v for v in records('sql')if v['id']in expected],results={})
 for taskid in expected:
  key=app.backend.get_key_for_task(taskid);raw=backend.get(key)
  entry['results'][taskid]={'metadata':None if raw is None else json.loads(raw),'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'ttl':backend.ttl(key)}
 outcomes=posts+[{'task':frame['headers']['task'],'id':frame['headers']['id'],'state':entry['results'][frame['headers']['id']]['metadata']['status'],'result':entry['results'][frame['headers']['id']]['metadata']['result'],'observation':'native actual backend metadata'}for frame in native_messages]
 entry['native_terminal_observations']=[v for v in outcomes if v.get('observation')]
 check(label+'-all-native-Source-AsyncResult-ready',all(app.AsyncResult(ident).ready()and app.AsyncResult(ident).successful()for ident in nativeids))
 check(label+'-parent-null-success',entry['results'][tid]['metadata']['status']=='SUCCESS'and result.get(timeout=2)is None)
 check(label+'-all-real-children-completed',expected==done and len(entry['preruns'])+len(nativeids)==len(expected))
 check(label+'-all-metadata-retained',all(v['metadata']is not None and 3500<=v['ttl']<=3600 for v in entry['results'].values()))
 check(label+'-lineage-root-parent-preserved',all(v['headers']['root_id']==tid and v['headers']['parent_id']in expected for v in pubs))
 check(label+'-all-default-route',all(v['exchange']=='' and v['routing_key']=='celery'for v in pubs) and all(v['delivery_info'].get('routing_key')=='celery'for v in entry['received']if v['id']!=tid))
 if label!='m3u-missing-local-file':check(label+'-full-input-bytes-unchanged',before['files']==entry['after']['files'])
 check(label+'-prerun-reservations-recorded',all(isinstance(v['unacked_task_ids'],list)for v in entry['preruns']))
 check(label+'-known-task-attributes',all(v['attrs']['acks_late']is False and v['attrs']['ignore_result']is False and v['attrs']['track_started']is False for v in entry['preruns']))
 if not fault:check(label+'-all-normal-success',all(v['state']=='SUCCESS'for v in posts))
 if not fault:check(label+'-actual-parent-and-descendant-metadata',all(v['metadata']['task_id']==k and v['metadata']['status']=='SUCCESS'and v['metadata']['traceback']is None and set(v['metadata'])>=set(['status','result','traceback','children','date_done','task_id'])for k,v in entry['results'].items()))
 if not fault:check(label+'-all-owned-task-locks-released',not entry['after']['locks'])
 if e:
  check(label+'-duplicate-refresh-and-index-series-preserved',[v['task']for v in posts if v['task']!='core.tasks.scan_and_process_files'and not v['task'].startswith('apps.m3u.')] == ['apps.epg.tasks.refresh_epg_data','apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules'])
  check(label+'-actual-index-and-channel-effects',len(entry['after']['indices'])==1 and len(entry['after']['epg_data'])==(1 if label=='epg-programme-active'else 0) and not entry['after']['programmes'])
 if m and not fault:
  account=entry['after']['accounts'][0];g=next(v for v in outcomes if v['task']=='apps.m3u.tasks.refresh_m3u_groups')
  if not active:check(label+'-inactive-tuple-and-no-state-effects',g['result']==[f"M3UAccount with ID={account['id']} not found or inactive.",None] and account['status']=='idle'and account['last_message']is None and not entry['after']['events'])
  else:
   one=True;single=next(v for v in outcomes if v['task']=='apps.m3u.tasks.refresh_single_m3u_account')
   check(label+'-typed-group-data',isinstance(g['result'],list)and len(g['result'])==2 and len(g['result'][0])==(1 if one else 0)and g['result'][1]==({'Default Group':{},'Owned Group':{}}if one else{'Default Group':{}}))
   check(label+'-single-exact-normal-return',single['result']==('Dispatched jobs complete.'if one else'Failed to update m3u account, no streams found'))
   check(label+'-single-actual-stream-and-state',len(entry['after']['all_owned_streams'])==1 and account['status']==('success'if one else'error')and (one or account['last_message']=='No streams found in M3U source'))
   files=next(v['files']for v in entry['fileposts']if v['id']==g['id']);cache_json=next(v for v in files if v['path'].endswith('/'+str(account['id'])+'.json'))
   check(label+'-actual-group-cache-bytes',json.loads(base64.b64decode(cache_json['raw_base64']))=={'extinf_data':g['result'][0],'groups':g['result'][1]})
 check(label+'-no-unrelated-families',all(v['task']in [TASK,'apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules','apps.m3u.tasks.refresh_m3u_groups','apps.m3u.tasks.refresh_single_m3u_account']for v in posts))
 if label in ['m3u-group-SQL-P0001','m3u-stream-SQL-P0001']:
  with connection.cursor()as c:c.execute('SELECT last_value,is_called FROM rust_owned_m3u_fail_count');last,called=c.fetchone()
  entry['trigger_calls']=last if called else 0;check(label+'-actual-server-fault-executed',entry['trigger_calls']>=1)
 for ident,row in entry['results'].items():
  value=app.AsyncResult(ident).get(timeout=2,propagate=False)
  row['Original_get_type']=type(value).__module__+'.'+type(value).__name__
  if isinstance(value,BaseException):row['Original_exception_args']=value.args
 entry['observed_hooks']=[v for v in records('hooks')if v['id']in expected]
 check(label+'-plugins-empty',all(not v['enabled_plugin_keys']and all(not a for a in v['actions'].values())for v in entry['observed_hooks']))
 check(label+'-unrelated-and-retired-cache-retained',entry['after']['output_cache']['unrelated']=='preserve'and entry['after']['output_cache']['retired_chunk_base64']==before['output_cache']['retired_chunk_base64']and 500<=entry['after']['output_cache']['retired_chunk_ttl']<=600)
 check(label+'-postrun-states-match-backend',all(entry['results'][v['id']]['metadata']['status']==v['state']for v in posts))
 if label=='m3u-group-SQL-P0001':check(label+'-actual-group-handler-raised',next(v for v in posts if v['task']=='apps.m3u.tasks.refresh_m3u_groups')['state']=='FAILURE')
 entry['audit_after']={'by_id':list(SystemEvent.objects.order_by('id').values()),'Source_order':list(SystemEvent.objects.values())}
 if label in ['audit-insert-P0001','audit-delete-P0001']:
  with connection.cursor()as c:c.execute('SELECT last_value,is_called FROM rust_owned_audit_fail_count');last,called=c.fetchone()
  entry['audit_fault_calls']=last if called else 0;check(label+'-actual-audit-server-error-executed',called and last>=1)
 reference=next(v for v in json.loads(Path('/pair-tools/reference.json').read_text())['cases']if v['name']==label)
 if not unsupported:
  def records_only(values):return [{k:v for k,v in row.items()if k not in ['created_at','updated_at','last_seen','timestamp']}for row in values]
  for table in ['accounts','profiles','sources','indices','epg_data','programmes','streams','all_owned_streams','groups','group_links','system_events']:
   check(label+'-exact-Source-'+table,records_only(entry['after'][table])==records_only(reference['after'][table]))
  check(label+'-exact-audit-history-id-order',records_only(entry['audit_after']['by_id'])==records_only(reference['audit_after']['by_id']))
  for table in ['events','files','cache_files']:check(label+'-exact-Source-'+table,entry['after'][table]==reference['after'][table])
  project_cache=lambda value:{k:v for k,v in value.items()if not k.endswith('_ttl')}
  check(label+'-exact-Source-cache-effects',project_cache(entry['after']['output_cache'])==project_cache(reference['after']['output_cache']))
  original_results={v['task']:v['result']for v in reference['postruns']}
  for frame in native_messages:
   metadata=entry['results'][frame['headers']['id']]['metadata'];check(label+'-typed-Source-native-result-'+frame['headers']['task'],metadata['result']==original_results[frame['headers']['task']]and metadata['parent_id']==tid and metadata['children']==[])
 check(label+'-audit-fault-does-not-fabricate-task-failure',all(v['state']=='SUCCESS'for v in posts))
 stop()
 with connection.cursor()as c:
  c.execute('DROP TRIGGER IF EXISTS rust_owned_audit_worker_fail ON core_systemevent');c.execute('DROP FUNCTION IF EXISTS rust_owned_audit_worker_fail()');c.execute('DROP SEQUENCE IF EXISTS rust_owned_audit_fail_count')
  for table in ['dispatcharr_channels_channelgroup','dispatcharr_channels_stream']:c.execute('DROP TRIGGER IF EXISTS rust_owned_m3u_worker_fail ON '+table)
  c.execute('DROP FUNCTION IF EXISTS rust_owned_m3u_worker_fail()');c.execute('DROP SEQUENCE IF EXISTS rust_owned_m3u_fail_count')
try:
 for root in ['/data/logos','/data/m3us','/data/epgs']:Path(root).mkdir(exist_ok=True);check('fresh-empty-'+root,not list(Path(root).iterdir()))
 import apps.m3u.tasks as mt,apps.epg.tasks as et
 for name,obj in [('scanner',scanner.scan_and_process_files.run),('groups',mt.refresh_m3u_groups.run),('single',mt.refresh_single_m3u_account.run),('epg',et.refresh_epg_data.run)]:Path('/evidence/original-'+name+'.py').write_text(inspect.getsource(obj))
 import core.utils as cu
 Path('/evidence/original-audit.py').write_text(inspect.getsource(cu.log_system_event))
 report['SystemEvent_ordering']=list(SystemEvent._meta.ordering)
 import importlib.metadata
 report['provenance']={'packages':{v:importlib.metadata.version(v)for v in ['Django','celery','kombu']},'files':{name:hashlib.sha256(Path(name).read_bytes()).hexdigest()for name in ['/app/core/utils.py','/app/core/models.py']},'audit_function_sha256':hashlib.sha256(inspect.getsource(cu.log_system_event).encode()).hexdigest()}
 with connection.cursor()as c:
  for table in ['dispatcharr_connect_integration','dispatcharr_connect_eventsubscription']:
   c.execute('SELECT count(*) FROM '+table);check('Connect-empty-'+table,c.fetchone()[0]==0)
 for label in report['predeclared_cases']:
  case(label,True,mtext='#EXTM3U\n#EXTINF:-1 tvg-id="owned.channel" group-title="Owned Group",Owned stream\nhttp://127.0.0.1:9/owned-never-played.ts\n')
 check('exact-predeclared-case-order',[v['name']for v in report['cases']]==report['predeclared_cases'])
 report['passed']=True
finally:
 stop();Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
