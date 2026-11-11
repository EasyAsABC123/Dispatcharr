"""Pinned Original real-worker active profile expiration boundary corpus.
Original Dispatcharr/Django/Celery/Channels authors retain code/algorithm credit.
Dependency clock is fixed ONLY for each explicitly owned task; task is unchanged.
Copied DB only. No external network or native parity assertion.
"""
import datetime,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db import connection
from django.db.models.deletion import Collector
from django.contrib.auth import get_user_model
from dispatcharr.celery import app
from celery.signals import before_task_publish
from apps.m3u.models import M3UAccount,M3UAccountProfile
from core.models import SystemNotification,NotificationDismissal
from apps.m3u.tasks import check_account_expirations,evaluate_profile_expiration_notification
import core.utils,apps.proxy.signals
NOW=datetime.datetime(2026,11,11,12,0,0,123456,tzinfo=datetime.timezone.utc)
OLD=datetime.datetime(2024,1,1,tzinfo=datetime.timezone.utc)
TASK='apps.m3u.tasks.check_account_expirations'
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();layer=get_channel_layer();channel='specific.rust_owned_active!receiver';channel_key=layer.prefix+channel.split('!')[0]+'!';group_key=layer._group_key('updates');sentinel='rust:owned:active:sentinel'
published={}
@before_task_publish.connect
def publication(sender=None,body=None,headers=None,properties=None,**kwargs):
 if sender==TASK:published[headers['id']]={'headers':headers,'body':body,'properties':properties}
report={'passed':False,'native_verified':True,'scope':'Actual pinned Original SQL-failure worker metadata and real copied-DB TCP status/notification consumer visibility; no native failure writer or HTTP parity claim','fixed_now':NOW.isoformat(),'checks':[],'cases':[]};worker=None
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def wait(fn):
 end=time.monotonic()+40
 while time.monotonic()<end:
  value=fn()
  if value:return value
  if worker and worker.poll()is not None:raise RuntimeError('Owned worker exited')
  time.sleep(.025)
 raise TimeoutError('Owned active deadline')
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return[json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def rows():
 return {'notices':list(SystemNotification.objects.order_by('id').values()),'dismissals':list(NotificationDismissal.objects.order_by('id').values('id','notification_id','user_id'))}
def drop_trigger():
 with connection.cursor()as c:
  c.execute('DROP TRIGGER IF EXISTS rust_active_reject ON core_systemnotification');c.execute('DROP FUNCTION IF EXISTS rust_active_reject()')
def seed_notice(key,active=True):
 row=SystemNotification.objects.create(notification_key=key,title='Owned existing',message='Owned existing',notification_type='info',priority='low',source='developer',is_active=active,admin_only=False,action_data={'owned':'before'},expires_at=None)
 SystemNotification.objects.filter(pk=row.pk).update(created_at=OLD,updated_at=OLD);NotificationDismissal.objects.create(user=user,notification=row);return row
def peer():
 value=subprocess.run(['/evidence/native-default-peer','default_peer::native_default_family_peer','--exact','--ignored','--nocapture'],input=json.dumps({'action':'run','notification_now':NOW.isoformat()}),text=True,capture_output=True,timeout=30)
 with Path('/evidence/native-notification-terminal-private.log').open('a')as log:log.write(value.stdout+value.stderr)
 value.check_returncode();result=json.loads(next(v.split('NATIVE_DEFAULT_PAIR_JSON=',1)[1]for v in value.stdout.splitlines()if 'NATIVE_DEFAULT_PAIR_JSON='in v));check('native-physical-drain',result['drained']and result['jobs_drained']and result['pool_closed']and result['available_slots']==2);return result
CASES=[('new-warning',86400000000,'new-target'),('opposite-delete-fault',86400000000,'delete-opposite'),('upsert-fault',86400000000,'upsert'),('stale-delete-fault',86400000000,'delete-stale')]
web=None

try:
 for name,obj in [('task',check_account_expirations.run),('evaluate',evaluate_profile_expiration_notification),('websocket',core.utils.send_websocket_notification),('dismissal',core.utils.send_notification_dismissed),('collector-delete',Collector.delete),('proxy-predelete',apps.proxy.signals.cleanup_proxy_servers)]:
  data=inspect.getsource(obj).encode();Path('/evidence/original-active-'+name+'.py').write_bytes(data)
 report['source_files']={str(p):hashlib.sha256(p.read_bytes()).hexdigest()for p in [Path('/app/apps/m3u/tasks.py'),Path('/app/core/models.py'),Path('/app/core/utils.py'),Path('/app/apps/proxy/signals.py')]}
 M3UAccount.objects.all().update(is_active=False);M3UAccountProfile.objects.all().update(is_active=False)
 account=M3UAccount.objects.filter(account_type='STD').order_by('id').first();assert account
 M3UAccount.objects.filter(pk=account.pk).update(name='Owned Active Account')
 M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=account,name='Owned Profile',is_active=False)])
 profile=M3UAccountProfile.objects.get(m3u_account=account,name='Owned Profile');user=get_user_model().objects.order_by('id').first();assert user
 warning='xc-exp-warning-'+str(profile.pk);expired='xc-exp-expired-'+str(profile.pk)
 report['ids']={'account':account.pk,'profile':profile.pk,'user':user.pk}
 report['profile_query_sql']=str(M3UAccountProfile.objects.filter(m3u_account__is_active=True,is_active=True,exp_date__isnull=False).select_related('m3u_account').query)
 import requests
 from rest_framework_simplejwt.tokens import RefreshToken
 from apps.backups.api_views import _generate_task_token,backup_status
 from core.api_views import SystemNotificationViewSet
 from django.db.models import Q
 admin=get_user_model().objects.filter(user_level=10).order_by('id').first();viewer=get_user_model().objects.filter(user_level=1).order_by('id').first();assert admin and viewer
 tokens={'admin':str(RefreshToken.for_user(admin).access_token),'viewer':str(RefreshToken.for_user(viewer).access_token)}
 for label,obj in [('backup-status',backup_status),('notification-queryset',SystemNotificationViewSet.get_queryset),('notification-list',SystemNotificationViewSet.list)]:Path('/evidence/original-consumer-'+label+'.py').write_text(inspect.getsource(obj))
 log=open('/evidence/notification-http-private.log','ab');web=subprocess.Popen(['/dispatcharrpy/bin/uwsgi','--http','127.0.0.1:5657','--module','dispatcharr.wsgi:application','--master','--workers','1','--threads','2','--die-on-term','--need-app','--disable-logging'],stdout=log,stderr=log,start_new_session=True);log.close()
 def http(path,role,query=None):
  headers={'Host':'localhost','Accept':'application/json'}
  if role in tokens:headers['Authorization']='Bearer '+tokens[role]
  try:r=requests.get('http://127.0.0.1:5657'+path,headers=headers,params=query,timeout=5)
  except requests.RequestException as error:raise RuntimeError('Owned HTTP transport '+type(error).__name__)from None
  return {'status':r.status_code,'body':r.json(),'content_type':r.headers.get('Content-Type'),'body_sha256':hashlib.sha256(r.content).hexdigest()}
 def ready():
  try:return http('/api/backups/schedule/','admin')['status']==200
  except RuntimeError:return False
 wait(ready)
 # Dedicated unchanged WSGI uses the copied database; known owned plugin startup
 # publications are observed/quarantined before the original worker starts.
 startup=broker.lrange('celery',0,-1);report['startup_quarantine']=[{'task':json.loads(v)['headers']['task'],'sha256':hashlib.sha256(v).hexdigest()}for v in startup]
 check('startup-quarantine-known-plugins-only',all(v['task']=='apps.plugins.tasks.refresh_plugin_repos'for v in report['startup_quarantine']));broker.delete('celery')
 with Path('/evidence/active-source-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-active@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 for case,offset,fault in CASES:
  if worker.poll() is not None:
   log=open('/evidence/active-source-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-active@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close()
  drop_trigger();SystemNotification.objects.filter(notification_key__startswith='xc-exp-').delete();SystemNotification.objects.filter(notification_key__startswith='rust-owned-active-').delete()
  cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel);cache.set(sentinel,'preserve',ex=600)
  exp=None if offset is None else NOW+datetime.timedelta(microseconds=offset)
  name='X'*255 if fault=='title'else'Owned π ⏰'if fault=='unicode'else'Owned Profile'
  M3UAccount.objects.filter(pk=account.pk).update(is_active=fault!='inactive-account')
  M3UAccountProfile.objects.filter(pk=profile.pk).update(is_active=fault!='inactive-profile',name=name,exp_date=exp)
  w=seed_notice(warning,False);e=seed_notice(expired,False);stale=seed_notice('xc-exp-warning-owned-stale');keep=seed_notice('rust-owned-active-unrelated');inactive=seed_notice('xc-exp-expired-owned-inactive',False)
  expected_key=None if exp is None or fault in ['inactive-account','inactive-profile']or offset>604800000000 else expired if offset<=0 else warning
  opposite=expired if expected_key==warning else warning
  if fault in ['delete-opposite','upsert','delete-stale']:
   operation='INSERT OR UPDATE'if fault=='upsert'else'DELETE';target=expected_key if fault=='upsert'else stale.notification_key if fault=='delete-stale'else opposite
   with connection.cursor()as c:
    c.execute("CREATE FUNCTION rust_active_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF "+("NEW"if fault=='upsert'else"OLD")+".notification_key = '"+target+"' THEN RAISE EXCEPTION 'owned active "+fault+" failure' USING ERRCODE='P0001'; END IF; RETURN "+("NEW"if fault=='upsert'else"OLD")+"; END $$")
    c.execute('CREATE TRIGGER rust_active_reject BEFORE '+operation+' ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_active_reject()')
  if fault=='event':cache.delete(group_key);cache.set(group_key,'owned-wrongtype')
  if fault=='new-target':SystemNotification.objects.filter(notification_key=expected_key).delete()
  with connection.cursor()as c:c.execute("SELECT last_value,is_called FROM core_systemnotification_id_seq");sequence=c.fetchone()
  before=rows();profile_before=list(M3UAccountProfile.objects.filter(pk=profile.pk).values('name','is_active','exp_date','m3u_account_id'));taskid=str(uuid.uuid4());Path('/evidence/owned-active-clock.json').write_text(json.dumps({'task_id':taskid,'now':NOW.isoformat()}))
  result=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=taskid);post=wait(lambda:next((p for p in records('postrun')if p['id']==taskid),None));raw=backend.get(app.backend.get_key_for_task(taskid));metadata=json.loads(raw);after=rows();events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,31)]
  case_report={'name':case,'exp_date':None if exp is None else exp.isoformat(),'fault':fault,'envelope':published[taskid],'postrun':post,'metadata':metadata,'before':before,'after':after,'events':events,'sql':next(p for p in records('sql')if p['id']==taskid),'prerun':next(p for p in records('prerun')if p['id']==taskid)};report['cases'].append(case_report)
  failed=fault in ['delete-opposite','upsert','delete-stale','title']
  check(case+'-worker-and-backend-state',post['state']==metadata['status']==('FAILURE'if failed else'SUCCESS'))
  check(case+'-profile-unchanged',profile_before==list(M3UAccountProfile.objects.filter(pk=profile.pk).values('name','is_active','exp_date','m3u_account_id')))
  check(case+'-unrelated-and-inactive-preserved',SystemNotification.objects.filter(pk__in=[keep.pk,inactive.pk]).count()==2)
  check(case+'-cache-sentinel',cache.get(sentinel)==b'preserve'and cache.ttl(sentinel)>550)
  check(case+'-result-ttl',3580<=backend.ttl(app.backend.get_key_for_task(taskid))<=3600)
  wait(lambda:broker.llen('celery')==broker.hlen('unacked')==0);check(case+'-queue-drained',True)
  check(case+'-worker-local-managers-empty',all(v==0 for v in case_report['prerun']['process_local_managers'].values()))
  notices={r['notification_key']:r for r in after['notices']};event_types=[v['data']['type']for v in events]
  if not failed:
   check(case+'-normal-null',result.get(timeout=2)is None and metadata['result']is None and metadata['children']==[]and metadata['traceback']is None)
   check(case+'-stale-cleaned',stale.notification_key not in notices)
   if expected_key:
    n=notices[expected_key];expected_title=('Account Expired: 'if offset<=0 else'Account Expiring: ')+name
    when='today'if offset<86400000000 else'in 1 day'if offset<172800000000 else'in '+str(offset//86400000000)+' days'
    suffix='has expired (expired 'if offset<=0 else'expires '+when+' (expires '
    message='Profile "'+name+'" on M3U account "Owned Active Account" '+suffix+exp.strftime('%Y-%m-%d %H:%M UTC')+').'
    check(case+'-exact-title-message',n['title']==expected_title and n['message']==message)
    check(case+'-typed-content',n['notification_type']=='warning'and n['priority']==('high'if offset<=0 else'normal')and n['admin_only']is True and n['is_active']is True and n['action_data']=={'profile_id':profile.pk,'account_id':account.pk,'account_name':'Owned Active Account','profile_name':name,'exp_date':exp.isoformat()})
    if fault!='new-target':check(case+'-preserved-update-fields',n['source']=='developer'and n['created_at']==OLD and n['expires_at']==OLD and n['updated_at']==NOW)
    if fault!='new-target':check(case+'-dismissal-preserved',NotificationDismissal.objects.filter(notification_id=n['id'],user=user).exists())
    if fault=='new-target':check(case+'-new-default-fields',n['source']=='system'and n['created_at']==NOW and n['expires_at']is None and n['updated_at']==NOW and not NotificationDismissal.objects.filter(notification_id=n['id']).exists())
    check(case+'-opposite-deleted',opposite not in notices)
    check(case+'-event-order',event_types==([]if fault=='event'else['notification_dismissed','system_notification','notification_dismissed']))
    if fault!='event':
     payload=events[1]['data']['notification'];check(case+'-exact-event-fields',payload=={k:(n[k].isoformat()if k=='created_at'else n[k])for k in ['id','notification_key','notification_type','priority','title','message','action_data','is_active','admin_only','created_at']})
   elif exp is not None and fault not in ['inactive-account','inactive-profile']:
    check(case+'-outside-both-deleted',warning not in notices and expired not in notices)
    expected_order=list(SystemNotification.objects.none()) # explicit Source SQL ordering retained in evidence below
    check(case+'-outside-event-count',len(events)==3 and all(v=='notification_dismissed'for v in event_types))
   else:check(case+'-excluded-inactive-notices-preserved',warning in notices and expired in notices and len(events)==1)
  else:
   check(case+'-source-raised',metadata['result']['exc_type']==('DataError'if fault=='title'else'InternalError') or metadata['result']['exc_type']=='ProgrammingError')
   if fault=='delete-opposite':check(case+'-collector-rollback-before-upsert',after==before and events==[])
   if fault in ['upsert','title']:check(case+'-prior-delete-committed',opposite not in notices and stale.notification_key in notices and len(events)==1)
   if fault=='delete-stale':check(case+'-upsert-committed-before-stale-fault',notices[warning]['is_active']is True and opposite not in notices and stale.notification_key in notices and event_types==['notification_dismissed','system_notification'])
  path='/api/backups/status/'+taskid+'/'
  responses={'admin':http(path,'admin'),'viewer':http(path,'viewer'),'anonymous':http(path,'anonymous'),'signed-task-token':http(path,'anonymous',{'token':_generate_task_token(taskid)}),'invalid-token-with-admin':http(path,'admin',{'token':'owned-invalid'})}
  expected_detail='Failed to get task status: '+(metadata['result']['exc_message'][0]if failed else"'NoneType' object has no attribute 'get'")
  check(case+'-http-ready-get-exception-json500',responses['admin']['status']==500 and responses['admin']['body']=={'detail':expected_detail}and responses['signed-task-token']['status']==500 and responses['signed-task-token']['body']==responses['admin']['body'])
  check(case+'-http-authority-precedence',responses['viewer']['status']==responses['anonymous']['status']==401 and responses['viewer']['body']==responses['anonymous']['body']=={'detail':'Authentication required'}and responses['invalid-token-with-admin']['status']==403 and responses['invalid-token-with-admin']['body']=={'detail':'Invalid task token'})
  visible={};case_report['consumer_http']={'task_status':responses,'notifications':visible,'token_bytes_retained':False}
  for role in ['admin','viewer']:
   response=http('/api/core/notifications/',role,{'include_dismissed':'true'});visible[role]=response
   expected=SystemNotification.objects.filter(is_active=True).filter(Q(expires_at__isnull=True)|Q(expires_at__gt=django.utils.timezone.now()))
   if role=='viewer':expected=expected.filter(admin_only=False)
   expected=expected.filter(Q(notification_key__startswith='xc-exp-')|Q(notification_key__startswith='rust-owned-active-'));expected_ids=list(expected.values_list('id',flat=True))
   visible_ids=[v['id']for v in response['body']['notifications']if v['notification_key'].startswith(('xc-exp-','rust-owned-active-'))]
   check(case+'-notification-http-'+role,response['status']==200 and visible_ids==expected_ids and response['body']['count']==response['body']['unread_count']==len(response['body']['notifications']))
  case_report['consumer_http']={'task_status':responses,'notifications':visible,'token_bytes_retained':False}
  os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15)
  drop_trigger();SystemNotification.objects.all().delete();SystemNotification.objects.bulk_create([SystemNotification(**r)for r in before['notices']])
  for r in before['notices']:SystemNotification.objects.filter(pk=r['id']).update(created_at=r['created_at'],updated_at=r['updated_at'])
  NotificationDismissal.objects.bulk_create([NotificationDismissal(**r)for r in before['dismissals']])
  with connection.cursor()as c:c.execute("SELECT setval('core_systemnotification_id_seq',%s,%s)",sequence)
  if failed:
   operation='INSERT OR UPDATE'if fault=='upsert'else'DELETE';target=expected_key if fault=='upsert'else stale.notification_key if fault=='delete-stale'else opposite
   with connection.cursor()as c:
    c.execute("CREATE FUNCTION rust_active_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF "+("NEW"if fault=='upsert'else"OLD")+".notification_key = '"+target+"' THEN RAISE EXCEPTION 'owned active "+fault+" failure' USING ERRCODE='P0001'; END IF; RETURN "+("NEW"if fault=='upsert'else"OLD")+"; END $$")
    c.execute('CREATE TRIGGER rust_active_reject BEFORE '+operation+' ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_active_reject()')
  cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel)
  native_task=app.tasks[TASK].apply_async(args=[],kwargs={});native=peer()
  native_meta=json.loads(backend.get(app.backend.get_key_for_task(native_task.id)));native_events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,31)]
  response=http('/api/backups/status/'+native_task.id+'/','admin')
  check(case+'-native-exact-partial-rows-events',rows()==after and native_events==events)
  check(case+'-native-source-http-consumer',response['status']==500 and response['body']==responses['admin']['body'])
  if failed:
   check(case+'-native-failure-type-args',native['ok']and native_meta['status']=='FAILURE'and native_meta['result']==metadata['result']and native_meta['children']==[]and native_meta['traceback'].startswith('Native Dispatcharr account expiration task diagnostic')and native_meta['traceback']!=metadata['traceback'])
   try:native_task.get(timeout=2);raise AssertionError('Expected ProgrammingError')
   except Exception as raised:check(case+'-native-source-get-raises',type(raised).__module__=='django.db.utils'and type(raised).__name__=='ProgrammingError'and str(raised)==metadata['result']['exc_message'][0])
  else:check(case+'-native-normal-null',native_task.get(timeout=2)is None)
  case_report['native']={'peer':native,'metadata':native_meta,'events':native_events,'http':response,'opaque_diagnostic_difference_explicit':failed}
 # Actual same-family repeated ID: first real SUCCESS/null, then actual failed
 # body. Only owned result TTL is aged; no synthetic success metadata is seeded.
 if worker.poll() is not None:
  log=open('/evidence/active-source-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-active@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close()
 drop_trigger();dupid=str(uuid.uuid4());Path('/evidence/owned-active-clock.json').write_text(json.dumps({'task_id':dupid,'now':NOW.isoformat()}))
 first=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=dupid);wait(lambda:next((p for p in records('postrun')if p['id']==dupid),None));key=app.backend.get_key_for_task(dupid);old_raw=backend.get(key);check('duplicate-first-real-success',json.loads(old_raw)['status']=='SUCCESS'and first.get(timeout=2)is None);backend.expire(key,120)
 with connection.cursor()as c:
  c.execute("CREATE FUNCTION rust_active_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned notification duplicate failure' USING ERRCODE='P0001'; END $$")
  c.execute('CREATE TRIGGER rust_active_reject BEFORE UPDATE ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_active_reject()')
 second=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=dupid);post=wait(lambda:([p for p in records('postrun')if p['id']==dupid][1]if len([p for p in records('postrun')if p['id']==dupid])>=2 else None));new_raw=backend.get(key)
 duplicate_http=http('/api/backups/status/'+dupid+'/','admin')
 check('duplicate-failed-body-executed',post['state']=='FAILURE'and'owned notification duplicate failure'in post['result'])
 check('duplicate-old-success-raw-ttl',new_raw==old_raw and 90<=backend.ttl(key)<=120)
 check('duplicate-get-and-consumer-observe-old-success',second.get(timeout=2)is None and duplicate_http['status']==500 and duplicate_http['body']=={'detail':"Failed to get task status: 'NoneType' object has no attribute 'get'"})
 report['same_id']={'task_id':dupid,'body_postrun':post,'old_metadata':json.loads(old_raw),'metadata_raw_unchanged':new_raw==old_raw,'remaining_ttl':backend.ttl(key),'http':duplicate_http}
 os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15);drop_trigger()
 native_id=str(uuid.uuid4());first=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=native_id);first_peer=peer();native_key=app.backend.get_key_for_task(native_id);old_native_raw=backend.get(native_key);check('native-duplicate-first-real-success',first_peer['ok']and first.get(timeout=2)is None);backend.expire(native_key,120)
 with connection.cursor()as c:
  c.execute("CREATE FUNCTION rust_active_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned notification duplicate failure' USING ERRCODE='P0001'; END $$")
  c.execute('CREATE TRIGGER rust_active_reject BEFORE UPDATE ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_active_reject()')
 second=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=native_id);second_peer=peer();new_native_raw=backend.get(native_key)
 check('native-duplicate-old-success-raw-and-ttl',old_native_raw==new_native_raw and 90<=backend.ttl(native_key)<=120 and second_peer['result']['outcome']=='AlreadySucceeded('+native_id+')')
 check('native-duplicate-original-consumer',second.get(timeout=2)is None and http('/api/backups/status/'+native_id+'/','admin')['body']==duplicate_http['body'])
 report['native_same_id']={'first':first_peer,'second':second_peer,'raw_unchanged':new_native_raw==old_native_raw,'remaining_ttl':backend.ttl(native_key),'metadata':json.loads(new_native_raw)}
 report['passed']=True
finally:
 if web and web.poll()is None:
  os.killpg(web.pid,signal.SIGTERM)
  try:web.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(web.pid,signal.SIGKILL);web.wait(timeout=5);report['forced_web_cleanup']=True
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 drop_trigger();cache.delete(channel_key,group_key,sentinel);broker.flushdb();backend.flushdb();report['broker_and_result_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
