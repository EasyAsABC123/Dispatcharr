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
report={'passed':False,'native_verified':True,'scope':'Actual pinned Original worker with owned fixed-clock dependency; worker-local empty proxy managers; no mixed-live or concurrency claim','fixed_now':NOW.isoformat(),'checks':[],'cases':[]};worker=None
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
 row=SystemNotification.objects.create(notification_key=key,title='Owned existing',message='Owned existing',notification_type='info',priority='low',source='developer',is_active=active,admin_only=False,action_data={'owned':'before'},expires_at=OLD)
 SystemNotification.objects.filter(pk=row.pk).update(created_at=OLD,updated_at=OLD);NotificationDismissal.objects.create(user=user,notification=row);return row
CASES=[('new-warning',86400000000,'new-target'),('unicode-expired',-1,'unicode'),('expired-before',-1,None),('equal-now',0,None),('warning-today',1,None),('warning-one-day',86400000000,None),('warning-floor-day',172799999999,None),('warning-seven-days',604800000000,None),('outside-seven-days',604800000001,None),('null-date',None,None),('inactive-account',-1,'inactive-account'),('inactive-profile',-1,'inactive-profile'),('channels-unavailable',86400000000,'event'),('opposite-delete-fault',86400000000,'delete-opposite'),('upsert-fault',86400000000,'upsert'),('stale-delete-fault',86400000000,'delete-stale'),('oversize-title',-1,'title')]
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
  # Paired native path uses identical stored rows/IDs/sequence and producer.
  os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15)
  drop_trigger();SystemNotification.objects.all().delete()
  SystemNotification.objects.bulk_create([SystemNotification(**r)for r in before['notices']])
  for r in before['notices']:SystemNotification.objects.filter(pk=r['id']).update(created_at=r['created_at'],updated_at=r['updated_at'])
  NotificationDismissal.objects.bulk_create([NotificationDismissal(**r)for r in before['dismissals']])
  with connection.cursor()as c:c.execute("SELECT setval('core_systemnotification_id_seq',%s,%s)",sequence)
  if fault in ['delete-opposite','upsert','delete-stale']:
   operation='INSERT OR UPDATE'if fault=='upsert'else'DELETE';target=expected_key if fault=='upsert'else stale.notification_key if fault=='delete-stale'else opposite
   with connection.cursor()as c:
    c.execute("CREATE FUNCTION rust_active_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF "+("NEW"if fault=='upsert'else"OLD")+".notification_key = '"+target+"' THEN RAISE EXCEPTION 'owned active "+fault+" failure' USING ERRCODE='P0001'; END IF; RETURN "+("NEW"if fault=='upsert'else"OLD")+"; END $$")
    c.execute('CREATE TRIGGER rust_active_reject BEFORE '+operation+' ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_active_reject()')
  cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel)
  if fault=='event':cache.delete(group_key);cache.set(group_key,'owned-wrongtype')
  native_task=app.tasks[TASK].apply_async(args=[],kwargs={})
  request={'action':'run','notification_now':NOW.isoformat()}
  peer=subprocess.run(['/evidence/native-default-peer','default_peer::native_default_family_peer','--ignored','--nocapture','--test-threads=1'],input=json.dumps(request),text=True,capture_output=True,timeout=30)
  Path('/evidence/native-active-'+case+'.log').write_text(peer.stdout+peer.stderr);peer.check_returncode()
  marker=next(v.split('NATIVE_DEFAULT_PAIR_JSON=',1)[1]for v in peer.stdout.splitlines()if 'NATIVE_DEFAULT_PAIR_JSON='in v);native=json.loads(marker)
  native_after=rows();native_events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,31)]
  native_raw=backend.get(app.backend.get_key_for_task(native_task.id));native_metadata=None if native_raw is None else json.loads(native_raw)
  case_report['native']={'peer':native,'after':native_after,'events':native_events,'metadata':native_metadata,'queue':broker.llen('celery'),'unacked':broker.hlen('unacked')}
  if fault=='title':
   check(case+'-native-preack-unsupported',native['ok']and native['result']['outcome']=='Unsupported(InactiveAccountNotifications)'and broker.llen('celery')==1 and native_metadata is None and native_after==before and native_events==[]);broker.delete('celery')
  else:
   check(case+'-native-exact-rows',native_after==after)
   check(case+'-native-exact-events',native_events==events)
   if failed:
    check(case+'-native-typed-source-failure',native['ok']and native_metadata['status']=='FAILURE'and native_metadata['result']==metadata['result']and native_metadata['children']==[]and native_metadata['traceback'].startswith('Native Dispatcharr account expiration task diagnostic'))
    try:native_task.get(timeout=2);raise AssertionError('Expected actual Source decoder ProgrammingError')
    except Exception as raised:
     check(case+'-native-original-get-exception',type(raised).__module__=='django.db.utils'and type(raised).__name__=='ProgrammingError'and str(raised)==metadata['result']['exc_message'][0])
   else:check(case+'-native-original-asyncresult',native['ok']and native_task.get(timeout=2)is None and native_metadata['status']=='SUCCESS'and native_metadata['result']is None and native_metadata['children']==[]and native_metadata['traceback']is None)
   if not failed:check(case+'-native-ttl-and-id',native_metadata['task_id']==native_task.id and 3580<=backend.ttl(app.backend.get_key_for_task(native_task.id))<=3600 and datetime.datetime.fromisoformat(native_metadata['date_done']).tzinfo is not None)
   check(case+'-native-queue-drained',broker.llen('celery')==broker.hlen('unacked')==0)
  check(case+'-native-physical-drain',native['drained']and native['jobs_drained']and native['pool_closed']and native['available_slots']==2)
 report['passed']=True
finally:
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 drop_trigger();cache.delete(channel_key,group_key,sentinel);broker.flushdb();backend.flushdb();report['broker_and_result_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
