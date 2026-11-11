"""Actual Original inactive-account stale-alert collector boundary. Task unchanged.
Original Dispatcharr/Celery/Django/Channels authors retain code/license credit.
Owned copied database only; no active profile evaluation/network or native claim.
"""
import base64,datetime,hashlib,inspect,json,os,signal,subprocess,sys,time
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
published={}
@before_task_publish.connect
def publication(sender=None,body=None,headers=None,properties=None,**kwargs):
 if sender=='apps.m3u.tasks.check_account_expirations':published[headers['id']]={'headers':headers,'body':body,'properties':properties,'scope':'Actual producer before_task_publish metadata; not post-consumption raw Redis envelope'}
from apps.m3u.models import M3UAccountProfile,M3UAccount
from core.models import SystemNotification,NotificationDismissal
from apps.m3u.tasks import check_account_expirations
import core.utils
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();layer=get_channel_layer();channel='specific.rust_owned_inactive!receiver';channel_key=layer.prefix+channel.split('!')[0]+'!';group_key=layer._group_key('updates');sentinel='rust:owned:inactive:sentinel'
report={'passed':False,'scope':'Actual pinned Original/native inert production library inactive accounts/collector/Channels normal boundaries; SQL failure metadata Source-only','checks':[],'cases':[]};worker=None

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
 raise TimeoutError('Owned inactive deadline')
def posts():
 p=Path('/evidence/periodic-worker-postrun.jsonl');return[json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def snapshot():
 return {'notices':list(SystemNotification.objects.order_by('id').values('id','notification_key','is_active','priority')),'dismissals':list(NotificationDismissal.objects.order_by('id').values('id','notification_id','user_id'))}
def native(action,**fields):
 value=subprocess.run(['/evidence/native-default-peer','default_peer::native_default_family_peer','--exact','--ignored','--nocapture'],input=json.dumps({'action':action,**fields}),capture_output=True,text=True,timeout=40)
 with Path('/evidence/native-inactive-private.log').open('a')as log:log.write(value.stdout+value.stderr)
 value.check_returncode();lines=[line.split('=',1)[1]for line in value.stdout.splitlines()if line.startswith('NATIVE_DEFAULT_PAIR_JSON=')];assert len(lines)==1
 result=json.loads(lines[0]);check('native-'+action+'-physical-drain',result['drained']and result['jobs_drained']and result['pool_closed']and result['available_slots']==2);return result

def drop_trigger():
 with connection.cursor()as c:
  for table in ['core_systemnotification','core_notificationdismissal']:c.execute('DROP TRIGGER IF EXISTS rust_inactive_reject ON '+table)
  c.execute('DROP FUNCTION IF EXISTS rust_inactive_reject()')
try:
 M3UAccountProfile.objects.all().update(is_active=False)
 account=M3UAccount.objects.order_by('id').first();assert account is not None
 M3UAccount.objects.filter(pk=account.pk).update(is_active=False)
 M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=account,name='Owned active expired profile inactive account',is_active=True,exp_date=datetime.datetime(2020,1,1,tzinfo=datetime.timezone.utc))])
 inactive_profile=M3UAccountProfile.objects.get(name='Owned active expired profile inactive account')
 check('actual-inactive-account-nonnull-expiring-active-profile',not M3UAccount.objects.get(pk=account.pk).is_active and inactive_profile.is_active and inactive_profile.exp_date is not None)
 profiles_before=list(M3UAccountProfile.objects.order_by('id').values('id','m3u_account_id','is_active','exp_date'))
 # Deliberately inactive profiles retain expiration values; do not erase dates.
 check('no-eligible-active-profiles',not M3UAccountProfile.objects.filter(is_active=True,m3u_account__is_active=True,exp_date__isnull=False).exists())
 report['schema']={'notification_table':SystemNotification._meta.db_table,'dismissal_table':NotificationDismissal._meta.db_table,'ordering':SystemNotification._meta.ordering,'notification_relations':[{'name':x.name,'related_model':x.related_model._meta.label}for x in SystemNotification._meta.related_objects]}
 for name,obj in [('task',check_account_expirations.run),('dismissal',core.utils.send_notification_dismissed),('collector-delete',Collector.delete),('collector-collect',Collector.collect)]:
  data=inspect.getsource(obj).encode();Path('/evidence/original-inactive-'+name+'.py').write_bytes(data)
 user=get_user_model().objects.order_by('id').first();assert user
 with Path('/evidence/inactive-source-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-inactive@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 cases=[('empty',0,False,None),('single',1,False,None),('multi-dismissals',3,True,None),('delete-notification-fault',2,True,'core_systemnotification'),('delete-dismissal-fault',2,True,'core_notificationdismissal'),('event-wrongtype',2,True,'event')]
 for direction,(case,count,dismissals,fault) in [('Original',c)for c in cases]+[('Native',c)for c in cases if c[3]not in ['core_systemnotification','core_notificationdismissal']]+[('NativePublisher',cases[1]),('NativePublisher',cases[2])]:
  if direction=='Native'and worker is not None:
   os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15);worker=None
  if direction=='NativePublisher'and worker is None:
   with Path('/evidence/inactive-source-worker-private.log').open('ab')as log:
    worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-inactive-published@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True)
  drop_trigger();SystemNotification.objects.filter(notification_key__startswith='xc-exp-').delete()
  SystemNotification.objects.filter(notification_key__startswith='rust-owned-inactive-').delete()
  cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel);cache.set(sentinel,'preserve',ex=600)
  keep=SystemNotification.objects.create(notification_key='rust-owned-inactive-unrelated',title='Owned unrelated',message='Owned',is_active=True)
  inactive=SystemNotification.objects.create(notification_key='xc-exp-warning-owned-inactive',title='Owned inactive',message='Owned',is_active=False)
  made=[]
  for n in range(count):
   row=SystemNotification.objects.create(notification_key=('xc-exp-warning-'if n%2==0 else'xc-exp-expired-')+'owned-'+str(n),title='Owned stale',message='Owned',is_active=True,priority=['high','normal','low'][n%3])
   # Exact distinct timestamps prevent relying on equal-key sort stability.
   SystemNotification.objects.filter(pk=row.pk).update(created_at=datetime.datetime(2024,1,n+1,tzinfo=datetime.timezone.utc));made.append(row)
   if dismissals:NotificationDismissal.objects.create(user=user,notification=row)
  expected_keys=list(SystemNotification.objects.filter(is_active=True).filter(__import__('django.db.models',fromlist=['Q']).Q(notification_key__startswith='xc-exp-warning-')|__import__('django.db.models',fromlist=['Q']).Q(notification_key__startswith='xc-exp-expired-')).values_list('notification_key',flat=True))
  if fault in ['core_systemnotification','core_notificationdismissal']:
   with connection.cursor()as c:
    c.execute("CREATE FUNCTION rust_inactive_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned inactive delete failure' USING ERRCODE='P0001'; END $$")
    c.execute('CREATE TRIGGER rust_inactive_reject BEFORE DELETE ON '+fault+' FOR EACH ROW EXECUTE FUNCTION rust_inactive_reject()')
  if fault=='event':cache.delete(group_key);cache.set(group_key,'owned-wrongtype')
  before=snapshot()
  if direction=='NativePublisher':
   import uuid
   ident=str(uuid.uuid4());outcome=native('publish',kind='inactive',task_id=ident);check(case+'-native-published',outcome['ok']);result=app.AsyncResult(ident);envelope={'scope':'Actual native publication consumed by Original worker'}
  else:
   result=app.tasks['apps.m3u.tasks.check_account_expirations'].apply_async(args=[],kwargs={});envelope=published[result.id]
  if direction=='Native':
   outcome=native('run');check(case+'-native-completed',outcome['ok']and outcome['result']['outcome']=='Completed('+result.id+')');post={'state':'SUCCESS'}
  else:post=wait(lambda:next((p for p in posts()if p['id']==result.id),None))
  raw=backend.get(app.backend.get_key_for_task(result.id));metadata=json.loads(raw);after=snapshot();events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,15)]
  failed=fault in ['core_systemnotification','core_notificationdismissal']
  check(case+'-result-status',post['state']==metadata['status']==('FAILURE'if failed else'SUCCESS'))
  check(case+'-rows',after==before if failed else not SystemNotification.objects.filter(pk__in=[x.pk for x in made]).exists())
  check(case+'-related-dismissals',NotificationDismissal.objects.filter(notification_id__in=[x.pk for x in made]).count()==(count if failed and dismissals else 0))
  check(case+'-unrelated-inactive-preserved',SystemNotification.objects.filter(pk__in=[keep.pk,inactive.pk]).count()==2)
  event_keys=[e['data']['notification_key']for e in events]
  check(case+'-events',event_keys==([]if failed or fault=='event'else expected_keys))
  check(case+'-cache-preserved',cache.get(sentinel)==b'preserve'and cache.ttl(sentinel)>550)
  check(case+'-result-ttl',3580<=backend.ttl(app.backend.get_key_for_task(result.id))<=3600)
  check(case+'-profiles-unchanged',profiles_before==list(M3UAccountProfile.objects.order_by('id').values('id','m3u_account_id','is_active','exp_date')))
  check(case+'-queue-drained',broker.llen('celery')==broker.hlen('unacked')==0)
  if not failed:check(case+'-normal-null',result.get(timeout=2)is None and metadata['result']is None and metadata['children']==[]and metadata['traceback']is None)
  report['cases'].append({'name':case,'direction':direction,'envelope':envelope,'metadata':metadata,'before':before,'after':after,'events':events,'expected_event_keys':expected_keys})
 # Unported active evaluation is explicitly refused before native earlyACK.
 if worker is not None:
  os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15);worker=None
 M3UAccount.objects.filter(pk=account.pk).update(is_active=True)
 pending=app.tasks['apps.m3u.tasks.check_account_expirations'].apply_async(args=[],kwargs={});queued=broker.lindex('celery',-1);refused=native('run')
 check('unported-active-profile-unchanged-FIFO',refused['ok']and refused['result']['outcome']=='Unsupported(InactiveAccountNotifications)'and broker.lindex('celery',-1)==queued and backend.get(app.backend.get_key_for_task(pending.id))is None)
 M3UAccount.objects.filter(pk=account.pk).update(is_active=False)
 broker.flushdb()
 report['passed']=True
finally:
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 drop_trigger();cache.delete(channel_key,group_key,sentinel);broker.flushdb();backend.flushdb()
 report['broker_and_result_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
