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
report={'passed':False,'native_verified':True,'scope':'Actual Original/native bounded multi-profile fixed-clock worker comparison; no mixed-live or concurrent mutation claim','fixed_now':NOW.isoformat(),'checks':[],'cases':[]};worker=None
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

try:
 for name,obj in [('task',check_account_expirations.run),('evaluate',evaluate_profile_expiration_notification)]:Path('/evidence/original-multi-'+name+'.py').write_text(inspect.getsource(obj))
 M3UAccount.objects.all().update(is_active=False);M3UAccountProfile.objects.all().update(is_active=False)
 account=M3UAccount.objects.filter(account_type='STD').order_by('id').first();assert account
 M3UAccount.objects.filter(pk=account.pk).update(name='Owned Multi Account',is_active=True)
 M3UAccount.objects.bulk_create([M3UAccount(name='Owned Second Account',account_type='STD',is_active=False)]);other=M3UAccount.objects.get(name='Owned Second Account')
 M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=account,name='Owned Multi '+str(n),is_active=True,exp_date=NOW+datetime.timedelta(days=d))for n,d in enumerate([-1,1,8])])
 ids=list(M3UAccountProfile.objects.filter(name__startswith='Owned Multi ').values_list('id',flat=True));assert len(ids)==3
 user=get_user_model().objects.order_by('id').first();assert user
 query=lambda:M3UAccountProfile.objects.filter(m3u_account__is_active=True,is_active=True,exp_date__isnull=False).select_related('m3u_account')
 report['exact_profile_query_sql']=str(query().query)
 with Path('/evidence/active-source-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-active@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 for case in ['three-profiles','reversed-physical-profile-order','cross-account','second-upsert-failure','all-warning-and-shared-account','new-and-existing-mixed','thirty-two-profile-boundary','single-profile-unused-large-json']:
  if worker.poll() is not None:
   log=open('/evidence/active-source-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-active@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close()
  drop_trigger();SystemNotification.objects.filter(notification_key__startswith='xc-exp-').delete();SystemNotification.objects.filter(notification_key__startswith='rust-owned-active-').delete()
  cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel);cache.set(sentinel,'preserve',ex=600)
  if case=='reversed-physical-profile-order':
   with connection.cursor()as c:c.execute('CREATE INDEX rust_owned_multi_profile_desc ON m3u_m3uaccountprofile(id DESC)');c.execute('CLUSTER m3u_m3uaccountprofile USING rust_owned_multi_profile_desc')
  if case=='all-warning-and-shared-account':M3UAccountProfile.objects.filter(pk__in=ids).update(exp_date=NOW+datetime.timedelta(days=2))
  if case=='cross-account':M3UAccount.objects.filter(pk=other.pk).update(is_active=True);M3UAccountProfile.objects.filter(pk=ids[1]).update(m3u_account=other)
  if case=='thirty-two-profile-boundary':
   M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=account,name='Owned Multi Extra '+str(n),is_active=True,exp_date=NOW+datetime.timedelta(days=2))for n in range(29)]);ids=list(M3UAccountProfile.objects.filter(is_active=True).values_list('id',flat=True))
  if case=='single-profile-unused-large-json':
   selected=ids[0];M3UAccountProfile.objects.all().update(is_active=False);M3UAccountProfile.objects.filter(pk=selected).update(is_active=True,custom_properties={'owned':'x'*70000});ids=[selected]
  profiles=list(query());expected_order=[p.pk for p in profiles];assert set(expected_order)==set(ids)
  for profile in profiles:
   seed_notice('xc-exp-warning-'+str(profile.pk),False);seed_notice('xc-exp-expired-'+str(profile.pk),False)
  stale=seed_notice('xc-exp-warning-owned-stale');keep=seed_notice('rust-owned-active-unrelated');inactive=seed_notice('xc-exp-expired-owned-inactive',False)
  if case=='new-and-existing-mixed':SystemNotification.objects.filter(notification_key='xc-exp-warning-'+str(profiles[1].pk)).delete()
  fault=case=='second-upsert-failure';target=None
  if fault:
   target=('xc-exp-expired-'if profiles[1].exp_date<=NOW else'xc-exp-warning-')+str(profiles[1].pk)
   with connection.cursor()as c:
    c.execute("CREATE FUNCTION rust_active_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.notification_key = '"+target+"' THEN RAISE EXCEPTION 'owned multi second upsert failure' USING ERRCODE='P0001'; END IF; RETURN NEW; END $$")
    c.execute('CREATE TRIGGER rust_active_reject BEFORE INSERT OR UPDATE ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_active_reject()')
  with connection.cursor()as c:c.execute("SELECT last_value,is_called FROM core_systemnotification_id_seq");sequence=c.fetchone()
  before=rows();taskid=str(uuid.uuid4());Path('/evidence/owned-active-clock.json').write_text(json.dumps({'task_id':taskid,'now':NOW.isoformat()}))
  result=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=taskid);post=wait(lambda:next((p for p in records('postrun')if p['id']==taskid),None));metadata=json.loads(backend.get(app.backend.get_key_for_task(taskid)));after=rows();events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,127)]
  expected_events=[];active_keys=[]
  for pos,profile in enumerate(profiles):
   warning='xc-exp-warning-'+str(profile.pk);expired='xc-exp-expired-'+str(profile.pk)
   if profile.exp_date<=NOW+datetime.timedelta(days=7):
    key=expired if profile.exp_date<=NOW else warning;opposite=warning if key==expired else expired
    expected_events.append(('notification_dismissed',opposite))
    if fault and pos==1:break
    expected_events.append(('system_notification',key));active_keys.append(key)
   else:
    keys=[r['notification_key']for r in sorted([r for r in before['notices']if r['notification_key']in[warning,expired]],key=lambda r:(r['priority'],r['created_at']),reverse=True)]
    expected_events.extend(('notification_dismissed',key)for key in keys)
  if not fault:expected_events.append(('notification_dismissed',stale.notification_key))
  actual_events=[(e['data']['type'],e['data'].get('notification_key',e['data'].get('notification',{}).get('notification_key')))for e in events]
  report['cases'].append({'name':case,'expected_profile_iteration':expected_order,'expected_event_sequence':expected_events,'events':events,'before':before,'after':after,'metadata':metadata,'envelope':published[taskid],'prerun':next(p for p in records('prerun')if p['id']==taskid),'sql':next(p for p in records('sql')if p['id']==taskid)})
  check(case+'-source-state',metadata['status']==post['state']==('FAILURE'if fault else'SUCCESS'))
  check(case+'-predeclared-iteration-events',actual_events==expected_events)
  check(case+'-profile-order-control',expected_order==list(reversed(sorted(ids)))if case=='reversed-physical-profile-order'else set(expected_order)==set(ids))
  check(case+'-stale-commit-boundary',SystemNotification.objects.filter(pk=stale.pk).exists()==fault)
  check(case+'-unrelated-preserved',SystemNotification.objects.filter(pk__in=[keep.pk,inactive.pk]).count()==2)
  check(case+'-cache-preserved',cache.get(sentinel)==b'preserve'and cache.ttl(sentinel)>550)
  check(case+'-result-ttl',3580<=backend.ttl(app.backend.get_key_for_task(taskid))<=3600)
  wait(lambda:broker.llen('celery')==broker.hlen('unacked')==0);check(case+'-queue-drained',True)
  if not fault:check(case+'-normal-null',result.get(timeout=2)is None and metadata['children']==[]and metadata['traceback']is None)
  os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15)
  drop_trigger();SystemNotification.objects.all().delete();SystemNotification.objects.bulk_create([SystemNotification(**r)for r in before['notices']])
  for r in before['notices']:SystemNotification.objects.filter(pk=r['id']).update(created_at=r['created_at'],updated_at=r['updated_at'])
  NotificationDismissal.objects.bulk_create([NotificationDismissal(**r)for r in before['dismissals']])
  with connection.cursor()as c:c.execute("SELECT setval('core_systemnotification_id_seq',%s,%s)",sequence)
  if fault:
   with connection.cursor()as c:
    c.execute("CREATE FUNCTION rust_active_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.notification_key = '"+target+"' THEN RAISE EXCEPTION 'owned multi second upsert failure' USING ERRCODE='P0001'; END IF; RETURN NEW; END $$")
    c.execute('CREATE TRIGGER rust_active_reject BEFORE INSERT OR UPDATE ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_active_reject()')
  cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel)
  native_task=app.tasks[TASK].apply_async(args=[],kwargs={});request={'action':'run','notification_now':NOW.isoformat()}
  peer=subprocess.run(['/evidence/native-default-peer','default_peer::native_default_family_peer','--ignored','--nocapture','--test-threads=1'],input=json.dumps(request),text=True,capture_output=True,timeout=30)
  Path('/evidence/native-multi-'+case+'.log').write_text(peer.stdout+peer.stderr);peer.check_returncode()
  marker=next(v.split('NATIVE_DEFAULT_PAIR_JSON=',1)[1]for v in peer.stdout.splitlines()if 'NATIVE_DEFAULT_PAIR_JSON='in v);native=json.loads(marker)
  native_after=rows();native_events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,127)];native_raw=backend.get(app.backend.get_key_for_task(native_task.id));native_metadata=None if native_raw is None else json.loads(native_raw)
  report['cases'][-1]['native']={'peer':native,'after':native_after,'events':native_events,'metadata':native_metadata}
  check(case+'-native-exact-rows',native_after==after);check(case+'-native-exact-event-order',native_events==events)
  if fault:
    check(case+'-native-typed-source-failure',native['ok']and native_metadata['status']=='FAILURE'and native_metadata['result']==metadata['result']and native_metadata['children']==[]and native_metadata['traceback'].startswith('Native Dispatcharr account expiration task diagnostic'))
    try:native_task.get(timeout=2);raise AssertionError('Expected actual Source decoder ProgrammingError')
    except Exception as raised:
     check(case+'-native-original-get-exception',type(raised).__module__=='django.db.utils'and type(raised).__name__=='ProgrammingError'and str(raised)==metadata['result']['exc_message'][0])
  else:check(case+'-native-source-asyncresult',native['ok']and native_task.get(timeout=2)is None and native_metadata['status']=='SUCCESS'and native_metadata['result']is None and native_metadata['children']==[]and native_metadata['traceback']is None and native_metadata['task_id']==native_task.id and 3580<=backend.ttl(app.backend.get_key_for_task(native_task.id))<=3600)
  check(case+'-native-owned-queue-drain',broker.llen('celery')==broker.hlen('unacked')==0)
  check(case+'-native-physical-drain',native['drained']and native['jobs_drained']and native['pool_closed']and native['available_slots']==2)
 report['passed']=True
finally:
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 drop_trigger();cache.delete(channel_key,group_key,sentinel);broker.flushdb();backend.flushdb();report['broker_and_result_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
