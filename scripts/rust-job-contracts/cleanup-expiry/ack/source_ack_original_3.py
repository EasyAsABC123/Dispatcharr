"""Actual Original expired cleanup result-before-ACK and backend error boundary.
Original Dispatcharr/Celery/Kombu/Redis authors retain behavior and license credit.
Only copied DB and internal broker8/backend9; no task body or framework patch.
"""
import json,os,signal,subprocess,sys,time,uuid,datetime as dt
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.utils import timezone
from dispatcharr.celery import app
from owned_resp_proxy import OwnedResultProxy
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();worker=None;proxy=None;acl_user=None
report={'passed':False,'scope':'Actual Original cleanup-only expired request result-before-ACK and backend SETEX denial; no native acceptance or elapsed visibility/recovery claim','checks':[],'cases':[]}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def rows(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return[json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn):
 end=time.monotonic()+35
 while time.monotonic()<end:
  v=fn()
  if v:return v
  if worker and worker.poll()is not None:raise RuntimeError('Owned worker exited')
  time.sleep(.025)
 raise TimeoutError('Owned Source ACK deadline')
def snapshot():
 tags=broker.hkeys('unacked');check('snapshot-bounded-owned-unacked',len(tags)<=8)
 unacked={}
 for tag in tags:
  check('snapshot-bounded-tuple',broker.hstrlen('unacked',tag)<=131072);unacked[tag.decode()]=json.loads(broker.hget('unacked',tag))
 q=broker.lrange('celery',0,8);check('snapshot-bounded-owned-queue',len(q)<=8 and all(len(v)<=65536 for v in q))
 return {'queue':[json.loads(v)for v in q],'unacked':unacked,'scores':[(v.decode(),s)for v,s in broker.zrange('unacked_index',0,8,withscores=True)]}
def start(result_url,broker_url=None):
 global worker
 with Path('/evidence/cleanup-ack-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-cleanup-ack@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app',CELERY_RESULT_BACKEND=result_url,CELERY_BROKER_URL=broker_url or os.environ['CELERY_BROKER_URL']),stdout=log,stderr=log,start_new_session=True)
def stop():
 global worker
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_worker_cleanup']=True
 worker=None
try:
 for label in ['held-backend-EXEC-reply','backend-SETEX-denied']:
  broker.flushdb();backend.flushdb();ident=str(uuid.uuid4());key=app.backend.get_key_for_task(ident)
  proxy=OwnedResultProxy(key.decode(),hold=label.startswith('held'))
  user=''
  if label=='backend-SETEX-denied':
   acl_user='rust_cleanup_result_'+uuid.uuid4().hex[:8];check('owned-ACL-user-new',cache.execute_command('ACL','GETUSER',acl_user)is None)
   cache.execute_command('ACL','SETUSER',acl_user,'on','>playback-fixture-only','~celery-tasks:*','&celery-tasks:*','+@all','-set','-setex','-psetex');user=acl_user
  url='redis://'+user+':playback-fixture-only@127.0.0.1:'+str(proxy.port)+'/9'
  result=app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={},expires=timezone.now()-dt.timedelta(seconds=60));wire=json.loads(broker.lindex('celery',-1));start(url)
  if label.startswith('held'):
   check('actual-backend-reply-held',proxy.blocked.wait(20));held=snapshot();raw=backend.get(key);meta=json.loads(raw)
   check('backend-committed-REVOKED-before-ACK',meta['status']=='REVOKED'and meta['result']['exc_message']==['expired']and len(held['unacked'])==1 and len(held['scores'])==1 and held['queue']==[])
   tag=wire['properties']['delivery_tag'];tuple_=held['unacked'][tag];check('exact-Source-owned-unacked-geometry',len(tuple_)==3 and tuple_[1:]==['','celery']and tuple_[0]['headers']['id']==ident and tuple_[0]['properties']['delivery_tag']==tag)
   check('held-no-body-or-revoked-completion',not any(v['id']==ident for v in rows('prerun'))and not any(v['id']==ident for v in rows('revoked')))
   proxy.release.set();wait(lambda:any(v['id']==ident for v in rows('revoked')));wait(lambda:broker.hlen('unacked')==broker.llen('celery')==0);after=snapshot();check('actual-ACK-only-after-result-reply',after['unacked']=={}and after['queue']==[]and after['scores']==[])
   report['cases'].append({'name':label,'envelope':wire,'metadata':meta,'held':held,'after_reply':after,'backend_reply_observed_before_ACK':True})
  else:
   check('actual-backend-command-denied',proxy.denied.wait(20));time.sleep(.25);first=snapshot();time.sleep(.75);second=snapshot();raw=backend.get(key);meta=json.loads(raw)if raw else None
   check('denied-never-ran-body',not any(v['id']==ident for v in rows('prerun')))
   before_stop={'first_250ms':first,'second_1000ms':second,'metadata':meta,'AsyncResult_state':app.AsyncResult(ident).state,'worker_alive':worker.poll()is None}
   stop();after_stop=snapshot();report['cases'].append({'name':label,'envelope':wire,'before_stop':before_stop,'after_warm_stop':after_stop,'revoked_signals':[v for v in rows('revoked')if v['id']==ident]})
  stop();proxy.close();proxy=None
  if acl_user:cache.execute_command('ACL','DELUSER',acl_user);acl_user=None
 # Actual unexpired arrival whose ACK response is delayed past its expiry.
 broker.flushdb();backend.flushdb();proxy=OwnedResultProxy('',hold=True,mode='ack');start(os.environ['CELERY_RESULT_BACKEND'],'redis://:playback-fixture-only@127.0.0.1:'+str(proxy.port)+'/8')
 wait(lambda:any(v.get('cmd')=='brpop'and str(v.get('db'))=='8'for v in cache.client_list()))
 proxy.armed.set();ident=str(uuid.uuid4());result=app.tasks['celery.backend_cleanup'].apply_async(task_id=ident,args=[],kwargs={},expires=1)
 check('unexpired-ACK-real-response-held',proxy.blocked.wait(10));time.sleep(1.25);wait(lambda:any(v['id']==ident for v in rows('postrun')));meta=json.loads(backend.get(app.backend.get_key_for_task(ident)));held=snapshot()
 check('unexpired-body-runs-despite-late-ACK-response',meta['status']=='SUCCESS'and meta['result']is None and any(v['id']==ident for v in rows('prerun'))and not any(v['id']==ident for v in rows('revoked')))
 check('Source-ACK-server-applied-before-response',held['unacked']=={}and held['queue']==[]and proxy.ack_tag is not None)
 proxy.release.set();stop();report['cases'].append({'name':'unexpired-ACK-response-held-past-expiry','task_id':ident,'ack_tag':proxy.ack_tag,'metadata':meta,'held':held,'scope':'Real short-TTL unexpired arrival/body acceptance before delayed broker ACK response; no clock patch'})
 proxy.close();proxy=None
 report['passed']=True
finally:
 stop()
 if proxy:proxy.close()
 if acl_user:cache.execute_command('ACL','DELUSER',acl_user)
 broker.flushdb();backend.flushdb();report['broker_and_result_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
