"""Replay observed Source136 exception payloads through native bounded writer.
No application worker, native classifier, native traceback generation or202.
Original Dispatcharr/Celery/Kombu authors retain all application/framework credit.
"""
import datetime as dt,hashlib,json,os,subprocess,sys,time,uuid
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_')or len(database)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in database):raise RuntimeError('Copied DB required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from celery.result import AsyncResult
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
queued=broker.lrange('celery',0,32)
if len(queued)>32 or any(len(raw)>65536 for raw in queued):raise RuntimeError('Startup queue bound')
quarantined=[]
for raw in queued:
 header=json.loads(raw).get('headers',{});name=header.get('task')
 if name!='apps.plugins.tasks.refresh_plugin_repos':raise RuntimeError('Unrelated startup task refused')
 quarantined.append({'task':name,'sha256':hashlib.sha256(raw).hexdigest()})
broker.flushdb();backend.flushdb()
corpus=json.loads(Path('/evidence/failure-corpus.json').read_text());cases=[case for case in corpus['cases']if case['result']['status']=='FAILURE'];assert len(cases)==5
checks=[];report={'passed':False,'scope':'Bounded observed exception-payload native publication/actualSource reader interoperability; no task execution/classifier/native traceback equivalence','checks':checks,'quarantined':quarantined,'opaque_traceback_origin':'Exact supplied Original136 observation replay; not generated/executed native Python frames','native_exception_execution_verified':False}
def check(name,passed,**detail):
 checks.append({'name':name,'passed':bool(passed),**detail})
 if not passed:raise AssertionError(name)
def native(metadata,written):
 task_id=metadata['task_id'];path=Path('/evidence/failure-input-'+uuid.uuid4().hex+'.json');path.write_text(json.dumps({'metadata':metadata,'task_id':task_id,'expect_written':written}))
 request=dict(os.environ,JOB_FAILURE_INPUT_PATH=str(path));done=subprocess.run(['/evidence/native-failure-writer','--ignored','--exact','index_failure::tests::native_failure_payload_peer','--nocapture'],env=request,text=True,capture_output=True,timeout=15)
 with Path('/evidence/native-peer.log').open('a')as file:file.write(done.stdout+done.stderr)
 done.check_returncode();assert 'test result: ok. 1 passed; 0 failed'in done.stdout
 path.unlink()
def message(subscription):
 deadline=time.monotonic()+3
 while time.monotonic()<deadline:
  value=subscription.get_message(ignore_subscribe_messages=True,timeout=.1)
  if value:return value
 raise TimeoutError('Owned result event')
try:
 for case in cases:
  metadata=json.loads(json.dumps(case['result']));metadata['task_id']=str(uuid.uuid4());metadata['date_done']=dt.datetime.now(dt.UTC).isoformat(timespec='microseconds')
  task_id=metadata['task_id'];key=app.backend.get_key_for_task(task_id);subscription=backend.pubsub();subscription.subscribe(key);subscription.get_message(timeout=1)
  native(metadata,True);raw=backend.get(key);actual=json.loads(raw);label=case['name']
  check('metadata-'+label,actual==metadata)
  check('ttl-backend-separation-'+label,3590<=backend.ttl(key)<=3600 and not broker.exists(key))
  event=message(subscription);check('committed-pubsub-'+label,event['channel']==key and event['data']==raw)
  result=AsyncResult(task_id,app=app);error=result.get(timeout=1,propagate=False);expected=metadata['result']
  check('actual-source-exception-'+label,result.ready()and result.failed()and not result.successful()and type(error).__module__==expected['exc_module']and type(error).__name__==expected['exc_type']and list(error.args)==expected['exc_message'])
  # Existing SUCCESS suppresses a failure result write, not task execution.
  # Use a fresh ID for pre-existing SUCCESS. Replacing the observed terminal
  # FAILURE in-place with rawSET would leave Source's result consumer cache
  # stale: that artificial transition is not the SUCCESS guard contract.
  success_id=str(uuid.uuid4());success_key=app.backend.get_key_for_task(success_id)
  success={'status':'SUCCESS','result':None,'traceback':None,'children':[],'date_done':dt.datetime.now(dt.UTC).isoformat(timespec='microseconds'),'task_id':success_id}
  success_raw=json.dumps(success).encode();backend.set(success_key,success_raw,ex=300);time.sleep(1.1);before=backend.ttl(success_key)
  guarded=dict(metadata,task_id=success_id);guard_subscription=backend.pubsub();guard_subscription.subscribe(success_key);guard_subscription.get_message(timeout=1)
  native(guarded,False)
  check('existing-success-raw-ttl-guard-'+label,backend.get(success_key)==success_raw and 0<backend.ttl(success_key)<=before and AsyncResult(success_id,app=app).get(timeout=1)is None)
  check('existing-success-no-event-'+label,guard_subscription.get_message(ignore_subscribe_messages=True,timeout=.1)is None)
  guard_subscription.close();subscription.close()
  # FAILURE remains replaceable; repeat writer refreshes expiry and publishes.
  backend.set(key,json.dumps(case['result']).encode(),ex=300);native(metadata,True)
  check('existing-failure-rewrite-'+label,json.loads(backend.get(key))==metadata and 3590<=backend.ttl(key)<=3600)
 check('no-broker-task-or-reservation-sideeffects',broker.llen('celery')==0 and broker.hlen('unacked')==0 and broker.zcard('unacked_index')==0)
 report['passed']=all(c['passed']for c in checks)
finally:
 Path('/evidence/source-failure-writer-private.json').write_text(json.dumps(report,indent=2)+'\n')
