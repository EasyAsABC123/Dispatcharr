"""Actual pinned Celery/HTTP contract capture. All application operations use a
precreated owned copied DB; never restore/drop/migrate the default fixture DB.
Original Dispatcharr authors retain application credit and AGPL licensing.
"""
import base64,hashlib,hmac,importlib.metadata,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Owned fixture guard required')
db=os.environ.get('POSTGRES_DB','')
if not db.startswith('rust_probe_backup_')or not db.replace('_','').isalnum()or len(db)>63:raise RuntimeError('Copied DB authority required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis,requests
from dispatcharr.celery import app
from celery.result import AsyncResult
from apps.accounts.models import User
from rest_framework_simplejwt.tokens import RefreshToken
from apps.backups import tasks
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Isolated broker/result DB8/9 required')
broker.flushdb();backend.flushdb()
# Exercise Source's actual startup-dispatch lock path; do not launch recovery or
# external version-check jobs unrelated to this transport/backup fixture.
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
roles={}
for role,level,active,props in [('admin',10,True,{}),('viewer',1,True,{}),('inactive',10,False,{}),('blocked',10,True,{'allowed_networks':{'UI':'203.0.113.0/24'}})]:
 user=User.objects.create(username='rust-probe-job-'+role,password='!',user_level=level,is_active=active,custom_properties=props);roles[role]=str(RefreshToken.for_user(user).access_token)
root=Path(os.environ['BACKUP_ROOT']);root.mkdir(parents=True,exist_ok=True);(root/'invalid.zip').write_bytes(b'not ZIP fixture')
from worker_tasks import default,started
web=None;worker=None;checks=[];jobs={};report={'passed':False,'scope':'Actual original Celery transport/status/backup publication; no native job implementation','checks':checks,'versions':{name:importlib.metadata.version(name)for name in ['celery','kombu','redis','Django']},'configuration':{k:app.conf.get(k)for k in ['task_default_queue','task_default_exchange','task_default_routing_key','task_serializer','result_serializer','accept_content','task_track_started','task_acks_late','task_acks_on_failure_or_timeout','task_reject_on_worker_lost','worker_prefetch_multiplier','broker_pool_limit','result_expires','broker_transport_options','result_backend_transport_options']}}
def record(name,passed,**details):
 checks.append({'name':name,'passed':bool(passed),**details})
 if not passed:raise AssertionError(name)
def wait(predicate,timeout=30):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  try:value=predicate()
  except(requests.ConnectionError,requests.Timeout):value=None
  if value:return value
  time.sleep(.05)
 raise TimeoutError('Owned job condition deadline')
def stop(process,hard=False):
 if process is not None and process.poll()is None:
  os.killpg(process.pid,signal.SIGKILL if hard else signal.SIGTERM)
  try:process.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
def quarantine_unrelated():
 # Real web startup enqueues plugin-repository refresh. Retain its type/hash and
 # remove only that unrelated work from this owned broker before any worker runs;
 # never execute provider/plugin/version network jobs in the capture fixture.
 rows=broker.lrange('celery',0,100)
 if len(rows)>100:raise RuntimeError('Owned broker queue bound')
 for raw in rows:
  item=json.loads(raw);name=item['headers']['task']
  if not(name.startswith('rust_probe.')or name in ['apps.backups.tasks.create_backup_task','apps.backups.tasks.restore_backup_task']):
   report.setdefault('quarantined_startup_tasks',[]).append({'task_name':name,'envelope_sha256':hashlib.sha256(raw).hexdigest()})
   broker.lrem('celery',0,raw)
def launch_worker():
 global worker
 log=open('/evidence/celery-private.log','ab')
 worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','worker_tasks:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-probe@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 # Redis pidbox consumer binding proves real broker consumer registration.
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'));time.sleep(.5)
def token(taskid):return hmac.new(b'playback-fixture-only',taskid.encode(),hashlib.sha256).hexdigest()[:32]
def http(path,role='admin',method='GET',body=None,headers=None,query=None):
 h={'Host':'localhost','Accept':'application/json'}
 if role is not None:h['Authorization']='Bearer '+('invalid'if role=='invalid'else roles[role])
 h.update(headers or{});r=requests.request(method,'http://127.0.0.1:5657/api/backups/'+path,headers=h,data=body,params=query,timeout=15,allow_redirects=False)
 value={'status':r.status_code,'mime':r.headers.get('content-type','').split(';')[0]}
 if 'json'in value['mime']:value['body']=r.json()
 else:value.update(body_bytes=len(r.content),body_sha256=hashlib.sha256(r.content).hexdigest())
 return value
def status(taskid,role='admin',provided=None):return http('status/'+taskid+'/',role=role,query={}if provided is None else{'token':provided})
def state(taskid):return AsyncResult(taskid,app=app).state
def ready(taskid):return AsyncResult(taskid,app=app).ready()
def result_key(taskid):return app.backend.get_key_for_task(taskid)
def unacked_tasks():
 values=broker.hvals('unacked')
 if len(values)>100:raise RuntimeError('Owned unacked reservation ceiling')
 result=[]
 for raw in values:
  message=json.loads(raw)[0];header=message['headers']
  result.append({'task_name':header['task'],'id':header['id']})
 return result
def issue(kind,tracked=False):
 marker='rust-probe-job:'+uuid.uuid4().hex;t=(started if tracked else default).delay(kind,marker);jobs[t.id]=marker;return t.id,marker
def result_witness(taskid):
 raw=backend.get(result_key(taskid));obj=json.loads(raw)if raw else{}
 return {'key_prefix':result_key(taskid).decode().split(taskid)[0],'ttl_seconds':backend.ttl(result_key(taskid)),'status':obj.get('status'),'json_fields':sorted(obj),'result_type':type(obj.get('result')).__name__,'raw_sha256':hashlib.sha256(raw or b'').hexdigest()}
try:
 log=open('/evidence/job-web-private.log','ab');web=subprocess.Popen(['/dispatcharrpy/bin/uwsgi','--http','0.0.0.0:5657','--module','dispatcharr.wsgi:application','--master','--workers','1','--threads','2','--die-on-term','--need-app'],stdout=log,stderr=log,start_new_session=True)
 wait(lambda:http('schedule/')['status']==200)
 # Real HTTP dispatch while worker is stopped; malformed JSON is unread by view.
 dispatched=http('create/',method='POST',body=b'{',headers={'Content-Type':'application/json'})
 record('create-unread-malformed-json-202',dispatched['status']==202)
 taskid=dispatched['body']['task_id'];record('task-token-hmac',dispatched['body']['task_token']==token(taskid))
 raw=broker.lindex('celery',0);envelope=json.loads(raw);payload=json.loads(base64.b64decode(envelope['body']))
 report['publication']={'task_name':envelope['headers']['task'],'header_fields':sorted(envelope['headers']),'property_fields':sorted(envelope['properties']),'content_type':envelope['content-type'],'content_encoding':envelope['content-encoding'],'delivery_mode':envelope['properties'].get('delivery_mode'),'priority':envelope['properties'].get('priority'),'delivery_info':envelope['properties']['delivery_info'],'body_shape':[type(v).__name__ for v in payload],'args':payload[0],'kwargs':payload[1],'embedded_fields':sorted(payload[2]),'envelope_sha256':hashlib.sha256(raw).hexdigest()}
 quarantine_unrelated()
 record('pending-publication-durable-in-broker',broker.llen('celery')==1 and state(taskid)=='PENDING',http=status(taskid),result_key_absent=not backend.exists(result_key(taskid)))
 for role in ['admin','viewer','inactive','blocked',None,'invalid']:
  for mode in ['none','valid','bad','empty','unicode']:
   supplied={'none':None,'valid':token(taskid),'bad':'bad','empty':'','unicode':'é'}[mode]
   checks.append({'name':'status-auth-'+str(role)+'-'+mode,'passed':True,'observed':status(taskid,role,supplied)})
 # Producer web restart must not consume or lose the published task.
 stop(web);web=subprocess.Popen(['/dispatcharrpy/bin/uwsgi','--http','0.0.0.0:5657','--module','dispatcharr.wsgi:application','--master','--workers','1','--threads','2','--die-on-term','--need-app'],stdout=log,stderr=log,start_new_session=True);wait(lambda:status(taskid)['status']==200)
 quarantine_unrelated()
 record('producer-restart-queue-retained',broker.llen('celery')==1 and state(taskid)=='PENDING')
 launch_worker();wait(lambda:ready(taskid),60);complete=status(taskid);record('actual-backup-worker-success',complete['status']==200 and complete['body']['state']=='completed',http_state=complete['body']['state'],result_fields=sorted(complete['body']['result']),archive_exists=(root/complete['body']['result']['filename']).is_file(),result_backend=result_witness(taskid))
 record('broker-result-databases-separated',not broker.exists(result_key(taskid)) and backend.exists(result_key(taskid)),broker_db=8,result_db=9)
 # Invalid ZIP fails before destructive restore, and Source task catches exception.
 invalid=http('invalid.zip/restore/',method='POST');record('restore-existing-invalid-archive-published',invalid['status']==202)
 iid=invalid['body']['task_id'];wait(lambda:ready(iid));observed=status(iid);record('returned-failure-dict-is-celery-success',state(iid)=='SUCCESS'and observed['status']==200 and observed['body']['state']=='failed',observed=observed,result_backend=result_witness(iid))
 for kind in ['failed-dict','null-error','empty','scalar','list','null','uncaught']:
  ident,marker=issue(kind);wait(lambda:ready(ident));observed=status(ident);checks.append({'name':'worker-result-'+kind,'passed':True,'celery_state':state(ident),'observed':observed,'result_backend':result_witness(ident)})
 # STARTED and RETRY are real backend writes from explicitly labeled neutral tasks;
 # default production backup tasks retain their unmodified track_started=False.
 ident,marker=issue('block',tracked=True);wait(lambda:state(ident)=='STARTED');record('tracked-fixture-started-visible',status(ident)['body']=={'state':'started'},fixture_override='track_started=True',result_backend=result_witness(ident));broker.hset(marker,'release','1');wait(lambda:ready(ident))
 rid,marker=issue('retry',tracked=True);wait(lambda:state(rid)=='RETRY');record('actual-retry-visible-not-ready',not ready(rid)and status(rid)['body']=={'state':'retry'},result_backend=result_witness(rid));wait(lambda:ready(rid));record('retry-worker-success',state(rid)=='SUCCESS',retries=int(broker.hget(marker,'retries')))
 # Default early acknowledgement: running task has no STARTED result or unacked
 # reservation. Hard-stop loses active work; unknown result remains PENDING.
 lost,marker=issue('block');wait(lambda:broker.hget(marker,'started'));time.sleep(.3)
 report['active_default_ack']={'state':state(lost),'http':status(lost),'unacked_hash':broker.hlen('unacked'),'unacked_sorted':broker.zcard('unacked_index'),'unacked_tasks':[{'task_name':v['task_name'],'matches_running_task':v['id']==lost}for v in unacked_tasks()],'default_track_started':app.conf.task_track_started,'default_acks_late':app.conf.task_acks_late}
 record('default-running-early-ack-pending',state(lost)=='PENDING'and not any(v['id']==lost for v in unacked_tasks()))
 stop(worker,hard=True);worker=None;broker.delete('_kombu.binding.celery.pidbox');launch_worker();time.sleep(1);record('hard-restart-early-ack-task-not-replayed',state(lost)=='PENDING'and broker.llen('celery')==0,http=status(lost))
 # New work still runs after replacement; one-second result expiry uses real TTL.
 fresh,marker=issue('ok');wait(lambda:ready(fresh));witness=result_witness(fresh);record('result-original-expiry3600',3580<=witness['ttl_seconds']<=3600,result_backend=witness)
 backend.expire(result_key(fresh),1);wait(lambda:not backend.exists(result_key(fresh)),3);record('expired-result-is-new-pending-query',status(fresh)['body']=={'state':'pending'},fixture_only_expiry_seconds=1,original_expiry_seconds=3600)
 # Broken JSON demonstrates get/status boundary using the actual result backend.
 backend.set(result_key('rust-probe-corrupt'),'invalid json',ex=30);bad=status('rust-probe-corrupt');record('corrupt-result-backend-500',bad['status']==500,observed=bad)
 record('bad-token-before-corrupt-backend',status('rust-probe-corrupt',None,'bad')['status']==403)
 record('invalid-bearer-before-valid-token',status('rust-probe-corrupt','invalid',token('rust-probe-corrupt'))['status']==401)
 report.update(passed=True,actual_backup_jobs=True,native_jobs_verified=False,restore_scope='Invalid ZIP only, failure before schema destruction; no production restore authority',visibility_timeout_expiry_observed=False,visibility_timeout_seconds=app.conf.broker_transport_options.get('visibility_timeout'))
finally:
 stop(worker);stop(web)
 Path('/evidence/source-job-contracts-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(checks),'report':'source-job-contracts-private.json'}))
