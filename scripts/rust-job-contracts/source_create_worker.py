"""Actual Source producer/status/ZIP versus owned native Create worker.
No native application202, scheduler, destructive restore or generic queue claim.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import base64,hashlib,hmac,json,os,signal,subprocess,sys,time,uuid,zipfile,shutil
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Owned fixture guard required')
db=os.environ.get('POSTGRES_DB','')
if not db.startswith('rust_probe_backup_')or len(db)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in db):raise RuntimeError('Copied DB required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis,requests
from dispatcharr.celery import app
from celery.result import AsyncResult
from apps.backups import tasks,services
from apps.accounts.models import User
from rest_framework_simplejwt.tokens import RefreshToken
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
user=User.objects.create(username='rust-probe-create-worker-admin',password='!',user_level=10,is_active=True,custom_properties={});jwt=str(RefreshToken.for_user(user).access_token)
root=Path(os.environ['BACKUP_ROOT']);root.mkdir(parents=True,exist_ok=True);scratch=Path('/evidence/worker-scratch');scratch.mkdir(exist_ok=True)
checks=[];children=[];web=None
report={'passed':False,'scope':'Fixture-only Source-created CreateBackup/native execution and Source result/ZIP/status readback; no publicnative202/generalworker/scheduler/restore','checks':checks,'native_application_dispatch_verified':False,'destructive_restore_executed':False}
def record(name,passed,**details):
 checks.append({'name':name,'passed':bool(passed),**details})
 if not passed:raise AssertionError(name)
def wait(fn,seconds=20):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  try:
   value=fn()
   if value:return value
  except(requests.RequestException,redis.RedisError):pass
  time.sleep(.02)
 raise RuntimeError('Bounded fixture wait exceeded')
def http_status(taskid):
 r=requests.get('http://127.0.0.1:5657/api/backups/status/'+taskid+'/',headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+jwt},timeout=10);return r.status_code,r.json()
def request(handler='archive',gate=None):
 return {'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],'handler':handler,'backup_root':str(root),'scratch_root':str(scratch),'pg_dump':shutil.which('pg_dump'),**({'gate':gate}if gate else{})}
def spawn_worker(handler='archive',gate=None):
 log=open('/evidence/native-create-worker-private.log','ab');p=subprocess.Popen(['/evidence/native-create-worker'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,start_new_session=True);children.append(p);p.stdin.write(json.dumps(request(handler,gate)));p.stdin.close();return p
def finish(p):
 p.wait(timeout=45);raw=p.stdout.read();record('native-process-exit',p.returncode==0)
 return json.loads(raw.splitlines()[-1])
def key(taskid):return app.backend.get_key_for_task(taskid)
def metadata(taskid):
 raw=backend.get(key(taskid));return json.loads(raw)if raw else None
def source_signature(result):return {'status':result.get('status'),'filename_type':type(result.get('filename')).__name__,'size_type':type(result.get('size')).__name__,'error':result.get('error')}
try:
 log=open('/evidence/source-status-web-private.log','ab');web=subprocess.Popen(['/dispatcharrpy/bin/uwsgi','--http','0.0.0.0:5657','--module','dispatcharr.wsgi:application','--master','--workers','1','--threads','2','--die-on-term','--need-app'],stdout=log,stderr=log,start_new_session=True)
 wait(lambda:requests.get('http://127.0.0.1:5657/api/backups/schedule/',headers={'Authorization':'Bearer '+jwt},timeout=2).status_code==200)
 # Startup plugin work is quarantined only in this owned namespace; unsupported
 # task families are not passed to the native consumer and no scheduler runs.
 startup=broker.lrange('celery',0,-1);report['startup_quarantine']=[{'task':json.loads(v)['headers']['task'],'sha256':hashlib.sha256(v).hexdigest()}for v in startup];broker.delete('celery')
 record('startup-quarantine-known-plugin-only',all(v['task']=='apps.plugins.tasks.refresh_plugin_repos'for v in report['startup_quarantine']))
 # Unsupported Restore remains byte-identical at head, without handler/result.
 unsupported=tasks.restore_backup_task.delay('invalid-never-opened.zip');before=broker.lrange('celery',0,-1);p=spawn_worker();p.wait(timeout=10)
 record('unsupported-head-retained-without-result',p.returncode!=0 and broker.lrange('celery',0,-1)==before and metadata(unsupported.id)is None)
 broker.delete('celery')
 # Original producer accepted, native process executes actual bounded PG17 ZIP.
 issued=tasks.create_backup_task.delay();raw=broker.lindex('celery',-1);record('source-created-queued-before-native',json.loads(raw)['headers']['id']==issued.id and metadata(issued.id)is None)
 p=spawn_worker(gate='native-worker-success');entered=Path('/evidence/native-worker-success.entered');release=entered.with_suffix('.release');wait(entered.exists)
 record('early-ack-before-handler-pending',broker.llen('celery')==0 and metadata(issued.id)is None and not AsyncResult(issued.id,app=app).ready())
 record('source-pending-status-before-result',http_status(issued.id)==(200,{'state':'pending'}))
 release.write_text('release');finish(p);meta=metadata(issued.id);result=AsyncResult(issued.id,app=app).get(timeout=5)
 record('source-ready-success-after-native-result',meta['status']=='SUCCESS'and AsyncResult(issued.id,app=app).successful()and result['status']=='completed')
 record('source-status-completed-native-result',http_status(issued.id)==(200,{'state':'completed','result':result}))
 record('source-result-key-fields-and-expiry',set(meta)=={'status','result','traceback','children','date_done','task_id'}and meta['task_id']==issued.id and meta['traceback']is None and meta['children']==[] and 3590<=backend.ttl(key(issued.id))<=3600)
 archive=root/result['filename'];record('native-result-size-actual-archive',archive.stat().st_size==result['size']and archive.stat().st_size>0)
 def zip_witness(path):
  with zipfile.ZipFile(path)as z:
   names=z.namelist();m=json.loads(z.read('metadata.json'));dump=z.read('database.dump');record('zip-db-only-schema',set(names)=={'metadata.json','database.dump'}and m['version']==2 and m['database_type']=='postgresql'and dump.startswith(b'PGDMP'))
   target=scratch/(uuid.uuid4().hex+'.dump');target.write_bytes(dump)
   try:r=subprocess.run([shutil.which('pg_restore'),'--list',str(target)],capture_output=True,timeout=10);record('pg17-read-both-archive-dumps',r.returncode==0 and b'TABLE DATA'in r.stdout)
   finally:target.unlink()
   return {k:m[k]for k in ['format','version','database_type','database_file']}
 native_zip=zip_witness(archive)
 # Source creates a separate comparison archive; no restore is invoked.
 time.sleep(1.05);source_file=services.create_backup();source_zip=zip_witness(source_file)
 record('source-native-archive-metadata-contract',native_zip==source_zip)
 record('source-list-sees-native-archive',any(v['name']==archive.name and v['size']==result['size']for v in services.list_backups()))
 # Original wrapper injected service failure is a separate semantic control;
 # it is NOT proof of native PG/OS diagnostic message equivalence.
 saved=services.create_backup
 def fail():raise RuntimeError('fixture create failure')
 try:services.create_backup=fail;source_failure=tasks.create_backup_task.run()
 finally:services.create_backup=saved
 failed=tasks.create_backup_task.delay();finish(spawn_worker(handler='failure'));native_failure=AsyncResult(failed.id,app=app).get(timeout=5)
 record('returned-handler-failure-is-celery-success',native_failure==source_failure and native_failure=={'status':'failed','error':'fixture create failure'}and metadata(failed.id)['status']=='SUCCESS')
 record('source-status-returned-failure',http_status(failed.id)==(200,{'state':'failed','error':'fixture create failure'}))
 # Kill after the actual earlyACK boundary but before any archive child; a new
 # worker sees no task/result and must not invent recovery or execute it twice.
 lost=tasks.create_backup_task.delay();marker='native-worker-earlyack-loss';before=set(root.iterdir());p=spawn_worker(gate=marker);wait(Path('/evidence/'+marker+'.entered').exists);os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5)
 record('worker-crash-after-earlyack-no-result',broker.llen('celery')==0 and metadata(lost.id)is None and set(root.iterdir())==before)
 restart=finish(spawn_worker());record('replacement-does-not-immediately-replay-earlyack',restart['outcome']=='Empty'and metadata(lost.id)is None and set(root.iterdir())==before)
 # Missing/expired key is read as Source PENDING; fixture1s expiry distinct from3600TTL.
 backend.expire(key(failed.id),1);wait(lambda:backend.get(key(failed.id))is None,seconds=3);record('expired-result-source-pending',http_status(failed.id)==(200,{'state':'pending'}))
 record('broker-result-separation',broker.get(key(issued.id))is None and backend.get(key(issued.id))is not None)
 report.update(passed=all(v['passed']for v in checks),source_result_signature=source_signature(result),native_create_worker_fixture_verified=True,archive_diagnostic_message_parity=False,source_pg_dump_version=subprocess.check_output([shutil.which('pg_dump'),'--version'],text=True).strip(),long_visibility_redelivery_verified=False)
finally:
 for p in children+[web]:
  if p is not None and p.poll()is None:
   os.killpg(p.pid,signal.SIGTERM)
   try:p.wait(timeout=5)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5)
 Path('/evidence/native-create-worker-private.json').write_text(json.dumps(report,indent=2)+'\n')
