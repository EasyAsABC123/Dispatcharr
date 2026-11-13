"""Bounded passive backup fixture observations; no app imported on module import."""
import hashlib,json,os,re,sys
from pathlib import Path
from capture_common import audit,literal,require,save,signal_registration,exception_record,connected_database
ROOT=Path('/backup-inputs');EVIDENCE=Path('/evidence')
LOCKS=('dvr:recover_dispatch_lock','core:version_check_dispatch_lock')
def nonce():
 value=os.environ['BACKUP_FIXTURE_NONCE'];require(re.fullmatch('[a-f0-9]{10}',value)is not None,'ExactOwnedNonce');return value
def ownership():
 n=nonce();db='rust_probe_backupjob_'+n
 require(os.environ['POSTGRES_DB']==db and os.environ['BACKUP_ROOT']=='/data/backups/'+db and os.environ['PLUGINS_DIR']==os.environ['DISPATCHARR_PLUGINS_DIR']=='/data/plugins/'+db and os.environ['REDIS_DB']=='1'and not os.environ.get('REDIS_URL'),'ExactOwnedEnvironment')
 from django.db import connection
 from dispatcharr.app_initialization import should_skip_initialization
 require(connected_database(connection)==db and should_skip_initialization()is True,'ActualOwnedDatabaseAndStartupSkip');return db

def model_rows(model):
 rows=literal(list(model.objects.order_by('id').values()[:10001]));require(len(rows)<=10000 and len(json.dumps(rows).encode())<=8388608,'BoundedRawRows');return rows

def redis_client(db):
 import redis
 return redis.Redis(host='127.0.0.1',port=6379,password=os.environ['REDIS_PASSWORD'],db=db)
def redis_snapshot(db):
 client=redis_client(db);rows=[];used=0
 for key in sorted(set(client.scan_iter(count=100))):
  require(len(rows)<1000,'RedisKeyCapacity');kind=client.type(key).decode();dump=client.dump(key);used+=len(key)+len(dump or b'');require(used<=8388608,'RedisDumpCapacity');row={'key_hex':key.hex(),'type':kind,'pttl':client.pttl(key),'dump_hex':None if dump is None else dump.hex()}
  if kind=='string':row['value_hex']=client.get(key).hex()
  elif kind=='list':
   require(client.llen(key)<=1000,'RedisListCapacity');values=client.lrange(key,0,-1);require(sum(map(len,values))<=8388608,'RedisListBytesCapacity');row['list_hex']=[x.hex()for x in values]
  rows.append(row)
 return {'database':db,'capture_bytes':used,'rows':rows,'read_only_observation':True}
def snapshot():
 from django.db import connection
 from django.contrib.auth import get_user_model
 from core.models import SystemEvent
 base=Path(os.environ['BACKUP_ROOT']);files=[]
 if base.exists():
  for path in sorted(base.rglob('*')):
   require(path.is_file()and not path.is_symlink(),'OwnedBackupFlatFiles');require(len(files)<16 and path.stat().st_size<=8388608,'OwnedFileCaptureCapacity');data=path.read_bytes();files.append({'name':str(path.relative_to(base)),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
 return {'audit':audit(connection),'users':model_rows(get_user_model()),'events':model_rows(SystemEvent),'redis':[redis_snapshot(db)for db in(1,2,3)],'files':files}
def locks():
 c=redis_client(1);return [{'key':key,'raw_hex':None if c.get(key)is None else c.get(key).hex(),'pttl':c.pttl(key)}for key in LOCKS]
def signal_snapshot():
 from celery import signals
 return {name:[signal_registration(row)for row in getattr(signals,name).receivers]for name in('worker_ready','worker_process_init','task_prerun','task_postrun')}
def dependencies():
 result=[]
 for name,count in(('dependency-proof.json',11),('auth-dependency-proof.json',23)):
  proof=json.loads((ROOT/name).read_text());require(proof['container_started']is False and proof['container_program_executions']==0 and proof['exact_CID_absent']is True and len(proof['files'])==count,'ActualNeverStartedDependencyClosure')
  for row in proof['files']:
   require(hashlib.sha256(Path(row['runtime_path']).read_bytes()).hexdigest()==row['sha256'],'ExactActualDependencyBytes');result.append(row['runtime_path'])
 return result
def configured_app(producer=False):
 from dispatcharr.celery import app
 from django.conf import settings
 queue='rust_probe_backupjob_'+nonce();original=literal(app.conf.task_routes)
 require(original=={'apps.channels.tasks.run_recording':{'queue':'dvr'}},'PinnedOriginalTaskRoutes');require(app.conf.task_always_eager is False,'NoEagerTaskExecution')
 if producer:app.conf.task_routes={**original,'apps.backups.tasks.create_backup_task':{'queue':queue}}
 actual=app.amqp.router.route({},'apps.backups.tasks.create_backup_task',args=(),kwargs={})
 from apps.backups.tasks import create_backup_task
 require(create_backup_task.app is app,'RealTaskUsesOriginalCeleryApp')
 if producer:require(actual['queue'].name==queue,'OwnedProducerEffectiveRoute')
 return app,{'Original_routes':original,'producer_only_owned_route_added':producer,'actual_create_route_queue':actual['queue'].name,'owned_queue':queue,'broker_url':app.conf.broker_url,'result_backend':app.conf.result_backend,'result_expires':app.conf.result_expires,'task_track_started':app.conf.task_track_started,'accept_content':list(app.conf.accept_content),'task_serializer':app.conf.task_serializer,'worker_task_events':app.conf.worker_send_task_events,'backend_task_key_prefix_hex':app.backend.task_keyprefix.hex(),'startup_locks':locks(),'receivers':signal_snapshot(),'full_startup_or_default_routing_claim':False}
