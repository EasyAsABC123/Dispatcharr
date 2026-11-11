"""Actual pinned Original producer/worker and two-family native fixture peer.
Original Dispatcharr/Celery/Kombu authors retain application/protocol credit.
Copied DB, broker8/result9; no active recording, external request or other family.
"""
import base64,datetime as dt,hashlib,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
database=os.environ['POSTGRES_DB'];assert database.startswith('rust_probe_backup_jobs_')and len(database)<=63 and all(x in 'abcdefghijklmnopqrstuvwxyz0123456789_'for x in database)
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from celery.result import AsyncResult
from dispatcharr.celery import app
from apps.channels.models import Channel,Recording,RecurringRecordingRule
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
broker.flushdb();backend.flushdb()
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules()
names={'cleanup':'celery.backend_cleanup','recurring':'apps.channels.tasks.maintain_recurring_recordings'}
report={'passed':False,'checks':[],'cases':[],'scope':'Actual Original/native fixture-only direct Redis cleanup-null and disabled recurring integer0; no default expiry/active rules/Main authority'}
worker=None

def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def wait(fn,seconds=35):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  value=fn()
  if value:return value
  if worker is not None and worker.poll()is not None:raise RuntimeError('Owned worker exited')
  time.sleep(.025)
 raise TimeoutError('Owned pair deadline')
def rows(kind):
 path=Path('/evidence/periodic-worker-'+kind+'.jsonl')
 return [json.loads(line)for line in path.read_text().splitlines()]if path.exists()else[]
def native(action,**fields):
 value=subprocess.run(['/evidence/native-default-peer','default_peer::native_default_family_peer','--exact','--ignored','--nocapture'],input=json.dumps({'action':action,**fields}),capture_output=True,text=True,timeout=40)
 with Path('/evidence/native-pair-private.log').open('a')as log:log.write(value.stdout+value.stderr)
 value.check_returncode()
 lines=[line.split('=',1)[1]for line in value.stdout.splitlines()if line.startswith('NATIVE_DEFAULT_PAIR_JSON=')]
 if len(lines)!=1:raise RuntimeError('Unique bounded native marker required')
 result=json.loads(lines[0]);check('native-drain-'+action,result['drained']and result['jobs_drained']and result['available_slots']==2 and result['pool_closed'])
 return result

def metadata(ident,expected,label):
 raw=backend.get(app.backend.get_key_for_task(ident));check(label+'-result-present',raw is not None and len(raw)<=262144)
 value=json.loads(raw);actual=AsyncResult(ident,app=app).get(timeout=2)
 check(label+'-exact-normal-type',type(actual)is type(expected)and actual==expected and type(value['result'])is type(expected)and value['result']==expected)
 check(label+'-metadata',set(value)=={'status','result','traceback','children','date_done','task_id'}and value['status']=='SUCCESS'and value['task_id']==ident and value['traceback']is None and value['children']==[])
 check(label+'-ttl',3580<=backend.ttl(app.backend.get_key_for_task(ident))<=3600)
 return value

def stop():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_worker_cleanup']=True
 worker=None
try:
 RecurringRecordingRule.objects.all().update(enabled=False)
 # Fixture setup bulk insertion bypasses save signals intentionally; task bodies
 # remain untouched. A real disabled row, not only an empty table, is inspected.
 rule=RecurringRecordingRule(channel=Channel.objects.first(),name='rust-owned-disabled-pair',days_of_week=[0,1],start_time=dt.time(10),end_time=dt.time(11),enabled=False)
 RecurringRecordingRule.objects.bulk_create([rule])
 before_rules=list(RecurringRecordingRule.objects.order_by('id').values());before_recordings=list(Recording.objects.order_by('id').values())
 sentinel=str(uuid.uuid4());app.backend.store_result(sentinel,None,'SUCCESS');sentinel_key=app.backend.get_key_for_task(sentinel);sentinel_bytes=backend.get(sentinel_key)
 for kind,name in names.items():
  expected=None if kind=='cleanup'else 0
  result=app.tasks[name].apply_async(args=[],kwargs={});raw=broker.lindex('celery',-1);envelope=json.loads(raw)
  check(kind+'-actual-noargs',envelope['headers']['task']==name and json.loads(base64.b64decode(envelope['body']))[:2]==[[],{}])
  report.setdefault('attempted_envelopes',[]).append({'kind':kind,'envelope':envelope})
  outcome=native('run');report.setdefault('native_attempts',[]).append({'kind':kind,'outcome':outcome});check(kind+'-native-completed',outcome['ok']and outcome['result']['outcome']=='Completed('+result.id+')')
  value=metadata(result.id,expected,kind+'-native')
  initial=backend.get(app.backend.get_key_for_task(result.id));ttl=backend.ttl(app.backend.get_key_for_task(result.id));time.sleep(1.1)
  app.tasks[name].apply_async(args=[],kwargs={},task_id=result.id)
  repeated=native('run');check(kind+'-native-repeat-result-guard',repeated['ok']and repeated['result']['outcome']=='AlreadySucceeded('+result.id+')'and backend.get(app.backend.get_key_for_task(result.id))==initial and backend.ttl(app.backend.get_key_for_task(result.id))<=ttl)
  report['cases'].append({'kind':kind,'direction':'Original_producer_native_worker','envelope':envelope,'metadata':value,'repeat_metadata_preserved':True})
 check('native-domains-preserve-rules',before_rules==list(RecurringRecordingRule.objects.order_by('id').values()))
 check('native-domains-preserve-recordings',before_recordings==list(Recording.objects.order_by('id').values()))
 check('native-noop-preserves-sentinel',backend.get(sentinel_key)==sentinel_bytes and backend.ttl(sentinel_key)>3500)
 pending=[]
 for kind,name in names.items():
  ident=str(uuid.uuid4());out=native('publish',kind=kind,task_id=ident);check(kind+'-native-published',out['ok']and out['result']['task_id']==ident)
  pending.append((kind,ident,None if kind=='cleanup'else 0))
 queued=[json.loads(x)for x in broker.lrange('celery',0,64)]
 check('only-two-owned-known-queued',len(queued)==2 and {x['headers']['id']for x in queued}=={x[1]for x in pending}and all(x['headers']['task']in names.values()for x in queued))
 with Path('/evidence/pair-source-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-default-pair@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 for kind,ident,expected in pending:
  wait(lambda:any(row['id']==ident for row in rows('postrun')))
  value=metadata(ident,expected,kind+'-Source')
  previous=backend.get(app.backend.get_key_for_task(ident));ttl=backend.ttl(app.backend.get_key_for_task(ident));time.sleep(1.1)
  app.tasks[names[kind]].apply_async(args=[],kwargs={},task_id=ident)
  wait(lambda:sum(row['id']==ident for row in rows('postrun'))==2)
  check(kind+'-Source-repeat-executed-old-SUCCESS-guard',backend.get(app.backend.get_key_for_task(ident))==previous and backend.ttl(app.backend.get_key_for_task(ident))<=ttl)
  report['cases'].append({'kind':kind,'direction':'native_publisher_Original_worker','metadata':value,'repeat_executed':True,'repeat_metadata_preserved':True})
 stop()
 check('exact-four-Source-worker-executions',len(rows('prerun'))==len(rows('postrun'))==4 and all(x['task']in names.values()for x in rows('prerun')))
 check('Source-domains-preserve-rules',before_rules==list(RecurringRecordingRule.objects.order_by('id').values()))
 check('Source-domains-preserve-recordings',before_recordings==list(Recording.objects.order_by('id').values()))
 check('Source-noop-preserves-sentinel',backend.get(sentinel_key)==sentinel_bytes and backend.ttl(sentinel_key)>3500)
 check('owned-queue-unacked-drained',broker.llen('celery')==broker.hlen('unacked')==0)
 # Explicit unsupported active domain: never let a recording task execute.
 RecurringRecordingRule.objects.filter(pk=rule.pk).update(enabled=True)
 ident=app.tasks[names['recurring']].apply_async(args=[],kwargs={}).id;queued=broker.lindex('celery',-1)
 out=native('run');check('enabled-rule-refuses-before-ACK',out['ok']and out['result']['outcome']=='Unsupported(RecurringMaintenance)'and broker.lindex('celery',-1)==queued and backend.get(app.backend.get_key_for_task(ident))is None)
 broker.flushdb();RecurringRecordingRule.objects.filter(pk=rule.pk).update(enabled=False)
 unknown=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],kwargs={}).id;queued=broker.lindex('celery',-1)
 out=native('run');check('unported-head-unchanged',out['ok']and out['result']['outcome']=='UnsupportedHead'and broker.lindex('celery',-1)==queued and backend.get(app.backend.get_key_for_task(unknown))is None)
 broker.flushdb()
 ids=[app.tasks[name].apply_async(args=[],kwargs={}).id for name in names.values()]
 queued=list(broker.lrange('celery',0,4));out=native('prepare')
 check('prepare-does-not-start-intake',out['result']['drained']and list(broker.lrange('celery',0,4))==queued and all(backend.get(app.backend.get_key_for_task(ident))is None for ident in ids))
 out=native('poll',task_ids=ids);check('bounded-poller-real-two-family-execution',out['ok']and out['result']['drained'])
 for (kind,_name),ident in zip(names.items(),ids):metadata(ident,None if kind=='cleanup'else 0,kind+'-poller')
 check('polled-queue-unacked-drained',broker.llen('celery')==broker.hlen('unacked')==0)
 report['passed']=True
finally:
 stop()
 queued=broker.lrange('celery',0,63)
 if len(queued)<=64 and all(len(x)<=65536 for x in queued):report['remaining_queue_before_cleanup']=[json.loads(x)for x in queued]
 report['actual_rule_table']=RecurringRecordingRule._meta.db_table
 broker.flushdb();backend.flushdb()
 report['broker_and_result_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
