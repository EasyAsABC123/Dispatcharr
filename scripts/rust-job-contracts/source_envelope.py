"""Original-only bounded protocol2 envelope capture extension.
Does not import/run the accepted Source55 controller. No worker/native publisher.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import base64,hashlib,hmac,json,os,signal,subprocess,sys,time
from pathlib import Path
from urllib.parse import quote
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only' or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':
 raise RuntimeError('Owned fixture guard required')
db=os.environ.get('POSTGRES_DB','')
if not db.startswith('rust_probe_backup_') or len(db)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in db):
 raise RuntimeError('Copied database authority required')
if os.environ.get('DJANGO_SECRET_KEY')!='playback-fixture-only':raise RuntimeError('Owned signing secret required')
sys.path[:0]=['/app'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis,requests
from apps.accounts.models import User
from rest_framework_simplejwt.tokens import RefreshToken
from dispatcharr.celery import app
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned DB8/9 namespaces required')
broker.flushdb();backend.flushdb()
user=User.objects.create(username='rust-probe-envelope-admin',password='!',user_level=10,is_active=True)
auth=str(RefreshToken.for_user(user).access_token)
root=Path(os.environ['BACKUP_ROOT'])
if not str(root).startswith('/evidence/'):raise RuntimeError('Owned file root required')
root.mkdir(exist_ok=True,parents=True)
filename='owned invalid-é &+=.zip';(root/filename).write_bytes(b'no archive: worker must stay stopped')
private=Path('/evidence/job-envelopes-private');private.mkdir(mode=0o700,exist_ok=False)
owner=Path('/evidence').stat();os.chown(private,owner.st_uid,owner.st_gid)
report={'passed':False,'scope':'Original-only full protocol2 publication extension; no worker/restore/native transport','captures':[],'checks':[],'quarantined':[], 'configuration':{key:app.conf.get(key)for key in ['task_protocol','task_publish_retry','task_publish_retry_policy','broker_connection_timeout','task_default_queue','task_default_exchange','task_default_routing_key','task_serializer','result_serializer','result_expires','broker_transport_options','result_backend_transport_options']}}
web=None
def digest(raw):return hashlib.sha256(raw).hexdigest()
def check(name,passed):
 report['checks'].append({'name':name,'passed':bool(passed)})
 if not passed:raise AssertionError(name)
def stop_web():
 global web
 if web is not None and web.poll()is None:
  os.killpg(web.pid,signal.SIGTERM)
  try:web.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(web.pid,signal.SIGKILL);web.wait(timeout=5)
 web=None
def request(path,method='GET'):
 response=requests.request(method,'http://127.0.0.1:5657/api/backups/'+path,headers={'Host':'localhost','Authorization':'Bearer '+auth,'Accept':'application/json'},timeout=10)
 return response
def launch_web():
 global web
 log=open('/evidence/envelope-web-private.log','ab')
 web=subprocess.Popen(['/dispatcharrpy/bin/uwsgi','--http','0.0.0.0:5657','--module','dispatcharr.wsgi:application','--master','--workers','1','--threads','2','--die-on-term','--need-app'],stdout=log,stderr=log,start_new_session=True)
 deadline=time.monotonic()+30
 while time.monotonic()<deadline:
  try:
   if request('schedule/').status_code==200:return
  except(requests.ConnectionError,requests.Timeout):pass
  time.sleep(.05)
 raise TimeoutError('Owned producer readiness deadline')
def queue():
 raw=broker.lrange('celery',0,100)
 if len(raw)>100:raise RuntimeError('Owned queue100 bound')
 if any(len(v)>65536 for v in raw):raise RuntimeError('Envelope64KiB bound')
 return raw
def quarantine():
 for raw in queue():
  name=json.loads(raw)['headers']['task']
  if name not in ['apps.backups.tasks.create_backup_task','apps.backups.tasks.restore_backup_task']:
   report['quarantined'].append({'task_name':name,'sha256':digest(raw)})
   broker.lrem('celery',0,raw)
def typed(value,field='',depth=0):
 if depth>16:raise RuntimeError('Typed depth16 bound')
 # Preserve exact deterministic fields/types. Dynamic IDs/origin/reply addresses
 # remain hashed; argument values are only the declared synthetic filename.
 if field in ['id','root_id','parent_id','correlation_id','reply_to','delivery_tag','origin'] and value is not None:
  return {'type':type(value).__name__,'sha256':digest(str(value).encode())}
 if value is None or type(value)in[bool,int,float,str]:
  if isinstance(value,str) and len(value)>4096:raise RuntimeError('Typed scalar4KiB bound')
  return {'type':type(value).__name__,'value':value}
 if isinstance(value,list):
  if len(value)>32:raise RuntimeError('Typed list32 bound')
  return {'type':'list','items':[typed(v,depth=depth+1)for v in value]}
 if isinstance(value,dict):
  if len(value)>64:raise RuntimeError('Typed dictionary64 bound')
  return {'type':'dict','fields':{key:typed(v,key,depth+1)for key,v in value.items()}}
 raise RuntimeError('Unsupported envelope shape')
def capture(kind,epoch):
 path='create/'if kind=='create'else quote(filename,safe='')+'/restore/'
 response=request(path,'POST');check(kind+'-'+str(epoch)+'-202',response.status_code==202)
 body=response.json();ident=body['task_id'];signed=hmac.new(b'playback-fixture-only',ident.encode(),hashlib.sha256).hexdigest()[:32]
 check(kind+'-'+str(epoch)+'-hmac',hmac.compare_digest(body['task_token'],signed))
 raws=queue();matching=[raw for raw in raws if json.loads(raw)['headers']['id']==ident]
 check(kind+'-'+str(epoch)+'-one-durable-envelope',len(matching)==1)
 raw=matching[0];envelope=json.loads(raw);decoded=base64.b64decode(envelope['body'],validate=True)
 if len(decoded)>32768:raise RuntimeError('Decoded body32KiB bound')
 payload=json.loads(decoded)
 check('body-three-element-array',isinstance(payload,list)and len(payload)==3)
 check('body-arguments',payload[0]==([]if kind=='create'else[filename])and payload[1]=={})
 check('backend-still-pending-no-worker',not backend.exists(app.backend.get_key_for_task(ident)))
 index=len(report['captures']);destination=private/('message-'+str(index)+'.json')
 fd=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'wb')as file:file.write(raw)
 os.chown(destination,owner.st_uid,owner.st_gid)
 header=envelope['headers'];properties=envelope['properties']
 record={'kind':kind,'producer_epoch':epoch,'private_raw_file':destination.name,'raw_bytes':len(raw),'raw_sha256':digest(raw),
         'decoded_body_bytes':len(decoded),'decoded_body_sha256':digest(decoded),
         'headers':typed(header),'properties':typed(properties),
         'content_type':envelope.get('content-type'),'content_encoding':envelope.get('content-encoding'),
         'body':typed(payload),'relations':{'response_equals_header_id':header.get('id')==ident,
          'correlation_equals_header_id':properties.get('correlation_id')==ident,
          'root_equals_header_id':header.get('root_id')==ident,'parent_is_none':header.get('parent_id')is None,
          'delivery_tag_equals_task_id':properties.get('delivery_tag')==ident},
         'task_id_sha256':digest(ident.encode())}
 report['captures'].append(record)
try:
 for epoch in [1,2]:
  launch_web();quarantine()
  capture('create',epoch);capture('restore',epoch)
  quarantine();queued=queue()
  # Record physical list order; no assumed FIFO/BRPOP behavior without a worker.
  report.setdefault('queue_orders',[]).append({'producer_epoch':epoch,'head_to_tail_task_id_sha256':[digest(json.loads(v)['headers']['id'].encode())for v in queued]})
  check('expected-queued-count-'+str(epoch),len(queued)==epoch*2)
  stop_web()
 # Complete deterministic routing evidence in the isolated broker, with no
 # consumer and no inference that other priority/routing options are supported.
 keys=[]
 for key in broker.scan_iter(count=100):
  if len(keys)>=128:raise RuntimeError('Owned broker key128 bound')
  keys.append(key)
 report['broker_keys']=[]
 for key in sorted(keys):
  if len(key)>256:raise RuntimeError('Broker key256byte bound')
  kind=broker.type(key).decode();entry={'key':key.decode(),'type':kind}
  if kind=='set'and key.startswith(b'_kombu.binding.'):
   members=broker.smembers(key)
   if len(members)>100 or any(len(v)>1024 for v in members):raise RuntimeError('Binding metadata bound')
   entry['members_utf8']=[v.decode()for v in sorted(members)]
  elif kind=='list':entry['length']=broker.llen(key)
  report['broker_keys'].append(entry)
 report['cross_capture_relations']=[]
 for left,right in [(0,1),(0,2),(1,3)]:
  a,b=report['captures'][left],report['captures'][right]
  ah,bh=a['properties']['fields'],b['properties']['fields']
  report['cross_capture_relations'].append({'left':left,'right':right,
    'reply_to_equal':ah['reply_to']==bh['reply_to'],'delivery_tag_equal':ah['delivery_tag']==bh['delivery_tag'],
    'origin_equal':a['headers']['fields']['origin']==b['headers']['fields']['origin'],
    'task_id_equal':a['task_id_sha256']==b['task_id_sha256']})
 report.update(passed=True,worker_started=False,restore_executed=False,native_transport_verified=False)
finally:
 stop_web()
 Path('/evidence/source-job-envelope-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'captures':len(report['captures'])}))
