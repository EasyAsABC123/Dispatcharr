"""New actual Original four-channel-task normal/error authority; authors retain credit."""
import base64,hashlib,inspect,json,os,secrets,signal,subprocess,sys,time,traceback
from pathlib import Path
DEADLINE=time.monotonic()+480
assert os.environ['POSTGRES_DB'].startswith('rust_probe_channel_')
assert os.environ['JOB_DISPOSABLE_FIXTURE']=='playback-fixture-only'
os.environ['DJANGO_SECRET_KEY']=secrets.token_hex(32)  # per-owned-context, never recorded
sys.path.insert(0,'/app');sys.argv.append('shell')
import redis
cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=10)
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=600)
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from dispatcharr.celery import app
from apps.channels.models import Channel,Stream,Logo,ChannelProfile
from apps.epg.models import EPGSource,EPGData
from apps.channels import tasks
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
report={'passed':False,'Source_API_calls':0,'cases':[],'source_functions':{},'worker':{}}
layer=get_channel_layer();channel='specific.owned_channel_contract!receiver'
broker=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=8)
backend=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=9)
worker=None

def save():Path('/evidence/source-report.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
def rows():
 result={}
 with connection.cursor()as c:
  for table in ['dispatcharr_channels_channel','dispatcharr_channels_logo','dispatcharr_channels_channelstream','dispatcharr_channels_channelprofilemembership']:
   c.execute('SELECT to_jsonb(x) FROM '+table+' x ORDER BY id LIMIT 513');values=[x[0]for x in c.fetchall()];assert len(values)<=512;result[table]=values
 return result

def events():
 key=(layer.prefix+channel.split('!')[0]+'!').encode();data=cache.zrange(key,0,-1);assert len(data)<=1024
 return {'key_base64':base64.b64encode(key).decode(),'raw_base64':[base64.b64encode(v).decode()for v in data],'decoded':[layer.deserialize(v)for v in data]}

def one(name,task,args,kwargs,expected,trigger=None):
 assert time.monotonic()<DEADLINE
 key=(layer.prefix+channel.split('!')[0]+'!').encode();cache.delete(key);async_to_sync(layer.group_add)('updates',channel)
 row={'name':name,'task':task.name,'args':args,'kwargs':kwargs,'expected_state':expected,'before':rows()};report['cases'].append(row);save()
 if trigger:
  table,verb,code,message=trigger
  with connection.cursor()as c:
   c.execute("CREATE FUNCTION owned_channel_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='"+code+"', MESSAGE='"+message+"'; END $$")
   c.execute('CREATE TRIGGER owned_channel_failure BEFORE '+verb+' ON '+table+' FOR EACH ROW EXECUTE FUNCTION owned_channel_failure()')
  row['controlled_owned_PG_error']={'table':table,'verb':verb,'sqlstate':code,'message':message}
 try:
  sent=task.apply_async(args=args,kwargs=kwargs);row['actual_task_id']=sent.id;save();end=min(DEADLINE,time.monotonic()+35)
  while time.monotonic()<end:
   raw=backend.get(app.backend.get_key_for_task(sent.id))
   if raw:
    metadata=json.loads(raw)
    if metadata.get('status')in ['SUCCESS','FAILURE','REVOKED']:
     row.update(raw_backend_base64=base64.b64encode(raw).decode(),metadata=metadata,ttl=backend.ttl(app.backend.get_key_for_task(sent.id)));break
   if worker.poll()is not None:raise RuntimeError('Actual Original worker exited')
   time.sleep(.025)
  else:raise TimeoutError('Actual Original terminal result deadline')
  row['after']=rows();row['Channels']=events();row['expected_state_match']=metadata['status']==expected
 except BaseException as error:
  row['capture_error']={'type':type(error).__name__,'message':str(error),'traceback':traceback.format_exc()};row['expected_state_match']=False
 finally:
  if trigger:
   with connection.cursor()as c:c.execute('DROP TRIGGER owned_channel_failure ON '+trigger[0]);c.execute('DROP FUNCTION owned_channel_failure()')
  save()
try:
 app.loader.import_default_modules()
 for function in [tasks.bulk_create_channels_from_streams,tasks.set_channels_names_from_epg,tasks.set_channels_logos_from_epg,tasks.set_channels_tvg_ids_from_epg]:
  raw=inspect.getsource(function.run).encode();report['source_functions'][function.name]={'sha256':hashlib.sha256(raw).hexdigest(),'attributes':{k:getattr(function,k)for k in ['acks_late','ignore_result','track_started','reject_on_worker_lost']}}
 # No external URLs/services are contacted. All task arguments target this new
 # copied database; direct EPG associations don't fetch their public icon URLs.
 source=EPGSource(name='owned-channel-authority',source_type='xmltv',is_active=False);EPGSource.objects.bulk_create([source])
 epg=EPGData(name='Owned EPG name',tvg_id='owned.epg.id',icon_url='http://owned.invalid/logo.png',epg_source=source);EPGData.objects.bulk_create([epg])
 channels=[Channel(name='Old owned name',tvg_id='old.owned.id',epg_data=epg,channel_number=7000+i)for i in range(6)];Channel.objects.bulk_create(channels)
 stream=Stream(name='Owned stream',url='http://owned.invalid/stream',tvg_id='owned.epg.id',logo_url='http://owned.invalid/stream.png',is_custom=True);Stream.objects.bulk_create([stream])
 broker.flushdb();backend.flushdb()
 env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/app:/tools')
 log=Path('/evidence/worker.log').open('wb')
 worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','dispatcharr.celery:app','worker','--pool=prefork','--concurrency=1','--queues=celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=owned-channels@fixture'],env=env,stdout=log,stderr=log,start_new_session=True)
 report['worker']['pid']=worker.pid;report['worker']['start_generation']=Path('/proc',str(worker.pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 # The actual consumer can accept tasks immediately after startup; no readiness
 # guess or sleep-only proof is used: each case waits its real backend terminal.
 one('normal_names',tasks.set_channels_names_from_epg,[[channels[0].id]],{},'SUCCESS')
 one('normal_tvg',tasks.set_channels_tvg_ids_from_epg,[[channels[0].id]],{},'SUCCESS')
 one('normal_logos',tasks.set_channels_logos_from_epg,[],{'channel_ids':[channels[0].id]},'SUCCESS')
 one('normal_bulk',tasks.bulk_create_channels_from_streams,[[stream.id],[],0],{},'SUCCESS')
 one('names_P0001',tasks.set_channels_names_from_epg,[[channels[1].id]],{},'FAILURE',('dispatcharr_channels_channel','UPDATE','P0001','owned channel name update'))
 one('tvg_22001',tasks.set_channels_tvg_ids_from_epg,[[channels[2].id]],{},'FAILURE',('dispatcharr_channels_channel','UPDATE','22001','owned channel tvg update'))
 # A distinct URL ensures real logo INSERT rather than an existing-logo shortcut.
 EPGData.objects.filter(pk=epg.pk).update(icon_url='http://owned.invalid/other.png')
 one('logos_23505',tasks.set_channels_logos_from_epg,[],{'channel_ids':[channels[3].id]},'FAILURE',('dispatcharr_channels_logo','INSERT','23505','owned channel logo insert'))
 one('bulk_23514',tasks.bulk_create_channels_from_streams,[[stream.id],[],0],{},'FAILURE',('dispatcharr_channels_channel','INSERT','23514','owned bulk channel insert'))
 one('logos_missing_arguments',tasks.set_channels_logos_from_epg,[],{},'FAILURE')
 report['passed']=len(report['cases'])==9 and all(r.get('expected_state_match')for r in report['cases'])
except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error),'traceback':traceback.format_exc()}
finally:
 if worker:
  worker.send_signal(signal.SIGTERM)
  try:report['worker']['actual_wait_exit']=worker.wait(timeout=20)
  except subprocess.TimeoutExpired:report['worker']['forced_kill']=True;os.killpg(worker.pid,signal.SIGKILL);report['worker']['actual_wait_exit']=worker.wait(timeout=10)
  report['worker']['leader_absent']=not Path('/proc',str(worker.pid)).exists();log.flush();os.fsync(log.fileno());log.close()
  group=[]
  for p in Path('/proc').iterdir():
   if p.name.isdecimal():
    try:
     fields=(p/'stat').read_text().rsplit(')',1)[1].split()
     if int(fields[2])==worker.pid:group.append({'pid':int(p.name),'state':fields[0],'start_generation':fields[19]})
    except (FileNotFoundError,ProcessLookupError,PermissionError):pass
  report['worker']['remaining_group']=group
 report['passed']=report['passed'] and report['worker'].get('actual_wait_exit')==0 and not report['worker'].get('forced_kill') and report['worker'].get('remaining_group')==[]
 connection.close();save()
print(json.dumps({'passed':report['passed'],'cases':len(report['cases']),'worker':report['worker']}))
raise SystemExit(0 if report['passed']else 1)
