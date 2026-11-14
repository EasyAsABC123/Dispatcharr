"""Only owned fixture ORM/readers plus real Native API and retained-frame consumers.
No Original API or task handler is invoked. Original authors retain all credit.
"""
import argparse,base64,copy,datetime,hashlib,json,os,sys,time,uuid,threading,urllib.request,urllib.error
from pathlib import Path
ROOT=Path('/evidence');TOOLS=Path('/m6-tools')
def save(name,value):
 p=ROOT/name;temp=p.with_suffix(p.suffix+'.pending');data=(json.dumps(value,indent=2,default=str)+'\n').encode()
 if len(data)>32*1024*1024:raise ValueError('OwnedReport32MiB')
 with temp.open('wb')as f:f.write(data);f.flush();os.fsync(f.fileno())
 os.replace(temp,p)
def start():
 if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('CopiedOwnedFixtureOnly')
 sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
 import django;django.setup()
 from django.db import connection
 with connection.cursor()as c:c.execute('SELECT current_database()');assert c.fetchone()[0]==os.environ['POSTGRES_DB']
def seed():
 start()
 from apps.epg.models import EPGSource,EPGData
 from apps.channels.models import Channel
 from core.models import CoreSettings
 from django.contrib.auth import get_user_model
 from apps.vod.models import VODLogo,Movie
 from django.utils import timezone
 from django.db import connection
 for model in(EPGSource,EPGData,Channel):
  if model.objects.exists():raise ValueError('FreshModelNamespace:'+model.__name__)
 now=timezone.now().replace(microsecond=0);directory=Path('/app/media');(directory/'m6-tvg').mkdir(parents=True);(directory/'rust-owned-parented-artwork').mkdir();(directory/'m6-tvg').chmod(0o755);(directory/'rust-owned-parented-artwork').chmod(0o755)
 a=now+datetime.timedelta(minutes=5);b=now+datetime.timedelta(minutes=35)
 xml=f'<?xml version="1.0"?><tv><channel id="m6.batch.owned"><display-name>Owned</display-name></channel><programme channel="m6.batch.owned" start="{a:%Y%m%d%H%M%S %z}" stop="{b:%Y%m%d%H%M%S %z}"><title>Owned M6 Programme</title></programme></tv>'
 local=directory/'m6-tvg/input.xml';local.write_text(xml);local.chmod(0o644)
 tvg=EPGSource(name='Owned TVG',source_type='xmltv',file_path=str(local),is_active=False,refresh_interval=0);dummy=EPGSource(name='Owned Dummy',source_type='dummy',is_active=False,refresh_interval=0);api=EPGSource(name='rust-owned-parented-artwork-source',source_type='xmltv',is_active=True,refresh_interval=0,file_path='/app/media/rust-owned-parented-artwork/guide.xml');EPGSource.objects.bulk_create([tvg,dummy,api])
 e=EPGData(epg_source=tvg,tvg_id='m6.batch.owned',name='Owned');d=EPGData(epg_source=dummy,tvg_id='m6.batch.dummy',name='Dummy');u=EPGData(epg_source=tvg,tvg_id='m6.batch.unmapped',name='Unmapped');p=EPGData(epg_source=api,tvg_id='rust.owned.parented.artwork',name='Owned Parented Channel');EPGData.objects.bulk_create([e,d,u,p])
 channel=Channel(name='Owned M6 Channel',channel_number=71001,epg_data=e);parented=Channel(name='Owned Parented Channel',channel_number=999902,epg_data=p,tvg_id=p.tvg_id);Channel.objects.bulk_create([channel,parented])
 s=now+datetime.timedelta(hours=4);end=s+datetime.timedelta(hours=1)
 guide=f'<?xml version="1.0"?><tv><channel id="{p.tvg_id}"><display-name>Owned Parented Channel</display-name></channel><programme channel="{p.tvg_id}" start="{s:%Y%m%d%H%M%S %z}" stop="{end:%Y%m%d%H%M%S %z}"><title>Owned Parented Artwork</title><sub-title>Owned Episode One</sub-title><episode-num system="xmltv_ns">0.0.</episode-num></programme></tv>'
 Path(api.file_path).write_text(guide);Path(api.file_path).chmod(0o644)
 logo_path=directory/'rust-owned-parented-artwork/logo.png';logo_path.write_bytes(base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jWZkAAAAASUVORK5CYII='));logo_path.chmod(0o644)
 logo=VODLogo(name='Owned Local VOD Logo',url=str(logo_path));VODLogo.objects.bulk_create([logo]);Movie.objects.bulk_create([Movie(name='Owned Parented Artwork',logo=logo,year=2026)])
 value={'series_rules':[{'title':'Owned Parented Artwork','tvg_id':p.tvg_id,'title_mode':'exact','mode':'all','channel_id':parented.pk,'epg_source_id':api.pk}],'pre_offset_minutes':0,'post_offset_minutes':0}
 if not CoreSettings.objects.filter(key='dvr_settings').update(value=value):CoreSettings.objects.bulk_create([CoreSettings(key='dvr_settings',name='Owned native M6',value=value)])
 User=get_user_model();user=User(username='owned-native-m6',password='!',user_level=10,is_active=True,is_staff=True,is_superuser=True,api_key='owned-fixture-no-api-key');User.objects.bulk_create([user])
 save('seed.json',{'database':os.environ['POSTGRES_DB'],'tvg_epg':e.pk,'dummy_epg':d.pk,'unmapped_epg':u.pk,'api_source':api.pk,'api_epg':p.pk,'api_channel':parented.pk,'user_id':user.pk,'anchor':now.isoformat(),'tvg_start':a.isoformat(),'tvg_end':b.isoformat(),'programme_start':s.isoformat(),'programme_end':end.isoformat(),'Original_tasks_or_API_executed':False,'bulk_fixture_setup_skips_model_signals':True,'files':{str(local):hashlib.sha256(local.read_bytes()).hexdigest(),api.file_path:hashlib.sha256(Path(api.file_path).read_bytes()).hexdigest()}})
 connection.close()
class Journal:
 def __init__(self,client):self.client=client;self.monitor=client.monitor();self.monitor.__enter__();self.rows=[];self.invalid=None;self.closing=False;self.used=0;self.thread=threading.Thread(target=self.run);self.thread.start()
 def run(self):
  try:
   for row in self.monitor.listen():
    self.rows.append(row)
    if len(self.rows)>20000:raise ValueError('Monitor20000')
    data=(json.dumps(row,default=str)+'\n').encode();self.used+=len(data)
    if self.used>16777216:raise ValueError('Monitor16MiB')
    with(ROOT/'redis-monitor.jsonl').open('ab')as f:f.write(data);f.flush();os.fsync(f.fileno())
  except BaseException as e:self.invalid=None if self.closing else type(e).__name__
 def close(self):
  self.closing=True;self.monitor.connection.disconnect();self.thread.join(5);return {'thread_settled':not self.thread.is_alive(),'actual_rows':len(self.rows),'terminal_reader_error':self.invalid,'close_after_cases':True}
def exercise():
 start();import redis,jwt
 from django.db import connections,connection
 from core.models import SystemNotification
 from apps.epg.models import EPGSource,EPGData,ProgramData,EPGSourceIndex
 from apps.channels.models import Channel,Recording,RecurringRecordingRule
 from django_celery_beat.models import PeriodicTask,ClockedSchedule,PeriodicTasks
 from channels.layers import get_channel_layer
 from asgiref.sync import async_to_sync
 from dispatcharr.celery import app
 seed=json.loads((ROOT/'seed.json').read_text());broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
 layer=get_channel_layer();receiver='specific.m6_'+os.environ['M6_NONCE']+'!receiver';async_to_sync(layer.group_add)('updates',receiver)
 monitor_client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=8);journal=Journal(monitor_client);known=[];report={'passed':False,'fullM6':False,'performance_verified':False,'Original_API_or_task_replay':False,'cases':[],'scope':'Packaged production Main; retained Source-frame consumer cells separately from actual Native API producer/descendants','image':os.environ['M6_IMAGE']}
 def raw_result(ident):
  key=app.backend.get_key_for_task(ident);raw=backend.get(key)
  if raw is not None and len(raw)>1048576:raise ValueError('Backend1MiB')
  result={'key_base64':base64.b64encode(key).decode(),'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'metadata':None if raw is None else json.loads(raw),'ttl':backend.ttl(key)}
  if raw is not None:
   value=app.AsyncResult(ident);result['Original_configured_result_consumer']={'state':value.state,'value':str(value.get(timeout=1,propagate=False))if value.state=='FAILURE'else value.get(timeout=1)}
  return result
 def snapshot():
  tables={}
  for model in(EPGSource,EPGData,ProgramData,EPGSourceIndex,Channel,Recording,RecurringRecordingRule,PeriodicTask,ClockedSchedule,PeriodicTasks,SystemNotification):
   rows=list(model.objects.order_by('pk').values()[:4097])
   if len(rows)>4096:raise ValueError('Snapshot4096Rows')
   tables[model._meta.db_table]=rows
  queues={}
  for q in('celery','dvr'):
   raw=broker.lrange(q,0,128)
   if len(raw)>128 or sum(map(len,raw))>4194304:raise ValueError('BoundedRawFIFO')
   queues[q]=[base64.b64encode(x).decode()for x in raw]
  key=(layer.prefix+receiver.split('!')[0]+'!').encode();members=cache.zrange(key,0,128)
  if len(members)>128:raise ValueError('Channels128')
  from source_context import Context
  raw_owner=Context.__new__(Context);raw_owner.cache=cache;raw_owner.layer=layer;raw_owner.channel=receiver
  return {'cache_raw':raw_owner.raw_cache(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'monotonic':time.monotonic(),'tables':tables,'queues':queues,'backend':{k:raw_result(k)for k in known},'Channels_raw_base64':[base64.b64encode(v).decode()for v in members],'Channels_decoded':[layer.deserialize(v)for v in members],'redis_dbsize':{'0':cache.dbsize(),'8':broker.dbsize(),'9':backend.dbsize()}}
 tvg_raw=(TOOLS/'tvg-source-frame.bin').read_bytes();version_template=json.loads((TOOLS/'version-source-frame.json').read_text())
 def publish(template,epg=None):
  frame=copy.deepcopy(template);ident=str(uuid.uuid4());frame['headers']['id']=frame['headers']['root_id']=ident;frame['properties']['correlation_id']=ident;frame['properties']['delivery_tag']=str(uuid.uuid4())
  if epg is not None:
   body=json.loads(base64.b64decode(frame['body']));body[0]=[epg];frame['body']=base64.b64encode(json.dumps(body,separators=(',',':')).encode()).decode();frame['headers']['argsrepr']=f'({epg},)'
  raw=json.dumps(frame,separators=(',',':')).encode();known.append(ident);broker.lpush('celery',raw);return ident,raw
 def wait_result(ident,seconds):
  end=time.monotonic()+seconds
  while time.monotonic()<end:
   result=raw_result(ident)
   if result['metadata']is not None:return result
   time.sleep(.025)
  return raw_result(ident)
 def case(name,template,expected,epg=None,seconds=15):
  row={'name':name,'entrypoint':'retained_Source_frame_projection→packaged_Main_consumer','native_API_producer':False,'before':snapshot(),'expected_backend_status':expected}
  report['cases'].append(row);save('native-cohort.json',report);ident,raw=publish(template,epg);row.update(task_id=ident,actual_injected_frame_base64=base64.b64encode(raw).decode(),projection={'source_template_sha256':hashlib.sha256(json.dumps(template,separators=(',',':')).encode()).hexdigest(),'changed_fields':['headers.id','headers.root_id','properties.correlation_id','properties.delivery_tag']+(['body.args[0]','headers.argsrepr']if epg is not None else[])})
  row['result']=wait_result(ident,seconds);row['after']=snapshot();row['passed']=row['result']['metadata']is not None and row['result']['metadata']['status']==expected;save('native-cohort.json',report);return row
 try:
  for name,epg in [('tvg-initial',seed['tvg_epg']),('tvg-repeat',seed['tvg_epg']),('tvg-missing',2147483601),('tvg-dummy',seed['dummy_epg']),('tvg-unmapped',seed['unmapped_epg'])]:case(name,json.loads(tvg_raw),'SUCCESS',epg)
  SystemNotification.objects.bulk_create([SystemNotification(notification_key='version-0.33.0',notification_type='version_update',title='Owned obsolete notification',message='Owned obsolete',is_active=True)])
  for name,payload in [('no-update',{'tag_name':'v0.33.0','html_url':'https://owned.invalid/unchanged'}),('update',{'tag_name':'v0.34.0','html_url':'https://owned.invalid/update'}),('repeat',{'tag_name':'v0.34.0','html_url':'https://owned.invalid/update'}),('malformed',{'tag_name':'not-a-version','html_url':'https://owned.invalid/invalid'})]:
   save('version-response.json',{'status':200,'body_base64':base64.b64encode(json.dumps(payload,separators=(',',':')).encode()).decode()});case('version200-'+name,version_template,'SUCCESS')
  trigger='m6_'+os.environ['M6_NONCE']+'_insert_guard'
  with connection.cursor()as c:c.execute('CREATE FUNCTION '+trigger+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='owned M6 programme insert'; END $$");c.execute('CREATE TRIGGER '+trigger+' BEFORE INSERT ON epg_programdata FOR EACH ROW EXECUTE FUNCTION '+trigger+'()')
  try:case('tvg-insert-error',json.loads(tvg_raw),'FAILURE',seed['tvg_epg'],seconds=8)
  finally:
   with connection.cursor()as c:c.execute('DROP TRIGGER '+trigger+' ON epg_programdata');c.execute('DROP FUNCTION '+trigger+'()')
  # Actual Native HTTP publication is a separate producer-to-worker phase.
  claims={'token_type':'access','exp':int(time.time())+600,'iat':int(time.time()),'jti':uuid.uuid4().hex,'user_id':seed['user_id']};token=jwt.encode(claims,'playback-fixture-only',algorithm='HS256');body=json.dumps({'id':seed['api_source'],'force':False},separators=(',',':')).encode();before_keys=set(backend.scan_iter(match='celery-tasks:celery-task-meta-*'))
  row={'name':'Native-API-configured-parent','entrypoint':'realNativeHTTP→realNativeRefreshproducer→workers','native_API_producer':True,'before':snapshot(),'request_body_base64':base64.b64encode(body).decode(),'authorization_sha256':hashlib.sha256(token.encode()).hexdigest()};report['cases'].append(row);save('native-cohort.json',report)
  request=urllib.request.Request('http://127.0.0.1:9192/api/epg/import/',data=body,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json','Accept':'application/json'})
  try:response=urllib.request.urlopen(request,timeout=15)
  except urllib.error.HTTPError as error:response=error
  with response:raw=response.read(65537);row['response']={'status':response.status,'headers':list(response.headers.items()),'body_base64':base64.b64encode(raw).decode()}
  end=time.monotonic()+25
  while time.monotonic()<end:
   new=set(backend.scan_iter(match='celery-tasks:celery-task-meta-*'))-before_keys
   for key in new:
    ident=key.decode().rsplit('celery-task-meta-',1)[1]
    if ident not in known:known.append(ident)
   frames=broker.lrange('celery',0,128)
   if len(new)>=3 and any(json.loads(f)['headers']['task']=='apps.channels.tasks.prefetch_recording_artwork'for f in frames):break
   time.sleep(.05)
  row['after']=snapshot();row['new_backend_ids']=[k for k in known if ('celery-tasks:celery-task-meta-'+k).encode()not in before_keys];row['partial_HTTP202']=row['response']['status']==202;row['passed']=False;row['reason']='Full Source37 API shapes and artwork/default descendants require whole matrix; report real producer/parent/child outcomes without skipping FIFO'
  report['capture_complete']=True;report['final_before_native_stop']=snapshot();save('native-cohort.json',report)
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error)};save('native-cohort.json',report)
 finally:
  report['monitor_close']=journal.close();monitor_client.close();broker.close();backend.close();cache.close();connections.close_all();save('native-cohort.json',report)
 return 0 if report.get('capture_complete')and report['monitor_close']['thread_settled']and report['monitor_close']['terminal_reader_error']is None else 1
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--phase',choices=['seed','exercise'],required=True);a=p.parse_args();raise SystemExit(seed()or 0 if a.phase=='seed'else exercise())
