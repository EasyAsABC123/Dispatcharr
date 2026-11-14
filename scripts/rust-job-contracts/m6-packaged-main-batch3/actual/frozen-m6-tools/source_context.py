"""Common Source cohort bindings. Explicit construction only; no import-time app.
Host lifecycle/default audit/Docker ownership stays in Root's common harness.
Small owned fixture snapshots are not a claim of arbitrary DB/Redis byte bounds.
"""
import base64,datetime,hashlib,json,os,re,time
from pathlib import Path
class Context:
 def __init__(self,*,root,prefix,app,broker,backend,cache,worker_factory,observer,owned_version_origin,predeclared_xmltv_bytes,channel_number=71001,missing_epg_id=2147483601):
  if not re.fullmatch(r'm6_[a-f0-9]{10}',prefix):raise ValueError('FixedOwnedPrefix')
  self.root=Path(root);self.prefix=prefix;self.app=app;self.broker=broker;self.backend=backend;self.cache=cache;self.worker_factory=worker_factory;self.observer=observer;self.origin=owned_version_origin;self.predeclared_xmltv_bytes=predeclared_xmltv_bytes;self.channel_number=channel_number;self.predeclared_missing_epg_id=missing_epg_id;self.owned_input_directory=str(self.root/'inputs');self.trigger=None
 def assert_copied_owned_context(self):
  if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise ValueError('OwnedCopiedContext')
  from django.db import connection
  with connection.cursor()as c:c.execute('SELECT current_database()');actual=c.fetchone()[0]
  if actual!=os.environ['POSTGRES_DB']:raise ValueError('ActualCopiedDatabase')
  if self.broker.connection_pool.connection_kwargs['db']!=8 or self.backend.connection_pool.connection_kwargs['db']!=9:raise ValueError('DeclaredIsolatedRedis8And9')
 def assert_empty_plugins_and_integrations(self):
  from apps.plugins.models import PluginConfig,PluginRepo
  from apps.connect.models import Integration,EventSubscription,DeliveryLog
  if any(m.objects.exists()for m in [PluginConfig,PluginRepo,Integration,EventSubscription,DeliveryLog]):raise ValueError('OwnedEmptyIntegrationPluginRows')
  directory=Path(os.environ['DISPATCHARR_PLUGINS_DIR'])
  if not directory.is_dir()or any(directory.iterdir()):raise ValueError('ActualOwnedEmptyPluginDirectory')
  from channels.layers import get_channel_layer
  from asgiref.sync import async_to_sync
  self.layer=get_channel_layer();self.channel='specific.'+self.prefix+'!receiver'
  async_to_sync(self.layer.group_add)('updates',self.channel)
 def persist_partial(self,name,value):
  if not re.fullmatch('[A-Za-z0-9_-]+',name):raise ValueError('OwnedReceiptName')
  path=self.root/(name+'.json');data=(json.dumps(value,indent=2,default=str,allow_nan=False)+'\n').encode()
  if len(data)>4*1024*1024:raise ValueError('PartialReceipt4MiB')
  fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
  with os.fdopen(fd,'wb')as out:out.write(data);out.flush();os.fsync(out.fileno())
 def _queue(self,name):
  script="local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return {} end;if t~='list'or redis.call('LLEN',KEYS[1])>128 then return redis.error_reply('owned queue bound')end;local v=redis.call('LRANGE',KEYS[1],0,-1);local n=0;for _,x in ipairs(v)do n=n+#x;if #x>65536 or n>4194304 then return redis.error_reply('owned frame bound')end end;return v"
  return [base64.b64encode(v).decode()for v in self.broker.eval(script,1,name)]
 def capture(self):
  from django.db import connection
  from apps.epg.models import EPGSource,EPGData,ProgramData
  from apps.channels.models import Channel,Recording
  from core.models import SystemNotification,NotificationDismissal,SystemEvent
  tables={}
  for model in [EPGSource,EPGData,ProgramData,Channel,Recording,SystemNotification,NotificationDismissal,SystemEvent]:
   if model.objects.count()>4096:raise ValueError('OwnedSnapshotRows4096')
   tables[model._meta.db_table]=list(model.objects.order_by('pk').values())
  result={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'monotonic':time.monotonic(),'actual_database':connection.settings_dict['NAME'],'tables':tables,'queues':{q:self._queue(q)for q in ['celery','dvr']},'observer':self.observer.snapshot(),'origin':self.origin.snapshot(),'cache':self.raw_cache()}
  if len(json.dumps(result,default=str).encode())>4*1024*1024:raise ValueError('OwnedSnapshot4MiB')
  return result
 def actual_natural_task_id(self,snapshot,family):
  ids=[]
  for encoded in snapshot['queues']['celery']:
   frame=json.loads(base64.b64decode(encoded,validate=True))
   if frame['headers']['task']==family:ids.append(frame['headers']['id'])
  if len(ids)!=1:raise ValueError('ExactlyOneRealSignalFrame')
  return ids[0]
 def start_original_worker(self,queues):
  if queues!=['celery']:raise ValueError('ActualDefaultQueueOnly')
  return self.worker_factory.start(queues=queues,observer_module='m6_source_observer',owned_version_origin=self.origin.url)
 def enqueue_actual(self,task,args,kwargs):
  if task not in ['apps.epg.tasks.parse_programs_for_tvg_id','core.tasks.check_for_version_update']:raise ValueError('DeclaredMissingSourceFamiliesOnly')
  sent=self.app.tasks[task].apply_async(args=args,kwargs=kwargs)
  return sent.id
 def consume_until_postrun(self,worker,task_id):
  deadline=time.monotonic()+40
  while time.monotonic()<deadline:
   observations=self.observer.snapshot();post=[v for v in observations['postrun']if v['id']==task_id]
   if post:
    key=self.app.backend.get_key_for_task(task_id);raw=self.backend.get(key)
    if raw is not None:
     # Retain exact configured-backend raw bytes before parsing or asserting.
     return {'postrun':post,'observer':observations,'backend':{'key_base64':base64.b64encode(key).decode(),'raw_base64':base64.b64encode(raw).decode(),'ttl':self.backend.ttl(key),'metadata':json.loads(raw)},'Original_AsyncResult':{'value':self.app.AsyncResult(task_id).get(timeout=2,propagate=False),'state':self.app.AsyncResult(task_id).state}}
   if self.worker_factory.poll(worker)is not None:raise RuntimeError('OriginalWorkerExitedBeforeObservation')
   time.sleep(.025)
  raise TimeoutError('ActualSourceTask40s; no task cancellation implied')
 def raw_cache(self):
  # Small owned namespace: enumerate all keys and bound every returned value
  # inside one atomic read. No SCAN/get race or silently omitted unknown type.
  script="""local keys=redis.call('KEYS','*');if #keys>128 then return redis.error_reply('owned keys128')end;table.sort(keys);local rows={};local total=0;for _,key in ipairs(keys)do local t=redis.call('TYPE',key).ok;local data={};if t=='string'then if redis.call('STRLEN',key)>1048576 then return redis.error_reply('string1MiB')end;data={redis.call('GET',key)}elseif t=='zset'then if redis.call('ZCARD',key)>128 then return redis.error_reply('zset128')end;data=redis.call('ZRANGE',key,0,-1,'WITHSCORES')elseif t=='list'then if redis.call('LLEN',key)>128 then return redis.error_reply('list128')end;data=redis.call('LRANGE',key,0,-1)elseif t=='set'then if redis.call('SCARD',key)>128 then return redis.error_reply('set128')end;data=redis.call('SMEMBERS',key);table.sort(data)elseif t=='hash'then if redis.call('HLEN',key)>128 then return redis.error_reply('hash128')end;data=redis.call('HGETALL',key)else return redis.error_reply('unknown type')end;for _,v in ipairs(data)do total=total+#v;if #v>1048576 or total>4194304 then return redis.error_reply('total4MiB')end end;rows[#rows+1]={key,t,redis.call('DUMP',key),redis.call('TTL',key),data}end;return rows"""
  rows=self.cache.eval(script,0);result=[];channel_key=(self.layer.prefix+self.channel.split('!')[0]+'!').encode()
  for key,kind,dump,ttl,data in rows:
   row={'key_base64':base64.b64encode(key).decode(),'type':kind.decode(),'DUMP_base64':base64.b64encode(dump).decode(),'ttl':ttl,'raw_values_base64':[base64.b64encode(v).decode()for v in data]}
   if key==channel_key:
    if kind!=b'zset':raise ValueError('ActualChannelZset')
    # Exactly these same captured member bytes are passed to the real pinned
    # serializer. Scores remain literal in raw_values_base64.
    row['Channels_raw_base64']=[base64.b64encode(v).decode()for v in data[::2]];row['Channels_decoded']=[self.layer.deserialize(v)for v in data[::2]]
   result.append(row)
  return result
 def configure_owned_version_response(self,status,body):
  if status!=200 or len(body)>8192:raise ValueError('PredeclaredOwnedVersion200')
  self.origin.set_response(status=status,body=body)
 def install_owned_insert_trigger(self,table,sqlstate,message):
  if table!='epg_programdata'or sqlstate!='P0001'or message!='owned M6 programme insert'or self.trigger is not None:raise ValueError('ExactOwnedTrigger')
  from django.db import connection
  name=self.prefix+'_insert_guard'
  with connection.cursor()as c:
   c.execute('CREATE FUNCTION '+name+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='owned M6 programme insert'; END $$")
   c.execute('CREATE TRIGGER '+name+' BEFORE INSERT ON epg_programdata FOR EACH ROW EXECUTE FUNCTION '+name+'()')
  self.trigger=name
 def remove_owned_insert_trigger(self):
  if self.trigger is None:return
  from django.db import connection
  name=self.trigger
  with connection.cursor()as c:c.execute('DROP TRIGGER '+name+' ON epg_programdata');c.execute('DROP FUNCTION '+name+'()')
  self.trigger=None
 def stop_and_collect_original_worker(self,worker):return self.worker_factory.stop_and_collect(worker)
