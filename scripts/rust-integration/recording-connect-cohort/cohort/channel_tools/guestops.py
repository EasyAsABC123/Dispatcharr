"""Finite owned-fixture setup/readback. No Source API/task invocation."""
import base64,json,os,sys,time
from pathlib import Path
import psycopg,redis
TABLES=['dispatcharr_channels_channel','dispatcharr_channels_logo','dispatcharr_channels_channelstream','dispatcharr_channels_channelprofilemembership']

def connection():return psycopg.connect(dbname=os.environ['POSTGRES_DB'],user='dispatch',password='playback-fixture-only',host='127.0.0.1',autocommit=True,options='-c statement_timeout=10000')
def seed():
 sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
 import django;django.setup()
 from apps.channels.models import Channel,Stream,Logo
 from apps.epg.models import EPGSource,EPGData
 if not os.environ['POSTGRES_DB'].startswith('normal_dvr_'):raise ValueError('DedicatedOwnedContextRequired')
 from django.db import connection as original_connection
 with original_connection.cursor()as cursor:
  for table in ['dispatcharr_channels_recording','dispatcharr_channels_recurringrecordingrule']:
   cursor.execute('SELECT count(*) FROM '+table)
   if cursor.fetchone()[0]:raise RuntimeError('DedicatedEmptyChannelContextRequired')
  # Fixture setup only, before journal/business. This fresh independent DB is
  # explicitly reset to Source9's initially-empty channel/reservation domain.
  cursor.execute('TRUNCATE dispatcharr_channels_channel RESTART IDENTITY CASCADE')
 urls=['http://owned.invalid/logo.png','http://owned.invalid/stream.png','http://owned.invalid/other.png']
 if Logo.objects.filter(url__in=urls).exists():raise RuntimeError('OwnedURLNamespaceNotFresh')
 source=EPGSource(name='owned-channel-authority',source_type='xmltv',is_active=False);EPGSource.objects.bulk_create([source])
 epg=EPGData(name='Owned EPG name',tvg_id='owned.epg.id',icon_url=urls[0],epg_source=source);EPGData.objects.bulk_create([epg])
 channels=[Channel(name='Old owned name',tvg_id='old.owned.id',epg_data=epg,channel_number=7000+i)for i in range(6)];Channel.objects.bulk_create(channels)
 stream=Stream(name='Owned stream',url='http://owned.invalid/stream',tvg_id='owned.epg.id',logo_url=urls[1],is_custom=True);Stream.objects.bulk_create([stream])
 from django.db import connection as original_connection
 original_connection.close()
 return {'channel_ids':[c.pk for c in channels],'source_id':source.pk,'epg_id':epg.pk,'stream_id':stream.pk,'owned_logo_urls':urls,'Source_API_calls':0,'Source_task_calls':0,'Original_Django_setup_processes':1}
def snapshot(seed,receiver):
 c=connection();tables={}
 try:
  ids=[r[0]for r in c.execute('SELECT id::bigint FROM dispatcharr_channels_channel WHERE id=ANY(%s) OR id IN(SELECT channel_id FROM dispatcharr_channels_channelstream WHERE stream_id=%s)',(seed['channel_ids'],seed['stream_id'])).fetchall()]
  for table in TABLES:
   if table==TABLES[0]:sql='SELECT to_jsonb(x) FROM '+table+' x WHERE id=ANY(%s) ORDER BY id';params=(ids,)
   elif table==TABLES[1]:sql='SELECT to_jsonb(x) FROM '+table+' x WHERE url=ANY(%s) ORDER BY id';params=(seed['owned_logo_urls'],)
   else:sql='SELECT to_jsonb(x) FROM '+table+' x WHERE channel_id=ANY(%s) ORDER BY id';params=(ids,)
   rows=[r[0]for r in c.execute(sql,params).fetchall()];assert len(rows)<=512;tables[table]=rows
 finally:c.close()
 cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
 from channels_redis.core import RedisChannelLayer
 layer=RedisChannelLayer(hosts=['redis://:playback-fixture-only@127.0.0.1:6379/0']);key=(layer.prefix+receiver.split('!')[0]+'!').encode();raw=cache.zrange(key,0,1024);assert len(raw)<=1024 and sum(map(len,raw))<=4*1024*1024
 value={'tables':tables,'Channels_raw_base64':[base64.b64encode(v).decode()for v in raw],'Channels_decoded':[layer.deserialize(v)for v in raw],'monotonic':time.monotonic(),'redis_time':cache.time()};cache.close();assert len(json.dumps(value,default=str).encode())<=4*1024*1024;return value

def backend(ident):
 client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=2);key='celery-tasks:celery-task-meta-'+ident;raw=client.get(key);assert raw is None or len(raw)<=262144
 value={'key':key,'raw_base64':None if raw is None else base64.b64encode(raw).decode(),'metadata':None if raw is None else json.loads(raw),'ttl':client.ttl(key)};client.close();return value

def clear(receiver):
 client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0);key='asgi'+receiver.split('!')[0]+'!';client.delete(key);value=client.time();client.close();return value

def trigger(table=None,verb=None,code=None,message=None,remove=False,gate=False):
 allowed={'dispatcharr_channels_channel','dispatcharr_channels_logo'}
 if table not in allowed:raise ValueError('OwnedChannelTablesOnly')
 c=connection();name='owned_channel_gate'if gate else'owned_channel_failure'
 try:
  if remove:c.execute('DROP TRIGGER '+name+' ON '+table);c.execute('DROP FUNCTION '+name+'()');return {'removed':True}
  if verb not in ['INSERT','UPDATE']:raise ValueError('OwnedMutationOnly')
  if gate:body='PERFORM pg_advisory_xact_lock(87310472); RETURN NEW;'
  else:
   if code not in ['P0001','22001','23505','23514']or not message.startswith('owned '):raise ValueError('DeclaredControlledErrorOnly')
   if "'"in message:raise ValueError('NoSQLInterpolation')
   body="RAISE EXCEPTION USING ERRCODE='"+code+"', MESSAGE='"+message+"';"
  c.execute('CREATE FUNCTION '+name+'() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN '+body+' END $$')
  c.execute('CREATE TRIGGER '+name+' BEFORE '+verb+' ON '+table+' FOR EACH ROW EXECUTE FUNCTION '+name+'()');return {'installed':True}
 finally:c.close()

def change_icon(seed):
 c=connection()
 try:c.execute('UPDATE epg_epgdata SET icon_url=%s WHERE id=%s',(seed['owned_logo_urls'][2],seed['epg_id']));return {'updated':True}
 finally:c.close()

def held():
 c=connection()
 try:
  rows=[{'pid':x[0],'state':x[1],'wait_event_type':x[2],'wait_event':x[3],'query':x[4]}for x in c.execute("SELECT pid,state,wait_event_type,wait_event,query FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND wait_event='advisory'").fetchall()];return rows
 finally:c.close()

def project_missing(template):
 import uuid
 client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=1);ident=str(uuid.uuid4());frame=json.loads(json.dumps(template));frame['headers'].update(task='apps.channels.tasks.set_channels_logos_from_epg',id=ident,root_id=ident,parent_id=None,argsrepr='()',kwargsrepr='{}');frame['properties'].update(correlation_id=ident,delivery_tag=str(uuid.uuid4()));frame['body']=base64.b64encode(json.dumps([[],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}],separators=(',',':')).encode()).decode();raw=json.dumps(frame,separators=(',',':')).encode();client.lpush('celery',raw);client.close();return {'id':ident,'raw_frame_base64':base64.b64encode(raw).decode(),'entrypoint':'retained-Source-common-frame task-name projection; NOT API producer/completeSource9wire authority'}

def queue():
 client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=1);raw=client.lrange('celery',0,128);assert len(raw)<=128 and sum(map(len,raw))<=4*1024*1024;result=[{'raw_base64':base64.b64encode(v).decode(),'frame':json.loads(v)}for v in raw];client.close();return result


def pool_sessions(excluded_owned_pids):
 c=connection()
 try:
  rows=[dict(zip(['pid','state','wait_event_type','wait_event','application_name','backend_start','query'],r))for r in c.execute("SELECT pid,state,wait_event_type,wait_event,application_name,backend_start::text,query FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid() ORDER BY pid").fetchall()]
  excluded=[r for r in rows if r['pid']in excluded_owned_pids];counted=[r for r in rows if r['pid']not in excluded_owned_pids]
  return {'all_other_database_clients':rows,'excluded_exact_owned_gate_pids':excluded,'counted_clients':counted,'counted':len(counted),'observer_query_connection_excluded_by_actual_backend_pid':True,'source_of_limit':'inspected Main RUST_DATABASE_POOL_SIZE=8; all other current-database clients count conservatively, no application_name guess'}
 finally:c.close()


def seed_http():
 sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
 import django;django.setup()
 from apps.channels.models import Channel,Stream
 from apps.epg.models import EPGSource,EPGData
 source=EPGSource(name='Owned channel HTTP EPG',source_type='xmltv',is_active=False);EPGSource.objects.bulk_create([source])
 epg=EPGData(name='Owned channel HTTP guide',tvg_id='owned.http.channel',icon_url='http://owned.invalid/http-logo.png',epg_source=source);EPGData.objects.bulk_create([epg])
 channel=Channel(name='Owned channel HTTP',epg_data=epg,channel_number=7070);Channel.objects.bulk_create([channel])
 stream=Stream(name='Owned HTTP stream',url='http://owned.invalid/http-stream',tvg_id='owned.http.channel',is_custom=True);Stream.objects.bulk_create([stream])
 from django.db import connection as dc
 dc.close();return dict(source=source.pk,epg=epg.pk,channel=channel.pk,stream=stream.pk)

def http_network(control,admin_id):
 sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
 import django;django.setup()
 from core.models import CoreSettings
 from apps.accounts.models import User
 CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Network Access','value':{'UI':'203.0.113.0/24'}if control=='global_deny'else{}})
 u=User.objects.get(pk=admin_id);original=u.custom_properties or {};original=dict(original)
 original.pop('allowed_networks',None)
 if control=='user_deny':original['allowed_networks']={'UI':'203.0.113.0/24'}
 u.custom_properties=original;u.save(update_fields=['custom_properties'])
 from django.db import connection as dc
 dc.close();return {'actual_CoreSettings_save_cache_invalidation':True,'actual_admin_custom_properties_save':True,'control':control}


def representations(args,kwargs):
 from celery.utils.saferepr import saferepr
 import inspect,hashlib
 path=Path(inspect.getsourcefile(saferepr));data=path.read_bytes()
 return {'argsrepr':saferepr(tuple(args),maxlen=1024),'kwargsrepr':saferepr(kwargs,maxlen=1024),'evaluator':'pinned Original Celery pure saferepr; no application setup/API/task body','module_sha256':hashlib.sha256(data).hexdigest(),'module_path':str(path)}

def http_effects(receiver):
 import hashlib
 c=connection();tables={}
 try:
  for table in TABLES:
   last=-1;digest=hashlib.sha256();count=0;first=[];tail=[]
   while True:
    rows=c.execute('SELECT id::bigint,to_jsonb(x)::text FROM '+table+' x WHERE id>%s ORDER BY id LIMIT 256',(last,)).fetchall()
    if not rows:break
    for ident,text in rows:
     count+=1
     if count>32768:raise RuntimeError('OwnedHTTPRowReadBudget32768')
     digest.update(text.encode()+b'\n');row=json.loads(text)
     if len(first)<4:first.append(row)
     tail=(tail+[row])[-4:];last=ident
   tables[table]={'count':count,'ordered_row_JSON_SHA256':digest.hexdigest(),'first4':first,'last4':tail,'query_page_bound':256,'row_budget':32768}
 finally:c.close()
 cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
 from channels_redis.core import RedisChannelLayer
 layer=RedisChannelLayer(hosts=['redis://:playback-fixture-only@127.0.0.1:6379/0']);key=(layer.prefix+receiver.split('!')[0]+'!').encode();raw=cache.zrange(key,0,1024)
 if len(raw)>1024 or sum(map(len,raw))>4194304:raise RuntimeError('ProgressReadBudget')
 result={'tables':tables,'Channels_decoded':[layer.deserialize(v)for v in raw],'Channels_raw_base64':[base64.b64encode(v).decode()for v in raw],'scope':'Bounded actual Native post-cell row hashes/counts/samples and complete retained progress; Source98 task effects were not executed. Exact Source9 effect comparison remains separate.'};cache.close();return result
