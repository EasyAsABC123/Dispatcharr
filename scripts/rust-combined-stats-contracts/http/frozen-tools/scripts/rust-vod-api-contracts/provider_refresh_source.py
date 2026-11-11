"""Pinned Original actual TCP refresh/effects oracle; disposable local XC only.
No native/provider-refresh equivalence is inferred. Original authors retain credit.
"""
import datetime,hashlib,http.client,json,os,sys,threading,time,socket,subprocess
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import parse_qs,urlsplit,quote
from contextlib import nullcontext
from unittest.mock import patch
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import transaction,connection
from django.utils import timezone
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile,M3UFilter
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,Stream
from core.models import CoreSettings,SystemEvent
class Encoder(DjangoJSONEncoder):
 def default(self,v):
  if isinstance(v,(datetime.datetime,datetime.date,datetime.time)):return v.isoformat()
  return super().default(v)
prefix='rust-probe-vod-rest-';assert Movie.objects.count()==27 and Series.objects.count()==4 and Episode.objects.count()==12
accounts=list(M3UAccount.objects.filter(name__startswith=prefix).order_by('-priority','id'));assert len(accounts)==3
movie=Movie.objects.order_by('id').first();series=Series.objects.order_by('id').first();mr=list(M3UMovieRelation.objects.filter(movie=movie).order_by('id'));sr=M3USeriesRelation.objects.get(series=series)
fixed=datetime.datetime(2026,11,9,12,37,29,123456,tzinfo=datetime.timezone.utc)
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent,M3UFilter,ChannelGroup,ChannelGroupM3UAccount,Stream]
touched=[M3UAccount,Movie,Series,Episode,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation]
saved={m:list(m.objects.order_by('id').values())for m in touched};records=[];requests=[];mode={};unexpected=[]
def state():return {m._meta.db_table:list(m.objects.order_by('id').values())for m in touched}
def fingerprints():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('id').values()),sort_keys=True,cls=Encoder).encode()).hexdigest()for m in models}
baseline=fingerprints();baseline_rows={m._meta.db_table:list(m.objects.order_by('id').values())for m in models}
def restore():
 with transaction.atomic():
  for m in reversed(touched):m.objects.exclude(pk__in=[r['id']for r in saved[m]]).delete()
  # Conflict merges can remove/reassign an identity; clear ONLY fixture IDs first.
  Movie.objects.filter(pk__in=[r['id']for r in saved[Movie]]).update(tmdb_id=None,imdb_id=None)
  for m in touched:
   for row in saved[m]:
    values={k:v for k,v in row.items()if k!='id'}
    if not m.objects.filter(pk=row['id']).update(**values):
     m.objects.bulk_create([m(**row)])
     # bulk_create invokes auto_now/auto_now_add. Restore captured timestamps
     # after recreating a relation deliberately deleted by refresh.
     m.objects.filter(pk=row['id']).update(**values)
 if 'baseline_sequences'in globals():
  with connection.cursor()as cursor:
   for name,seq in baseline_sequences.items():cursor.execute('SELECT setval(%s::regclass,%s,%s)',[name,seq['last_value'],seq['is_called']])
  assert sequences()==baseline_sequences
 actual=fingerprints()
 if actual!=baseline:
  diffs={m._meta.db_table:{'expected':baseline_rows[m._meta.db_table],'actual':list(m.objects.order_by('id').values())}for m in models if actual[m._meta.db_table]!=baseline[m._meta.db_table]}
  path='/evidence/restore-difference-private.json';open(path,'w').write(json.dumps(diffs,cls=Encoder,indent=2));os.chmod(path,0o600)
  raise AssertionError('Restoration differences: '+','.join(diffs))
def sequences():
 result={}
 with connection.cursor()as cursor:
  for model in touched:
   cursor.execute("SELECT pg_get_serial_sequence(%s,'id')",[model._meta.db_table]);name=cursor.fetchone()[0]
   if name:
    # Sequence names are database-owned introspection, never request strings.
    cursor.execute('SELECT last_value,is_called FROM '+name);value=cursor.fetchone();result[name]={'last_value':value[0],'is_called':value[1]}
 return result
baseline_sequences=sequences()
class Origin(BaseHTTPRequestHandler):
 def do_GET(self):
  query=parse_qs(urlsplit(self.path).query,keep_blank_values=True);action=query.get('action',['authenticate'])[0]
  request_record={'method':self.command,'raw_target':self.path,'path':urlsplit(self.path).path,'query':query,'headers':dict(self.headers),'case':mode.get('label')};requests.append(request_record)
  allowed=(urlsplit(self.path).path in ['/player_api.php','/owned/base/player_api.php']or urlsplit(self.path).path.startswith('/owned/redirect/')) and action in ['authenticate','get_vod_info','get_series_info']
  if not allowed:unexpected.append(self.path)
  auth=action=='authenticate'
  redirects=mode.get('redirect_auth'if auth else'redirect_info',0)
  current=int(urlsplit(self.path).path.split('/')[3])if urlsplit(self.path).path.startswith('/owned/redirect/')else 0
  if current<redirects:
   status=mode.get('redirect_status',302);target='/owned/redirect/'+str(current+1)+'/player_api.php?'+urlsplit(self.path).query
   request_record['response']={'status':status,'location':target,'body_base64':'','body_bytes':0}
   self.send_response(status);self.send_header('Location',target);self.send_header('Content-Length','0');self.end_headers();return
  drop_key='close_auth'if auth else'close_info'
  if mode.get(drop_key,0)>0:
   mode[drop_key]-=1
   self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
  status=mode.get('auth_status',200)if auth else mode.get('status',200)
  payload=mode.get('auth',{'user_info':{'auth':1,'status':'Active'}})if auth else mode.get('payload',{})
  raw=mode.get('auth_raw')if auth else mode.get('raw')
  if not auth and mode.get('info_delay'):time.sleep(mode['info_delay'])
  if not auth and mode.get('concurrent_edit')and not mode.get('concurrent_edit_done'):
   mode['concurrent_edit_done']=True;edit=mode['concurrent_edit']
   if edit=='movie_parent':Movie.objects.filter(pk=movie.pk).update(name=prefix+'Concurrent movie name',description='Concurrent movie description',custom_properties={'concurrent':True})
   elif edit=='movie_relation':M3UMovieRelation.objects.filter(pk=mr[0].pk).update(custom_properties={'concurrent':True})
   elif edit=='series_parent':Series.objects.filter(pk=series.pk).update(name=prefix+'Concurrent series name',description='Concurrent series description',custom_properties={'concurrent':True})
   elif edit=='series_relation':M3USeriesRelation.objects.filter(pk=sr.pk).update(custom_properties={'concurrent':True})
   else:raise RuntimeError('Unknown predeclared concurrent edit')
  text=raw if raw is not None else json.dumps(payload,separators=(',',':'),ensure_ascii=False)
  body=text.encode(mode.get('body_encoding','utf-8'))
  if mode.get('body_hex')and not auth:body=bytes.fromhex(mode['body_hex'])
  codec=mode.get('auth_content_encoding'if auth else'content_encoding')
  if codec:
   import gzip,zlib
   for encoding in codec.split(','):
    encoding=encoding.strip()
    if encoding in ['gzip','x-gzip']:body=gzip.compress(body,mtime=0)
    elif encoding=='deflate':body=zlib.compress(body)
    elif encoding=='raw-deflate':body=zlib.compress(body,wbits=-15)
    elif encoding=='identity':pass
    else:raise AssertionError('Uncaptured fixture compressor '+encoding)
  import base64
  request_record['response']={'status':status,'content_type':mode.get('content_type','application/json'),'content_encoding':codec.replace('raw-deflate','deflate')if codec else None,'body_base64':base64.b64encode(body).decode(),'body_bytes':len(body)}
  self.send_response(status);self.send_header('Content-Type',mode.get('content_type','application/json'));self.send_header('Content-Length',str(len(body)))
  if codec:self.send_header('Content-Encoding',codec.replace('raw-deflate','deflate'))
  for key,value in mode.get('auth_headers'if auth else'info_headers',{}).items():self.send_header(key,value)
  self.end_headers()
  delay=mode.get('auth_body_delay'if auth else'body_delay',0)
  if delay:time.sleep(delay)
  request_record['response']['body_released']=True
  try:self.wfile.write(body)
  except (BrokenPipeError,ConnectionResetError):request_record['response']['peer_closed_before_write']=True
 def log_message(self,*args):pass
origin=ThreadingHTTPServer(('127.0.0.1',0),Origin);threading.Thread(target=origin.serve_forever,daemon=True).start();base='http://127.0.0.1:'+str(origin.server_port)
movie_payload={'info':{'plot':'Refreshed plot','genre':'Drama','rating':'8.4','duration_secs':'3600','releasedate':'2024-02-03','trailer':'fixture-trailer','actors':['One','Two'],'director':'Director'},'movie_data':{'container_extension':'mkv','category_id':1}}
episode_payload={'info':{'plot':'Refreshed series plot','rating':'8.5','genre':'Drama','releaseDate':'2024-03-04'},'episodes':{'4':[{'id':'refresh-stream-1','title':'Refreshed episode','episode_num':'1','container_extension':'mkv','info':{'plot':'Episode plot','rating':'8.0','duration_secs':'1800'}}]}}
def transport_control(record,options):
 binary=os.environ.get('VOD_REFRESH_HTTP_NATIVE')
 if not binary or not record['provider_requests'] or options.get('skip_transport'):return
 from core.xtream_codes import Client
 source_account=record['provider_requests'][0]['query'].get('username',[None])[0]
 account=M3UAccount.objects.get(username=source_account);series_request=record['kind']=='series'
 q=parse_qs(urlsplit(record['path']).query);relation_id=q.get('relation_id',[None])[0]
 qs=(M3USeriesRelation.objects.filter(series=series)if series_request else M3UMovieRelation.objects.filter(movie=movie)).filter(m3u_account__is_active=True)
 relation=qs.get(pk=int(relation_id))if relation_id else qs.order_by('-m3u_account__priority','id').first()
 identifier=relation.external_series_id if series_request else relation.stream_id;agent=account.get_user_agent_string()
 def reset_retries():
  for key in ['close_auth','close_info']:mode[key]=options.get(key,0)
 reset_retries();start=len(requests)
 try:
  with Client(account.server_url,account.username,account.password,agent)as client:value=client.get_series_info(identifier)if series_request else client.get_vod_info(identifier)
  expected={'outcome':'success','value':value}
 except Exception:expected={'outcome':'remote'}
 source_requests=requests[start:];reset_retries();start=len(requests)
 config={'fixture_guard':'playback-fixture-only','account':{'server_url':account.server_url,'username':account.username,'password':account.password},'agent':agent,'identifier':str(identifier),'series':series_request}
 process=subprocess.run([binary,'vod_api_draft::vod_api_provider_refresh_http::tests::native_refresh_http_peer','--ignored','--nocapture','--test-threads=1'],input=json.dumps(config),text=True,capture_output=True,timeout=40)
 line=next((line.split('REFRESH_TRANSPORT_RESULT=',1)[1]for line in process.stdout.splitlines()if 'REFRESH_TRANSPORT_RESULT='in line),None)
 actual=json.loads(line)if line else{'outcome':'peer-error','exit_code':process.returncode}
 native_requests=requests[start:]
 def wire(rows):return [{'method':r['method'],'raw_target':r['raw_target'],'headers':{key:r['headers'].get(key)for key in ['User-Agent','Accept','Accept-Encoding','Connection','Cookie']}}for r in rows]
 exact_data=json.dumps(expected,separators=(',',':'))==json.dumps(actual,separators=(',',':'))
 control={'scope':'Source Client/native transport only; no native SQL mutation actor','passed':process.returncode==0 and exact_data and wire(source_requests)==wire(native_requests),'expected':expected,'actual':actual,'source_requests':source_requests,'native_requests':native_requests,'exact_decoded_JSON':exact_data,'exact_selected_request_wire':wire(source_requests)==wire(native_requests),'peer_exit_code':process.returncode,'peer_stderr':process.stderr[-4096:]}
 record['native_transport_control']=control
def load_phase(snapshot):
 with transaction.atomic():
  for model in reversed(touched):model.objects.exclude(pk__in=[r['id']for r in snapshot[model._meta.db_table]]).delete()
  Movie.objects.all().update(tmdb_id=None,imdb_id=None)
  for model in touched:
   for row in snapshot[model._meta.db_table]:
    values={k:v for k,v in row.items()if k!='id'}
    if not model.objects.filter(pk=row['id']).update(**values):model.objects.bulk_create([model(**row)]);model.objects.filter(pk=row['id']).update(**values)

def actor_control(record,options):
 binary=os.environ.get('VOD_REFRESH_ACTOR_NATIVE')
 if not binary or record['expected']['status']!=200:return
 load_phase(record['before'])
 allowed=sequences()
 with connection.cursor()as cursor:
  for name,seq in record['sequence_before'].items():
   assert name in allowed
   cursor.execute('SELECT setval(%s::regclass,%s,%s)',[name,seq['last_value'],seq['is_called']])
 q=parse_qs(urlsplit(record['path']).query);relation_id=q.get('relation_id',[None])[0]
 qs=(M3UMovieRelation.objects.filter(movie_id=movie.pk,m3u_account__is_active=True)if record['kind']=='movie'else M3USeriesRelation.objects.filter(series_id=series.pk,m3u_account__is_active=True))
 relation=qs.get(pk=int(relation_id))if relation_id else qs.order_by('-m3u_account__priority','id').first()
 config={'fixture_guard':'playback-fixture-only','kind':record['kind'],'refresh_hours':int(q.get('refresh_interval',['24'])[0]),'relation_id':relation.pk,'force':q.get('force_refresh',['false'])[0].lower()=='true','fixed_clock':fixed.isoformat()if options.get('factory')else None}
 for key in ['close_auth','close_info']:mode[key]=options.get(key,0)
 mode['concurrent_edit_done']=False
 fault_table=options.get('sql_fault')
 if fault_table:
  with connection.cursor()as cursor:
   cursor.execute("CREATE FUNCTION owned_refresh_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Owned refresh write failure'; END $$")
   cursor.execute('CREATE TRIGGER owned_refresh_fault BEFORE INSERT OR UPDATE ON '+fault_table+' FOR EACH ROW EXECUTE FUNCTION owned_refresh_fault()')
 begin=timezone.now();start=len(requests)
 try:process=subprocess.run([binary,'vod_api_draft::vod_api_provider_refresh_peer::native_refresh_movie_actor_peer','--ignored','--nocapture','--test-threads=1'],input=json.dumps(config),text=True,capture_output=True,timeout=40)
 finally:
  if fault_table:
   with connection.cursor()as cursor:cursor.execute('DROP TRIGGER owned_refresh_fault ON '+fault_table);cursor.execute('DROP FUNCTION owned_refresh_fault()')
 native_actor_finished=timezone.now()
 native_reply_after_body_release=all(r.get('response',{}).get('body_released',False)for r in requests[start:]if r.get('response',{}).get('status',200)not in [301,302,303,307,308])
 if options.get('body_delay')or options.get('auth_body_delay'):
  deadline=time.monotonic()+max(options.get('body_delay',0),options.get('auth_body_delay',0))+2
  while not all(r.get('response',{}).get('body_released',False)for r in requests[start:]if r.get('response',{}).get('status',200)not in [301,302,303,307,308]):
   if time.monotonic()>deadline:raise AssertionError('Owned provider body-release cleanup deadline')
   time.sleep(.01)
 end=native_actor_finished;native=state();line=next((line.split('REFRESH_ACTOR_RESULT=',1)[1]for line in process.stdout.splitlines()if 'REFRESH_ACTOR_RESULT='in line),None);result=json.loads(line)if line else{'outcome':'peer-error','exit_code':process.returncode}
 differences=[];clocks=[]
 native_sequences=sequences()
 if native_sequences!=record['sequence_after']:differences.append({'kind':'sequence_state','Source':record['sequence_after'],'native':native_sequences})
 for table,expected_rows in record['after'].items():
  actual_rows=native[table];before={r['id']:r for r in record['before'][table]}
  if [r['id']for r in actual_rows]!=[r['id']for r in expected_rows]:differences.append({'table':table,'kind':'ordered_ID_set'});continue
  for expected,actual in zip(expected_rows,actual_rows):
   for key,value in expected.items():
    old=before.get(expected['id'],{}).get(key)
    changing_clock=key in ['created_at','updated_at','last_advanced_refresh','last_episode_refresh','last_seen'] and value!=old
    if changing_clock and not options.get('factory'):
     source_ok=isinstance(value,datetime.datetime)and record['wall_started']<=value<=record['wall_finished']
     actual_ok=isinstance(actual[key],datetime.datetime)and begin<=actual[key]<=end and actual[key]!=old
     clocks.append({'table':table,'id':expected['id'],'field':key,'source':value,'native':actual[key],'Source_own_interval':source_ok,'native_own_interval':actual_ok})
     if not(source_ok and actual_ok):differences.append({'table':table,'id':expected['id'],'field':key,'kind':'causal_clock'})
    elif table=='vod_episode' and key=='uuid' and expected['id']not in before:
     import uuid
     source_uuid=uuid.UUID(str(value));native_uuid=uuid.UUID(str(actual[key]));valid=source_uuid.version==4 and native_uuid.version==4
     clocks.append({'table':table,'id':expected['id'],'field':'uuid','Source_raw':str(value),'native_raw':str(actual[key]),'new_episode_only_UUIDv4':valid})
     if not valid:differences.append({'table':table,'id':expected['id'],'field':key,'kind':'new_episode_UUIDv4'})
    elif json.dumps(actual[key],cls=Encoder,separators=(',',':'))!=json.dumps(value,cls=Encoder,separators=(',',':')):differences.append({'table':table,'id':expected['id'],'field':key,'expected':value,'actual':actual[key]})
 def wire(rows):return [{'method':r['method'],'raw_target':r['raw_target'],'headers':{key:r['headers'].get(key)for key in ['User-Agent','Accept','Accept-Encoding','Connection','Cookie']}}for r in rows]
 wire_equal=wire(record['provider_requests'])==wire(requests[start:])
 full_headers_equal=[{k.lower():v for k,v in row['headers'].items()}for row in record['provider_requests']]==[{k.lower():v for k,v in row['headers'].items()}for row in requests[start:]]
 record['native_movie_actor_control'if record['kind']=='movie'else'native_series_actor_control']={'passed':process.returncode==0 and result.get('outcome')=='complete'and not differences and wire_equal and full_headers_equal and (native_reply_after_body_release or not(options.get('body_delay')or options.get('auth_body_delay'))),'native_reply_after_body_release':native_reply_after_body_release,'exact_selected_provider_wire':wire_equal,'exact_all_request_header_values':full_headers_equal,'scope':'Task-phase actual SQL, no native API route or SourceTCP byte normalization','peer_result':result,'peer_exit_code':process.returncode,'peer_stderr':process.stderr[-4096:],'differences':differences,'clocks':clocks,'native_after':native,'native_provider_requests':requests[start:],'clock_mode':'fixed exact'if options.get('factory')else'raw Source/native timestamps retained, each changed clock checked against its OWN request interval'}
 restore()
def case(label,kind='movie',payload=None,query='',role='standard',method='GET',fresh=False,**options):
 selected_labels=os.environ.get('VOD_REFRESH_CASE_FILTER','').split(',')
 if selected_labels!=['']and label not in selected_labels:return
 restore();mode.clear();mode.update(label=label,payload=movie_payload if kind=='movie'else episode_payload,**options)
 if payload is not None:mode['payload']=payload
 for index,a in enumerate(accounts):M3UAccount.objects.filter(pk=a.pk).update(server_url=base+options.get('server_suffix',''),username='owned-user-'+str(index),password='owned-pass-'+str(index))
 if options.get('encoded_credentials'):
  M3UAccount.objects.filter(pk=accounts[0].pk).update(username='owned +/é',password='owned &?=')
  M3UMovieRelation.objects.filter(pk=mr[0].pk).update(stream_id='owned +/é')
 now=fixed if options.get('factory')else timezone.now();timestamp=now-datetime.timedelta(hours=1 if fresh else 25)
 if kind=='movie':
  for rel in mr:M3UMovieRelation.objects.filter(pk=rel.pk).update(custom_properties={'detailed_fetched':True,'detailed_info':{'plot':'Old plot'},'movie_data':{}},last_advanced_refresh=timestamp)
  if options.get('missing_flags'):M3UMovieRelation.objects.filter(pk=mr[0].pk).update(custom_properties={})
  if options.get('null_timestamp'):M3UMovieRelation.objects.filter(pk=mr[0].pk).update(last_advanced_refresh=None)
 else:
  M3USeriesRelation.objects.filter(pk=sr.pk).update(custom_properties={'episodes_fetched':True,'detailed_fetched':True},last_episode_refresh=timestamp)
  if options.get('missing_flags'):M3USeriesRelation.objects.filter(pk=sr.pk).update(custom_properties={})
  if options.get('null_timestamp'):M3USeriesRelation.objects.filter(pk=sr.pk).update(last_episode_refresh=None)
  if options.get('no_stored_episodes'):M3UEpisodeRelation.objects.filter(episode__series=series).delete()
 if options.get('blank_series_metadata'):Series.objects.filter(pk=series.pk).update(description=None,genre=None,rating=None,year=None)
 if options.get('duplicate_movie_identity'):Movie.objects.filter(pk=saved[Movie][1]['id']).update(tmdb_id='8675309')
 if options.get('second_series'):
  M3USeriesRelation.objects.bulk_create([M3USeriesRelation(series=series,m3u_account=accounts[1],external_series_id='owned-second-series',custom_properties={'detailed_fetched':True,'episodes_fetched':True},last_episode_refresh=timestamp)])
  selected=M3USeriesRelation.objects.get(series=series,m3u_account=accounts[1]);query='?relation_id='+str(selected.pk)
 fault_table=options.get('sql_fault')
 if fault_table:
  assert fault_table in ['vod_movie','vod_m3umovierelation','vod_episode']
  with connection.cursor()as cursor:
   cursor.execute("CREATE FUNCTION owned_refresh_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Owned refresh write failure'; END $$")
   cursor.execute('CREATE TRIGGER owned_refresh_fault BEFORE INSERT OR UPDATE ON '+fault_table+' FOR EACH ROW EXECUTE FUNCTION owned_refresh_fault()')
 sequence_before=sequences()
 before=state();before_hash=fingerprints();start=len(requests);obj=movie if kind=='movie'else series;path='/api/vod/'+('movies'if kind=='movie'else'series')+'/'+str(obj.pk)+'/provider-info/'+query
 headers={'Host':'localhost','Accept':'application/json'}
 if role:headers['Authorization']='Bearer '+str(AccessToken.for_user(User.objects.get(username=prefix+role)))
 client=http.client.HTTPConnection('127.0.0.1',5656,timeout=20);begin=time.monotonic();wall_started=timezone.now()
 try:
  if options.get('factory'):
   from django.urls import resolve
   from rest_framework.test import APIRequestFactory
   with patch('django.utils.timezone.now',return_value=fixed):
    request=APIRequestFactory().generic(method,path,**{'HTTP_'+k.upper().replace('-','_'):v for k,v in headers.items()});view=resolve(path.split('?')[0]);response=view.func(request,**view.kwargs);response.render();body=response.content
   response.status=response.status_code;response.getheader=lambda key:response.get(key)
  else:
   client.request(method,quote(path,safe='/?=&|,'),headers=headers);response=client.getresponse();body=response.read(4*1024*1024+1)
  assert len(body)<=4*1024*1024
  expected={'status':response.status,'headers':{k:response.getheader(k)for k in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate','X-Content-Type-Options','X-Frame-Options','Referrer-Policy','Cross-Origin-Opener-Policy']},'body_base64':__import__('base64').b64encode(body).decode(),'body_sha256':hashlib.sha256(body).hexdigest()}
  try:expected['json']=json.loads(body)
  except ValueError:pass
 finally:
  client.close()
  if fault_table:
   with connection.cursor()as cursor:
    cursor.execute('DROP TRIGGER owned_refresh_fault ON '+fault_table);cursor.execute('DROP FUNCTION owned_refresh_fault()')
 records.append({'label':label,'kind':kind,'path':path,'role':role,'method':method,'clock_scope':('Source APIRequestFactory fixed django.utils.timezone.now; NOT Source network'if options.get('factory')else'real unpatched TCP clock; fresh1h vs stale25h, no exact24h-boundary claim'),'provider_scenario':dict(mode),'before':before,'before_fingerprints':before_hash,'sequence_before':sequence_before,'sequence_after':sequences(),'after':state(),'after_fingerprints':fingerprints(),'provider_requests':requests[start:],'expected':expected,'wall_started':wall_started,'wall_finished':timezone.now(),'elapsed_seconds_diagnostic_only':time.monotonic()-begin})
 transport_control(records[-1],options)
 actor_control(records[-1],options)

try:
 for kind in ['movie','series']:
  case(kind+'_stale',kind);case(kind+'_fresh',kind,fresh=True);case(kind+'_force',kind,fresh=True,query='?force_refresh=true');case(kind+'_force_upper',kind,fresh=True,query='?force_refresh=TRUE');case(kind+'_force_false',kind,fresh=True,query='?force_refresh=false');case(kind+'_force_one',kind,fresh=True,query='?force_refresh=1');case(kind+'_missing_flags',kind,fresh=True,missing_flags=True);case(kind+'_null_timestamp',kind,fresh=True,null_timestamp=True)
  for label,payload in [('empty_object',{}),('blank_info',{'info':{}}),('info_list',{'info':[{'plot':'List plot'}]}),('info_null',{'info':None}),('info_text',{'info':'bad'}),('empty_episodes',{'info':{},'episodes':{}}),('nonobject_body',[]),('xc_error',{'error':'Owned provider denied'})]:case(kind+'_'+label,kind,payload=payload)
  for label,kwargs in [('http500',{'status':500}),('http403',{'status':403}),('empty_bytes',{'raw':''}),('invalid_json',{'raw':'not-json'}),('auth_denied',{'auth':{'user_info':None}}),('auth_zero',{'auth':{'user_info':{'auth':0}}}),('auth_http403',{'auth_status':403})]:case(kind+'_'+label,kind,**kwargs)
  for role in ['admin','basic',None,'movies_off'if kind=='movie'else'series_off']:case(kind+'_role_'+str(role),kind,role=role)
  case(kind+'_head_refresh',kind,method='HEAD');case(kind+'_bad_relation',kind,query='?relation_id=bad');case(kind+'_absent_relation',kind,query='?relation_id=99999999')
 case('movie_specific_second',query='?relation_id='+str(mr[1].pk));case('movie_movie_data_list',payload={'info':[{'plot':'Array plot'}],'movie_data':[{'container_extension':'avi'}]});case('movie_duplicate_tmdb',payload={'info':{'tmdb_id':str(saved[Movie][1]['tmdb_id'])}})if saved[Movie][1]['tmdb_id'] else None
 case('series_no_stored_empty','series',payload={'info':{},'episodes':{}},no_stored_episodes=True);case('series_invalid_season_partial','series',payload={'info':{'plot':'Committed before invalid season'},'episodes':{'bad':[{'id':'bad','title':'Bad season','episode_num':1}]}});case('series_no_include_still_refresh','series',query='?include_episodes=false');case('series_bad_interval','series',query='?refresh_interval=bad');case('series_flags_false_huge_interval','series',query='?refresh_interval=99999999999999999999999999999999999',missing_flags=True)
 case('movie_duplicate_identity_merge',payload={'info':{'tmdb_id':'8675309','plot':'Merge identity'}},duplicate_movie_identity=True)
 case('movie_write_failure',sql_fault='vod_movie');case('movie_relation_failure_after_parent_save',sql_fault='vod_m3umovierelation')
 case('series_episode_failure_after_metadata','series',blank_series_metadata=True,sql_fault='vod_episode');case('series_invalid_season_after_metadata','series',blank_series_metadata=True,payload={'info':{'plot':'Committed metadata'},'episodes':{'bad':[{'id':'bad','episode_num':1}]}})
 case('series_specific_second','series',second_series=True)
 for kind in ['movie','series']:
  for suffix,payload in [('null_body',None),('empty_list_info',{'info':[]}),('list_head_nondict',{'info':[3,{'plot':'Ignored'}]}),('false_info',{'info':False})]:
   case(kind+'_'+suffix,kind,**({'raw':'null'}if suffix=='null_body'else{'payload':payload}))
  case(kind+'_fixed_clock_factory',kind,factory=True,blank_series_metadata=kind=='series')
 for kind in ['movie','series']:
  case(kind+'_auth_cookie',kind,auth_headers={'Set-Cookie':'owned_refresh=one; Path=/'})
  case(kind+'_close_auth_twice',kind,close_auth=2);case(kind+'_close_info_twice',kind,close_info=2)
  case(kind+'_base_php_query',kind,server_suffix='/owned/base/pasted.php?discard=one')
 case('movie_encoded_credentials',encoded_credentials=True)
 case('movie_concurrent_parent_overwrite',concurrent_edit='movie_parent',skip_transport=True)
 case('movie_concurrent_relation_overwrite',concurrent_edit='movie_relation',skip_transport=True)
 case('series_concurrent_blank_parent_overwrite','series',concurrent_edit='series_parent',blank_series_metadata=True,skip_transport=True)
 case('series_concurrent_nonblank_parent_preserved','series',concurrent_edit='series_parent',skip_transport=True)
 case('series_concurrent_relation_props_preserved','series',concurrent_edit='series_relation',skip_transport=True)
 existing_episode=Episode.objects.filter(series=series).order_by('season_number','episode_number').first();existing_rel=M3UEpisodeRelation.objects.filter(episode__series=series,m3u_account=accounts[0]).first()
 variants=[
  ('update_existing',{'1':[{'id':existing_rel.stream_id,'title':'Updated existing','episode_num':existing_episode.episode_number,'info':{'duration_secs':0,'rating':'7,5','air_date':'2024-2-3'}}]}),
  ('two_streams_one_new',{'9':[{'id':'new-a','title':'First','episode_num':99,'info':{'plot':'first'}},{'id':'new-b','title':'Last','episode_num':99,'info':{'plot':'last'}}]}),
  ('one_new_stream_two_episodes',{'9':[{'id':'same-new','title':'First','episode_num':1},{'id':'same-new','title':'Second','episode_num':2}]}),
  ('one_existing_stream_two_episodes',{'1':[{'id':existing_rel.stream_id,'title':'First','episode_num':1},{'id':existing_rel.stream_id,'title':'Second','episode_num':2}]}),
  ('array_seasons',[[{'id':'array-special','title':'Special','episode_num':1}],[],[{'id':'array-main','title':'Main','episode_num':'2'}]]),
  ('truthy_bad_info',{'1':[{'id':'bad-info','episode_num':1,'info':['truthy']}]}),
  ('falsey_info',{'1':[{'id':'empty-info','episode_num':1,'info':[]}]}),
  ('unhashable_number',{'1':[{'id':'unhashable','episode_num':[]}]}),
  ('bad_number_falls_zero',{'1':[{'id':'bad-num','title':'Zero','episode_num':'bad'}]}),
  ('fractional_number',{'1':[{'id':'fraction-num','title':'One','episode_num':1.9}]}),
  ('bool_number',{'1':[{'id':'bool-num','title':'One','episode_num':True}]}),
  ('missing_everything',{'1':[{}]}),('nondicts_only',{'1':[None,3,'bad']}),('bad_season_nondicts',{'bad':[None]}),
  ('unsupported_outer',3),('null_title',{'1':[{'id':'null-title','title':None,'episode_num':1}]}),
  ('overlong_stream',{'1':[{'id':'x'*256,'title':'Long ID','episode_num':1}]}),
  ('overlong_new_title',{'9':[{'id':'long-new','title':'x'*256,'episode_num':1}]}),
  ('overflow_integer',{'9':[{'id':'overflow-num','title':'Large','episode_num':99999999999}]}),
  ('date_utc',{'9':[{'id':'date-utc','title':'UTC date','episode_num':1,'info':{'air_date':'2024-01-01T00:30:00+02:00'}}]}),
  ('date_overflow_new',{'9':[{'id':'date-overflow','title':'Bad date','episode_num':1,'info':{'air_date':'0001-01-01T00:00:00+23:00'}}]}),
  ('new_then_existing_date_overflow',{'1':[{'id':'new-valid','title':'New valid','episode_num':99},{'id':existing_rel.stream_id,'title':'Existing invalid','episode_num':existing_episode.episode_number,'info':{'air_date':'0001-01-01T00:00:00+23:00'}}]}),
  ('text_fields',{'9':[{'id':True,'title':{'title':1},'episode_num':'3','info':{'plot':{'plot':1},'tmdb_id':0,'imdb_id':False,'crew':['One'],'movie_image':['/fixture.svg']}}]}),
 ]
 for label,episodes in variants:case('series_episodes_'+label,'series',blank_series_metadata=True,payload={'info':{'plot':'New series metadata','year':'2024','genre':['Drama']},'episodes':episodes})
 for kind in ['movie','series']:
  for label,codec in [('gzip','gzip'),('xgzip','x-gzip'),('zlib','deflate'),('raw_deflate','raw-deflate'),('stacked','gzip, deflate'),('identity','identity')]:
   case(kind+'_response_codec_'+label,kind,content_encoding=codec,skip_transport=True)
  case(kind+'_auth_gzip',kind,auth_content_encoding='gzip',skip_transport=True)
  case(kind+'_charset_latin1',kind,payload={'info':{'plot':'caf\u00e9'},**({'episodes':{}}if kind=='series'else{'movie_data':{}})},content_type='application/json; charset=iso-8859-1',body_encoding='iso-8859-1',skip_transport=True)
  case(kind+'_utf16_octet',kind,content_type='application/octet-stream',body_encoding='utf-16',skip_transport=True)
  case(kind+'_invalid_utf8',kind,body_hex='7b22696e666f223a7b22706c6f74223a22636166e9227d7d',skip_transport=True)
  case(kind+'_nan_fields',kind,raw='{"info":{"plot":"Before NaN","rating":NaN},"episodes":{},"movie_data":{}}',skip_transport=True)
 for kind in ['movie','series']:
  for status in [301,302,303,307,308]:case(kind+'_info_redirect_'+str(status),kind,redirect_info=1,redirect_status=status,skip_transport=True)
  case(kind+'_auth_redirect',kind,redirect_auth=1,skip_transport=True)
  case(kind+'_info_redirect30',kind,redirect_info=30,skip_transport=True)
  case(kind+'_info_redirect31',kind,redirect_info=31,skip_transport=True)
 for kind in ['movie','series']:
  for label,raw in [('positive_infinity','Infinity'),('negative_infinity','-Infinity'),('overflow_float','1e309')]:
   case(kind+'_number_'+label,kind,raw='{"info":{"plot":"Before special float","rating":'+raw+'},"episodes":{},"movie_data":{}}',blank_series_metadata=kind=='series',skip_transport=True)
 for label,raw in [('nan_number','{"info":{},"episodes":{"9":[{"id":"nan-number","episode_num":NaN,"title":"NaN number"}]}}'),('nan_props','{"info":{"plot":"Before episode failure"},"episodes":{"9":[{"id":"nan-props","episode_num":1,"info":{"crew":NaN}}]}}')]:
  case('series_'+label,'series',raw=raw,blank_series_metadata=True,skip_transport=True)
 case('series_duplicate_largeint_infinity','series',blank_series_metadata=True,raw='{"info":{},"episodes":{"9":[{"id":"large-title-first","episode_num":1,"title":1'+('0'*400)+'},{"id":"infinity-title-last","episode_num":1,"title":Infinity}]}}',skip_transport=True)
 for kind in ['movie','series']:
  case(kind+'_slow_error_body',kind,status=500,body_delay=6.2,skip_transport=True)
  case(kind+'_slow_success_body',kind,body_delay=6.2,skip_transport=True)
  case(kind+'_slow_auth_error_body',kind,auth_status=403,auth_body_delay=6.2,skip_transport=True)
 case('movie_slow_info',info_delay=6.2,skip_transport=True)
 case('series_slow_info','series',info_delay=6.2,skip_transport=True)
 # Separate pinned Requests TCP control: explicit caller Referer must remain
 # untouched even though default redirects must not synthesize one.
 import requests as requests_library
 mode.clear();mode.update(label='explicit-caller-referer',redirect_auth=1)
 start=len(requests)
 with requests_library.Session()as session:
  response=session.get(base+'/player_api.php',headers={'Referer':'http://explicit.test/kept'},timeout=5)
  assert response.status_code==200
 explicit_referer_control={'Source_network':True,'passed':len(requests[start:])==2 and all(r['headers'].get('Referer')=='http://explicit.test/kept'for r in requests[start:]),'requests':requests[start:]}
 assert explicit_referer_control['passed']
finally:
 origin.shutdown();origin.server_close()
 path='/evidence/provider-refresh-partial-private.json';open(path,'w').write(json.dumps({'records':records,'requests':requests},cls=Encoder,indent=2));os.chmod(path,0o600)
 restore()
print(json.dumps({'passed':not unexpected and all(r.get('native_transport_control',{}).get('passed',True)and r.get('native_movie_actor_control',{}).get('passed',True)and r.get('native_series_actor_control',{}).get('passed',True)for r in records),'explicit_referer_control':explicit_referer_control,'observation':'actual Original TCP + before/after SQL rows; separate Factory cases labeled explicitly','records':records,'unexpected_origin_requests':unexpected,'restored18':fingerprints()==baseline,'sequences_restored':sequences()==baseline_sequences,'baseline_sequences':baseline_sequences,'baseline_fingerprints':baseline,'native_verified':False,'boundary_claims':False},cls=Encoder))
