"""Production TCP producer/role/frame authority; no Source task worker or APIClient.
Original Dispatcharr/Django/Celery authors retain all application/protocol credit.
"""
import base64,hashlib,json,os,secrets,socket,sys,time,traceback,uuid
from pathlib import Path
from absolute_http import Observation
from cases import plan,ROUTES
from role_owner import start,stop,processes
OUT=Path('/evidence');DEADLINE=time.monotonic()+480
assert os.environ['POSTGRES_DB'].startswith('rust_probe_channel_http_')
assert os.environ['JOB_DISPOSABLE_FIXTURE']=='playback-fixture-only'
os.environ['DJANGO_SECRET_KEY']=secrets.token_hex(32)
os.environ.update(DISPATCHARR_TRUSTED_PROXIES='none',DISPATCHARR_PLUGINS_DIR='/data/plugins')
for name in ['plugins','logs','logos','m3us','epgs']:
 p=Path('/data')/name;p.mkdir(mode=0o755,exist_ok=True);p.chmod(0o755);os.chown(p,65532,65532)
sys.path.insert(0,'/app');sys.argv.append('shell')
import redis
cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=10)
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=600)
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from django.db import connection
from apps.accounts.models import User
from rest_framework_simplejwt.tokens import RefreshToken
from apps.channels.models import Channel,Stream
from apps.epg.models import EPGSource,EPGData
from core.models import CoreSettings
broker=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=8);backend=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=9)
report={'passed':False,'Source_API_calls':0,'actual_TCP_HTTP_attempts':0,'Source_APIClient_calls':0,'Source_task_body_calls':0,'task_worker_or_Beat_started':False,'cases':[],'web_role':{},'startup_frames':[],'credential_values_recorded':False,'scope':'Actual unmodified Original uWSGI/gevent web role +production TCP routes/credentials/queue frames; attached Celery/Beat/media daemons intentionally absent','default_Django_management_setup_processes':1}
role=None

def save():
 raw=(json.dumps(report,indent=2,default=str)+'\n').encode();assert len(raw)<=32*1024*1024;(OUT/'source-report.json').write_bytes(raw)
def queue():
 raw=broker.lrange('celery',0,128);assert len(raw)<=128 and sum(map(len,raw))<=4*1024*1024
 return [{'sha256':hashlib.sha256(v).hexdigest(),'raw_base64':base64.b64encode(v).decode(),'frame':json.loads(v)}for v in raw]
def business():
 result={}
 with connection.cursor()as c:
  for table in ['dispatcharr_channels_channel','dispatcharr_channels_logo','dispatcharr_channels_channelstream','dispatcharr_channels_channelprofilemembership']:
   c.execute('SELECT to_jsonb(x) FROM '+table+' x ORDER BY id LIMIT 513');result[table]=[v[0]for v in c.fetchall()];assert len(result[table])<=512
 return result
def network(control):
 CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Network Access','value':{'UI':'203.0.113.0/24'}if control=='global_deny'else{}})
 admin.custom_properties={'allowed_networks':{'UI':'203.0.113.0/24'}}if control=='user_deny'else{};admin.save(update_fields=['custom_properties'])

def credential(name):
 headers={}
 if name=='anonymous':return headers
 if name=='admin_x_api_key':return {'X-API-Key':admin.api_key}
 if name=='admin_authorization_api_key':return {'Authorization':'ApiKey '+admin.api_key}
 if name in ['bad_bearer','bad_bearer_with_valid_api_key']:headers['Authorization']='Bearer invalid.owned.fixture'
 else:headers['Authorization']='Bearer '+tokens['admin'if name=='admin_spoof'else name]
 if name=='bad_bearer_with_valid_api_key':headers['X-API-Key']=admin.api_key
 if name=='admin_spoof':headers['X-Forwarded-For']='203.0.113.1';headers['X-Real-IP']='203.0.113.1'
 return headers

def origin(frame):
 import re
 match=re.fullmatch(r'gen([0-9]+)@(.+)',frame['headers']['origin']);assert match
 pid=int(match[1]);fields=Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split();cmd=Path('/proc',str(pid),'cmdline').read_bytes().replace(b'\0',b' ').decode()
 ancestors=[];parent=int(fields[1])
 for _ in range(8):
  if parent<=0:break
  ancestors.append(parent)
  if parent==role['pid']:break
  try:parent=int(Path('/proc',str(parent),'stat').read_text().rsplit(')',1)[1].split()[1])
  except FileNotFoundError:break
 return {'pid':pid,'start_generation':fields[19],'actual_pgid':int(fields[2]),'web_role_pgid_matches':int(fields[2])==role['actual_pgid'],'actual_parent_chain':ancestors,'web_role_ancestry_matches':pid==role['pid']or role['pid']in ancestors,'actual_cmdline':cmd,'actual_hostname_matches':match[2]==Path('/proc/sys/kernel/hostname').read_text().strip()}

def one(spec):
 assert time.monotonic()<DEADLINE
 before=queue();network(spec['control']);before_business=business();body=spec['raw_body'].encode()if spec['raw_body']is not None else(None if spec['body']is None else json.dumps(spec['body']).encode())
 headers=credential(spec['credential_role']);headers.update(Accept='application/json')
 if body is not None:headers['Content-Type']='application/json'
 row={'definition':spec,'before_queue_sha256':[v['sha256']for v in before],'actual_credential_header_names':sorted(headers),'credential_role':spec['credential_role'],'credential_value_hashes':{k:hashlib.sha256(v.encode()).hexdigest()for k,v in headers.items()if k in ['Authorization','X-API-Key']},'before_business':before_business,'attempted_monotonic':time.monotonic()};report['cases'].append(row);report['actual_TCP_HTTP_attempts']+=1;save();obs=Observation('http://127.0.0.1:5656',seconds=min(20,DEADLINE-time.monotonic()))
 try:
  response=obs.request('/api/channels/channels/'+ROUTES[spec['route']][0],headers,method=spec['method'],body=body);raw=response.raw.read(1048577);assert len(raw)<=1048576
  report['Source_API_calls']+=1
  row.update(status=response.status_code,headers=list(response.raw.getheaders()),raw_body_base64=base64.b64encode(raw).decode(),raw_body_sha256=hashlib.sha256(raw).hexdigest());save();row['absolute_HTTP']=obs.complete()
  try:row['JSON']=json.loads(raw)
  except ValueError:row['JSON']=None
 except BaseException as error:row['primary_error']={'type':type(error).__name__,'message':str(error)}
 finally:
  row['actual_physical_HTTP_close']=obs.cancel();after=queue();new=[v for v in after if v['sha256']not in row['before_queue_sha256']];row['all_new_frames']=new;family=[v for v in new if v['frame']['headers']['task']==ROUTES[spec['route']][1]];row['new_family_frames']=family;row['after_business']=business();row['backend_keys']=backend.dbsize();checks={'actual_status':row.get('status')==spec['expected_status'],'exact_family_publication_count':len(family)==spec['expected_new_family_frames'],'no_task_body_effects':row['before_business']==row['after_business'],'no_backend_results':row['backend_keys']==0}
  if family:
   for value in family:
    frame=value['frame'];args,kwargs,embed=json.loads(base64.b64decode(frame['body']));value['actual_origin']=origin(frame)
    checks['actual_live_web_origin']=value['actual_origin']['web_role_ancestry_matches']and value['actual_origin']['actual_hostname_matches'];checks['reply_UUIDv3']=uuid.UUID(frame['properties']['reply_to']).version==3;checks['delivery_UUIDv4']=uuid.UUID(frame['properties']['delivery_tag']).version==4;checks['root_and_correlation']=frame['headers']['root_id']==frame['headers']['id']==frame['properties']['correlation_id']and frame['headers']['parent_id']is None;checks['actual_tuple_repr']=frame['headers']['argsrepr']==repr(tuple(args))if len(str(args))<900 else isinstance(frame['headers']['argsrepr'],str);checks['kwargs_repr']=frame['headers']['kwargsrepr']==repr(kwargs);checks['no_result_ignore']=frame['headers']['ignore_result']is False;checks['no_callbacks']=embed=={'callbacks':None,'errbacks':None,'chain':None,'chord':None}
    value['decoded_body']=[args,kwargs,embed]
    if row.get('JSON',{}):
     if isinstance(row['JSON'],dict)and 'task_id'in row['JSON']:checks['response_actual_task_id']=row['JSON']['task_id']==frame['headers']['id']
  row['checks']=checks;row['passed']=all(checks.values())and 'primary_error'not in row;save()
try:
 users={}
 for name,level,active in [('admin',10,True),('standard',1,True),('inactive',10,False)]:
  users[name]=User.objects.create_user(username='owned-channel-http-'+name,password=secrets.token_hex(24),user_level=level,is_active=active,api_key=secrets.token_hex(24),custom_properties={})
 admin=users['admin'];tokens={name:str(RefreshToken.for_user(user).access_token)for name,user in users.items()};report['credential_subjects']={k:{'id':u.pk,'level':u.user_level,'active':u.is_active,'api_key_sha256':hashlib.sha256(u.api_key.encode()).hexdigest()}for k,u in users.items()}
 src=EPGSource(name='Owned channel HTTP EPG',source_type='xmltv',is_active=False);EPGSource.objects.bulk_create([src]);epg=EPGData(name='Owned channel HTTP guide',tvg_id='owned.http.channel',icon_url='http://owned.invalid/http-logo.png',epg_source=src);EPGData.objects.bulk_create([epg]);channel=Channel(name='Owned channel HTTP',epg_data=epg,channel_number=7070);Channel.objects.bulk_create([channel]);stream=Stream(name='Owned HTTP stream',url='http://owned.invalid/http-stream',tvg_id='owned.http.channel',is_custom=True);Stream.objects.bulk_create([stream]);report['seed_ids']={'source':src.pk,'epg':epg.pk,'channel':channel.pk,'stream':stream.pk}
 broker.flushdb();backend.flushdb();network(None);report['before_business']=business()
 role=start('channel-http-web',['/dispatcharrpy/bin/uwsgi','--ini','/tools/uwsgi.ini'],{'POSTGRES_DB':os.environ['POSTGRES_DB'],'CELERY_BROKER_URL':os.environ['CELERY_BROKER_URL'],'CELERY_RESULT_BACKEND':os.environ['CELERY_RESULT_BACKEND'],'DISPATCHARR_PLUGINS_DIR':'/data/plugins','DISPATCHARR_TRUSTED_PROXIES':'none'})
 report['web_role']['start']=role;save()
 while time.monotonic()<DEADLINE:
  try:
   with socket.create_connection(('127.0.0.1',5656),timeout=.5):break
  except OSError:time.sleep(.025)
 else:raise TimeoutError('ActualUWSGITCPListener')
 report['startup_frames']=queue();definitions=plan(channel.pk,stream.pk,src.pk);report['planned_cases']=len(definitions)
 for spec in definitions:
  try:one(spec)
  except BaseException as error:
   report.setdefault('independent_case_errors',[]).append({'name':spec['name'],'type':type(error).__name__,'message':str(error)});save()
 network(None);report['after_business']=business();report['all_production_HTTP_cells_attempted']=len(report['cases'])==len(definitions);report['task_processes_before_stop']=[v for v in processes()if'celery'in v['argv_text']];report['backend_final_keys']=backend.dbsize();report['passed']=report['all_production_HTTP_cells_attempted']and all(r.get('passed')for r in report['cases'])and report['before_business']==report['after_business']and not report['task_processes_before_stop']and backend.dbsize()==0
except BaseException as error:report['primary_error']={'type':type(error).__name__,'message':str(error),'traceback':traceback.format_exc()}
finally:
 if role:
  try:
   receipt=stop(role);report['web_role']['stop']=receipt
   for kind,path in receipt['log_paths'].items():
    raw=Path(path).read_bytes();assert len(raw)<=4*1024*1024;(OUT/('web-'+kind+'.log')).write_bytes(raw)
   report['passed']=report['passed']and receipt['actual_exit']==0 and receipt['stdout_EOF']and receipt['stderr_EOF']and not receipt['forced']and receipt['all_original_process_generations_absent']
   owner_pid=receipt['owner_pid'];owner_deadline=time.monotonic()+5;owner_wait=None
   while time.monotonic()<owner_deadline:
    try:
     child,status=os.waitpid(owner_pid,os.WNOHANG)
     if child:owner_wait={'PID':child,'actual_wait_status':status,'actual_exit':os.waitstatus_to_exitcode(status)};break
    except ChildProcessError:break
    time.sleep(.025)
   report['web_role']['actual_owner_wait']=owner_wait
   report['passed']=report['passed']and owner_wait is not None and owner_wait['actual_exit']==0 and not Path('/proc',str(owner_pid)).exists()
  except BaseException as error:report['web_role']['cleanup_error']={'type':type(error).__name__,'message':str(error)};report['passed']=False
 connection.close();broker.close();backend.close();cache.close();save()
print(json.dumps({'passed':report['passed'],'Source_API_calls':report['Source_API_calls'],'cases':len(report['cases']),'Source_task_body_calls':0,'web_stopped':report['web_role'].get('stop',{}).get('actual_exit')}));raise SystemExit(0 if report['passed']else 1)
