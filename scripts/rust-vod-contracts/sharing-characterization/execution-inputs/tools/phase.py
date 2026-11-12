"""Blocked sharing characterization: exactly two draining same-SID clients."""
import concurrent.futures,hashlib,json,os,socket,sys,time,urllib.request
from pathlib import Path
if os.environ.get('NORMAL_REVIEWED_EXECUTION')!='one-normal-max2-pair':raise RuntimeError('ReviewedNormalPairRequired')
from normal_guard import child_guard
child_guard()
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.db import connections
from apps.m3u.models import M3UAccount,M3UAccountProfile
from urllib.parse import urlsplit
from absolute_http import Observation
from reader import Reader
from decoder import decode
ROOT=Path('/evidence');seed=json.loads((ROOT/'pending-main-private.json').read_text());mode=os.environ['NORMAL_MODE'];base=os.environ['NORMAL_BASE'];address=socket.gethostbyname(urlsplit(base).hostname);nonce=os.environ['NORMAL_FIXTURE_NONCE'];auth={'Authorization':'Bearer '+seed['access_token']};began=time.monotonic();readers={};report={'mode':mode,'HTTP_capture_valid':False,'scenario_ready':False,'clients':[],'checkpoints':[],'Source_generator_read_claim':False};store=redis.Redis(host='127.0.0.1',port=6390 if mode=='source'else 6391,password='playback-fixture-only',socket_timeout=3,socket_connect_timeout=3,decode_responses=True)
def caps():
 value={'account':M3UAccount.objects.get(pk=seed['provider_account_id']).max_streams,'profile':M3UAccountProfile.objects.get(pk=seed['provider_profile_id']).max_streams}
 if value!={'account':2,'profile':2}:raise ValueError('FixedMax2')
 return value
def origin():
 request=urllib.request.Request('http://origin:8080/ledger',headers={'X-Fixture-Nonce':nonce})
 with urllib.request.urlopen(request,timeout=3)as response:raw=response.read(65537)
 if len(raw)>65536:raise ValueError('OriginSnapshotCapacity')
 value=json.loads(raw)
 if value.get('invalid')or value.get('nonce_sha256')!=hashlib.sha256(nonce.encode()).hexdigest():raise ValueError('LiteralOriginInvalid')
 return value
def snapshot(label,target):
 row={'label':label,'target':target,'at':time.monotonic(),'readers':{k:{**v.snapshot(),'socket_fd':v.sock.fileno()}for k,v in readers.items()}};report['checkpoints'].append(row)
 return row
def observe(row):
 row['origin_observation_began']=time.monotonic()
 try:row['origin']=origin();row['literal_profile_counter']=store.get('profile_connections:'+str(seed['provider_profile_id']))
 except Exception as error:row['observation_error']=type(error).__name__
 row['origin_observation_completed']=time.monotonic()
def open_client(role):
 row={'role':role,'began':time.monotonic(),'sid':'fixture-sharing-same','content_uuid':seed['movie_uuid']};report['clients'].append(row);owner=Observation(base,address=address)
 try:
  response=owner.request('/proxy/vod/movie/'+seed['movie_uuid']+'/fixture-sharing-same',{**auth,'User-Agent':'owned-sharing-'+role});row.update(status=response.status_code,headers=dict(response.headers),headers_at=time.monotonic())
  if response.status_code==200:
   reader=Reader(response,ROOT/(mode+'-'+role+'.mp4'),role);readers[role]=reader;prefix=reader.await_initial(max(.001,owner.deadline-time.monotonic()));row['prefix_sha256']=hashlib.sha256(prefix).hexdigest()
  else:
   body=response.raw.read(65537)
   if len(body)>65536:raise ValueError('ErrorBodyCapacity')
   row['body_hex']=body.hex()
  row['absolute_observation']=owner.complete()
  if response.status_code!=200:row['physical_close']=owner.cancel()
 except Exception as error:
  row['observation_error']=type(error).__name__
  if role in readers:row['physical_close']=readers.pop(role).close()
  else:row['physical_close']=owner.cancel()
 row['first_observation_complete']=time.monotonic();return row
try:
 report['caps_before']=caps();owner=Observation(base,seconds=8,address=address)
 try:
  response=owner.request('/api/accounts/users/me/',auth);raw=response.raw.read(65537);owner.complete();report['actual_SQL_Bearer_verified']=len(raw)<=65536 and response.status_code==200 and json.loads(raw)['id']==seed['real_user_id']
 finally:owner.cancel()
 if not report['actual_SQL_Bearer_verified']:raise ValueError('ActualBearerUser')
 first=open_client('first');target=first['first_observation_complete']+1
 time.sleep(max(0,target-time.monotonic()));report['second_dispatch_target']=target;report['second_dispatch_actual']=time.monotonic();second=open_client('second');end=second['first_observation_complete']+35;report['fixed_teardown_target']=end
 with concurrent.futures.ThreadPoolExecutor(max_workers=2)as pool:
  futures=[]
  for offset in [5,15,30]:
   target=second['first_observation_complete']+offset;time.sleep(max(0,target-time.monotonic()));row=snapshot('drain_'+str(offset),target);futures.append(pool.submit(observe,row))
  time.sleep(max(0,end-time.monotonic()));report['teardown_snapshot']=snapshot('fixed_teardown',end);report['teardown_began']=time.monotonic()
  with concurrent.futures.ThreadPoolExecutor(max_workers=2)as close_pool:
   closes={k:close_pool.submit(v.close)for k,v in list(readers.items())};report['client_closes']={k:future.result(timeout=12)for k,future in closes.items()}
  readers.clear()
  for future in futures:future.result(timeout=10)
 report['caps_after']=caps()
 with concurrent.futures.ThreadPoolExecutor(max_workers=2)as pool:
  jobs={role:pool.submit(decode,ROOT/(mode+'-'+role+'.mp4'),ROOT/(mode+'-'+role+'-decode.log'))for role in ['first','second']if report['clients'][['first','second'].index(role)].get('status')==200}
  report['decoders']={role:job.result(timeout=45)for role,job in jobs.items()}
except Exception as error:report['error_type']=type(error).__name__;report['error_message']=str(error)[:128]
finally:
 for role,reader in list(readers.items()):
  try:report.setdefault('cleanup_client_closes',{})[role]=reader.close()
  except Exception as error:report.setdefault('close_errors',[]).append(type(error).__name__)
 try:
  if mode=='source':
   until=time.monotonic()+10
   while True:
    from normal_signals import validate
    rows=[json.loads(line)for line in(ROOT/'normal-signals.jsonl').read_text().splitlines()];invalid=json.loads((ROOT/'normal-signals-invalid.json').read_text())if(ROOT/'normal-signals-invalid.json').exists()else[]
    try:report['Source_response_finalization']=validate({'invalid':invalid,'rows':len(rows)},rows,complete=True);report['Source_signal_rows']=rows;break
    except ValueError:
     if time.monotonic()>=until:raise
     time.sleep(.05)
  report['terminal_origin_before_service_stop']=origin();report['caps_terminal']=caps()
 except Exception as error:report['terminal_error']=type(error).__name__
 connections.close_all();report['elapsed_seconds']=time.monotonic()-began
 from sharing_acceptance import phase
 report.update(phase(report));(ROOT/('normal-'+mode+'-private.json')).write_text(json.dumps(report,indent=2)+'\n')
if not report['HTTP_capture_valid']:raise SystemExit(1)
