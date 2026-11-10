"""Owned same-service OutputProfile DELETE/cache restart comparison.
Original Dispatcharr authors retain full application and algorithm credit/AGPL.
Fixture JWTs are used only in memory; this output contains no authorization data.
"""
import ast,hashlib,json,os,subprocess,sys,tempfile,time
from pathlib import Path
assert os.environ.get('POSTGRES_PASSWORD')=='playback-fixture-only'
assert os.environ.get('OUTPUT_PROFILE_DISPOSABLE_FIXTURE')=='playback-fixture-only'
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import requests
from django.core.cache import cache
from django.db import connection
from apps.accounts.models import User
from core.models import CoreSettings,OutputProfile
from core.utils import RedisClient
fixture=json.loads(Path('/evidence/output-profile-fixture.json').read_text());PREFIX='rust-probe-output-profile-';IDS=(91001,91002,91003);base=fixture['profiles'];users=[User.objects.get(pk=pk)for pk in fixture['user_ids']]
KEYS=['coresettings:group:stream_settings','coresettings:group:dvr_settings','coresettings:groupver:stream_settings','coresettings:groupver:dvr_settings','coresettings:default_user_agent','coresettings:default_user_agent:ver']
for filename,names in [('output_profile_fixture.py',{'clear_faults','install_fault','restore','wire','witness','cache_witness'}),('output_profile_packaged_probe.py',{'reset'})]:
 tree=ast.parse(Path('/integration',filename).read_text());exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name in names],type_ignores=[]),'frozen-owned-fixture-helpers','exec'),globals())
client=RedisClient.get_client();assert client is not None;initial_client=id(client);work=Path(tempfile.mkdtemp(prefix='owned-profile-cache-'));down=False
case=next(c for c in fixture['cases'] if c['method']=='DELETE' and c.get('setup') is None and c['expected']['response']['status']==204)
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)
def web_pids():
 result=[]
 for p in Path('/proc').iterdir():
  if p.name.isdigit():
   try:
    if (p/'comm').read_text().strip()=='uwsgi':result.append(int(p.name))
   except (FileNotFoundError,PermissionError,ProcessLookupError):pass
 return sorted(result)
def start():
 global down
 subprocess.run(['redis-server','--daemonize','yes','--port','6379','--bind','0.0.0.0','--protected-mode','yes','--requirepass','playback-fixture-only','--save','','--appendonly','no','--dir',str(work),'--pidfile',str(work/'redis.pid')],capture_output=True,check=True,timeout=10)
 deadline=time.monotonic()+10
 while time.monotonic()<deadline:
  try:
   if client.ping():down=False;return
  except Exception:pass
  time.sleep(.05)
 raise TimeoutError('Owned Redis readiness')
def request(target):
 raw,mime=wire(case['body']);response=requests.request(case['method'],target+case['path'],data=raw,headers={**case['headers'],'Content-Type':mime},timeout=25,allow_redirects=False)
 try:
  h={k:response.headers[k]for k in ['content-type','content-length','www-authenticate','x-content-type-options']if k in response.headers};v={'status':response.status_code,'headers':h}
  if 'json'in h.get('content-type',''):v['json']=response.json()
  else:v.update(body_sha256=hashlib.sha256(response.content).hexdigest(),body_length=len(response.content))
  return {'response':v,'db':witness(),'cache':cache_witness()}
 finally:response.close()
def seed_cache_only():
 # Explicit peer rewarm after state loss. This does not restore a writer, SQL,
 # an ownership lease or unrelated Redis keys.
 for key,value in case['pre']['cache'].items():
  assert key in KEYS
  cache.delete(key)
  if value is not None:cache.set(key,123456 if key.endswith(':ver')or ':groupver:'in key else value,timeout=None if key.endswith(':ver')or ':groupver:'in key else 300)
report={'capture_complete':False,'passed':False,'scope':__doc__,'case':case['name'],'fixture_policy':'Only named original cache fixture keys re-warmed by an independent peer after each NOSAVE restart; same SQL rows, writer services and container generations remain','phases':[],'warmups':{},'performance_verified':False};initial_web=web_pids();assert initial_web,'Actual Original HTTP workers required'
try:
 for backend,target in [('candidate',os.environ.get('PROFILE_CACHE_NATIVE_BASE_URL','http://candidate:9192')),('source','http://127.0.0.1:5656')]:
  warm_attempts=[];report['warmups'][backend]=warm_attempts
  for ordinal in range(1,4):
   reset(case);warm=request(target);warm_attempts.append({'ordinal':ordinal,'matches_expected':canonical(warm)==canonical(case['expected']),'actual':warm})
   if warm_attempts[-1]['matches_expected']:break
   time.sleep(.1)
  assert warm_attempts[-1]['matches_expected'],'Warm same-service request did not settle'
  reset(case);before={'db':witness(),'cache':cache_witness()};assert canonical(before)==canonical(case['pre'])
  old=client.info('server')['run_id'];subprocess.run(['redis-cli','--no-auth-warning','SHUTDOWN','NOSAVE'],env=dict(os.environ,REDISCLI_AUTH='playback-fixture-only'),capture_output=True,check=True,timeout=10);down=True;start();new=client.info('server')['run_id'];assert old!=new
  assert all(not client.exists(':1:'+key) for key in KEYS)
  seed_cache_only();assert canonical(cache_witness())==canonical(case['pre']['cache']);assert canonical(witness())==canonical(before['db'])
  actual=request(target);record={'backend':backend,'warmup_attempts':warm_attempts,'run_id_changed':old!=new,'same_observer_client':id(client)==initial_client,'same_source_worker_pids':web_pids()==initial_web,'first_post_restart':actual,'matches_pre_restart_expected':canonical(actual)==canonical(case['expected'])}
  # Separate future logical attempts; never call this first-operation parity.
  attempts=[]
  for index in range(2):
   reset(case);value=request(target);attempts.append({'ordinal':index+2,'actual':value,'matches_expected':canonical(value)==canonical(case['expected'])})
  record['later_logical_attempts']=attempts;report['phases'].append(record)
 report['capture_complete']=True
 report['passed']=all(x['matches_pre_restart_expected'] and x['same_source_worker_pids'] and all(a['matches_expected']for a in x['later_logical_attempts'])for x in report['phases'])
finally:
 if down:start()
 restore();Path('/evidence/output-profile-restart.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,separators=(',',':')))
