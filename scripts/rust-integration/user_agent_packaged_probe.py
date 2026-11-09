"""Replay original UserAgent contracts against a source-bound native image.
Only disposable owned rows/cache keys are touched. Original authors retain credit.
"""
import ast,base64,datetime,hashlib,json,os,sys,threading,time,types
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import requests
from django.core.cache import cache
from django.db import connection,transaction
from apps.channels.models import Logo
from apps.m3u.models import M3UAccount
from core.models import CoreSettings,UserAgent,StreamProfile
from core.utils import RedisClient
fixture=json.loads(Path('/evidence/user-agent-fixture.json').read_text());base=fixture['agents'];profile=types.SimpleNamespace(pk=fixture['profile_id']);provider=types.SimpleNamespace(pk=fixture['provider_id']);UTC=datetime.timezone.utc
tree=ast.parse(Path('/integration/user_agent_fixture.py').read_text());wanted={'restore','normalize','witness','cached','wire'};exec(compile(ast.Module(body=[node for node in tree.body if isinstance(node,ast.FunctionDef)and node.name in wanted],type_ignores=[]),'frozen-source-fixture-helpers','exec'),globals())
target='http://candidate:9192';checks=[];HEADERS=['content-type','content-length','www-authenticate','x-content-type-options']
def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
for case in fixture['cases']:
 restore();cache.set('coresettings:default_user_agent','Rust-Probe/stale',300);cache.set('coresettings:default_user_agent:ver',123456,timeout=None)
 with connection.cursor()as cursor:cursor.execute("SELECT setval('core_useragent_id_seq',%s,true)",[case['sequence']])
 raw,mime=wire(case['body']);failures=[]
 if hashlib.sha256(raw).hexdigest()!=case['request_body_sha256']:failures.append('request_bytes')
 response=requests.request(case['method'],target+case['path'],data=raw,headers={**case['headers'],'Content-Type':mime},timeout=20,allow_redirects=False);expected=case['expected']['response'];headers={k:response.headers[k]for k in HEADERS if k in response.headers};expected_headers=dict(expected['headers'])
 if '<clock>'in json.dumps(expected.get('json')):headers.pop('content-length',None);expected_headers.pop('content-length',None)
 if response.status_code!=expected['status']:failures.append('status')
 if digest(headers)!=digest(expected_headers):failures.append('headers')
 if 'json'in expected:
  try:actual=response.json();equal=digest(normalize(actual))==digest(expected['json'])
  except ValueError:equal=False
  if not equal:failures.append('json')
 else:
  if len(response.content)!=expected['body_length']or hashlib.sha256(response.content).hexdigest()!=expected['body_sha256']:failures.append('bytes')
 response.close()
 if digest(witness())!=digest(case['expected']['db']):failures.append('db')
 if digest(cached())!=digest(case['expected']['cache_before_resolve']):failures.append('cache_invalidation')
 image=requests.get(target+'/api/channels/logos/'+str(fixture['logo_id'])+'/cache/',headers={'Host':'localhost'},timeout=20)
 if image.status_code!=case['expected']['original_image_reader']['status']or hashlib.sha256(image.content).hexdigest()!=case['expected']['original_image_reader']['sha256']:failures.append('native_default_reader')
 image.close()
 if CoreSettings.get_default_user_agent()!=case['expected']['resolved_default']:failures.append('original_consumer_of_native_cache')
 if digest(cached())!=digest(case['expected']['cache_after_resolve']):failures.append('cache_after_resolve')
 checks.append({'name':case['name'],'passed':not failures,'mismatches':failures})
restore();setting=CoreSettings.objects.get(key='stream_settings')
for index,case in enumerate(fixture['default_id_cases']):
 setting.value={**setting.value,'default_user_agent':case['raw']};setting.save();image=requests.get(target+'/api/channels/logos/'+str(fixture['logo_id'])+'/cache/',headers={'Host':'localhost'},timeout=20);expected=b'\x89PNG\r\n\x1a\nfixture-image'+case['expected'].encode();passed=image.status_code==200 and image.content==expected;image.close();checks.append({'name':'default-id-'+str(index),'passed':passed,'mismatches':[]if passed else['native_default_id_conversion']})
setting.value={**setting.value,'default_user_agent':90001};setting.save();restore();UserAgent.objects.filter(pk=90001).update(user_agent='Rust-Probe/race-old');cache.delete('coresettings:default_user_agent');cache.set('coresettings:default_user_agent:ver',123456,timeout=None)
loaded=threading.Event();resume=threading.Event();original=CoreSettings._load_default_user_agent_string;values=[]
def blocked(cls):
 value=original();loaded.set()
 if not resume.wait(10):raise RuntimeError('Reader deadline')
 return value
CoreSettings._load_default_user_agent_string=classmethod(blocked);thread=threading.Thread(target=lambda:values.append(CoreSettings.get_default_user_agent()));thread.start()
if not loaded.wait(10):raise RuntimeError('Original reader did not load')
headers={**fixture['cases'][0]['headers'],'Content-Type':'application/json'};response=requests.patch(target+'/api/core/useragents/90001/',headers=headers,data=b'{"user_agent":"Rust-Probe/race-new"}',timeout=10);resume.set();thread.join(10);CoreSettings._load_default_user_agent_string=classmethod(original.__func__)
race=response.status_code==200 and values==['Rust-Probe/race-old']and cache.get('coresettings:default_user_agent')is None and CoreSettings.get_default_user_agent()=='Rust-Probe/race-new';response.close();checks.append({'name':'original-reader-native-writer-stale-fill-fence','passed':race,'mismatches':[]if race else['inflight_cache_fence']})
print(json.dumps({'phase':'contracts-and-cache-race','passed':all(c['passed']for c in checks),'checks':checks}),flush=True)
# Hold SQL after body validation. Four native tasks must own their mutations
# after HTTP callers time out, while the fifth request is rejected before SQL.
restore();timeouts=[];threads=[]
def abandoned(index):
 try:requests.post(target+'/api/core/useragents/',json={'name':'rust-probe-ua-packaged-cancel-'+str(index),'user_agent':'Rust-Probe/packaged-cancel-'+str(index)},headers=fixture['cases'][0]['headers'],timeout=1)
 except requests.exceptions.RequestException:timeouts.append(index)
with transaction.atomic():
 with connection.cursor()as cursor:cursor.execute('LOCK TABLE core_useragent IN SHARE MODE')
 for index in range(4):thread=threading.Thread(target=abandoned,args=(index,));thread.start();threads.append(thread)
 deadline=time.monotonic()+3
 while True:
  with connection.cursor()as cursor:cursor.execute('SELECT pg_stat_clear_snapshot()');cursor.execute("SELECT count(*)FROM pg_stat_activity WHERE datname=current_database()AND wait_event_type='Lock'AND query LIKE 'INSERT INTO core_useragent(name,user_agent,%'");count=cursor.fetchone()[0]
  if count==4:break
  if time.monotonic()>deadline:raise RuntimeError('Four native mutations did not reach SQL')
  time.sleep(.005)
 response=requests.post(target+'/api/core/useragents/',data=b'{}',headers=headers,timeout=3);overload=response.status_code==503;response.close()
 for thread in threads:thread.join(2)
checks.append({'name':'four-owned-mutations-and-overload503','passed':overload and len(timeouts)==4,'mismatches':[]if overload and len(timeouts)==4 else['mutation_admission']})
deadline=time.monotonic()+5
while UserAgent.objects.filter(name__startswith='rust-probe-ua-packaged-cancel-').count()!=4:
 if time.monotonic()>deadline:raise RuntimeError('Abandoned native mutations did not finish')
 time.sleep(.01)
response=requests.post(target+'/api/core/useragents/',json={'name':'rust-probe-ua-packaged-recovery','user_agent':'Rust-Probe/packaged-recovery'},headers=fixture['cases'][0]['headers'],timeout=5);recovered=response.status_code==201;response.close();checks.append({'name':'abandoned-mutations-complete-and-service-recovers','passed':recovered,'mismatches':[]if recovered else['mutation_recovery']})
print(json.dumps({'passed':all(c['passed']for c in checks),'checks':checks,'source_cases':len(fixture['cases']),'default_id_cases':len(fixture['default_id_cases']),'resource_checks':3},separators=(',',':')))
