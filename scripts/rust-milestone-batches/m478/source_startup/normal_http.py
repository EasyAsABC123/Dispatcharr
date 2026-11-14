"""New Original normal WSGI startup context; no Original function replacements."""
import hashlib,json,os,sys,threading,time
from pathlib import Path
from wsgiref.simple_server import make_server
from absolute_http import Observation
from native_state import default, state, owned, need
OUT=Path('/evidence'); SCHEMA=json.loads(Path('/native-root/inputs/schema62-authority.json').read_bytes())
DATABASE=owned(os.environ['POSTGRES_DB']); result={'capture_complete':False,'actual_HTTP_calls':0,'Original_functions_replaced':False,'Source_APIClient_calls':0,'scope':'Original normal startup plus wsgiref HTTP, role django; not full uWSGI runtime','steps':[]}
def save():
 raw=(json.dumps(result,indent=2,allow_nan=False)+'\n').encode();need(len(raw)<33554432,'capture32MiB');p=OUT/'normal-startup.json.tmp';p.write_bytes(raw);p.replace(OUT/'normal-startup.json')
def zone():
 mod=sys.modules.get('dispatcharr.display_timezone');now=time.monotonic()
 return {'module_loaded':mod is not None,'monotonic':now,**({}if mod is None else {'zone':str(mod._cache['zone']),'checked':mod._cache['checked'],'age_seconds':now-mod._cache['checked']})}
def snapshot(label):
 row={'label':label,'display_timezone':zone(),'state':state(DATABASE,SCHEMA)}
 key=b':1:coresettings:group:system_settings'
 raw=next((r for db in row['state']['raw_Redis']if db['database']==1 for r in db['rows']if bytes.fromhex(r['key_hex'])==key),None)
 row['system_cache']={'present':raw is not None,'key_hex':key.hex()}
 if raw is not None:
  value=bytes.fromhex(raw['value_hex']);row['system_cache'].update(type=raw['type'],pttl=raw['pttl'],value_sha256=hashlib.sha256(value).hexdigest(),dump_sha256=hashlib.sha256(bytes.fromhex(raw['dump_hex'])).hexdigest())
  if 'django.core.cache' in sys.modules:
   from django.core.cache import cache
   row['system_cache']['typed_value']=cache.client.decode(value)
 result['steps'].append(row);save();return row
need(not any(n=='django' or n.startswith(('django.','dispatcharr.','apps.')) for n in sys.modules),'NoOriginalImportBeforeStart')
result['pid']=os.getpid();result['start_generation']=Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19];result['pid_namespace']=os.readlink('/proc/self/ns/pid');result['argv']=sys.argv.copy()
result['default_before']=default('dispatcharr',SCHEMA);snapshot('before-Original-import')
# Exact standard Original WSGI entry point; argv and functions stay unchanged.
from dispatcharr.wsgi import application
from dispatcharr.app_initialization import should_skip_initialization
from dispatcharr.db.process_label import get_process_role
result['actual_should_skip_initialization']=should_skip_initialization();result['actual_process_role']=get_process_role();need(result['actual_should_skip_initialization'] is False,'NormalStartupBranch');need(result['actual_process_role']=='django','ActualDjangoRole')
snapshot('after-normal-startup-before-HTTP')
server=make_server('127.0.0.1',9194,application);thread=threading.Thread(target=server.serve_forever,name='owned-normal-WSGI');thread.start()
users=result['steps'][0]['state']['users'];admin=next(u for u in users if u['user_level']==10);system=next(s for s in result['steps'][0]['state']['settings']if s['key']=='system_settings')
def request(label,path,method='GET',body=None):
 row={'label':label,'method':method,'path':path,'before_display_timezone':zone(),'attempted':True};result.setdefault('requests',[]).append(row);result['actual_HTTP_calls']+=1;save()
 obs=Observation('http://127.0.0.1:9194',seconds=20)
 try:
  r=obs.request(path,{'X-Api-Key':admin['api_key'],'Accept':'application/json','Content-Type':'application/json'},method=method,body=body);raw=r.raw.read(1048577);need(len(raw)<=1048576,'HTTP1MiB');row.update(status=r.status_code,headers=list(r.raw.getheaders()),body_hex=raw.hex(),body_sha256=hashlib.sha256(raw).hexdigest(),json=json.loads(raw),absolute_observation=obs.complete())
 finally:row['physical_close']=obs.cancel();save()
 row['after_display_timezone']=zone();snapshot(label);need(row['status']==200,'ActualHTTP200:'+label)
try:
 request('first-request','/api/plugins/repos/')
 request('actual-settings-setter','/api/core/settings/'+str(system['id'])+'/',method='PATCH',body=b'{"value":{"time_zone":"UTC"}}')
 request('immediate-after-setter','/api/plugins/repos/')
 result['actual_setter_checked']=zone()['checked'];need(result['requests'][-1]['after_display_timezone']['age_seconds']<5,'ImmediateWithinDebounce')
 delay=max(0,result['actual_setter_checked']+5.1-time.monotonic());need(delay<=5.1,'WaitBound5point1');time.sleep(delay)
 request('expired-after-setter','/api/plugins/repos/');need(result['requests'][-1]['before_display_timezone']['age_seconds']>=5.05,'ActuallyPastDebounce')
 result['capture_complete']=True
finally:
 server.shutdown();thread.join(3);server.server_close();result['WSGI_thread_joined']=not thread.is_alive();need(not thread.is_alive(),'ActualServerThreadJoined');result['default_after']=default('dispatcharr',SCHEMA);result['default_unchanged']=result['default_before']==result['default_after'];save()
need(result['default_unchanged'],'Default6259Unchanged')
print(json.dumps({'capture_complete':result['capture_complete'],'actual_HTTP_calls':result['actual_HTTP_calls'],'normal_startup':True,'WSGI_thread_joined':result['WSGI_thread_joined']}))
