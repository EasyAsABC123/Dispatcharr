"""Raw observer for actual packaged Main startup and settings debounce.
Storage-only pinned helper; no Original app import, mock callback, HTTP readiness,
or private process-state replacement. TCP readiness does not issue an HTTP request.
"""
import datetime,hashlib,http.server,json,os,pickle,pickletools,socket,sys,threading,time
from pathlib import Path
from absolute_http import Observation
from native_state import default,state,owned,need
OUT=Path('/evidence');SCHEMA=json.loads(Path('/native-root/inputs/schema62-authority.json').read_bytes());DATABASE=owned(os.environ['POSTGRES_DB']);TARGET=OUT/'native-startup.json';ACTION=sys.argv[1]
def save(result):
 raw=(json.dumps(result,indent=2,allow_nan=False)+'\n').encode();need(len(raw)<33554432,'Capture32MiB');p=TARGET.with_suffix('.json.tmp');p.write_bytes(raw);p.replace(TARGET)
def typed(raw):
 # Only bounded primitive pickle opcodes; never imports/classes/reducers.
 need(len(raw)<=65536,'CacheValue64KiB');allowed={'PROTO','FRAME','EMPTY_DICT','MEMOIZE','SHORT_BINUNICODE','BINUNICODE','SETITEM','SETITEMS','MARK','STOP','BINPUT','LONG_BINPUT','BINGET','LONG_BINGET'}
 need(all(op.name in allowed for op,arg,at in pickletools.genops(raw)),'PrimitiveCacheOpcodesOnly');value=pickle.loads(raw);need(value=={'time_zone':'UTC'},'TypedUTCGroup');return value

def snapshot(result,label):
 row={'label':label,'monotonic':time.monotonic(),'state':state(DATABASE,SCHEMA)};key=b':1:coresettings:group:system_settings';raw=next((r for d in row['state']['raw_Redis']if d['database']==1 for r in d['rows']if bytes.fromhex(r['key_hex'])==key),None);row['system_cache']={'present':raw is not None,'key_hex':key.hex()}
 if raw is not None:
  value=bytes.fromhex(raw['value_hex']);row['system_cache'].update(type=raw['type'],pttl=raw['pttl'],typed_value=typed(value),value_sha256=hashlib.sha256(value).hexdigest(),dump_sha256=hashlib.sha256(bytes.fromhex(raw['dump_hex'])).hexdigest())
 result['steps'].append(row);save(result);return row
need(not any(n=='django'or n.startswith(('django.','dispatcharr.','apps.'))for n in sys.modules),'NoOriginalApplicationImports')
if ACTION=='before':
 need(not TARGET.exists(),'FreshNativeCapture');x={'capture_complete':False,'production_Main':True,'Original_application_runs':0,'HTTP_readiness_requests':0,'actual_HTTP_calls':0,'steps':[],'requests':[],'default_before':default('dispatcharr',SCHEMA)};snapshot(x,'before-Main-start');sys.exit(0)
need(ACTION=='collect'and TARGET.exists(),'KnownCollectAction');x=json.loads(TARGET.read_bytes());deadline=time.monotonic()+45;readiness=[]
while True:
 need(time.monotonic()<deadline,'MainTCPReady45s');sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM);sock.settimeout(.5)
 try:sock.connect(('127.0.0.1',9194));ready=True
 except(ConnectionError,TimeoutError,OSError):ready=False
 finally:sock.close()
 readiness.append({'monotonic':time.monotonic(),'connected':ready,'socket_closed':sock.fileno()==-1,'HTTP_request_issued':False})
 if ready:break
 time.sleep(.05)
x['TCP_readiness']=readiness;snapshot(x,'after-Main-start-before-HTTP');seed=x['steps'][0]['state'];admin=next(u for u in seed['users']if u['user_level']==10);system=next(s for s in seed['settings']if s['key']=='system_settings')
def request(label,path,method='GET',body=None):
 row={'label':label,'method':method,'path':path,'began_monotonic':time.monotonic(),'began_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'attempted':True};x['requests'].append(row);x['actual_HTTP_calls']+=1;save(x);obs=Observation('http://127.0.0.1:9194',seconds=20)
 try:
  r=obs.request(path,{'X-Api-Key':admin['api_key'],'Accept':'application/json','Content-Type':'application/json'},method=method,body=body);raw=r.raw.read(1048577);need(len(raw)<=1048576,'HTTP1MiB');row.update(status=r.status_code,headers=list(r.raw.getheaders()),body_hex=raw.hex(),body_sha256=hashlib.sha256(raw).hexdigest(),absolute_observation=obs.complete());save(x)
  try:row['json']=json.loads(raw)
  except (ValueError,UnicodeDecodeError):row['json']=None
 finally:row['physical_close']=obs.cancel();row['completed_monotonic']=time.monotonic();row['completed_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save(x)
 snapshot(x,label);return row
try:
 request('first-request','/api/plugins/repos/')
 setter=request('actual-settings-setter','/api/core/settings/'+str(system['id'])+'/',method='PATCH',body=b'{"value":{"time_zone":"UTC"}}')
 immediate=request('immediate-after-setter','/api/plugins/repos/');need(immediate['completed_monotonic']-setter['began_monotonic']<5,'EntireSetterImmediateWindowUnder5s')
 # The actual setter must have happened before its response completed. Waiting
 # from response completion gives a measured conservative lower bound on age.
 wait_until=setter['completed_monotonic']+5.1;time.sleep(max(0,wait_until-time.monotonic()))
 expired=request('expired-after-setter','/api/plugins/repos/');need(expired['began_monotonic']-setter['completed_monotonic']>=5.05,'ActualMinimumSetterAgeOver5s')
 # Finite signing uses the same actual Main and owned NetNS; no new fixture,
 # Source API, or public request. Python canonical bytes were signed by actual
 # tool GPG in the pinned storage image and the private home already retired.
 assets=OUT/'finite-assets';tool=json.loads((assets/'asset-generation-report.json').read_bytes());need(tool['passed'],'ActualFiniteToolKeygenSignatureCleanup')
 origin_events=[]
 class Handler(http.server.BaseHTTPRequestHandler):
  def do_GET(self):
   need(self.path in ('/finite-valid.json','/finite-invalid.json'),'OwnedFiniteOriginRoute');raw=(assets/self.path[1:]).read_bytes();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw);self.wfile.flush();origin_events.append({'path':self.path,'bytes_written':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest(),'headers':list(self.headers.items())})
  def log_message(self,*args):pass
 origin=http.server.HTTPServer(('127.0.0.1',18085),Handler);origin_thread=threading.Thread(target=origin.serve_forever);origin_thread.start()
 try:
  public=(assets/'finite-public-key.asc').read_text()
  for label,route,expected in [('finite-signed-valid','finite-valid.json',True),('finite-signed-changed','finite-invalid.json',False)]:
   raw=json.dumps({'url':'http://127.0.0.1:18085/'+route,'public_key':public},separators=(',',':')).encode();response=request(label,'/api/plugins/repos/preview/',method='POST',body=raw);need(response['json']['signature_verified']is expected,'ActualFiniteSignatureVerdict:'+label)
 finally:
  origin.shutdown();origin_thread.join(3);origin.server_close();x['finite_origin_events']=origin_events;x['finite_origin_joined']=not origin_thread.is_alive();need(not origin_thread.is_alive(),'ActualFiniteOriginJoined')
 x['capture_complete']=True
finally:
 x['default_after']=default('dispatcharr',SCHEMA);x['default_unchanged']=x['default_before']==x['default_after'];save(x)
need(x['default_unchanged'],'Default6259Unchanged');need(not any(n=='django'or n.startswith(('django.','dispatcharr.','apps.'))for n in sys.modules),'ZeroOriginalApplicationImportsAtEnd')
print(json.dumps({'capture_complete':x['capture_complete'],'actual_HTTP_calls':x['actual_HTTP_calls'],'production_Main':True,'Source_application_runs':0}))
