"""Actual stdlib body/Origin adapter; no network operation until constructed/run.
Fixture setup/SQL/physical process ownership remain with the isolated driver.
"""
import http.client,json,socket,threading,time,urllib.error,urllib.request
from urllib.parse import urlsplit

class Body:
 def __init__(self,base,path,token,prefix_limit=2097152):
  target=urlsplit(base)
  if target.scheme!='http'or target.hostname not in{'127.0.0.1','localhost'}:raise ValueError('OwnedLoopbackBodyRequired')
  self.conn=http.client.HTTPConnection(target.hostname,target.port,timeout=50);self.path=path;self.token=token;self.limit=prefix_limit;self.response=None;self.sock=None;self.lock=threading.Lock();self.ready=threading.Event();self.stopping=False;self.bytes=0;self.prefix=bytearray();self.errors=[];self.eof=False;self.status=None;self.started=time.monotonic();self.ended=None;self.first_byte_monotonic=None
  self.thread=threading.Thread(target=self.read,name='m0-owned-original-body',daemon=True);self.thread.start()
 def read(self):
  try:
   self.conn.request('GET',self.path,headers={'Authorization':'Bearer '+self.token,'User-Agent':'dispatcharr-live-fixed-load'})
   self.response=self.conn.getresponse();self.status=self.response.status
   if self.response.fp is not None:self.sock=self.response.fp.raw._sock
   if self.status!=200:self.errors.append('HTTPStatus'+str(self.status));return
   while not self.stopping:
    data=self.response.read1(65536)
    if not data:self.eof=True;break
    with self.lock:
     self.bytes+=len(data)
     if self.first_byte_monotonic is None:self.first_byte_monotonic=time.monotonic()
     if len(self.prefix)<self.limit:self.prefix.extend(data[:self.limit-len(self.prefix)])
    self.ready.set()
  except BaseException as error:
   if not self.stopping:self.errors.append(type(error).__name__)
  finally:self.ended=time.monotonic();self.ready.set()
 def snapshot(self):
  with self.lock:return {'status':self.status,'bytes':self.bytes,'prefix_bytes':len(self.prefix),'EOF':self.eof,'errors':list(self.errors),'reader_alive':self.thread.is_alive(),'started_monotonic':self.started,'ended_monotonic':self.ended,'first_byte_monotonic':self.first_byte_monotonic}
 def close(self):
  self.stopping=True
  target=self.sock if self.sock is not None else self.conn.sock
  if target is not None:
   self.sock=target
   try:target.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  self.thread.join(5)
  if self.thread.is_alive():raise RuntimeError('PhysicalBodyThreadUndrained')
  if self.response is not None:self.response.close()
  self.conn.close()
  r=self.snapshot();r['physical_socket_closed']=self.sock is None or self.sock.fileno()==-1
  if not r['physical_socket_closed']:raise RuntimeError('PhysicalBodySocketUndrained')
  return r

class HTTPAdapter:
 def __init__(self,base,origin,path,token,callbacks,output):
  self.base=base;self.origin=origin;self.path=path;self.token=token;self.cb=callbacks;self.output=output;self.old=None;self.new=None;self.events=[];self.outcomes=[]
  target=urlsplit(origin)
  if target.scheme!='http'or target.hostname not in{'127.0.0.1','localhost'}:raise ValueError('OwnedOriginRequired')
 def effective_settings_and_path(self):return self.cb['effective']()
 def exact_owned_candidate_paths(self):return self.cb['candidates']()
 def verify_warmed_original_body(self,seconds):
  self.old=Body(self.base,self.path,self.token)
  if not self.old.ready.wait(20):raise TimeoutError('OriginalWarmFirstByte')
  until=time.monotonic()+seconds
  while time.monotonic()<until:
   if not self.old.thread.is_alive():raise RuntimeError('OriginalWarmBodyEnded')
   time.sleep(min(.1,max(0,until-time.monotonic())))
  if self.old.status!=200 or not self.old.bytes:raise RuntimeError('OriginalWarmNotHealthy')
  self.cb['verify_user'](1)
 def snapshot(self,label):
  value=self.cb['snapshot'](label);value['origin_socket_witness']=self.cb['origin_stats']();value['original_body']=None if self.old is None else self.old.snapshot();return value
 def require_existing_owned_input(self,before,paths):
  state=before['origin_socket_witness'];active=state['active_input_connections']
  if len(active)!=1 or active[0]['path']not in paths or active[0]['peer'][0]not in self.cb['source_IPs']:raise ValueError('ExactlyOneWarmedOwnedOriginInputRequired')
  return [active[0]['id']]
 def origin_control(self,path,action):
  control={'path':path,'action':action,'seconds':0}
  if action=='fail':
   state=self.cb['origin_stats']();targets=[r for r in state['active_input_connections']if r['path']==path]
   if any(r['peer'][0]not in self.cb['source_IPs']for r in targets):raise ValueError('ForeignOriginSocket')
   control['expected_connection_ids']=[r['id']for r in targets]
  data=json.dumps(control).encode();request=urllib.request.Request(self.origin+'/control',data=data,headers={'Content-Type':'application/json'})
  with urllib.request.urlopen(request,timeout=3)as response:
   raw=response.read(8193)
   reply=json.loads(raw)
   if len(raw)>8192 or reply.get('applied')is not True:raise ValueError('OriginControlWitness')
   receipt=reply.get('physical_close')
   if action=='fail'and(not receipt or receipt['all_closed']is not True or set(receipt['closed_ids'])!=set(control['expected_connection_ids'])):raise ValueError('ActualOriginSocketCloseMissing')
  return {'path':path,'action':action,'acknowledged_monotonic':time.monotonic(),'physical_close':receipt}
 def probe_all_unavailable(self,paths):
  result=[]
  for path in paths:
   began=time.monotonic()
   try:
    with urllib.request.urlopen(urllib.request.Request(self.origin+path,headers={'X-M0-Fixture-Probe':'outage-control-v1'}),timeout=3)as response:status=response.status;raw=response.read(8193)
   except urllib.error.HTTPError as error:
    status=error.code;raw=error.read(8193);error.close()
   if len(raw)>8192:raise ValueError('OriginDiagnosticReplyCapacity')
   result.append({'path':path,'status':status,'diagnostic_body_bytes':len(raw),'begin':began,'complete':time.monotonic(),'scope':'fixture diagnostic request, not Source application attempt'})
  return result
 def verify_origin_active_zero(self):
  end=time.monotonic()+5
  while True:
   value=self.cb['origin_stats']()
   if value['active']==0:return
   if time.monotonic()>=end:raise RuntimeError('OriginControlSettleNotEstablished')
   time.sleep(.05)
 def event(self,name,at,**fields):self.events.append({'name':name,'monotonic':at,**fields})
 def retain_original_body_outcome(self):
  if len(self.outcomes)>=1024:raise ValueError('OriginalOutcomeRecordCapacity')
  # Retain only terminal changes; no repeat padding or periodic resource sample.
  r=self.old.snapshot()
  if not self.outcomes or r['EOF']!=self.outcomes[-1]['EOF']or r['errors']!=self.outcomes[-1]['errors']:self.outcomes.append(r)
 def freeze_and_physically_close_original_body(self):
  before=self.old.snapshot();closed=self.old.close();(self.output/'original-prefix.ts').write_bytes(self.old.prefix)
  return {'before_controlled_close':before,'physical_close':closed,'terminal_changes':self.outcomes,'independent_of_fresh_request':True}
 def one_fresh_postrestore_request(self):
  self.new=Body(self.base,self.path,self.token);until=time.monotonic()+20
  while time.monotonic()<until:
   if len(self.new.prefix)>=self.new.limit or not self.new.thread.is_alive():break
   time.sleep(.05)
  r=self.new.snapshot();r['capture_observer_deadline_reached']=time.monotonic()>=until and len(self.new.prefix)<self.new.limit and self.new.thread.is_alive();r['physical_close']=self.new.close();(self.output/'fresh-prefix.ts').write_bytes(self.new.prefix);return r
 def close_owned_bodies_and_join(self):
  errors=[]
  for body in[self.old,self.new]:
   if body is not None:
    try:body.close()
    except BaseException as error:errors.append(type(error).__name__)
  if errors:raise RuntimeError('OwnedBodiesUndrained:'+','.join(errors))
 def same_run_retention(self,seconds):return self.cb['retention'](seconds)
 def retain_cleanup_result(self,errors):self.cb['cleanup_result'](errors)
