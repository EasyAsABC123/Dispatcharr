#!/usr/bin/env python3
"""Owned native-only relay derived from measured M3U physical proxies.
UNEXECUTED backup callback overlay; real frames only. No app import, fabricated PG response, credential/Bind logging.
"""
import argparse,hashlib,json,select,socket,socketserver,struct,sys,threading,time,traceback
from pathlib import Path
from owned_nodelay import configure as configure_nodelay
CAP=2097152
GATE_SAFETY_SECONDS=45  # fixed before measurement; actual hold remains <=25

def exact(stream,n):
 if type(n)is not int or not 0<=n<=CAP:raise ValueError('FrameCapacity')
 out=bytearray()
 while len(out)<n:
  b=stream.read(n-len(out))
  if not b:raise EOFError
  out.extend(b)
 return bytes(out)
def cstring(body):
 at=body.index(b'\0');return body[:at],body[at+1:]
def bind_parameters(body):
 # Preserve exact wire parameter bytes and NULL, never decode credentials.
 portal,tail=cstring(body);statement,tail=cstring(tail)
 def number():
  nonlocal tail
  if len(tail)<2:raise ValueError('BindShortCount')
  value=struct.unpack('!H',tail[:2])[0];tail=tail[2:];return value
 count=number()
 if count>64 or len(tail)<2*count:raise ValueError('BindFormatCapacity')
 formats=[struct.unpack('!H',tail[i:i+2])[0]for i in range(0,2*count,2)];tail=tail[2*count:]
 count=number()
 if count>64:raise ValueError('BindParameterCapacity')
 values=[]
 for _ in range(count):
  if len(tail)<4:raise ValueError('BindShortLength')
  size=struct.unpack('!i',tail[:4])[0];tail=tail[4:]
  if size==-1:values.append(None);continue
  if not 0<=size<=65536 or len(tail)<size:raise ValueError('BindValueCapacity')
  values.append(tail[:size].hex());tail=tail[size:]
 result_count=number()
 if result_count>64 or len(tail)!=result_count*2:raise ValueError('BindResultFormats')
 if len(formats)not in(0,1,count)or any(f not in(0,1)for f in formats):raise ValueError('BindParameterFormats')
 return portal,statement,values,formats
def backend_eof(stream):
 try:return bool(select.select([stream],[],[],0)[0])and stream.recv(1,socket.MSG_PEEK|socket.MSG_DONTWAIT)==b''
 except BlockingIOError:return False
def error_code(body):return next((f[1:].decode('ascii')for f in body.rstrip(b'\0').split(b'\0')if f[:1]==b'C'),None)
def literal_equal(left,right):
 if type(left)is not type(right):return False
 if type(left)is dict:return left.keys()==right.keys()and all(literal_equal(v,right[k])for k,v in left.items())
 if type(left)is list:return len(left)==len(right)and all(literal_equal(a,b)for a,b in zip(left,right))
 return left==right

def ready_match(want,state,tx):
 if not(type(state['rows'])is int and state['sql']==want['sql']and state['execute']is True and state['rows']==want['rows']and state['commands']==want['commands']and state['errors']==want['errors']and tx==want['tx']and state.get('sync_seen')is True):return False
 values=state.get('parameter_hex')
 if type(values)is not list:return False
 try:
  ident=bytes.fromhex(values[0]);recording=int.from_bytes(ident,'big',signed=True)
  if len(ident)!=8 or recording!=want['recording_id']:return False
  if want['parameter_kind']=='recording-id-only':return len(values)==1
  if want['parameter_kind']!='prime-properties-cas'or len(values)!=3:return False
  # SQLx JSONB binary values begin with the actual PostgreSQL format version1.
  def properties(raw):
   value=bytes.fromhex(raw)
   if not value or value[0]!=1:raise ValueError('ActualBinaryJSONBVersion')
   result=json.loads(value[1:])
   if type(result)is not dict:raise ValueError('ActualObjectProperties')
   return result
  new=properties(values[1]);old=properties(values[2])
  return literal_equal(old,want['expected_old_properties'])and new.get('status')=='recording'and literal_equal(new.get('owned_fixture_marker'),old.get('owned_fixture_marker'))and literal_equal(new.get('program'),old.get('program'))and all(literal_equal(new.get(k),old.get(k))for k in('season','episode','poster_logo_id'))and type(new.get('started_at'))is str
 except(ValueError,TypeError,IndexError):return False

class Proxy:
 def __init__(self,backend,stages,contracts,ledger=None):
  if not 1<=len(stages)<=3 or not set(stages)<=set(contracts):raise ValueError('FiniteStages')
  self.ledger=ledger;self.backend=backend;self.stages=stages;self.contracts=contracts;self.lock=threading.RLock();self.stop=threading.Event();self.release=threading.Event();self.current=None;self.index=0;self.connections={};self.next_id=0;self.events=[];self.errors=[];self.server=None;self.thread=None
  owner=self
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):owner.handle(self.request)
  class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=False;block_on_close=True
  self.server=Server(('127.0.0.1',62130),Handler);self.port=self.server.server_address[1];self.thread=threading.Thread(target=self.server.serve_forever,name='pg-listener');self.thread.start()
 def event(self,kind,**fields):
  with self.lock:
   if len(self.events)>=2000:raise ValueError('EventCapacity')
   row={'kind':kind,'epoch':time.time(),'monotonic':time.monotonic(),**fields};self.events.append(row)
   if self.ledger is not None:
    raw=json.dumps(row,allow_nan=False)+'\n'
    if self.ledger.tell()+len(raw.encode())>8388608:raise ValueError('Ledger8MiBCapacity')
    self.ledger.write(raw);self.ledger.flush()
 def fail(self,error):
  with self.lock:
   if len(self.errors)<64:
    def literal(raw):return {'full_byte_count':len(raw),'full_sha256':hashlib.sha256(raw).hexdigest(),'retained_hex':raw[:131072].hex(),'complete':len(raw)<=131072,'truncated':len(raw)>131072}
    row={'type':type(error).__name__,'message':literal(str(error).encode()),'traceback':literal(traceback.format_exc().encode())};self.errors.append(row);self.event('relay-exception',error=row)
 def handle(self,front):
  back=None;up=None;f=None;response=None
  with self.lock:
   ident=self.next_id;self.next_id+=1
   if ident>=16:raise ValueError('OwnedConnectionBound16')
   row={'id':ident,'backend_pid':None,'front_eof':False,'back_eof':False,'closed':False,'up_joined':False,'queries':[]};self.connections[ident]=(row,front,None)
  try:
   front_nodelay=configure_nodelay(front,'native-downstream');self.event('actual-owned-TCP_NODELAY',connection=ident,side='downstream',receipt=front_nodelay)
   f=front.makefile('rb')
   while True:
    header=exact(f,4);n=struct.unpack('!I',header)[0];body=exact(f,n-4)
    if n==8 and body==struct.pack('!I',80877103):front.sendall(b'N');continue
    if body[:4]in(struct.pack('!I',80877102),struct.pack('!I',80877104)):raise ValueError('NoCancelOrGSSStartup')
    break
   back=socket.create_connection(self.backend,timeout=5);back.settimeout(None);back_nodelay=configure_nodelay(back,'postgres-upstream');self.event('actual-owned-TCP_NODELAY',connection=ident,side='upstream',receipt=back_nodelay)
   with self.lock:self.connections[ident]=(row,front,back)
   back.sendall(header+body)
   state={'sql':None,'execute':False,'rows':0,'commands':[],'errors':[]};statements={};portals={}
   def begin(sql,parameters=None):
    if len(sql)>65536:raise ValueError('QueryCapacity')
    with self.lock:
     if len(row['queries'])>=300:raise ValueError('QueryCount')
     row['queries'].append(sql);state.update(sql=sql,execute=True,rows=0,commands=[],errors=[],parameter_hex=parameters,sync_seen=False)
    self.event('execute',connection=ident,sql=sql,parameter_hex=parameters if sql in {w['sql']for w in self.contracts.values()} else None)
   def upstream():
    try:
     while not self.stop.is_set():
      kind=exact(f,1);head=exact(f,4);body=exact(f,struct.unpack('!I',head)[0]-4)
      with self.lock:
       if kind==b'P':
        name,tail=cstring(body);query,_=cstring(tail);statements[name]=query.decode('utf-8')
       elif kind==b'B':
        portal,statement,parameters,formats=bind_parameters(body);portals[portal]=(statement,parameters)
        if statements[statement]in{w['sql']for w in self.contracts.values()}:self.event('target-bind',connection=ident,statement_hex=statement.hex(),parameter_hex=parameters,formats=formats)
       elif kind==b'E':
        portal,_=cstring(body);statement,parameters=portals[portal];begin(statements[statement],parameters)
       elif kind==b'S':state['sync_seen']=True;self.event('sync-read',connection=ident,sql=state['sql'])
       elif kind==b'Q':begin(body.rstrip(b'\0').decode('utf-8').strip().rstrip(';'),[]);state['sync_seen']=True
      back.sendall(kind+head+body)
    except (EOFError,ConnectionResetError,BrokenPipeError,OSError):pass
    except Exception as error:self.fail(error)
    finally:
     with self.lock:row['front_eof']=True
     self.event('front-eof',connection=ident)
     try:back.shutdown(socket.SHUT_WR)
     except OSError:pass
   up=threading.Thread(target=upstream,name='pg-up-'+str(ident));up.start();response=back.makefile('rb')
   while not self.stop.is_set():
    kind=exact(response,1);head=exact(response,4);body=exact(response,struct.unpack('!I',head)[0]-4)
    hold=None
    with self.lock:
     if kind==b'K':row['backend_pid']=struct.unpack('!I',body[:4])[0];self.event('backend-key',connection=ident,backend_pid=row['backend_pid'])
     elif kind==b'D':state['rows']+=1
     elif kind==b'C':state['commands'].append(body.rstrip(b'\0').decode('ascii'))
     elif kind==b'E':state['errors'].append(error_code(body))
     if state['sql']in{w['sql']for w in self.contracts.values()}and kind in(b'D',b'C',b'E',b'Z'):
      self.event('actual-target-backend-frame',connection=ident,backend_pid=row['backend_pid'],sql=state['sql'],kind_hex=kind.hex(),header_hex=head.hex(),body_hex=body.hex())
     if kind==b'Z':
      tx=body.decode('ascii');self.event('ready-observed',connection=ident,backend_pid=row['backend_pid'],sql=state['sql'],rows=state['rows'],commands=list(state['commands']),errors=list(state['errors']),tx=tx)
      if self.index<len(self.stages)and self.current is None and ready_match(self.contracts[self.stages[self.index]],state,tx):
       hold={'stage':self.stages[self.index],'ordinal':self.index,'connection':ident,'backend_pid':row['backend_pid'],'sql':state['sql'],'rows':state['rows'],'commands':list(state['commands']),'errors':list(state['errors']),'tx':tx,'parameter_hex':state['parameter_hex'],'sync_seen':state['sync_seen'],'epoch':time.time()};self.current=hold;self.release.clear();self.event('ready-held',**hold)
    if hold is not None:
     deadline=time.monotonic()+GATE_SAFETY_SECONDS
     while not self.release.wait(.01):
      if time.monotonic()>deadline:raise TimeoutError('OwnedReadyGateSafety45')
      if backend_eof(back):
       self.event('ready-discarded-after-actual-backend-EOF',stage=hold['stage'],connection=ident);return
    front.sendall(kind+head+body)
    if hold is not None:self.event('ready-forwarded',stage=hold['stage'],ordinal=hold['ordinal'],connection=ident)
  except (EOFError,ConnectionResetError,BrokenPipeError,OSError):pass
  except Exception as error:self.fail(error)
  finally:
   with self.lock:row['back_eof']=True
   self.event('back-eof',connection=ident)
   for s in(front,back):
    if s is not None:
     try:s.shutdown(socket.SHUT_RDWR)
     except OSError:pass
     s.close()
   if up is not None:up.join(timeout=3)
   for wrapper in (f,response):
    if wrapper is not None:wrapper.close()
   with self.lock:row.update(closed=True,up_joined=up is None or not up.is_alive())
   self.event('connection-closed',connection=ident,up_joined=row['up_joined'])
 def stats(self):
  with self.lock:return {'port':self.port,'held':self.current,'next_stage':self.index,'connections':[dict(row)for row,_,_ in self.connections.values()],'events':list(self.events),'errors':list(self.errors),'listener_alive':self.thread.is_alive()}
 def unlock(self,ordinal):
  with self.lock:
   if self.current is None or self.current['ordinal']!=ordinal:raise ValueError('ExactHeldOrdinal')
   self.index+=1;self.current=None;self.release.set()
 def close(self):
  self.stop.set();self.release.set();self.server.shutdown()
  with self.lock:peers=[s for _,front,back in self.connections.values()for s in(front,back)if s is not None]
  for s in peers:
   try:s.shutdown(socket.SHUT_RDWR)
   except OSError:pass
   s.close()
  self.server.server_close();self.thread.join(timeout=3)
  return self.stats()
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--backend-port',type=int,required=True);parser.add_argument('--contracts',type=Path,required=True);parser.add_argument('--stages',required=True);parser.add_argument('--evidence',type=Path,required=True);a=parser.parse_args()
 if not 1<=a.backend_port<=65535:raise ValueError('BackendPort')
 ledger=a.evidence.open('x');proxy=Proxy(('127.0.0.1',a.backend_port),json.loads(a.stages),json.loads(a.contracts.read_text()),ledger);closed=False
 print(json.dumps({'ready':True,'port':proxy.port}),flush=True)
 try:
  while True:
   line=sys.stdin.buffer.readline(65537)
   if not line:break
   if len(line)>65536 or not line.endswith(b'\n'):raise ValueError('ControlCapacity')
   command=json.loads(line);action=command['action']
   if action=='release':proxy.unlock(command['ordinal'])
   elif action=='stop':result=proxy.close();closed=True
   elif action!='stats':raise ValueError('KnownControl')
   if action!='stop':result=proxy.stats()
   raw=json.dumps({'id':command['id'],'result':result},allow_nan=False)
   if len(raw.encode())>4194304:raise ValueError('ReplyCapacity')
   print(raw,flush=True)
   if closed:break
 finally:
  if not closed:proxy.close()
  ledger.close()
if __name__=='__main__':main()
