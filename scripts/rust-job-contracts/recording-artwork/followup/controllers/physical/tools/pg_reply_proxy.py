"""Unexecuted bounded real-PG reply gates; no database emulation.
Same disposable loopback scope as qualified M3U proxies. TLS-decline is an
explicit fixture transport control; actual database payloads are forwarded.
"""
import base64,hashlib,json,socket,socketserver,struct,threading,time
from pathlib import Path
POLICY=json.loads(Path(__file__).with_name("sql-policy.json").read_text())

def exact(stream,n):
 if not 0<=n<=2097152:raise ValueError('PGFrameCapacity2MiB')
 data=stream.read(n)
 if len(data)!=n:raise EOFError
 return data
def target_query(mode,query,transaction):
 name={'recording-read-Ready':'recording_read','movie-read-Ready':'movie_read','movie-execution-Ready':'movie_read','update-Ready':'update','update-error-Ready':'update'}.get(mode)
 return name is not None and query.strip()==POLICY[name].encode()
def ready_for_hold(mode,state):
 if not state['target']:return False
 if mode in ('recording-read-Ready','movie-read-Ready','movie-execution-Ready'):
  return state['rows']==1 and state['commands']==[b'SELECT 1\0'] and not state['error']
 if mode=='update-Ready':return state['rows']==0 and state['commands']==[b'UPDATE 1\0'] and not state['error']
 if mode=='update-error-Ready':return state['error']and state.get('error_fields',{}).get('C')==POLICY['known_owned_trigger_error_code']and state.get('error_fields',{}).get('M')==POLICY['known_owned_trigger_error_message']and state['commands']==[]
 return False
def projected_capture(direction,kind,payload):
 item={'kind_hex':kind.hex(),'payload_bytes':len(payload),'payload_sha256':hashlib.sha256(payload).hexdigest()}
 if(direction=='frontend'and kind==b'p')or(direction=='backend'and kind==b'R'):
  return dict(item,payload_base64=None,authentication_payload_projected=True)
 if direction=='backend'and kind==b'K':
  if len(payload)!=8:raise ValueError('ActualBackendKeyDataLength8')
  return dict(item,payload_base64=None,backend_pid=struct.unpack('!I',payload[:4])[0],cancel_secret_projected=True)
 return dict(item,payload_base64=base64.b64encode(payload).decode())
def error_fields(payload):
 if type(payload)is not bytes or len(payload)>65536:raise ValueError('BoundedActualPGErrorPayload')
 fields={};position=0
 while position<len(payload):
  if payload[position:position+1]==b'\0':
   if position!=len(payload)-1:raise ValueError('PGErrorTrailingBytes')
   return fields
  key=payload[position:position+1].decode('ascii');end=payload.find(b'\0',position+1)
  if end<0 or key in fields:raise ValueError('PGErrorTruncatedOrDuplicateField')
  fields[key]=payload[position+1:end].decode('utf-8');position=end+1
 raise ValueError('PGErrorMissingTerminator')
class Exchanges:
 """Bounded frontend/backend correspondence, not a PostgreSQL emulator.
 Parse/Describe Sync has no execution. A following Execute and an error's
 queued ping have distinct response groups; matching Ready never uses latest SQL.
 """
 def __init__(self,mode,expected_rows=1):
  self.mode=mode;self.expected_rows=expected_rows;self.statements={};self.portals={};self.groups=[];self.building=None;self.transaction=set();self.serial=0
 def group(self):
  if self.building is None:
   if len(self.groups)>=128:raise ValueError('PGPendingExchangeCapacity128')
   self.serial+=1;self.building={'exchange':self.serial,'target':False,'rows':0,'commands':[],'error':False,'identity':None,'executions':[],'synced':False,'expected_rows':self.expected_rows};self.groups.append(self.building)
  return self.building
 def execute(self,query):
  group=self.group()
  if len(group['executions'])>=32:raise ValueError('PGExchangeExecutionCapacity32')
  group['executions'].append(query)
  q=query.strip().upper()
  if q==b'BEGIN':self.transaction.clear()
  if q.startswith(b'INSERT INTO DJANGO_CELERY_BEAT_CLOCKEDSCHEDULE '):self.transaction.add('Clocked')
  if q.startswith(b'INSERT INTO DJANGO_CELERY_BEAT_PERIODICTASK '):self.transaction.add('Periodic')
  group.update(target=target_query(self.mode,query,self.transaction),identity=query.decode('utf-8'))
 def frontend(self,kind,payload):
  query=None
  if kind==b'P':
   name,query=payload.split(b'\0',2)[:2]
   if name not in self.statements and len(self.statements)>=64:raise ValueError('PGStatementIdentityCapacity64')
   self.statements[name]=query;self.group()
  elif kind==b'B':
   portal,statement=payload.split(b'\0',2)[:2]
   if portal not in self.portals and len(self.portals)>=64:raise ValueError('PGPortalIdentityCapacity64')
   if statement not in self.statements:raise ValueError('PGUnknownBoundStatement')
   self.portals[portal]=self.statements[statement];self.group()
  elif kind==b'E':
   portal=payload.split(b'\0',1)[0]
   if portal not in self.portals:raise ValueError('PGUnknownExecutedPortal')
   query=self.portals[portal];self.execute(query)
  elif kind==b'S':
   self.group()['synced']=True;self.building=None
  elif kind==b'Q':
   if self.building is not None:raise ValueError('PGSimpleQueryInsideUnfinishedExchange')
   query=payload[:-1];self.execute(query);self.building['synced']=True;self.building=None
  return query
 def backend(self,kind,payload):
  if not self.groups:return None
  group=self.groups[0]
  if kind==b'D':group['rows']+=1
  if kind==b'C':group['commands'].append(payload)
  if kind==b'E':group['error']=True;group['error_fields']=error_fields(payload)
  if kind==b'Z'and group['synced']and len(group['executions'])==1 and ready_for_hold(self.mode,group):return dict(group)
  return None
 def forwarded_ready(self):
  if self.groups:self.groups.pop(0)
class Proxy:
 def __init__(self,mode,port=61651,expected_rows=1):
  if port!=61651:raise ValueError('FixedOwnedPort')
  if type(expected_rows)is not int or not 1<=expected_rows<=32:raise ValueError('ExpectedBoundedDataRows')
  if mode not in ('recording-read-Ready','movie-read-Ready','movie-execution-Ready','update-Ready','update-error-Ready'):raise ValueError('FiniteConfiguredPGMode')
  self.mode=mode;self.held=threading.Event();self.release=threading.Event();self.before_insert=threading.Event();self.insert_release=threading.Event();self.timeline=[];self.handlers=[];self.upstream_threads=[];self.slots=threading.BoundedSemaphore(4)
  self.lock=threading.Lock();self.frames=[];self.used=0;self.errors=[];self.active=set();self.sockets=set();self.serial=0;self.closed=False;self.subject_complete_count=0;self.held_at=None
  owner=self
  def capture(ident,direction,kind,payload):
   with owner.lock:
    if owner.used+len(payload)>4194304:raise ValueError('PGTranscriptCapacity4MiB')
    owner.used+=len(payload);owner.frames.append(dict(projected_capture(direction,kind,payload),connection=ident,direction=direction,epoch=time.time(),monotonic=time.monotonic()))
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    if not owner.slots.acquire(blocking=False):owner.errors.append({'error':'PGConnectionCapacity4'});return
    remote=None;up=None;backend_pid=None;exchanges=Exchanges(mode,expected_rows)
    with owner.lock:
     if len(owner.handlers)>=64:owner.errors.append({'error':'PGHistoricalConnectionCapacity64'});owner.slots.release();return
     owner.handlers.append(threading.current_thread())
    with owner.lock:owner.serial+=1;ident=owner.serial;owner.active.add(ident);owner.sockets.add(self.request)
    try:
     stream=self.request.makefile('rb')
     while True:
      head=exact(stream,4);size=struct.unpack('!I',head)[0];payload=exact(stream,size-4)
      if size==8 and payload==struct.pack('!I',80877103):
       capture(ident,'fixture-TLS-decline',b'',payload);self.request.sendall(b'N');continue
      break
     remote=socket.create_connection(('127.0.0.1',5432),timeout=5);remote.settimeout(None)
     with owner.lock:owner.sockets.add(remote)
     capture(ident,'frontend-startup',b'',payload);remote.sendall(head+payload)
     def upstream():
      try:
       while True:
        kind=exact(stream,1);header=exact(stream,4);payload=exact(stream,struct.unpack('!I',header)[0]-4);capture(ident,'frontend',kind,payload)
        with owner.lock:query=exchanges.frontend(kind,payload)
        # Install the real owned trigger before the first target UPDATE Parse reaches
        # PostgreSQL; doing it after Bind can deadlock on table locks.
        if query is not None and kind==b'P'and mode=='update-error-Ready'and target_query(mode,query,set())and not owner.before_insert.is_set():
         with owner.lock:owner.timeline.append({'stage':'actual-UPDATE-Parse-before-forward-after-schema-check','epoch':time.time(),'query':query.decode('utf-8')})
         owner.before_insert.set()
         if not owner.insert_release.wait(20):raise TimeoutError('OwnedUPDATEInjection20s')
        remote.sendall(kind+header+payload)
      except (EOFError,OSError):pass
      except BaseException as error:
       with owner.lock:owner.errors.append({'connection':ident,'direction':'frontend','error':type(error).__name__+':'+str(error)[:256]})
      finally:
       try:remote.shutdown(socket.SHUT_WR)
       except OSError:pass
     up=threading.Thread(target=upstream);owner.upstream_threads.append(up);up.start();back=remote.makefile('rb')
     while True:
      kind=exact(back,1);header=exact(back,4);payload=exact(back,struct.unpack('!I',header)[0]-4);capture(ident,'backend',kind,payload)
      if kind==b'K':backend_pid=projected_capture('backend',kind,payload)['backend_pid']
      with owner.lock:
       matching=exchanges.backend(kind,payload)
       if matching is not None:owner.subject_complete_count+=1
       occurrence_ok=mode!='movie-execution-Ready'or owner.subject_complete_count==2
       hold=matching is not None and occurrence_ok and backend_pid is not None and backend_pid>0 and not owner.held.is_set()
       if hold:owner.held_at=time.monotonic();owner.held.set();owner.timeline.append({'monotonic':owner.held_at,'stage':'actual-matching-Ready-held','epoch':time.time(),'connection':ident,'backend_pid':backend_pid,'exchange':matching['exchange'],'identity':matching['identity'],'subject_complete_occurrence':owner.subject_complete_count,'matching_rows':matching['rows'],'matching_commands_hex':[x.hex()for x in matching['commands']],'actual_error':matching['error'],'executions':len(matching['executions']),'frontend_Sync_or_simpleQuery_observed':matching['synced']})
      if hold:
       owner.held.set()
       if not owner.release.wait(20):raise TimeoutError('OwnedPGReady20s')
       with owner.lock:owner.timeline.append({'stage':'actual-release-observed-before-Ready-forward','connection':ident,'backend_pid':backend_pid,'epoch':time.time(),'monotonic':time.monotonic()})
      self.request.sendall(kind+header+payload)
      if kind==b'Z':
       with owner.lock:exchanges.forwarded_ready()
    except (EOFError,OSError):pass
    except BaseException as error:
     with owner.lock:owner.errors.append({'connection':ident,'direction':'backend','error':type(error).__name__+':'+str(error)[:256]})
    finally:
     for sock in [self.request,remote]:
      if sock:
       try:sock.shutdown(socket.SHUT_RDWR)
       except OSError:pass
       sock.close()
     if up:up.join(10)
     with owner.lock:
      if up and up.is_alive():owner.errors.append({'connection':ident,'error':'upstream-unsettled'})
      owner.active.discard(ident);owner.sockets.discard(self.request);owner.sockets.discard(remote);owner.slots.release()
  class Server(socketserver.ThreadingMixIn,socketserver.TCPServer):allow_reuse_address=True;daemon_threads=False;block_on_close=False
  self.server=Server(('127.0.0.1',port),Handler);self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
 def close(self):
  if self.closed:raise ValueError('ObserverClosedTwice')
  self.release.set();self.insert_release.set();self.closed=True;self.server.shutdown();self.server.server_close();self.thread.join(10)
  with self.lock:sockets=list(self.sockets)
  for sock in sockets:
   try:sock.shutdown(socket.SHUT_RDWR)
   except OSError:pass
   sock.close()
  deadline=time.monotonic()+10
  while time.monotonic()<deadline:
   with self.lock:active=list(self.active)
   if not active:break
   time.sleep(.01)
  for thread in self.handlers+self.upstream_threads:thread.join(max(0,deadline-time.monotonic()))
  with self.lock:
   alive=sum(thread.is_alive()for thread in self.handlers+self.upstream_threads)
   return {'handler_or_upstream_threads_alive':alive,'authentication_bytes_retained':False,'CancelKey_secret_retained':False,'mode':self.mode,'timeline':self.timeline,'frames':self.frames,'captured_payload_bytes':self.used,'errors':self.errors,'active_connections':list(self.active),'observer_thread_settled':not self.thread.is_alive(),'physical_close_verified':not self.active and not self.thread.is_alive()and alive==0}
