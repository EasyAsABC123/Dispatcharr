"""Unexecuted actual RESP2 reply gate; forwards exact bytes, no fake replies.
Authentication arguments never logged. Small disposable fixture only.
"""
import base64,hashlib,json,socket,socketserver,threading,time
from owned_nodelay import configure as configure_nodelay
MAX=1048576

def frame(stream,budget=None,depth=0):
 if depth>16:raise ValueError('RESPDepth')
 budget=[MAX]if budget is None else budget
 line=stream.readline(min(MAX,budget[0])+1)
 if not line:raise EOFError
 budget[0]-=len(line)
 if budget[0]<0 or not line.endswith(b'\r\n'):raise ValueError('RESPLineCapacity')
 tag=line[:1];body=line[1:-2]
 if tag==b'*':
  count=int(body)
  if not -1<=count<=256:raise ValueError('RESPArrayCapacity')
  children=[frame(stream,budget,depth+1)for _ in range(max(0,count))]
  return line+b''.join(v[0]for v in children),None if count<0 else[v[1]for v in children]
 if tag==b'$':
  size=int(body)
  if not -1<=size<=budget[0]-2:raise ValueError('RESPBulkCapacity')
  if size<0:return line,None
  data=stream.read(size+2);budget[0]-=len(data)
  if len(data)!=size+2 or not data.endswith(b'\r\n'):raise ValueError('RESPShortBulk')
  return line+data,data[:-2]
 if tag==b':':return line,int(body)
 if tag not in (b'+',b'-'):raise ValueError('RESP2Expected')
 return line,body

def target(mode,task_id,policy,channel_key,command,value,database):
 if type(command)is not list or len(command)<4 or command[0].upper()not in(b'EVAL',b'EVALSHA'):return False
 verb=command[0].upper()
 try:
  script_sha=command[1].decode('ascii')if verb==b'EVALSHA'else hashlib.sha1(command[1]).hexdigest()
  if len(script_sha)!=40 or any(c not in'0123456789abcdef'for c in script_sha):return False
 except (UnicodeDecodeError,AttributeError,TypeError):return False
 if mode=='cache':return database==0 and script_sha==policy['cache_read']['sha1']and command[2]==b'1'and command[3]==policy['cache_group_key'].encode()and type(value)is list and len(value)==2 and type(value[0])is int and ((value[0]==1 and type(value[1])is bytes)or(value[0]==0 and value[1]is None))
 if mode=='events':return database==0 and script_sha==policy['channels_publish']['sha1']and command[2]==b'1'and command[3]==channel_key and type(value)is int and value==0
 if mode in('result','result-lost'):return database==9 and script_sha==policy['result_write']['sha1']and command[2]==b'1'and command[3]==policy['result_key_prefix'].encode()+task_id.encode()and type(value)is int and value==1
 return False

class Proxy:
 def __init__(self,mode,parent,policy,channel_key):
  self.mode=mode;self.parent=parent.encode();self.policy=policy;self.channel_key=channel_key;self.held_at=None
  if mode not in ('cache','events','result','result-lost'):raise ValueError('FiniteConfiguredRESPMode')
  self.held=threading.Event();self.release=threading.Event();self.errors=[];self.receipts=[];self.count=0;self.target_replies=0;self.active={};self.handlers=[];self.lock=threading.RLock();self.slots=threading.BoundedSemaphore(4)
  owner=self
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    if not owner.slots.acquire(blocking=False):owner.errors.append('RESPConnectionCapacity');return
    remote=None;incoming=None;response=None;database=0;identity=threading.get_ident()
    with owner.lock:
     if len(owner.handlers)>=64:owner.errors.append('RESPHistoricalConnectionCapacity');owner.slots.release();return
     owner.handlers.append(threading.current_thread());owner.active[identity]=[self.request]
    try:
     front_nodelay=configure_nodelay(self.request,'redis-downstream')
     remote=socket.create_connection(('127.0.0.1',6379),timeout=5);remote.settimeout(None);back_nodelay=configure_nodelay(remote,'redis-upstream')
     with owner.lock:owner.receipts.append({'stage':'actual-owned-TCP_NODELAY','downstream':front_nodelay,'upstream':back_nodelay})
     with owner.lock:owner.active[identity].append(remote)
     incoming=self.request.makefile('rb');response=remote.makefile('rb')
     while True:
      raw,command=frame(incoming)
      if not isinstance(command,list)or not command:raise ValueError('RESPCommandArray')
      remote.sendall(raw);reply,value=frame(response)
      with owner.lock:owner.count+=1
      verb=command[0].upper();script_sha=None
      if verb==b'SELECT':
       if len(command)!=2 or reply!=b'+OK\r\n':raise ValueError('ActualSELECTDBAcknowledgmentRequired')
       database=int(command[1])
       if not 0<=database<=15:raise ValueError('ActualSELECTDBRange')
      if verb in(b'EVAL',b'EVALSHA'):script_sha=command[1].decode('ascii')if verb==b'EVALSHA'else hashlib.sha1(command[1]).hexdigest()
      selected=target(mode,parent,policy,channel_key,command,value,database)
      if selected:
       with owner.lock:owner.target_replies+=1
      if selected and not owner.held.is_set():
       with owner.lock:owner.held_at=time.monotonic();owner.held.set();owner.receipts.append({'monotonic':owner.held_at,'stage':'actual-target-reply-held','epoch':time.time(),'verb':verb.decode(),'reply_sha256':hashlib.sha256(reply).hexdigest(),'reply_bytes':len(reply),'actual_server_reply_observed':True,'mode':mode,'script_sha1':script_sha,'actual_selected_DB':database})
       owner.held.set()
       if not owner.release.wait(20):raise TimeoutError('OwnedRESPReply20s')
       with owner.lock:owner.receipts.append({'stage':'actual-release-before-reply-forward','epoch':time.time(),'monotonic':time.monotonic(),'mode':mode,'actual_selected_DB':database})
       if mode=='result-lost':
        self.request.shutdown(socket.SHUT_RDWR);remote.shutdown(socket.SHUT_RDWR);return
      self.request.sendall(reply)
      if selected:
       with owner.lock:owner.receipts.append({'stage':'actual-target-reply-forwarded','epoch':time.time(),'monotonic':time.monotonic(),'mode':mode,'script_sha1':script_sha,'reply_sha256':hashlib.sha256(reply).hexdigest(),'reply_bytes':len(reply),'actual_selected_DB':database})
    except (EOFError,ConnectionResetError,BrokenPipeError):pass
    except Exception as error:owner.errors.append(type(error).__name__+':'+str(error)[:256])
    finally:
     for stream in (self.request,remote):
      if stream:
       try:stream.shutdown(socket.SHUT_RDWR)
       except OSError:pass
     for stream in (incoming,response,remote):
      if stream:
       try:stream.close()
       except OSError:pass
     with owner.lock:owner.active.pop(identity,None)
     owner.slots.release()
  class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=False;block_on_close=False
  self.server=Server(('127.0.0.1',62132 if mode=='cache'else 62133 if mode=='events'else 62131),Handler);self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
 def close(self):
  self.release.set();self.server.shutdown()
  with self.lock:sockets=[s for pair in self.active.values()for s in pair]
  for sock in sockets:
   try:sock.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  self.server.server_close();self.thread.join(2)
  for thread in self.handlers:thread.join(2)
  return {'errors':list(self.errors),'receipts':list(self.receipts),'commands':self.count,'target_replies':self.target_replies,'active_connections':len(self.active),'handler_threads_alive':sum(t.is_alive()for t in self.handlers),'server_thread_alive':self.thread.is_alive(),'physical_close_verified':not self.active and not self.thread.is_alive()and not any(t.is_alive()for t in self.handlers)}
