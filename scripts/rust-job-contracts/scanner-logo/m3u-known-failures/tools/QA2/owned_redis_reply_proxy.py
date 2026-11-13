"""Unexecuted actual RESP2 reply gate; forwards exact bytes, no fake replies.
Authentication arguments never logged. Small disposable fixture only.
"""
import hashlib,socket,socketserver,threading,time
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

class Proxy:
 def __init__(self,mode,parent,child,account,policy,channel_key):
  self.mode=mode;self.parent=parent.encode();self.child=child.encode();self.account=account;self.policy=policy;self.channel_key=channel_key
  self.held=threading.Event();self.release=threading.Event();self.errors=[];self.receipts=[];self.count=0;self.target_replies=0;self.active={};self.handlers=[];self.lock=threading.RLock();self.slots=threading.BoundedSemaphore(4)
  owner=self
  class Handler(socketserver.BaseRequestHandler):
   def handle(self):
    if not owner.slots.acquire(blocking=False):owner.errors.append('RESPConnectionCapacity');return
    remote=None;incoming=None;response=None;identity=threading.get_ident()
    with owner.lock:
     if len(owner.handlers)>=64:owner.errors.append('RESPHistoricalConnectionCapacity');owner.slots.release();return
     owner.handlers.append(threading.current_thread());owner.active[identity]=[self.request]
    try:
     remote=socket.create_connection(('127.0.0.1',6379),timeout=5);remote.settimeout(None)
     with owner.lock:owner.active[identity].append(remote)
     incoming=self.request.makefile('rb');response=remote.makefile('rb')
     while True:
      raw,command=frame(incoming)
      if not isinstance(command,list)or not command:raise ValueError('RESPCommandArray')
      remote.sendall(raw);reply,value=frame(response)
      with owner.lock:owner.count+=1
      verb=command[0].upper();lua=verb in (b'EVAL',b'EVALSHA');key=b'celery-tasks:celery-task-meta-'+(owner.parent if mode=='parent'else owner.child)
      target=False
      if mode=='residual-confirm':target=lua and command[1]==policy['residual_confirm_lua_sha1'].encode()and ('task_lock_refresh_m3u_account_groups_'+str(account)).encode()in command and value==1
      elif mode=='parent':target=lua and key in command and isinstance(value,list)and len(value)==2 and value[0]==1
      elif mode=='claim':target=lua and b'celery'in command and value==1
      elif mode in ('result','result-lost'):target=lua and key in command and value==1
      elif mode=='events':target=lua and owner.channel_key in command and isinstance(value,int)
      if target:
       with owner.lock:owner.target_replies+=1
      if target and not owner.held.is_set():
       with owner.lock:owner.receipts.append({'epoch':time.time(),'verb':verb.decode(),'reply_sha256':hashlib.sha256(reply).hexdigest(),'reply_bytes':len(reply),'actual_server_reply_observed':True,'mode':mode})
       owner.held.set()
       if not owner.release.wait(20):raise TimeoutError('OwnedRESPReply20s')
       if mode=='result-lost':
        self.request.shutdown(socket.SHUT_RDWR);remote.shutdown(socket.SHUT_RDWR);return
      self.request.sendall(reply)
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
  self.server=Server(('127.0.0.1',61603),Handler);self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
 def close(self):
  self.release.set();self.server.shutdown()
  with self.lock:sockets=[s for pair in self.active.values()for s in pair]
  for sock in sockets:
   try:sock.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  self.server.server_close();self.thread.join(2)
  for thread in self.handlers:thread.join(2)
  return {'errors':list(self.errors),'receipts':list(self.receipts),'commands':self.count,'target_replies':self.target_replies,'active_connections':len(self.active),'handler_threads_alive':sum(t.is_alive()for t in self.handlers),'server_thread_alive':self.thread.is_alive(),'physical_close_verified':not self.active and not self.thread.is_alive()and not any(t.is_alive()for t in self.handlers)}
