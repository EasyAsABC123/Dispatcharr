"""Owned response reader: bounded retention, continued drain, actual close ack."""
import hashlib,os,threading,time
from pathlib import Path
CAP=1048576
class Reader:
 def __init__(self,response,path,role,cap=CAP,wire_cap=8388608,deadline=40,initial=b''):
  if type(cap)is not int or not 1<=cap<=CAP or type(wire_cap)is not int or not cap<=wire_cap<=8388608:raise ValueError('ReaderByteBounds')
  if type(initial)is not bytes or len(initial)>8192 or len(initial)>cap:raise ValueError('InitialPrefixBound')
  self.initial=initial;self.first=threading.Event()
  self.response=response;self.path=Path(path);self.role=role;self.cap=cap;self.wire_cap=wire_cap;self.deadline=time.monotonic()+deadline;self.lock=threading.Lock();self.retained=0;self.received=0;self.invalid=None;self.eof=False;self.stop=threading.Event();self.prefix=threading.Event();self.done=threading.Event();self.digest=hashlib.sha256();self.sock=socket_of(response);self.fd=self.sock.fileno()if self.sock is not None else-1;self.inode=os.fstat(self.fd).st_ino if self.fd>=0 else None;self.closed=False
  if self.sock is None or self.fd<0 or self.inode is None:raise ValueError('ReaderPhysicalSocketRequired')
  self.thread=threading.Thread(target=self._pump);self.thread.start()
 def _pump(self):
  try:
   with self.path.open('xb')as stream:
    if self.initial:
     stream.write(self.initial);stream.flush();self.digest.update(self.initial);self.retained=len(self.initial);self.received=len(self.initial)
     if self.retained>=8192:self.first.set()
     if self.retained==self.cap:self.prefix.set()
    while not self.stop.is_set():
     data=self.response.raw.read(8192)
     if not data:
      with self.lock:self.eof=True
      break
     if type(data)is not bytes or len(data)>8192:raise ValueError('ReaderFrameBounds')
     with self.lock:
      self.received+=len(data)
      if self.received>self.wire_cap:raise ValueError('ReaderWireCapacity')
      keep=data[:max(0,self.cap-self.retained)]
      if keep:
       if stream.write(keep)!=len(keep):raise OSError('ShortRetentionWrite')
       self.digest.update(keep);self.retained+=len(keep)
      if self.retained>=8192 and not self.first.is_set():stream.flush();self.first.set()
      if self.retained==self.cap:stream.flush();self.prefix.set()
      if not self.prefix.is_set()and time.monotonic()>=self.deadline:raise TimeoutError('ReaderPrefixDeadline')
     # Deliberately keep reading after retention cap. No sleep/pause/close here.
  except Exception as error:
   with self.lock:
    if not self.stop.is_set():self.invalid=type(error).__name__+':'+str(error)[:96]
  finally:self.done.set()
 def await_initial(self,seconds=8):
  if not self.first.wait(seconds):raise RuntimeError(self.invalid or'InitialBodyObservationDeadline')
  with self.path.open('rb')as stream:value=stream.read(8192)
  if len(value)!=8192:raise RuntimeError('InitialRetainedPrefixSize')
  return value
 def await_prefix(self):
  while not self.prefix.wait(.02):
   if self.done.is_set()or time.monotonic()>=self.deadline:raise RuntimeError(self.invalid or'ReaderPrefixIncomplete')
  if self.invalid:raise RuntimeError(self.invalid)
  return self.snapshot()
 def snapshot(self):
  with self.lock:return{'role':self.role,'retained':self.retained,'received':self.received,'cap':self.cap,'wire_cap':self.wire_cap,'prefix_ready':self.prefix.is_set(),'eof':self.eof,'invalid':self.invalid,'reader_done':self.done.is_set(),'retention_does_not_close_or_pause':True,'monotonic':time.monotonic()}
 def close(self):
  if self.closed:raise ValueError('SingleOwnedCloseOnly')
  began=time.monotonic();self.stop.set()
  if self.sock is not None:
   try:self.sock.shutdown(2)
   except OSError:pass
  self.thread.join(10)
  self.response.close()
  if self.sock is not None:self.sock.close()
  if self.thread.is_alive():raise RuntimeError('ReaderPhysicalDrainIncomplete')
  absent=descriptor_absent(self.fd,self.inode);after=self.sock.fileno()if self.sock is not None else None;self.closed=True
  row={'role':self.role,'close_begin':began,'close_complete':time.monotonic(),'fd_after':after,'original_inode_absent':absent,'server_cancellation_inferred':False,'reader_joined':True,'snapshot':self.snapshot()}
  if type(after)is not int or after!=-1 or not absent:raise RuntimeError('ReaderSocketOwnershipIncomplete')
  return row

def socket_of(response):
 direct=getattr(response,'_fixture_socket',None)
 if direct is not None:return direct
 original=getattr(response.raw,'_original_response',None);fp=getattr(original,'fp',None);raw=getattr(fp,'raw',None)
 return getattr(raw,'_sock',None)or getattr(getattr(response.raw,'_connection',None),'sock',None)
def descriptor_absent(fd,inode):
 if type(fd)is not int or fd<0 or type(inode)is not int:return False
 try:return os.fstat(fd).st_ino!=inode
 except OSError:return True

def close_response(response,role,ownership):
 sock,fd,inode=ownership;began=time.monotonic()
 if sock is None or type(fd)is not int or type(inode)is not int:raise ValueError('KnownOriginalSocketRequired')
 try:sock.shutdown(2)
 except OSError:pass
 response.close();sock.close();row={'role':role,'close_begin':began,'close_complete':time.monotonic(),'fd_after':sock.fileno(),'original_inode_absent':descriptor_absent(fd,inode),'server_cancellation_inferred':False}
 if row['fd_after']!=-1 or not row['original_inode_absent']:raise RuntimeError('PhysicalCloseNotAcknowledged')
 return row

def ownership(response):
 sock=socket_of(response)
 if sock is None or sock.fileno()<0:raise RuntimeError('ResponseSocketUnknown')
 return sock,sock.fileno(),os.fstat(sock.fileno()).st_ino
