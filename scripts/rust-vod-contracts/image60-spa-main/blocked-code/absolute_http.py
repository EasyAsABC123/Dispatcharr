"""Fixture-only HTTP with an absolute first-observation clock and owned socket.
Resolve fixed fixture host BEFORE scheduling. No retries, redirects or background
request worker: the calling thread owns I/O; one bounded timer shuts its socket.
"""
import http.client, socket, threading, time
from urllib.parse import urlsplit

class Observation:
 def __init__(self,base,seconds=24,address=None,io_seconds=8):
  url=urlsplit(base)
  if url.scheme!='http' or url.hostname not in ('127.0.0.1','candidate') or url.path not in ('','/') or url.username or url.password:raise ValueError('OwnedHTTPBase')
  if not 0<seconds<=24:raise ValueError('ObservationDeadlineBounds')
  if type(io_seconds)not in(int,float)or not 0<io_seconds<=24:raise ValueError('ObservationIOBounds')
  self.io_seconds=io_seconds
  self.host=url.hostname;self.port=url.port or 80
  # Production callers pass their pre-resolved owned fixture IP. Local controls
  # use numeric loopback. Never perform DNS inside the absolute observation.
  self.address=address or self.host
  socket.inet_pton(socket.AF_INET,self.address)
  self.socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM);self.fd=self.socket.fileno()
  import os
  self.inode=os.fstat(self.fd).st_ino
  self.deadline=time.monotonic()+seconds;self.expired=False;self.event=threading.Event();self.lock=threading.Lock();self.finished=False;self.response=None
  self.timer=threading.Thread(target=self._clock);self.timer.start()
 def _clock(self):
  if not self.event.wait(max(0,self.deadline-time.monotonic())):
   with self.lock:
    if self.finished or self.event.is_set():return
    self.expired=True
    try:self.socket.shutdown(socket.SHUT_RDWR)
    except OSError:pass
    self.socket.close()
 def _io_timeout(self):
  return max(.001,min(self.io_seconds,self.deadline-time.monotonic()))
 def request(self,path,headers,method='GET',body=None):
  try:
   self.socket.settimeout(self._io_timeout())
   self.socket.connect((self.address,self.port))
   owner=self
   class Connection(http.client.HTTPConnection):
    def connect(self):self.sock=owner.socket
   self.connection=Connection(self.host,self.port,timeout=self._io_timeout())
   class HeaderBudget:
    def __init__(self,stream):self.stream=stream;self.used=0;self.active=True
    def readline(self,limit=-1):
     if not self.active:return self.stream.readline(limit)
     value=self.stream.readline(min(65537-self.used,limit)if limit>=0 else 65537-self.used);self.used+=len(value)
     if self.used>65536:raise ValueError('ResponseHeaderCapacity')
     return value
    def __getattr__(self,key):return getattr(self.stream,key)
   class BoundedResponse(http.client.HTTPResponse):
    def begin(self):
     self.fp=HeaderBudget(self.fp)
     super().begin()
     if self.fp is not None:self.fp.active=False
   self.connection.response_class=BoundedResponse
   self.connection.request(method,path,body=body,headers={'Accept':'*/*',**headers})
   raw=self.connection.getresponse()
   if sum(len(k)+len(v)for k,v in raw.getheaders())>65536:raise ValueError('ResponseHeaderCapacity')
   self.response=Response(self,raw);return self.response
  except BaseException:
   receipt=self.cancel()
   error=__import__('sys').exc_info()[1];error.physical_close_receipt=receipt
   raise
 def complete(self):
  with self.lock:
   if self.expired or time.monotonic()>=self.deadline:
    self.expired=True
   else:self.finished=True
   self.event.set()
  self.timer.join(2)
  if self.timer.is_alive():raise RuntimeError('ObservationTimerNotJoined')
  if self.expired:
   self.cancel();raise TimeoutError('AbsoluteFirstObservation24Seconds')
  return {'deadline':self.deadline,'completed':time.monotonic(),'timer_joined':True}
 def interrupt(self):
  # Signaling is not a physical-close acknowledgment: a reader-owned makefile
  # can retain the same descriptor until its thread unwinds and is joined.
  self.event.set()
  try:self.socket.shutdown(socket.SHUT_RDWR)
  except OSError:pass
  self.socket.close()
  return {'interrupt_requested':True,'physical_close_claimed':False}
 def cancel(self):
  self.interrupt()
  if hasattr(self,'connection'):self.connection.close()
  if self.response is not None:self.response.raw.close()
  if threading.current_thread()is not self.timer:self.timer.join(2)
  from reader import descriptor_absent
  if self.timer.is_alive()or self.socket.fileno()!=-1 or not descriptor_absent(self.fd,self.inode):raise RuntimeError('ObservationPhysicalCloseIncomplete')
  return {'fd_after':-1,'original_inode_absent':True,'timer_joined':True,'request_worker':'calling thread; returned after actual I/O unwind'}
class Response:
 def __init__(self,owner,raw):self.owner=owner;self.raw=raw;self.status_code=raw.status;self.headers=dict(raw.getheaders());self._fixture_socket=owner.socket
 def close(self):
  # The same owner is transferred to Reader after complete(), never pooled.
  self.owner.cancel()
