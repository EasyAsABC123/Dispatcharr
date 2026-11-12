"""Single owned read-only INFO connection; no connection on import/construction.
No reconnect or counter normalization. Caller owns close in independent cleanup.
Only endpoint-validated loopback fixture sockets may be opened after Root review.
"""
import socket,time
from urllib.parse import urlsplit
from metrics import redis_info,redis_delta
EXECUTION_BLOCKED=True
class RedisInfoSession:
 def __init__(self,host,port,password,connector=socket.create_connection):
  if host not in {'127.0.0.1','localhost'}or type(port)is not int or not 0<port<65536:raise ValueError('OwnedRedisEndpointRequired')
  if not isinstance(password,bytes)or not 0<len(password)<=256:raise ValueError('FixtureCredentialBound')
  self.endpoint=(host,port);self.password=password;self.connector=connector;self.socket=None;self.pending=bytearray();self.failed=False;self.closed=False;self.sent=0;self.received=0;self.commands=0;self.infos=0;self.previous=None;self.records=[]
 def _read(self,count):
  while len(self.pending)<count:
   remaining=self.command_deadline-time.monotonic()
   if remaining<=0:raise TimeoutError('RedisObserverCommandDeadline')
   self.socket.settimeout(remaining);raw=self.socket.recv(min(4096,count-len(self.pending)))
   if not raw:raise ConnectionError('RedisInfoUnexpectedEOF')
   self.received+=len(raw);self.pending.extend(raw)
  value=bytes(self.pending[:count]);del self.pending[:count];return value
 def _line(self):
  value=bytearray()
  while not value.endswith(b'\r\n'):
   value.extend(self._read(1))
   if len(value)>128:raise ValueError('RedisInfoProtocolLineCapacity')
  return bytes(value[:-2])
 def _command(self,parts,deadline=None):
  if self.failed or self.closed:raise RuntimeError('RedisInfoSessionUnavailable')
  encoded=b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(p)).encode()+b'\r\n'+p+b'\r\n'for p in parts)
  if len(encoded)>512:raise ValueError('ObserverCommandCapacity')
  try:
   self.command_deadline=min(time.monotonic()+3,deadline)if deadline is not None else time.monotonic()+3
   remaining=self.command_deadline-time.monotonic()
   if remaining<=0:raise TimeoutError('RedisObserverCommandDeadline')
   self.socket.settimeout(remaining);self.commands+=1;self.socket.sendall(encoded);self.sent+=len(encoded)
   prefix=self._read(1);line=self._line()
   if prefix==b'+':return line
   if prefix!=b'$'or not line.isdigit()or int(line)>65536:raise ValueError('RedisInfoProtocolOrReplyCapacity')
   raw=self._read(int(line)+2)
   if raw[-2:]!=b'\r\n':raise ValueError('RedisInfoBulkTerminator')
   return raw[:-2]
  except BaseException:
   self.failed=True;raise
 def open(self):
  if EXECUTION_BLOCKED:raise RuntimeError('SupplementalRedisExecutionBlockedPendingRootReview')
  if self.socket is not None or self.closed:raise RuntimeError('RedisInfoSessionAlreadyUsed')
  try:
   self.socket=self.connector(self.endpoint,timeout=3);self.socket.settimeout(3)
   if self._command([b'AUTH',self.password])!=b'OK':raise ValueError('RedisInfoAuthenticationFailed')
   self.password=b''
   primed=self.sample('prime-before-first-callback')
   return {'prime':primed,'AUTH_commands':1,'credential_recorded':False,'reconnect_supported':False}
  except BaseException:
   self.failed=True;self.close();raise
 def sample(self,label,deadline=None):
  if self.socket is None:raise RuntimeError('RedisInfoSessionNotOpen')
  if label not in {'prime-before-first-callback','before-reader','after-warm-before-window','after-physical-close','retention'}:raise ValueError('FixedCallbackLabelRequired')
  if len(self.records)>=5:raise ValueError('RedisInfoPointCapacity')
  began=time.monotonic();raw=self._command([b'INFO'],deadline)
  try:
   value=redis_info(raw.decode('utf-8',errors='strict'));delta=None if self.previous is None else redis_delta(self.previous,value)
  except BaseException:self.failed=True;raise
  self.infos+=1;record={'label':label,'begin_monotonic':began,'end_monotonic':time.monotonic(),'server':value,'delta_from_previous':delta,'observer_INFO_commands':self.infos,'observer_commands_total':self.commands,'observer_outbound_RESP_bytes':self.sent,'observer_inbound_RESP_bytes':self.received,'scope':'whole-daemon Redis counters including observers; current INFO reply need not be reflected in its own reported output counter'}
  self.previous=value;self.records.append(record);return record
 def close(self):
  self.password=b''
  if self.socket is not None:
   try:self.socket.shutdown(socket.SHUT_RDWR)
   except OSError:pass
   self.socket.close()
   if self.socket.fileno()!=-1:raise RuntimeError('RedisObserverSocketUndrained')
  self.closed=True
if __name__=='__main__':raise RuntimeError('SupplementalRedisExecutionBlockedPendingRootReviewAndQuiet')
