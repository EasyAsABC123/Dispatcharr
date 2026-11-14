"""Bounded read-only actual common-candidate FIFO observation; no claim/ACK."""
import base64,hashlib,json,socket,time
from pathlib import Path
SCRIPT="local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return {0,''}end;if t~='list'then return redis.error_reply('owned queue type')end;local n=redis.call('LLEN',KEYS[1]);if n>10000 then return redis.error_reply('owned queue10000')end;local raw=redis.call('LINDEX',KEYS[1],-1);if not raw then return {n,''}end;if #raw>65536 then return redis.error_reply('owned frame64KiB')end;return {n,raw}"
class ProtocolError(ValueError):pass
class Reader:
 def __init__(self,stream):self.stream=stream;self.used=0
 def take(self,n):
  if n<0 or self.used+n>131072:raise ProtocolError('Response128KiB')
  data=self.stream.read(n);self.used+=len(data)
  if len(data)!=n:raise ProtocolError('ActualEOF')
  return data
 def line(self):
  data=bytearray()
  while not data.endswith(b'\r\n'):
   if len(data)>=4096:raise ProtocolError('Line4096')
   data.extend(self.take(1))
  return bytes(data[:-2])
 def value(self,depth=0):
  if depth>3:raise ProtocolError('Depth3')
  kind=self.take(1);text=self.line()
  if kind==b'+':return text
  if kind==b'-':raise ProtocolError('ActualRedisError:'+text[:256].decode(errors='replace'))
  if kind==b':':return int(text)
  if kind==b'$':
   n=int(text)
   if n==-1:return None
   if not 0<=n<=65536:raise ProtocolError('Bulk64KiBBeforeAllocation')
   result=self.take(n)
   if self.take(2)!=b'\r\n':raise ProtocolError('BulkCRLF')
   return result
  if kind==b'*':
   n=int(text)
   if not 0<=n<=16:raise ProtocolError('Array16')
   return [self.value(depth+1)for _ in range(n)]
  raise ProtocolError('RESPType')
def encode(parts):
 parts=[p if type(p)is bytes else str(p).encode()for p in parts]
 return b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(p)).encode()+b'\r\n'+p+b'\r\n'for p in parts)
def observe(inputs):
 r=inputs['broker']
 if r['host']!='127.0.0.1'or type(r['port'])is not int or r['port']not in inputs['owned_loopback_ports']or type(r['db'])is not int or r['db']!=8:raise ValueError('ExactOwnedBrokerDB8')
 password=Path(r['password_file']).read_bytes().strip()
 if not 1<=len(password)<=8192:raise ValueError('BoundedOwnedRedisCredential')
 rows=[];began=time.monotonic()
 with socket.create_connection((r['host'],r['port']),timeout=2)as connection:
  connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1);actual_nodelay=connection.getsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY)
  with connection.makefile('rb')as stream:
   def command(parts):
    if time.monotonic()-began>10:raise TimeoutError('ActualFIFOProbe10s')
    connection.sendall(encode(parts));return Reader(stream).value()
   if command(['AUTH',password])!=b'OK'or command(['SELECT',8])!=b'OK':raise ProtocolError('ActualAuthSelect')
   for index in range(2):
    raw=command(['EVAL',SCRIPT,1,'celery'])
    if type(raw)is not list or len(raw)!=2 or type(raw[0])is not int or type(raw[1])is not bytes:raise ProtocolError('ActualTypedPeekReply')
    frame=json.loads(raw[1])if raw[1]else None
    rows.append({'queue_length':raw[0],'raw_head_base64':base64.b64encode(raw[1]).decode(),'raw_head_sha256':hashlib.sha256(raw[1]).hexdigest(),'task':frame.get('headers',{}).get('task')if type(frame)is dict else None,'observed_monotonic':time.monotonic()})
    if index==0:time.sleep(.1)
 return {'actual_readonly_snapshots':rows,'same_raw_head_in_two_observations':rows[0]['raw_head_base64']==rows[1]['raw_head_base64'],'actual_TCP_NODELAY':actual_nodelay,'script_sha256':hashlib.sha256(SCRIPT.encode()).hexdigest(),'commands':{'AUTH':1,'SELECT':1,'EVAL_read_only':2},'queue_mutations':0,'claim_ACK_or_worker_completion_inferred':False,'actual_client_socket_closed':connection.fileno()==-1,'elapsed_seconds':time.monotonic()-began}
