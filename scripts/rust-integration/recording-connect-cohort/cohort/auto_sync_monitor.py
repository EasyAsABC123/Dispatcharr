"""Observe actual Redis LPUSH task frames, without altering task/queue methods.
Dedicated auto-only declared monitor budget; old recording observer unchanged.
"""
import json,os,re,signal,socket,time
from pathlib import Path
running=[True]
def stop(*_):
 running[0]=False
 active=globals().get('sock')
 if active is not None:
  try:active.shutdown(socket.SHUT_RD)
  except OSError:pass
signal.signal(signal.SIGTERM,stop)
password=os.environ['REDIS_PASSWORD'];path=Path(os.environ['NORMAL_DVR_LEDGER'])
sock=socket.create_connection(('127.0.0.1',6379),timeout=5)
def send(*args):
 parts=[str(x).encode()for x in args];sock.sendall(b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(b'$'+str(len(p)).encode()+b'\r\n'+p+b'\r\n'for p in parts))
stream=sock.makefile('rb');send('AUTH',password)
if stream.readline()!=b'+OK\r\n':raise RuntimeError('ActualRedisObserverAuthentication')
send('MONITOR')
if stream.readline()!=b'+OK\r\n':raise RuntimeError('ActualRedisObserverMonitor')
sock.settimeout(None)
path.with_suffix('.budget.json').write_text(json.dumps({'provider_rows':6001,'phases':4,'unique_EPGs':5004,'line_bytes':33554432,'records':8192,'retained_bytes':33554432,'reason':'two API parents plus bounded EPG/profile/startup descendants; full Groups result MONITOR escape expansion for6001 rows admitted up to32MiB; not a performance measurement'}));path.with_suffix('.ready').write_text('actual MONITOR reply OK')
count=0
with path.open('xb')as output:
 while running[0]:
  # A quiet future-only context must not produce a >1MiB command. Bound line
  # residency before decoding; an unexpected large command fails this observer.
  line=stream.readline(33554433)
  if not line:break
  if len(line)>33554432:raise RuntimeError('RedisObserverLineBudget')
  text=line.decode('utf8',errors='strict')
  if not re.search(r'\[1 (?:[^\]]+)\]',text):continue
  tokens=re.findall(r'"((?:[^"\\]|\\.)*)"',text)
  values=[json.loads('"'+re.sub(r'\\x([0-9a-fA-F]{2})',r'\\u00\1',token)+'"')for token in tokens]
  if len(values)<3 or values[0].upper()not in('LPUSH','RPUSH')or values[1]not in('celery','dvr'):continue
  for raw in values[2:]:
   frame=json.loads(raw);record={'observed_epoch':time.time(),'queue':values[1],'raw_frame':raw,'headers':frame.get('headers')}
   encoded=json.dumps(record,allow_nan=False).encode()+b'\n'
   if count>=8192 or output.tell()+len(encoded)>33554432:raise RuntimeError('ActualPublicationObserverBudget')
   output.write(encoded);output.flush();count+=1
sock.close()
