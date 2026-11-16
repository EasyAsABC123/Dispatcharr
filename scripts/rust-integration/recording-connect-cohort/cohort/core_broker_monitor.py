"""Observe actual Redis LPUSH task frames, without altering task/queue methods.
Separate Core scale observer; existing recording observer remains byte-identical.
Bounds declared from actual SQL context/input counts before either backend boot.
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
path.with_suffix('.ready').write_text('actual MONITOR reply OK')
budget=json.loads(Path('/normal-dvr-tools/core_monitor_budget.json').read_text())
path.with_suffix('.budget.json').write_text(json.dumps(budget,sort_keys=True))
count=0
with path.open('xb')as output:
 while running[0]:
  # A quiet future-only context must not produce a >1MiB command. Bound line
  # residency before decoding; an unexpected large command fails this observer.
  line=stream.readline(1048577)
  if not line:break
  if len(line)>1048576:raise RuntimeError('RedisObserverLineBudget')
  text=line.decode('utf8',errors='strict')
  if not re.search(r'\[1 (?:[^\]]+)\]',text):continue
  tokens=re.findall(r'"((?:[^"\\]|\\.)*)"',text)
  values=[json.loads('"'+re.sub(r'\\x([0-9a-fA-F]{2})',r'\\u00\1',token)+'"')for token in tokens]
  if len(values)<3 or values[0].upper()not in('LPUSH','RPUSH')or values[1]not in('celery','dvr'):continue
  for raw in values[2:]:
   frame=json.loads(raw);record={'observed_epoch':time.time(),'queue':values[1],'raw_frame':raw,'headers':frame.get('headers')}
   encoded=json.dumps(record,allow_nan=False).encode()+b'\n'
   parent=frame.get('headers',{}).get('task')in('core.tasks.rehash_streams','apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change')
   record_budget=budget['parent_record_bytes']if parent else budget['child_record_bytes']
   if len(encoded)>record_budget:raise RuntimeError('ActualCorePerPublicationBudget')
   if count>=budget['maximum_records'] or output.tell()+len(encoded)>budget['bytes']:raise RuntimeError('ActualPublicationObserverBudget')
   output.write(encoded);output.flush();count+=1
sock.close()
