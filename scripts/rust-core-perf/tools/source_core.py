"""DRAFT actual pinned StreamBuffer core workload; no benchmark acceptance.
Original Dispatcharr authors retain all application/algorithm credit.
"""
from gevent import monkey
monkey.patch_all()
import gevent
import hashlib,json,logging,os,sys,time,uuid
from pathlib import Path
if os.environ.get('M1_CORE_DISPOSABLE_FIXTURE')!='owned-core-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from core.models import CoreSettings
from apps.proxy.config import TSConfig
from apps.proxy.live_proxy.input.buffer import StreamBuffer
logging.disable(logging.INFO)
plan=json.loads(Path(os.environ['M1_CORE_PLAN']).read_text())
viewers=int(os.environ['M1_CORE_VIEWERS']);period=int(os.environ['M1_CORE_PERIOD_MS'])/1000
assert viewers in plan['viewers'] and int(period*1000) in plan['source_only_period_candidates_ms']
assert TSConfig.BUFFER_CHUNK_SIZE==plan['chunk_bytes'] and TSConfig.get_redis_chunk_ttl()==plan['retention_seconds']
client=redis.Redis.from_url(os.environ['REDIS_URL']);assert client.ping()
channel='m1-core-'+uuid.uuid4().hex;buffer=StreamBuffer(channel_id=channel,redis_client=client)
assert buffer.target_chunk_size==255868 and buffer.chunk_ttl==60
packet=bytes([0x47,1,0,0x30,1,0]+[71]*182);expected=packet*1361
frames=tuple(expected[i:i+8192]for i in range(0,len(expected),8192))
warm=plan['warmup_seconds'];duration=warm+plan['measure_seconds'];total=round(duration/period);assert total*period==duration
start=int(os.environ['M1_CORE_START_MS'])/1000;measure_start=start+warm;end=start+duration
drain_release=Path(os.environ['M1_CORE_DRAIN_RELEASE']);assert str(drain_release).startswith('/evidence/')
def drain_gate(force=False):
 if force or time.time()>=end:
  while not drain_release.exists():gevent.sleep(.005)

readers=[{'delivered':0,'measured':0,'bytes':0,'errors':[],'max_prefetched_bytes':0}for _ in range(viewers)];produced=0;producer_late=0
ready=Path(os.environ['M1_CORE_OUTPUT']+'.ready');ready.write_text(json.dumps({'ready':True,'chunk_bytes':len(expected),'viewers':viewers})+'\n')
def wait_start():
 while time.time()<start:gevent.sleep(max(0,min(.02,start-time.time())))
def consume(index):
 wait_start();cursor=0;row=readers[index];slow=viewers>1 and index%10==0
 deadline=end+30
 while row['delivered']<total and time.time()<deadline:
  drain_gate()
  chunks,next_cursor=buffer.get_optimized_client_data(cursor)
  if not chunks:
   buffer.chunk_available.wait(timeout=.05);continue
  row['max_prefetched_bytes']=max(row['max_prefetched_bytes'],sum(len(value)for value in chunks))
  for offset,data in enumerate(chunks):
   drain_gate()
   sequence=cursor+offset+1
   if sequence!=row['delivered']+1 or data!=expected:raise AssertionError('Full Source bytes/sequence mismatch')
   now=time.time();row['delivered']+=1;row['bytes']+=len(data)
   if measure_start<=now<end:row['measured']+=1
   if slow and now<end:gevent.sleep(period*2)
  cursor=next_cursor
 if row['delivered']!=total:raise AssertionError('Source drain incomplete')
def produce():
 global produced,producer_late
 wait_start()
 for index in range(total):
  drain_gate()
  target=start+index*period
  while time.time()<target:gevent.sleep(max(0,min(.01,target-time.time())))
  if time.time()-target>period/2:producer_late+=1
  for frame in frames:
   drain_gate()
   if not buffer.add_chunk(frame):raise AssertionError('Source publication failed')
  produced+=1
  if buffer.index!=produced:raise AssertionError('Canonical Source publication count mismatch')
try:
 tasks=[gevent.spawn(consume,index)for index in range(viewers)];producer=gevent.spawn(produce)
 gevent.joinall([producer,*tasks],timeout=duration+40,raise_error=True)
 assert all(task.ready()and task.exception is None for task in [producer,*tasks])
 assert produced==total and all(row['delivered']==total for row in readers)
 drain_gate(force=True)
 keys=list(client.scan_iter(match=buffer.buffer_prefix+'*',count=100));report={'passed':True,'backend':'source','scope':__doc__,'viewers':viewers,'period_ms':int(period*1000),'produced':produced,'producer_late':producer_late,'chunk_bytes':len(expected),'expected_payload_sha256':hashlib.sha256(expected).hexdigest(),'final_full_bytes_per_viewer':total*len(expected),'readers':readers,'retained_key_count':len(keys),'actual_original_StreamBuffer':True,'drain_release_seen':drain_release.exists()}
finally:
 buffer.stop();keys=list(client.scan_iter(match='live:channel:'+channel+':*',count=100));assert len(keys)<2048
 if keys:client.delete(*keys)
 assert not list(client.scan_iter(match='live:channel:'+channel+':*',count=100))
Path(os.environ['M1_CORE_OUTPUT']).write_text(json.dumps(report,indent=2)+'\n')
