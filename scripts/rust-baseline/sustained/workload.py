"""Bounded continuous authenticated live readers, with post-window close barrier.
Original Dispatcharr authors retain application/algorithm credit and licensing.
"""
import concurrent.futures,hashlib,http.client,threading,time
from urllib.parse import urlsplit

def reader(base,path,token,shared,ready,prefix_file,timeout):
 target=urlsplit(base);connection=http.client.HTTPConnection(target.hostname,target.port,timeout=timeout)
 result={'status':None,'requests':1,'first_byte_ms':None,'warmup_bytes':0,'measured_bytes':0,'prefix_bytes':0,'errors':[],'unexpected_eof':False,'post_window_barrier':False}
 started=time.monotonic();prefix=bytearray();response=None
 try:
  connection.request('GET',path,headers={'Authorization':'Bearer '+token,'User-Agent':'dispatcharr-live-fixed-load'})
  response=connection.getresponse();result['status']=response.status
  if response.status!=200:raise RuntimeError('PlaybackStatus')
  while not shared['stop'].is_set():
   data=response.read1(65536);now=time.monotonic()
   if not data:result['unexpected_eof']=True;break
   if result['first_byte_ms']is None:result['first_byte_ms']=(now-started)*1000;ready.set()
   end=shared.get('end');begin=shared.get('begin')
   if end is not None and now>=end:
    result['post_window_barrier']=True;shared['release'].wait(timeout+10);break
   if len(prefix)<shared['prefix_limit']:prefix.extend(data[:shared['prefix_limit']-len(prefix)])
   if begin is not None and begin<=now:result['measured_bytes']+=len(data)
   else:result['warmup_bytes']+=len(data)
 except Exception as error:result['errors'].append(type(error).__name__)
 finally:
  ready.set()
  if shared.get('physical_close_required') and response is not None:response.close()
  connection.close()
  if shared.get('physical_close_required'):result['physical_socket_closed']=True
  prefix_file.write_bytes(prefix);result['prefix_bytes']=len(prefix);result['prefix_sha256']=hashlib.sha256(prefix).hexdigest()
 return result

def run(base,path,token,count,warmup,duration,directory,before_window,after_window,inspect_active,prefix_limit=1048576,timeout=20,physical_close_required=False):
 shared={'stop':threading.Event(),'release':threading.Event(),'prefix_limit':prefix_limit,'physical_close_required':physical_close_required};ready=[threading.Event()for _ in range(count)];directory.mkdir()
 with concurrent.futures.ThreadPoolExecutor(max_workers=count)as pool:
  tasks=[pool.submit(reader,base,path,token,shared,event,directory/(str(i)+'.ts'),timeout)for i,event in enumerate(ready)]
  try:
   deadline=time.monotonic()+timeout+5
   for event in ready:
    if not event.wait(max(0,deadline-time.monotonic())):raise TimeoutError('ViewerReady')
   if any(t.done()for t in tasks):raise RuntimeError('EarlyReaderExit')
   identity=inspect_active(count)
   until=time.monotonic()+warmup
   while time.monotonic()<until:
    if any(t.done()for t in tasks):raise RuntimeError('WarmupReaderExit')
    time.sleep(min(.1,max(0,until-time.monotonic())))
   state=before_window();shared['begin']=time.monotonic();shared['end']=shared['begin']+duration
   while time.monotonic()<shared['end']:
    if any(t.done()for t in tasks):raise RuntimeError('MeasuredReaderExit')
    time.sleep(min(.05,max(0,shared['end']-time.monotonic())))
   final=after_window(state,shared['begin'],shared['end']);shared['release'].set()
   results=[t.result(timeout=timeout+5)for t in tasks]
  finally:shared['release'].set();shared['stop'].set()
 return {'viewers':results,'authenticated_identity':identity,'continuous_connections':True,'requests':sum(r['requests']for r in results),'warmup_seconds':warmup,'measure_seconds':duration,'resources':final}
