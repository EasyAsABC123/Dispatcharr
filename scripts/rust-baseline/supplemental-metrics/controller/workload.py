"""Bounded continuous authenticated live readers, with post-window close barrier.
Original Dispatcharr authors retain application/algorithm credit and licensing.
"""
import concurrent.futures,hashlib,http.client,threading,time,socket
from urllib.parse import urlsplit

def reader(base,path,token,shared,ready,prefix_file,timeout):
 target=urlsplit(base);connection=http.client.HTTPConnection(target.hostname,target.port,timeout=timeout)
 result={'status':None,'requests':1,'first_byte_ms':None,'warmup_bytes':0,'measured_bytes':0,'prefix_bytes':0,'errors':[],'unexpected_eof':False,'post_window_barrier':False}
 response=None;physical_socket=None;started=time.monotonic();prefix=bytearray();captures={'import1':bytearray(),'import2':bytearray()};trace=[]
 try:
  connection.request('GET',path,headers={'Authorization':'Bearer '+token,'User-Agent':'dispatcharr-live-fixed-load'})
  response=connection.getresponse();result['status']=response.status
  if response.fp is not None:physical_socket=response.fp.raw._sock
  if response.status!=200:raise RuntimeError('PlaybackStatus')
  while not shared['stop'].is_set():
   data=response.read1(65536);now=time.monotonic()
   if not data:result['unexpected_eof']=True;break
   if result['first_byte_ms']is None:result['first_byte_ms']=(now-started)*1000;ready.set()
   end=shared.get('end');begin=shared.get('begin')
   if end is not None and now>=end:
    result['post_window_barrier']=True;shared['release'].wait(timeout+10);break
   if len(prefix)<shared['prefix_limit']:prefix.extend(data[:shared['prefix_limit']-len(prefix)])
   if begin is not None and begin<=now:
    result['measured_bytes']+=len(data)
    if not trace or now-trace[-1]['monotonic']>=.25:
     if len(trace)>=1024:raise RuntimeError('TraceBound')
     trace.append({'monotonic':now,'bytes':result['measured_bytes']})
    for label,offset in [('import1',5),('import2',60)]:
     if now>=begin+offset and len(captures[label])<shared['prefix_limit']:captures[label].extend(data[:shared['prefix_limit']-len(captures[label])])
   else:result['warmup_bytes']+=len(data)
 except Exception as error:result['errors'].append(type(error).__name__)
 finally:
  ready.set()
  if physical_socket is not None:
   try:physical_socket.shutdown(socket.SHUT_RDWR)
   except OSError:pass
  try:
   if response is not None:response.close()
  except Exception as error:result['errors'].append('ResponseClose'+type(error).__name__)
  finally:
   try:connection.close()
   except Exception as error:result['errors'].append('ConnectionClose'+type(error).__name__)
  result['physical_close']={'socket_closed':physical_socket is not None and physical_socket.fileno()==-1,'reader_returning_after_close':True,'completed_monotonic':time.monotonic()}
  prefix_file.write_bytes(prefix);result['prefix_bytes']=len(prefix);result['prefix_sha256']=hashlib.sha256(prefix).hexdigest();result['reader_start_monotonic']=started;result['reader_end_monotonic']=time.monotonic();result['byte_trace']=trace
  for label,payload in captures.items():
   prefix_file.with_name(prefix_file.stem+'-'+label+'.ts').write_bytes(payload)
  result['captures']={label:{'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest()}for label,payload in captures.items()}
 return result

def run(base,path,token,count,warmup,duration,directory,before_window,after_window,inspect_active,prefix_limit=1048576,timeout=20):
 shared={'stop':threading.Event(),'release':threading.Event(),'prefix_limit':prefix_limit};ready=[threading.Event()for _ in range(count)];directory.mkdir()
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
