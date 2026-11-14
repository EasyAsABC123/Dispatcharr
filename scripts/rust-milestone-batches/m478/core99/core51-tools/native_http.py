"""Fixed finite Native HTTP observation; actual source socket, no retry/pooling."""
import hashlib,json,time
from absolute_http import Observation

def request(base,path,headers,method='GET',body=b'',source_ip='127.0.0.1',seconds=8,io_seconds=8):
 o=Observation(base,seconds=seconds,io_seconds=io_seconds,source_ip=source_ip)
 row={'began_epoch':time.time(),'began_monotonic':time.monotonic(),'source_ip':source_ip,'path':path,'method':method,'request_body_sha256':hashlib.sha256(body).hexdigest(),'request_body_bytes':len(body),'actual_request_attempted':False}
 try:
  row['actual_request_attempted']=True;r=o.request(path,{'Connection':'close',**headers},method=method,body=body)
  raw=r.raw.read(65537)
  if len(raw)>65536:raise ValueError('NativeResponseBodyCapacity')
  row.update(status=r.status_code,headers=r.headers,response_body_hex=raw.hex(),response_body_bytes=len(raw),response_body_sha256=hashlib.sha256(raw).hexdigest())
  try:row['json']=json.loads(raw)
  except(ValueError,UnicodeDecodeError):row['json']=None
  row['absolute_observation']=o.complete()
 except BaseException as e:
  row['exception']={'type':type(e).__name__,'message':str(e)[:2048]}
 finally:
  try:row['physical_close']=o.cancel()
  except BaseException as e:row['close_exception']={'type':type(e).__name__,'message':str(e)[:2048]}
  row.update(observed_socket_source=getattr(o,'connected_source',None),observed_socket_peer=getattr(o,'connected_peer',None),completed_epoch=time.time(),completed_monotonic=time.monotonic())
 return row

def control(base,path,method='GET',body=b'',headers=None,source_ip='127.0.0.1',seconds=8,io_seconds=8):
 row=request(base,path,headers or{},method,body,source_ip,seconds,io_seconds)
 if row.get('status')!=200 or row.get('exception') or row.get('close_exception'):raise ValueError('BoundedControlResponse:'+str(row))
 return row['json']
