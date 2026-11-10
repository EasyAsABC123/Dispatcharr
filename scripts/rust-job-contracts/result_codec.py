"""Bounded Source-result evidence projection; no Redis/worker imports.
Original Dispatcharr authors retain application credit and AGPL licensing.
"""
import hashlib,json,math
MAX_RAW=262144
def project(raw,taskid):
 if len(raw)>MAX_RAW:raise ValueError('Result256KiB evidence bound')
 value=json.loads(raw);nodes=0
 def typed(value,field='',depth=0):
  nonlocal nodes
  nodes+=1
  if nodes>8192 or depth>32:raise ValueError('Result evidence node/depth bound')
  if field=='task_id':
   return {'type':type(value).__name__,'sha256':hashlib.sha256(str(value).encode()).hexdigest(),'matches_requested_task':value==taskid}
  if field=='traceback'and value is not None:
   if not isinstance(value,str):raise ValueError('Unexpected traceback type')
   return {'type':'str','utf8_bytes':len(value.encode()),'sha256':hashlib.sha256(value.encode()).hexdigest()}
  if isinstance(value,dict):
   if len(value)>8192:raise ValueError('Result dictionary bound')
   return {'type':'dict','fields':{key:typed(v,key,depth+1)for key,v in value.items()}}
  if isinstance(value,list):
   if len(value)>8192:raise ValueError('Result array bound')
   return {'type':'list','items':[typed(v,depth=depth+1)for v in value]}
  if isinstance(value,str)and len(value.encode())>65536:raise ValueError('Result string64KiB bound')
  if isinstance(value,float)and not math.isfinite(value):return {'type':'float','nonfinite':str(value)}
  if value is None or type(value)in[bool,int,float,str]:return {'type':type(value).__name__,'value':value}
  raise ValueError('Unsupported evidence type')
 return {'raw_bytes':len(raw),'raw_sha256':hashlib.sha256(raw).hexdigest(),'metadata':typed(value)}
