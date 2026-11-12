"""Compile pinned code objects WITHOUT executing/importing Original application.
Trace rules require exact bytecode equality on the same interpreter/optimization.
Actual WSGI integration remains separately blocked and unqualified.
"""
import hashlib,types
from pathlib import Path

MANAGER='apps/proxy/vod_proxy/multi_worker_connection_manager.py'
POOL='apps/m3u/connection_pool.py'
VIEWS='apps/proxy/vod_proxy/views.py'
PINNED={VIEWS:'6ede6aedcf0182780978c156051ab80562a5c726ceabfd40149f0f0a34b5080e',MANAGER:'8a63d669219d149293da592d1d9a4e77db4d0c8b253baaf29c44e2e766c72c4a',POOL:'e79c8d663818068d71b23b2404e9a313ce566bd6a51d4ae217211ac72fc83bfb'}
def walk(code):
 yield code
 for value in code.co_consts:
  if isinstance(value,types.CodeType):yield from walk(value)
def rules(source_root):
 result=[];found={}
 for name,digest in PINNED.items():
  raw=(Path(source_root)/name).read_bytes()
  if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('PinnedSourceHash '+name)
  codes=list(walk(compile(raw,'/app/'+name,'exec')));found[name]=codes
  for code in codes:
   if name==MANAGER:
    q=code.co_qualname;kind=None;markers={}
    if q=='_ClosingVODStream.__init__':kind='generator_ctor'
    elif q=='RedisBackedVODConnection.__init__':kind='connection_ctor'
    elif q.endswith('.stream_generator'):
     kind='generator';markers={1640:'finally_enter',1575:'callback_create',1609:'callback_create',1634:'callback_create',1661:'callback_create'}
    elif code.co_name=='delayed_cleanup':kind='callback'
    elif q.endswith('._rollback_setup_reservations'):kind='function';markers={1174:'callback_create'}
    elif q.endswith('.release_unstarted_active_streams'):kind='function';markers={1702:'callback_create'}
    elif code.co_name=='_decrement_profile_connections'or q in['MultiWorkerVODConnectionManager.create_connection','MultiWorkerVODConnectionManager.remove_connection']:kind='counter'
    elif code.co_name=='decrement_active_streams_and_check':kind='active_decr'
    elif code.co_name=='_get_connection_state':kind='state_read'
    elif code.co_name in['close','_close_local_http','cleanup','stream_content_with_session','cleanup_persistent_connection','cleanup_stale_persistent_connections','get_stream','_get_connection_state']:kind='function'
    if kind is not None:
     executable={line for _,_,line in code.co_lines()if line is not None}
     if not set(markers)<=executable:raise ValueError('PinnedMarkerMissing '+q)
     result.append((code,kind,markers))
   elif name==VIEWS and code.co_name=='stop_vod_client':result.append((code,'function',{}))
   elif code.co_name in['reserve_profile_slot','release_profile_slot','_release_credential_slot_by_profile_id']:result.append((code,'counter',{}))
 if sum(kind=='callback'for _,kind,_ in result)!=6:raise ValueError('SixPinnedCallbackCodeObjectsRequired')
 if len(result)>48:raise ValueError('PinnedCodeRegistryCapacity')
 return result

def bind(probe,source_root):
 selected=rules(source_root)
 for code,kind,markers in selected:probe.add(code,kind,markers)
 return[{'qualname':code.co_qualname,'firstline':code.co_firstlineno,'code_sha256':hashlib.sha256(code.co_code).hexdigest(),'kind':kind,'markers':markers}for code,kind,markers in selected]
