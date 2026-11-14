"""BLOCKED guest proposal, no import-time Source/model/network effects.
Requires a separately reviewed full host/bootstrap/audit/once cleanup closure.
"""
import hashlib,json,os,sys,time,uuid
from pathlib import Path

def main():
 import shlex
 from django.db import connection,connections,transaction
 from django.test.utils import CaptureQueriesContext
 from core.models import StreamProfile,OutputProfile,PROXY_PROFILE_NAME
 from core.utils import RedisClient
 from apps.proxy.live_proxy.output.profile.manager import OutputProfileManager
 from apps.proxy.live_proxy.redis_keys import RedisKeys
 from dispatcharr.app_initialization import should_skip_initialization
 from capture_common import audit,literal,save,require,connected_database,exception_record
 from public_manifest import manifest
 from profile_cases import cases
 root=Path('/special-root/inputs');out=Path('/evidence/profile-arguments-owner-private.json');nonce=os.environ['PROFILE_SOURCE_NONCE'];report={'passed':False,'capture_valid':False,'actual_API_requests':0,'actual_argument_attempts':0,'argument_cases':[],'owner_calls':[],'process_spawn_invocations':0,'Native_or_Main_or_media':False};client=None;observer=None;owned_keys=[];first=None
 def flush():save(out,report)
 def snapshot():
  with transaction.atomic():
   with connection.cursor()as c:c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
   return {'full6259':manifest(connection,json.loads((root/'schema62-authority.json').read_text())),'named11':audit(connection),'StreamProfile_rows':literal(list(StreamProfile.objects.order_by('id').values())),'OutputProfile_rows':literal(list(OutputProfile.objects.order_by('id').values()))}
 def key_state():return [{'key':k,'type':client.type(k),'value':client.get(k),'pttl':client.pttl(k)}for k in owned_keys]
 def owner(label,fn):
  r={'label':label,'before':key_state(),'began_wall':time.time(),'began_monotonic':time.monotonic()};report['owner_calls'].append(r);flush()
  try:r['actual_attempted']=True;r['returned']=literal(fn());r['complete']=True
  except BaseException:r['exception']=exception_record(sys.exc_info())
  finally:r.update(after=key_state(),completed_wall=time.time(),completed_monotonic=time.monotonic());flush()
  require(r.get('complete')is True,'CompleteOwnerCallNoSyntheticReturn')
 try:
  require(os.environ.get('PROFILE_SOURCE_AUTHORITY')=='StreamProfile18-owner6-only'and'shell'in sys.argv and should_skip_initialization()is True,'ExplicitManagementAuthority')
  require(connected_database(connection)=='rust_health_'+nonce,'ExactOwnedDatabase');report['before_setup']=snapshot()
  report['dependencies']={'python':sys.version,'shlex_file_sha256':hashlib.sha256(Path(shlex.__file__).read_bytes()).hexdigest(),'DB_driver':connection.Database.__name__,'DB_driver_version':str(getattr(connection.Database,'__version__',None))}
  models=[]
  with transaction.atomic():
   for n,spec in enumerate(cases(),1):
    m=StreamProfile.objects.create(name=PROXY_PROFILE_NAME if spec['use_Source_Proxy_name']else'owned-profile-'+nonce+'-'+str(n),command=spec['command'],parameters=spec['parameters'],locked=spec['locked'],is_active=True);models.append(m.id)
   outputs=[OutputProfile.objects.create(name='owned-output-'+nonce+'-'+str(n),command='ffmpeg',parameters='-i pipe:0 -c:v copy -c:a copy -f mpegts pipe:1',is_active=True)for n in (1,2)]
  report['actual_returned_StreamProfile_ids']=models;report['actual_returned_OutputProfile_ids']=[x.id for x in outputs];report['after_setup']=snapshot();flush()
  for spec,pk in zip(cases(),models):
   r={'name':spec['name'],'input':spec,'SQL_id':pk,'before':snapshot(),'began_wall':time.time(),'began_monotonic':time.monotonic()};report['argument_cases'].append(r);flush()
   with CaptureQueriesContext(connection)as journal:
    m=StreamProfile.objects.get(pk=pk);r['actual_SQL_profile']=literal({k:getattr(m,k)for k in ['id','name','command','parameters','locked','is_active']});report['actual_argument_attempts']+=1
    try:
     r['observed']={'kind':'return','argv':m.build_command(spec['stream_url'],spec['user_agent'],channel_id=spec['channel_id'])}
     require(len(json.dumps(r['observed']).encode())<=65536,'ArgumentCapacity')
    except BaseException:r['observed']={'kind':'exception','exception':exception_record(sys.exc_info())}
   r.update(actual_SQL=literal(list(journal.captured_queries)),after=snapshot(),completed_wall=time.time(),completed_monotonic=time.monotonic());flush()
  import redis
  observer=redis.Redis(host='127.0.0.1',port=6379,db=1,password=os.environ['REDIS_PASSWORD'],decode_responses=True,socket_timeout=2,socket_connect_timeout=2,max_connections=2)
  report['Redis_server_config_before_Source_client']={k:observer.config_get(k)for k in ['save','appendonly','timeout']}
  client=RedisClient.get_client();require(client is not None,'ActualSourceRedisClient');kwargs=client.connection_pool.connection_kwargs;require(int(kwargs.get('db',0))==1,'ActualOwnedRedisDatabase')
  channel=str(uuid.uuid4());report['owned_namespace_channel']=channel;pid1,pid2=[x.id for x in outputs];workerA='owned-profile-A-'+nonce;workerB='owned-profile-B-'+nonce
  command=['ffmpeg','-i','pipe:0','-c:v','copy','-c:a','copy','-f','mpegts','pipe:1']
  a=OutputProfileManager(channel,pid1,command,None,workerA);b=OutputProfileManager(channel,pid1,command,None,workerB);other=OutputProfileManager(channel,pid2,command,None,workerB)
  owned_keys=[RedisKeys.output_owner(channel,'mpegts:p'+str(p))for p in [pid1,pid2]];require(all(client.exists(k)==0 for k in owned_keys),'FreshExactOwnerKeys')
  report['Redis_server_config_after_Source_client']={k:observer.config_get(k)for k in ['save','appendonly','timeout']}
  report['key_namespaces']={str(p):{'owner':RedisKeys.output_owner(channel,'mpegts:p'+str(p)),'state':RedisKeys.output_state(channel,'mpegts:p'+str(p)),'index':RedisKeys.output_buffer_index(channel,'mpegts:p'+str(p)),'chunk_prefix':RedisKeys.output_buffer_chunk_prefix(channel,'mpegts:p'+str(p)),'timestamps':RedisKeys.output_chunk_timestamps(channel,'mpegts:p'+str(p))}for p in [pid1,pid2]}
  for label,fn in [('A-acquire-p1',a._acquire_owner_lock),('B-same-p1',b._acquire_owner_lock),('A-own-reacquire-p1',a._acquire_owner_lock),('B-other-p2',other._acquire_owner_lock),('A-release-own-p1',a._release_owner_lock),('B-acquire-released-p1',b._acquire_owner_lock)]:owner(label,fn)
  require(all(m._process is None and not m.running and m._writer_thread is None and m._reader_thread is None for m in [a,b,other]),'ActualManagersNeverStarted');report['after']=snapshot();report['capture_valid']=len(report['argument_cases'])==report['actual_argument_attempts']==18 and len(report['owner_calls'])==6
 except BaseException:report['exception']=exception_record(sys.exc_info())
 finally:
  errors=[]
  if client is not None:
   try:
    if owned_keys:client.delete(*owned_keys)
    report['owned_keys_absent_after_delete']=all(client.exists(k)==0 for k in owned_keys)
   except BaseException:errors.append(exception_record(sys.exc_info()))
   finally:
    try:client.close();client.connection_pool.disconnect();report['actual_Redis_connection_retired']=True
    except BaseException:errors.append(exception_record(sys.exc_info()))
  if observer is not None:
   try:observer.close();observer.connection_pool.disconnect();report['observer_Redis_connection_retired']=True
   except BaseException:errors.append(exception_record(sys.exc_info()))
  try:connections.close_all();report['actual_Django_connections_closed']=True
  except BaseException:errors.append(exception_record(sys.exc_info()))
  if errors:report['cleanup_errors']=errors;report['capture_valid']=False
  flush()
 require(report['capture_valid'],'FiniteCompleteCaptureNoOutcomeFilter')
if __name__=='__main__':main()
