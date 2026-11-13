"""Unexecuted finite native known-failure physical controls; Original producer/decoder.
No Source timing/reference rerun. SQL and event gates observe real completed replies.
"""
import base64,json,os,threading,time
from pathlib import Path
from fixture_runtime import Fixture,Peer,wait,utc
from owned_pg_gate_proxy import Proxy as PGProxy
from owned_redis_reply_proxy import Proxy as RedisProxy
from acl_probes import permission_profile,denied_write_observation

MODES=('result-denied','old-SUCCESS')
TEXT='#EXTM3U\n#EXTINF:-1 tvg-id="owned.channel" group-title="Owned m3u-group-SQL-P0001",Owned stream\nhttp://127.0.0.1:9/owned-never-played.ts\n'
POLICY=json.loads(Path('/pair-tools/qa-policy.json').read_text())
report={'passed':False,'native_verified':False,'scope':__doc__,'checks':[],'cases':[],'predeclared_modes':list(MODES),'actual_Source_reference_rerun':False,'performance_verified':False,'pending_FS_syscall_held':False,'Source_renewal_after120s':'OPEN','raw_token_equality':'OPEN deliberate difference'}
f=None;peer=None;pg=None;reply=None;injector=None;acl=None

def save():Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
def check(name,value):
 if f is not None and 'case'in globals():
  case.setdefault('before_assertions',[]).append({'name':name,'utc':utc(),'raw_backend':{ident:f.raw_result(ident)for ident in (case['parent_id'],case['group_id'],case['single_id'])},'effects':f.snapshot(account.id)})
 save()
 report['checks'].append({'name':name,'passed':bool(value)});save()
 if not value:raise AssertionError(name)
def inject_sql_fault(label):
 from django.db import connection
 with connection.cursor()as c:
  c.execute('SELECT current_database()');check(label+'-owned-copied-PG-confirmed',c.fetchone()[0]==os.environ['POSTGRES_DB'])
  c.execute('CREATE SEQUENCE rust_owned_m3u_fail_count')
  c.execute("CREATE FUNCTION rust_owned_m3u_worker_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('rust_owned_m3u_fail_count'); RAISE EXCEPTION 'rust-owned M3U worker failure' USING ERRCODE='P0001'; END$$")

def drop_fault():
 from django.db import connection
 with connection.cursor()as c:
  c.execute('DROP TRIGGER IF EXISTS rust_owned_m3u_worker_fail ON dispatcharr_channels_channelgroup')
  c.execute('DROP FUNCTION IF EXISTS rust_owned_m3u_worker_fail()')
  c.execute('DROP SEQUENCE IF EXISTS rust_owned_m3u_fail_count')

def consume_tail(parent,account,label):
 # A fresh same-source Worker is explicit after abort closes the prior intake.
 # Every legitimate remaining frame is consumed, never deleted/quarantined.
 global peer
 if not f.broker.llen('celery'):return []
 peer=Peer(parent,account,label+'-tail');out=[]
 while f.broker.llen('celery'):
  check(label+'-only-one-tail',len(out)==0)
  head=json.loads(f.broker.lindex('celery',-1));check(label+'-tail-is-real-Single',head['headers']['task']=='apps.m3u.tasks.refresh_single_m3u_account')
  answer=peer.command({'action':'run'});check(label+'-tail-normal-completed',answer['run'].startswith('Ok(Completed('));out.append({'id':head['headers']['id'],'answer':answer,'result':f.raw_result(head['headers']['id'])})
 closed=peer.close();peer=None;check(label+'-tail-actual-close',closed['process_settled'])
 return out

try:
 f=Fixture()
 from channels.layers import get_channel_layer
 from asgiref.sync import async_to_sync
 for index,mode in enumerate(MODES,start=8):
  label=f'{index:02d}-{mode}';sql_case=mode in ('group-error-ready','parsed-cache-event');parent,account,frames,path=f.produce(label,TEXT if sql_case else'#EXTM3U\n')
  group=frames[0]['headers']['id'];single=frames[1]['headers']['id'];target=single if mode=='events'else group
  if not sql_case:path.unlink() # The protected Groups open must really observe ENOENT.
  channel='specific.rust_owned_m3u_failure!receiver';layer=get_channel_layer();channel_key=(layer.prefix+channel.split('!')[0]+'!').encode();f.cache.delete(channel_key);async_to_sync(layer.group_add)('updates',channel)
  case={'name':mode,'parent_id':parent,'group_id':group,'single_id':single,'started_utc':utc(),'Source_parent_result':f.raw_result(parent),'native_results':{},'observations':[],'before':f.snapshot(account.id)};report['cases'].append(case);save()
  cfg={};injection_errors=[]
  if sql_case:
   inject_sql_fault(label);pg=PGProxy(POLICY,hold_preflight=True,hold_error=mode=='group-error-ready')
   cfg['database_url']='postgres://dispatch:playback-fixture-only@127.0.0.1:61651/'+os.environ['POSTGRES_DB']
   def install():
    from django.db import connection
    try:
     if not pg.preflight_held.wait(25):raise TimeoutError('ActualSuccessfulAdmissionReady')
     with connection.cursor()as c:
      c.execute('SELECT current_database()');assert c.fetchone()[0]==os.environ['POSTGRES_DB']
      c.execute('CREATE TRIGGER rust_owned_m3u_worker_fail BEFORE INSERT OR UPDATE ON dispatcharr_channels_channelgroup FOR EACH ROW EXECUTE FUNCTION rust_owned_m3u_worker_fail()')
    except Exception as error:injection_errors.append(type(error).__name__+':'+str(error)[:256])
    finally:connection.close();pg.preflight_release.set()
   injector=threading.Thread(target=install);injector.start()
  if mode in ('parent','claim','residual-confirm','events','parsed-cache-event','result','result-lost'):
   proxy_mode='events'if mode=='parsed-cache-event'else mode
   reply=RedisProxy(proxy_mode,parent,target,account.id,POLICY,channel_key)
   field={'parent':'result_url','claim':'broker_url','residual-confirm':'locks_url','events':'events_url','parsed-cache-event':'events_url','result':'result_url','result-lost':'result_url'}[mode]
   db=8 if mode=='claim'else 9 if mode in ('parent','result','result-lost')else 0
   cfg[field]='redis://:playback-fixture-only@127.0.0.1:61603/'+str(db)
  if mode=='result-denied':
   acl='owned_m3u_failure_'+str(account.id)
   f.cache.execute_command('ACL','SETUSER',acl,'on','>playback-fixture-only','~*','&*','+@all','-setex')
   case['ACL_preflight']=permission_profile(acl,f.app.backend.get_key_for_task(group))
   check(label+'-preflight-owned-ACL-profile',case['ACL_preflight']['owned_profile_exact'])
   check(label+'-preflight-SETEX-denied-without-write',case['ACL_preflight']['setex_denied'])
   check(label+'-preflight-PUBLISH-allowed-without-write',case['ACL_preflight']['publish_allowed'])
   check(label+'-preflight-dryrun-no-task-metadata-or-channel-mutation',f.raw_result(group)['metadata']is None and case['ACL_preflight']['nonmutating'])
   reply=RedisProxy('result-denied',parent,group,account.id,POLICY,channel_key)
   cfg['result_url']='redis://'+acl+':playback-fixture-only@127.0.0.1:61603/9'
  if mode=='old-SUCCESS':
   f.app.backend.store_result(group,[[],{'Default Group':{}}],'SUCCESS');case['old_success']=f.raw_result(group)
  peer=Peer(parent,account.id,label,**cfg)
  if mode=='events':
   answer=peer.command({'action':'run'});check(label+'-groups-failure-metadata-completed',answer['run'].startswith('Ok(Completed(')and f.raw_result(group)['metadata']['status']=='FAILURE')
  if mode in ('parent','claim','residual-confirm','group-error-ready','events','parsed-cache-event','result'):
   head=f.broker.lindex('celery',-1);check(label+'-start',peer.command({'action':'start'})=={'started':True})
   gate=pg.error_held if mode=='group-error-ready'else reply.held
   check(label+'-actual-target-reply-held',gate.wait(20))
   if mode=='group-error-ready':
    # Fixed existing6.2s SQL control, not a retuned completion budget.
    time.sleep(6.2);case['fixed_sleep_seconds']=6.2
   if injector:injector.join(2);check(label+'-injection-thread-settled',not injector.is_alive()and not injection_errors);injector=None
   raw=f.raw_result(target);case['held_backend']=raw;case['held_effects']=f.snapshot(account.id)
   if mode=='parent':check(label+'-peek-before-ACK-and-result',f.broker.lindex('celery',-1)==head and raw['metadata']is None)
   elif mode=='result':check(label+'-real-failure-result-already-stored',raw['metadata']['status']=='FAILURE')
   else:check(label+'-no-premature-task-metadata',raw['metadata']is None)
   if mode=='parsed-cache-event':
    cached=Path('/app/media/cached_m3u')/(str(account.id)+'.json');data=cached.read_bytes();case['actual_completed_cache_write']={'raw_base64':base64.b64encode(data).decode(),'bytes':len(data)}
    check(label+'-actual-cache-written-before-Group-SQL',json.loads(data)['extinf_data'][0]['name']=='Owned stream')
   observation=peer.command({'action':'abort'});case['observations'].append(observation)
   check(label+'-caller-admission-short-shutdown-retained',observation=={'caller_retained':True,'slots':0,'admission':'Err(Capacity)','drain':'Err(DrainTimeout)'})
   if pg:pg.error_release.set()
   if reply:reply.release.set()
   check(label+'-actual-completion-releases-owner',peer.command({'action':'settled'})=={'caller_released':True,'slots':1})
  elif mode=='result-lost':
   def lose_reply():
    if not reply.held.wait(20):injection_errors.append('ActualWriteReplyNotObserved')
    else:case['written_before_lost_reply']=f.raw_result(target)
    reply.release.set()
   injector=threading.Thread(target=lose_reply);injector.start();answer=peer.command({'action':'run'});case['run']=answer;injector.join(2);check(label+'-lost-reply-control-settled',not injector.is_alive()and not injection_errors);injector=None
   check(label+'-actual-AmbiguousResult',answer['run'].startswith('Err(Transport(AmbiguousResult('))
   check(label+'-actual-server-result-retained',case['written_before_lost_reply']['metadata']['status']=='FAILURE')
  else:
   answer=peer.command({'action':'run'});case['run']=answer
   if mode=='result-denied':
    case['denied_backend_before_assertion']=f.raw_result(group)
    case['denied_server_before_assertion']=list(reply.receipts)
    check(label+'-denied-is-operational-with-no-fake-metadata',answer['run'].startswith('Err(Transport(')and case['denied_backend_before_assertion']['metadata']is None)
   elif mode=='old-SUCCESS':
    check(label+'-old-SUCCESS-guard',answer['run'].startswith('Ok(ResultAlreadySucceeded(')and f.raw_result(group)['raw_base64']==case['old_success']['raw_base64'])
    check(label+'-old-SUCCESS-TTL-not-reset',0<f.raw_result(group)['ttl']<=case['old_success']['ttl'])
  case['native_results'][target]=f.raw_result(target);case['after_target']=f.snapshot(account.id);case['actual_native_commands']=list(peer.commands)
  if mode not in ('result-denied','old-SUCCESS'):
   value=f.app.AsyncResult(target).get(timeout=2,propagate=False)
   case['Original_decoder_type']=type(value).__module__+'.'+type(value).__name__
   case['Original_decoder_value']=list(value.args)if isinstance(value,BaseException)else value
   if mode=='events':check(label+'-Source-decodes-normal-Single-string',type(value)is str and value=='Failed to update m3u account - download failed or other error')
   else:
    expected=['rust-owned M3U worker failure\nCONTEXT:  PL/pgSQL function rust_owned_m3u_worker_fail() line 1 at RAISE']if sql_case else[2,'No such file or directory']
    check(label+'-Source-decodes-known-failure',isinstance(value,BaseException)and type(value).__name__==('ProgrammingError'if sql_case else'FileNotFoundError')and list(value.args)==expected)
  case['peer_close']=peer.close();peer=None
  if pg:case['pg']=pg.close();pg=None;check(label+'-PG-actual-close',case['pg']['physical_close_verified']and not case['pg']['errors'])
  if reply:case['redis']=reply.close();reply=None;check(label+'-Redis-actual-close',case['redis']['physical_close_verified']and not case['redis']['errors'])
  if mode=='result-denied':check(label+'-actual-write-attempt-one-permission-error',denied_write_observation(case['redis'],POLICY['result_write_lua_sha1']))
  if sql_case:drop_fault()
  if acl:f.cache.execute_command('ACL','DELUSER',acl);acl=None
  if mode=='result-lost':check(label+'-actual-write-not-replayed',case['redis']['target_replies']==1)
  case['tail']=consume_tail(parent,account.id,label)
  check(label+'-all-legitimate-children-consumed',f.broker.llen('celery')==f.broker.hlen('unacked')==0)
  key='task_lock_refresh_m3u_account_groups_'+str(account.id);case['residual_group_lock']={'raw_base64':base64.b64encode(f.cache.get(key)).decode(),'ttl':f.cache.ttl(key)}
  check(label+'-residual-owned-groups-key-not-deleted',1<=case['residual_group_lock']['ttl']<=300)
  case['after_tail']=f.snapshot(account.id);case['finished_utc']=utc();save()
 report['passed']=True;report['native_verified']=True
finally:
 cleanup_errors=[]
 for name,closer in [('PG',lambda:pg.close()if pg else None),('Redis',lambda:reply.close()if reply else None),('native',lambda:peer.abort_cleanup()if peer and not peer.closed else None),('Source',lambda:f.stop()if f else None)]:
  try:value=closer();report.setdefault('finally_closers',{})[name]=value
  except BaseException as error:cleanup_errors.append(name+':'+type(error).__name__)
 if injector:injector.join(2);cleanup_errors+=['injection-thread-unsettled']if injector.is_alive()else[]
 if acl and f:
  try:f.cache.execute_command('ACL','DELUSER',acl)
  except Exception as error:cleanup_errors.append('ACL:'+type(error).__name__)
 report['Source_parent_exit_receipts']=list(f.parent_exits)if f else[]
 report['cleanup_errors']=cleanup_errors
 if cleanup_errors:report['passed']=False
 save()
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))

if not report['passed']:raise SystemExit(1)
