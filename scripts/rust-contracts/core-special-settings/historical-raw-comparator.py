"""Recompute Native51 component predicates from complete raw cohort captures.
No Source call or application import. Expected data is retained Source51 only.
Opaque full-SQL digests establish within-epoch equality/change sets; source and
native user/date/receiver/cache-version epochs are deliberately not byte equal.
"""
import datetime,hashlib,json,math
from pathlib import Path
GROUPS=['system_settings','network_access','reverse_proxy_auth','proxy_settings','stream_settings','dvr_settings']
def need(value,label):
 if not value:raise ValueError(label)
def changed(before,after):return sorted(k for k in before if before[k]!=after[k])
def notices(rows):return [{k:v for k,v in r.items()if k not in('created_at','updated_at')}for r in rows]
def tasks(snap):return {r['raw_frame_sha256']:r for r in snap['broker']['frames']}
def channels(snap):return {r['raw_sha256']:r for r in snap['Channels']['frames']}
def conditions(snap):
 return {r['key_hex']:r for db in snap['raw_Redis']for r in db['rows']if bytes.fromhex(r['key_hex']).startswith(b':1:dev_notif_condition_')}
def source_bindings(reference):
 return {g:{'source_path':str(reference/(g+'-source-private.json')),'sha256':hashlib.sha256((reference/(g+'-source-private.json')).read_bytes()).hexdigest()}for g in GROUPS}
def compare(source,native,allow_controls=False):
 need(allow_controls or not native.get('Synthetic_unit_data_ONLY_not_witness'),'NoSyntheticNativeWitness')
 proofs=[]
 def check(value,label):need(value,label);proofs.append(label)
 check(source['capture_valid']is True and source['passed']is False and native['capture_valid']is True and native['passed']is False,'RawCaptureOnlyAndComplete')
 a=source['requests'];b=native['requests'];check([r['id']for r in a]==[r['id']for r in b] and len(a)>0,'ExactFiniteCaseCoverage')
 check(native['Source_API_requests']==0 and native['Native_API_requests']==native['completed_capture_count']==len(b),'ActualNativeOnlyCount')
 check(native['Native_peer']['actual_exit']==0 and native['Native_peer']['physically_settled']is True and native['Native_peer']['pipes_settled']is True and native['Native_peer']['PID_original_gone']is True,'ActualPeerWaitPipesAndPID')
 check(native['observer_clients_closed']is None and native['connections_close_called']is True and not native.get('observer_cleanup_errors'),'AllObserversRetired')
 control=native['control_requests'];expected=['/__special_state__']
 for _ in b:
  expected+=['/__special_state__','/__special_warm_group__']
  if native['group']in('proxy_settings','stream_settings'):expected+=['/__special_warm_proxy__'if native['group']=='proxy_settings'else'/__special_warm_ua__']
  expected+=['/__special_state__']*3
 expected+=['/__special_state__','/__special_stop__']
 check([c['path']for c in control]==expected and native['Native_control_requests']==len(control),'ExactControlJournalCoverageAndOrder')
 for ordinal,c in enumerate(control):
  check(c['actual_request_attempted']is True and c.get('status')==200 and c.get('physical_close',{}).get('original_inode_absent')is True and c['physical_close'].get('timer_joined')is True and not c.get('exception')and not c.get('close_exception'),'ControlPhysical:'+str(ordinal))
  if c['path'].startswith('/__special_warm_'):check(c['json'].get('loaded')is True,'ActualWarmCompleted:'+str(ordinal))
  if c['path']=='/__special_warm_group__':check(c['json'].get('group')==native['group'],'ActualWarmGroupIdentity:'+str(ordinal))
  if c['path']=='/__special_stop__':check(c['json']=={'stop_requested':True},'ActualStopResponse')
  raw=bytes.fromhex(c['response_body_hex']);check(len(raw)==c['response_body_bytes']<=65536 and hashlib.sha256(raw).hexdigest()==c['response_body_sha256']and json.loads(raw)==c['json'],'ActualControlBodyAndJSON:'+str(ordinal))
 health=[c for c in control if c['path']=='/__special_state__']
 snapshots=[native['after_setup']]
 for row in b:snapshots +=[row['declared_pre_request_warm']['before'],row['declared_pre_request_warm']['after'],row['before'],row['after']]
 snapshots +=[native['terminal_before_receiver_cleanup']]
 check(len(health)==len(snapshots),'CompleteLocalStateReadoutCoverage')
 for ordinal,(wire,snapshot)in enumerate(zip(health,snapshots)):
  value=wire['json'];check(type(value)is list and len(value)==2 and type(value[0])is dict,'LiteralNativeReadoutType:'+str(ordinal))
  check(snapshot['local']['display_zone']==value[0]['display_zone']and snapshot['local']['process_proxy_cache']==value[1],'ProcessStateFromActualNativeWire:'+str(ordinal))
 for s,n in zip(a,b):
  tag=str(s['id'])
  for key in ('id','name','group','role','method','request_body_hex','body_sha256'):
   check(n[key]==s[key],'LiteralInput:'+tag+':'+key)
  check(n['path']==s['path'],'ActualNaturalIdentityPath:'+tag)
  check(n['actual_TCP_source_ip']==n['observed_socket_source'][0]==s['REMOTE_ADDR'] and n['observed_socket_peer'][0]=='127.0.0.1','ActualBoundActorIP:'+tag)
  raw=bytes.fromhex(n['response_body_hex']);check(hashlib.sha256(raw).hexdigest()==n['response_body_sha256']and len(raw)==n['response_body_bytes']<=65536,'WholeBodyHash:'+tag)
  check(n['status']==s['status']and n['response_body_hex']==s['response_body_hex'],'LiteralStatusBodyParity:'+tag)
  check(next((v for k,v in n['headers'].items()if k.lower()=='content-type'),None)==next((v for k,v in s['headers'].items()if k.lower()=='content-type'),None),'ContentTypeParity:'+tag)
  check(n['physical_close']['fd_after']==-1 and n['physical_close']['original_inode_absent']is True and n['physical_close']['timer_joined']is True,'APISocketTimerClosed:'+tag)
  check(n['capture_complete']is True and math.isfinite(n['began_monotonic'])and n['completed_monotonic']>=n['began_monotonic'] and n['completed_monotonic']-n['began_monotonic']<=8,'OwnHTTPClock:'+tag)
  sb,sa=s['before'],s['after'];nb,na=n['before'],n['after']
  check(nb['SQL']['settings']==sb['SQL']['settings']and na['SQL']['settings']==sa['SQL']['settings'],'ActualSettingsRows:'+tag)
  check(notices(nb['SQL']['notices'])==notices(sb['SQL']['notices'])and notices(na['SQL']['notices'])==notices(sa['SQL']['notices']),'NoticeLiteralScalars:'+tag)
  source_before={v['id']:v for v in sb['SQL']['notices']};native_before={v['id']:v for v in nb['SQL']['notices']}
  for source_notice,native_notice in zip(sa['SQL']['notices'],na['SQL']['notices']):
   previous=source_before.get(source_notice['id']);own_previous=native_before.get(native_notice['id'])
   if previous is not None and previous['created_at']==source_notice['created_at']:
    check(own_previous is not None and own_previous['created_at']==native_notice['created_at'],'NoticeCreatedAtRetained:'+tag)
   if previous is None or previous['updated_at']!=source_notice['updated_at']:
    updated=datetime.datetime.fromisoformat(native_notice['updated_at']).timestamp()
    check(n['began_epoch']<=updated<=n['completed_epoch'],'NoticeUpdatedOwnClock:'+tag)
  for part in ('columns','indexes','constraints'):
   check(nb['SQL']['full6259'][part]==na['SQL']['full6259'][part]==sb['SQL']['full6259'][part]==sa['SQL']['full6259'][part],'NoSchemaMutation:'+tag+':'+part)
  for part in ('tables','sequences'):
   check(set(nb['SQL']['full6259'][part])==set(na['SQL']['full6259'][part])==set(sb['SQL']['full6259'][part])and len(nb['SQL']['full6259'][part])==(62 if part=='tables'else 59),'MandatorySQLCoverage:'+tag+':'+part)
   check(changed(nb['SQL']['full6259'][part],na['SQL']['full6259'][part])==changed(sb['SQL']['full6259'][part],sa['SQL']['full6259'][part]),'FullSQLDeltaSet:'+tag+':'+part)
  for key,old in sb['named_group_cache'].items():
   expected=sa['named_group_cache'][key];before=nb['named_group_cache'][key];after=na['named_group_cache'][key]
   if ':groupver:'in key:
    check((old is None and before is None)or(type(old)is int and type(before)is int and before>0),'BeforeVersionRepresentation:'+tag+':'+key)
    check((expected is None and after is None)or(type(expected)is int and type(after)is int and after>0),'AfterVersionRepresentation:'+tag+':'+key)
   if ':groupver:'in key:
    if old==expected:check(before==after,'UnchangedVersion:'+tag+':'+key)
    else:
     check(type(after)is int and after>0 and after!=before,'ActualVersionBump:'+tag+':'+key)
     check(int(n['began_epoch']*1e9)<=after<=int(n['completed_epoch']*1e9),'VersionOwnRequestClock:'+tag+':'+key)
   else:check(before==old and after==expected,'GroupCacheLiteralValue:'+tag+':'+key)
  for key in ('display_zone','process_proxy_cache','owned_log_files'):
   def local(v):
    value=v['local'][key]
    if key=='owned_log_files':return [{k:x for k,x in item.items()if k in('relative','bytes','sha256','content_hex')}for item in value]
    return value
   check(local(nb)==local(sb)and local(na)==local(sa),'ActualProcessFileEffect:'+tag+':'+key)
  from observables import brokers
  for label,snapshot in [('before',nb),('after',na)]:
   check(snapshot['broker']==brokers(snapshot['raw_Redis']),'BrokerDerivedFromLiteralRedis:'+tag+':'+label)
   channel=snapshot['Channels'];db=next(r for r in snapshot['raw_Redis']if r['database']==channel['Redis_database']);entries={r['key_hex']:r for r in db['rows']};mail=entries.get(channel['channel_key_hex']);actual=[]if mail is None else mail['zset'];frames=channel['frames']
   check(len(actual)==len(frames),'ChannelsRawRedisCoverage:'+tag+':'+label)
   for frame,member in zip(frames,actual):
    raw=bytes.fromhex(frame['raw_hex']);check(frame['raw_hex']==member['member_hex']and frame['raw_sha256']==hashlib.sha256(raw).hexdigest()and frame['raw_bytes']==len(raw),'ChannelsRawHashAndMembership:'+tag+':'+label)
  # Source's five new publications are compared per actual DB/queue, without
  # double-counting retained frames from earlier cohort databases.
  new_s=[v for k,v in tasks(sa).items()if k not in tasks(sb)];new_n=[v for k,v in tasks(na).items()if k not in tasks(nb)]
  check(len(new_s)==len(new_n),'NewBrokerFrameCount:'+tag)
  for x,y in zip(new_s,new_n):
   check(x['database']==y['database']and x['queue_key_hex']==y['queue_key_hex']and x['headers']['task']==y['headers']['task'],'RealBrokerTaskRoute:'+tag)
   # Token/UUID are newly generated Native values. Preserve their relationships
   # and prove them against actual raw Redis, never copy the Source token.
   xb,yb=x['decoded_body'],y['decoded_body'];check(xb[0]==yb[0],'ActualTaskArguments:'+tag)
   xkw,ykw=dict(xb[1]),dict(yb[1]);token=ykw.pop('preacquired_lock_token',None);xkw.pop('preacquired_lock_token',None);check(xkw==ykw,'ActualTaskKeywords:'+tag)
   if token is not None:
    check(type(token)is str and len(token)==32 and all(c in '0123456789abcdef'for c in token),'NativeLockTokenShape:'+tag)
    db=next(r for r in na['raw_Redis']if r['database']==y['database']);values={bytes.fromhex(r['key_hex']):r for r in db['rows']}
    for account in ykw['preacquired_lock_ids']:
     lock=values[('task_lock_refresh_single_m3u_account_'+str(account)).encode()];check(lock['type']=='string'and bytes.fromhex(lock['value_hex']).decode()==token and 0<lock['pttl']<=7200000,'ActualOwnedHandoffLock:'+tag+':'+str(account))
  cs=[v['actual_layer_decoded']for k,v in channels(sa).items()if k not in channels(sb)];cn=[v['actual_layer_decoded']for k,v in channels(na).items()if k not in channels(nb)]
  strip=lambda row:{k:v for k,v in row.items()if k!='__asgi_channel__'}
  check([strip(v)for v in cs]==[strip(v)for v in cn],'RealChannelsEnvelopeDelta:'+tag)
  check(len(conditions(nb))==len(conditions(sb))and len(conditions(na))==len(conditions(sa)),'ConditionCacheEffectCount:'+tag)
  if n['group']=='network_access':
   expected_key=(':1:dev_notif_condition_owned_special_'+native['nonce']).encode().hex();check(set(conditions(nb))=={expected_key},'ActualOwnedConditionSentinel:'+tag)
   check(set(conditions(na))==({expected_key}if conditions(sa)else set()),'ActualOwnedConditionAfter:'+tag)
 return {'passed':True,'predicates':len(proofs),'checks':proofs,'case_ids':[r['id']for r in b],'Source_API_requests':0,'Main_or_consumers_or_downstream_or_fullM5_qualified':False}
