"""Read-only physical5/6 predicate reconstruction. No app/socket/SQL calls."""
import base64,hashlib,json,uuid,re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal'}
def require(ok,label):
 if not ok:raise ValueError(label)
def equal(a,b):
 return type(a)is type(b)and(set(a)==set(b)and all(equal(a[k],b[k])for k in a)if isinstance(a,dict)else len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))if isinstance(a,list)else a==b)
def inventory(value):
 require(value['database']in(2,3)and type(value['rows'])is list and len(value['rows'])<=1000,'WholeOwnedBrokerBackend')
 keys=[]
 for row in value['rows']:
  key=bytes.fromhex(row['key_hex']);keys.append(key);require(len(key)<=1024 and row['dump_hex']is not None and type(row['pttl'])is int and row['pttl']>=-1,'RawKeyTypeDUMPAndTTL')
  require(row['type']in('string','list','set'),'DeclaredOwnedKeyTypes')
  required={'string':'value_hex','list':'list_hex','set':'set_hex'}[row['type']];require(required in row,'RawLiteralValueMembersRetained')
 require(len(keys)==len(set(keys)),'UniqueWholeKeySet')
def unchanged(a,b):
 inventory(a);inventory(b);require(a['database']==b['database'],'ExactDBBinding');left={r['key_hex']:r for r in a['rows']};right={r['key_hex']:r for r in b['rows']};require(set(left)==set(right),'AllKeysNoEffects')
 for key,row in left.items():
  other=right[key];require(equal({k:v for k,v in row.items()if k!='pttl'},{k:v for k,v in other.items()if k!='pttl'}),'AllTypeDUMPValueListMembersExact');x,y=row['pttl'],other['pttl'];require(x==y==-1 or 0<y<=x,'FiniteTTLMonotonic')
def sql(value):
 require(set(value)=={'tables','sequences'}and set(value['tables'])==set(value['sequences'])==TABLES,'ExactElevenSQLScopes')
 for row in value['sequences'].values():require(type(row['last_value'])is int and type(row['is_called'])is bool,'ExactSequencePrimitiveTypes')
def slots(value,held):
 require(type(value['calls'])is int and value['calls']==1 and value['logical_done']is True,'ActualRealAdapterLogicalCompletionBeforePhysicalProof')
 require(value['weak_observer_stores_no_Arc']is True and value['Weak_Some']is held,'ObservedOriginalCallerWeakNotInjectedOwner')
 require(value['real_application_http_slots_available']==(3 if held else 4)and value['real_jobs_slots_available']==(3 if held else 4),'RealExistingFourSlots')
def payload(encoded):
 raw=bytes.fromhex(encoded);require(len(raw)<=65536,'BoundedActualFrame');frame=json.loads(raw);id=uuid.UUID(frame['headers']['id']);require(id.version==4,'ActualOwnUUID4')
 require(frame['headers']['task']=='apps.backups.tasks.create_backup_task'and frame['headers']['root_id']==str(id)and frame['properties']['correlation_id']==str(id),'SourceCreateTaskAndIDRelations')
 require(equal(frame['properties']['delivery_info'],{'exchange':'','routing_key':'celery'}),'ActualConfiguredRouteMatchesLPUSH')
 require(equal(json.loads(base64.b64decode(frame['body'],validate=True)),[[],{}, {'callbacks':None,'errbacks':None,'chain':None,'chord':None}]),'ActualSourceBackedCreateBody')
 return str(id),raw

def validate(report,case,output,*,allow_synthetic=False):
 require(allow_synthetic or report.get('_synthetic_only')is not True,'ActualCannotBeSynthetic')
 require(re.fullmatch(r'rust_probe_backup_jobs_physical_[a-f0-9]{10}',report['database'])is not None,'ExactOwnedDBReport')
 require(case in(5,6)and type(report['case'])is int and report['case']==case and type(report['request_count'])is int and report['request_count']==1,'FiniteOnlyCase5Or6OneRequest')
 require(report['passed']is True and report['capture_complete']is True and report['phase']=='complete','CompletePhysicalCapture')
 require(report['production_changes']is False and report['Source_app_runs']==report['Main_runs']==report['worker_polls']==0,'NoSourceMainWorkerOrProductionChanges')
 require(report['weak_only_observer']is True and report['FD_loss_is_not_handler_abort']is True,'DistinctActualCallerFates')
 slots(report['logical_completion'],True);slots(report['while_held_after_logical_completion'],True);slots(report['after_physical_reply'],False)
 for name in('before','held_raw_state','after'):sql(report[name]['SQL'])
 require(equal(report['before']['SQL'],report['held_raw_state']['SQL'])and equal(report['before']['SQL'],report['after']['SQL']),'AllElevenTablesSequencesUnchanged')
 require(equal(report['before']['credentials'],report['held_raw_state']['credentials'])and equal(report['before']['credentials'],report['after']['credentials']),'NoCredentialEventMaskingOrMutation')
 contract=json.loads((ROOT/'inputs/resp-contracts.json').read_text())['publish'if case==5 else'read']
 rows=[json.loads(line)for line in(output/'proxy-events.jsonl').read_text().splitlines()];require(len(rows)<=2048,'BoundedActualWireLedger')
 held=[r for r in rows if r['kind']=='backend-target-reply-read-held'];released=[r for r in rows if r['kind']=='backend-target-reply-released'];require(len(held)==len(released)==1,'OneActualPositiveTargetReplyNoReplay')
 held,released=held[0],released[0];reply=bytes.fromhex(held['reply_hex']);require(hashlib.sha256(reply).hexdigest()==held['reply_sha256'],'ActualHeldReplyHash')
 forwarded=[r for r in rows if r['kind']=='reply-forwarded'and r['target']is True and r['reply_sha256']==held['reply_sha256']];require(len(forwarded)==1,'ActualHeldReplyEventuallyForwardedOnce')
 require(0<=released['monotonic']-held['monotonic']<=25 and held['monotonic']<=released['monotonic']<=forwarded[0]['monotonic'],'ActualHoldReleaseForward25')
 actual_requests=[]
 for row in rows:
  if row['kind']=='request-read'and row['target']is True:
   args=[bytes.fromhex(v)for v in row['target_arguments_hex']];require(len(args)==(7 if case==5 else 4),'ExactLuaArgumentCount');require(args[0].upper()in(b'EVAL',b'EVALSHA'),'ActualLuaNotStandaloneGET')
   sha=args[1].decode()if args[0].upper()==b'EVALSHA'else hashlib.sha1(args[1]).hexdigest();require(sha==contract['sha1']and args[2]==str(len(contract['keys_hex'])).encode()and args[3:3+len(contract['keys_hex'])]==[bytes.fromhex(k)for k in contract['keys_hex']],'ExactCompiledLuaSHAAndKeys');actual_requests.append(args)
 require(actual_requests and any(r['kind']=='request-forwarded'and r['target']is True for r in rows),'ActualTargetCommandForwarded')
 before,held_state,after=(report[n]for n in('before','held_raw_state','after'))
 if case==5:
  require(report['actual_client_FD_shutdown_and_drop']is True and report['handler_abort_claimed']is False,'ActualFDLossNotHandlerAbort')
  fd=report['actual_client_FD_receipt'];require(type(fd['fd'])is int and type(fd['original_link'])is str and re.fullmatch(r'socket:\[[0-9]+\]',fd['original_link'])is not None and fd['original_FD_absent_or_reused']is True and fd['after_link']!=fd['original_link'],'ActualObservedClientFDGone')
  require(report['before_FD_loss']['logical_done']is False and report['pre_FD_loss_read']['successful_HTTP_body_or_status_observed']is False and'WouldBlock'in report['pre_FD_loss_read']['result'],'FDLossBeforeActualLogicalCompletionNoEarlyHTTP')
  require(reply==b':1\r\n','CommittedFirstLPUSHReply')
  require(before['all_broker']['rows']==[] and len(held_state['all_broker']['rows'])==2,'OnlyNewQueueAndBinding')
  unchanged(held_state['all_broker'],after['all_broker']);unchanged(before['all_backend'],held_state['all_backend']);unchanged(before['all_backend'],after['all_backend'])
  values=held_state['broker_queue']['value']['list_hex'];require(len(values)==1,'ExactlyOneUnconsumedCreateFrame');id,raw=payload(values[0]);require(id==report['published_owned_task_id']and hashlib.sha256(raw).hexdigest()==report['published_unconsumed_create_frame_sha256'],'ActualProducerFrameIDHash')
  require(all(args[5]==raw and args[6]==b'1000'for args in actual_requests),'ActualLuaARGVBoundToCommittedFrame')
 else:
  response=report['HTTP_response_while_reply_held'];raw=bytes.fromhex(response['body_hex']);require(response['status']==500 and response['Source_HTTP_error_parity_claimed']is False and len(raw)==response['bytes']and hashlib.sha256(raw).hexdigest()==response['sha256'],'ActualNativeLogicalDeadlineHTTP500NotSourceErrorClaim')
  unchanged(before['all_broker'],held_state['all_broker']);unchanged(before['all_broker'],after['all_broker']);unchanged(before['all_backend'],held_state['all_backend']);unchanged(before['all_backend'],after['all_backend'])
  value=bytes.fromhex(before['backend']['value']['value_hex']);expected=b'*2\r\n:1\r\n$'+str(len(value)).encode()+b'\r\n'+value+b'\r\n';require(reply==expected,'ActualREADLuaReplyBoundToWholeSeededBackend')
  require(equal(json.loads(value),report['Source16_result_seed_context']),'LiteralSource16BackendFixtureContext')
 require(report['proxy_connections_absent_before_stop']['connections']==0 and report['proxy_stop']['connections']==0 and report['proxy_stop']['listener_joined']is True and report['proxy_stop']['invalid']==[],'ActualRelayClientEndpointsThenListenerSettled')
 receipt=json.loads((output/'proxy-process.json').read_text());require(type(receipt['exit'])is int and type(receipt['wait'])is int and receipt['exit']==receipt['wait']==0 and receipt['signal']is None and receipt['forced']is False and receipt['proc_PID_absent_after_wait']is True and receipt['stdout_reader_joined']is True and type(receipt['starttime'])is str and receipt['starttime'].isdigit(),'ActualProxyWaitPipesPIDNoForcedKill')
 require(type(report['closed_pool_size'])is int and report['closed_pool_size']==0,'ActualSinglePoolCloseSize0')
 require(type(report['closed_pool_idle'])is int and 0<=report['closed_pool_idle']<=4,'SourceBoundFiniteResidualIdleDiagnostic')
 return {'passed':True,'physical_case':case,'actual_requests':1,'real_adapter_logical_completion':True,'original_caller_retained_until_actual_reply_then_released':True,'actual_Lua_wire_and_reply_validated':True,'Source_or_Main_runs':0,'Root_cases1_4_replayed':False,'full_M8_or_production_bug_claimed':False,'original_idle0_gate_unmet_preserved':True,'closed_idle_diagnostic_literal':report['closed_pool_idle'],'compiled_final_pool_max':4,'qualification_kind':'OFFLINE_SOURCE_DERIVED_ORACLE_CORRECTION_NOT_ORIGINAL_CONTROLLER_PASS','no_HTTP_FD_loss_equals_handler_abort_claim':True}
