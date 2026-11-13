"""Predeclared actual PluginRepo metadata4 gates; no Source/Main transfer."""
import copy,datetime,hashlib,json,math,struct,re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
from public_inputs import load_source,load_contracts
NAMES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal','plugins_pluginrepo','plugins_pluginconfig'}
def need(ok,label):
 if not ok:raise ValueError(label)
def exact(a,b):
 if type(a)is not type(b):return False
 if type(a)is dict:return set(a)==set(b)and all(exact(a[k],b[k])for k in a)
 if type(a)is list:return len(a)==len(b)and all(exact(x,y)for x,y in zip(a,b))
 return a==b
def integer(v,lo,hi):return type(v)is int and lo<=v<=hi
def finite(v,lo,hi):return type(v)in(int,float)and math.isfinite(v)and lo<=v<=hi
def clock(s):
 need(type(s)is str,'ActualUTCString');v=datetime.datetime.fromisoformat(s.replace('Z','+00:00'));need(v.utcoffset()==datetime.timedelta(0),'ActualUTC');return v
def keyrow(snapshot,table,id):
 rows=[r for r in snapshot['tables'][table]if type(r['id'])is int and r['id']==id];need(len(rows)==1,'ExactlyOneTarget');return rows[0]
def delta(before,after,case):
 for snap in(before,after):need(set(snap['tables'])==set(snap['sequences'])==NAMES,'Exact13TablesAndSequences')
 expected=copy.deepcopy(before)
 if case==1:
  row=keyrow(expected,'plugins_pluginrepo',801);changed=keyrow(after,'plugins_pluginrepo',801);need(clock(row['updated_at'])!=clock(changed['updated_at']),'RealUpdateClockEvenSameValue');row.update(public_key='owned-physical-update',updated_at=changed['updated_at'])
 elif case==2:
  need(len([r for r in expected['tables']['plugins_pluginrepo']if r['id']==801])==1,'DeleteTargetPresentBefore');expected['tables']['plugins_pluginrepo']=[r for r in expected['tables']['plugins_pluginrepo']if r['id']!=801]
  children=[r for r in expected['tables']['plugins_pluginconfig']if r['source_repo_id']==801];source=load_source();source_children=[r['id']for r in source['seed']['configs']if type(r['source_repo_id'])is int and r['source_repo_id']==801];need(exact([r['id']for r in children],source_children),'ExactlyBothSourceDerivedOwnedChildren901902')
  for row in children:row['source_repo_id']=None
 need(exact(expected,after),'OnlyIntendedMetadataRowsAll13SeqUnchanged')
def dumps(before,after,allow_group_fills):
 a,b=before['raw_DUMP_PTTL'],after['raw_DUMP_PTTL'];allowed={x.hex()for x in(b':1:coresettings:group:network_access',b':1:coresettings:group:system_settings')}if allow_group_fills else set()
 need(set(a)<=set(b)and set(b)-set(a)<=allowed,'RawDumpExactKeySetOrTwoDeclaredGroupFills')
 for key,left in a.items():
  right=b[key];need(set(left)==set(right)=={'dump_hex','pttl'},'LiteralDumpFields');need(type(left['dump_hex'])is str and left['dump_hex']==right['dump_hex'],'RawDumpUnchanged');x,y=left['pttl'],right['pttl'];need(integer(x,-1,2**63-1)and integer(y,-1,2**63-1)and(x==y==-1 or 0<=y<=x),'TTLNoResetTypedMonotonic')
def parameters(actual,expected):
 need(type(actual)is list and len(actual)==len(expected),'ExactParameters')
 for a,e in zip(actual,expected):
  if type(e)is dict:
   need(exact(e,{'timestamptz_binary8':True})and type(a)is str and len(bytes.fromhex(a))==8,'ActualGeneratedTimestampBinary8')
  else:need(exact(a,e),'ActualStableParameterBytes')
def wire(held,want,stage,pid):
 need(held['stage']==stage and integer(held['backend_pid'],1,2**31-1)and held['backend_pid']==pid,'SameActualPreparedBackend')
 need(held['sql']==want['sql']and hashlib.sha256(held['sql'].encode()).hexdigest()==want['sql_sha256'],'FullHeldSQLFingerprintNotPgStatPrefix')
 for name in('rows','errors','tx'):need(exact(held[name],want[name]),'ExactActualWire:'+name)
 need(exact(held['commands'],[]if want['command']is None else[want['command']])and held['sync_seen']is True,'ActualExecuteCommandErrorSyncReady');parameters(held['parameter_hex'],want['parameter_hex'])
def duration(events,stage,minimum=.25):
 a=[e for e in events if e['kind']=='ready-held'and e['stage']==stage];b=[e for e in events if e['kind']=='ready-forwarded'and e['stage']==stage];need(len(a)==len(b)==1,'OneActualHeldAndForwardedPerStage')
 start,end=a[0]['monotonic'],b[0]['monotonic'];need(finite(start,0,float('inf'))and finite(end,start,float('inf'))and finite(end-start,minimum,25),'FiniteOrderedActualHeldDuration250msTo25');return end-start

def sample_pair(rows,held,hold):
 need(finite(hold,.25,25),'TypedFiniteBoundedActualHold')
 need(type(rows)is list and len(rows)==2,'ExactTwoSustainedSamples')
 for s in rows:
  need(finite(s['elapsed_monotonic_seconds'],0,25)and exact(s['original_weak_alive'],[True])and integer(s['writer_slots'],3,3)and exact(s['actual_held'],held),'SustainedActualOriginalWeakAndThreeSlots')
 span=rows[1]['elapsed_monotonic_seconds']-rows[0]['elapsed_monotonic_seconds'];need(finite(span,.25,25)and hold>=span,'ActualHoldCoversSamples')
def pool(v):
 need(integer(v['closed_pool_size'],0,0)and integer(v['closed_pool_idle_diagnostic'],0,4)and integer(v['pool_max_connections'],4,4)and integer(v['actual_pool_reuse_value'],424242,424242),'ActualFirstCloseSize0Max4IdleDiagnosticAndPoolReuse')
def existing_sql_instant(value):
 need(type(value)is str and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)',value)is not None,'PinnedSQLTimestampUTCStringMaxSixFractionDigits')
 return clock(value)
def imported_rows(table,expected,actual,columns):
 need(type(expected)is list and type(actual)is list and len(expected)==len(actual),'ExactImportedRowCount:'+table)
 timestamp_columns={row[1]for row in columns if row[0]==table and row[2]=='timestamp with time zone'}
 ledger=[]
 for source_row,actual_row in zip(expected,actual):
  need(type(source_row)is dict and type(actual_row)is dict and set(source_row)==set(actual_row),'ExactImportedFieldSet:'+table)
  for key,left in source_row.items():
   right=actual_row[key]
   if key in timestamp_columns and left is not None:
    need(existing_sql_instant(left)==existing_sql_instant(right),'ExactImportedExistingTimestampInstant:'+table+'.'+key)
    if left!=right:ledger.append({'table':table,'id':source_row['id'],'column':key,'Source_literal':left,'Native_literal':right,'same_UTC_microsecond_instant':True})
   else:need(exact(left,right),'ExactImportedStableFieldTypeValue:'+table+'.'+key)
 return ledger
def imported_seed(before,source):
 need(source['actual_request_count']==24 and source['force_authenticate_used']is False,'UntouchedSource24ReferenceOnly');seed=source['seed'];mapping={'repos':'plugins_pluginrepo','configs':'plugins_pluginconfig','settings':'core_coresettings','users':'accounts_user','events':'core_systemevent','system_notifications':'core_systemnotification','notification_dismissals':'core_notificationdismissal'}
 ledger=[]
 for key,table in mapping.items():
  if table in NAMES:ledger+=imported_rows(table,seed[key],before['tables'][table],seed['public_manifest']['columns'])
 for table in NAMES-set(mapping.values()):need(exact(before['tables'][table],[]),'NoForeignUnseededRows')
 for table,row in before['sequences'].items():need(exact([row['last_value'],row['is_called']],seed['public_manifest']['sequences'][table+'_id_seq']),'SourceInitialSequence:'+table)
 return ledger

def validate(v,case,out):
 need(integer(case,1,4)and type(v['case'])is int and v['case']==case and v.get('_synthetic_only')is not True and v['capture_complete']is True and v['passed']is True and v['Main']is False and integer(v['Source_requests'],0,0),'ActualFixedCaseOnlyNoSourceOrMain')
 imported_clock_lexemes=imported_seed(v['before'],load_source())
 delta(v['before'],v['after'],case);need(exact(v['credentials_before'],v['credentials_after']),'ExactCredentialHistoryNoEffects');need(integer(v['writer_slots_after_settlement'],4,4),'ActualFourSlotsReturned');need(exact(v['original_weak_after_settlement'],[False]*(5 if case==4 else 1)),'OriginalActualWeakRetired');pool(v['pool']);need(exact(v['while_ELF_alive_subject_backends'],[]),'WhileELFSubjectBackendObserverEmpty')
 if case==4:
  need(exact(v['actual_statuses'],[200,200,401,401,503,200]),'ActualFrameStatuses');need(len(v['actual_first_four'])==4,'FourActualFrames')
  for i,e in enumerate(v['actual_first_four']):
   need(integer(e['request'],i+1,i+1)and integer(e['status'],v['actual_statuses'][i],v['actual_statuses'][i])and integer(e['actual_frame_bytes'],1,1048576)and type(e['actual_frame_sha256'])is str and len(e['actual_frame_sha256'])==64 and exact(e['original_weak'],[True]*(i+1))and integer(e['available_slots'],3-i,3-i),'ActualSuccessAndAuthErrorFrameClones')
  need(exact(v['fifth_SQL_before'],v['fifth_SQL_after'])and exact(v['fifth_credentials_before'],v['fifth_credentials_after']),'FifthActualSQLAndCredentialSnapshotsNoEffects')
  need(exact(v['fifth_original_weak'],[True]*4)and integer(v['fifth_slots'],0,0)and exact(v['sixth_original_weak'],[False,True,True,True,True])and integer(v['sixth_slots'],0,0),'FifthNoAdmissionSixthRealReuse')
  for e in v['actual_first_four']:
   raw=bytes.fromhex(e['actual_frame_hex']);need(len(raw)==e['actual_frame_bytes']and hashlib.sha256(raw).hexdigest()==e['actual_frame_sha256'],'ActualRetainedFrameBytesAndHash')
  raw=bytes.fromhex(v['sixth_actual_frame_hex']);need(1<=len(raw)<=1048576 and hashlib.sha256(raw).hexdigest()==v['sixth_actual_frame_sha256'],'SixthActualFrameBytes')
  for i,e in enumerate(v['actual_first_four']):
   observed=json.loads((Path(out)/('actual-response-frame-'+str(i+1)+'.json')).read_text());need(observed['capture_complete']is True and observed['frame_observed']is True and observed['status']==e['status']and observed['frame_bytes']==e['actual_frame_bytes']and observed['frame_hex']==e['actual_frame_hex']and observed['frame_sha256']==e['actual_frame_sha256'],'IndependentActualPolledFrameReceipt')
  observed=json.loads((Path(out)/'actual-response-frame-6.json').read_text());need(observed['status']==200 and observed['frame_hex']==v['sixth_actual_frame_hex']and observed['frame_sha256']==v['sixth_actual_frame_sha256'],'ActualSixthFrameReceipt')
  refusal=json.loads((Path(out)/'actual-response-fifth-refusal.json').read_text());raw=bytes.fromhex(v['fifth_actual_body_hex']);need(refusal['status']==503 and refusal['body_hex']==v['fifth_actual_body_hex']and refusal['body_bytes']==len(raw)and refusal['body_sha256']==hashlib.sha256(raw).hexdigest(),'ActualPreauthFifthRawResponseReceipt')
  dumps(v['fifth_raw_cache_before'],v['fifth_raw_cache_after'],False)
  return {'passed':True,'case':case,'imported_SQL_timestamp_lexical_differences_same_UTC_instant':imported_clock_lexemes,'scope':'actual original success/error frames +preauth refusal and reuse; inprocess Router not Main/TCP'}
 contracts=load_contracts();warm=v['actual_warm_and_pg_setting'];need(warm['observed_before_any_hold']is True and integer(warm['actual_backend_pid'],1,2**31-1)and integer(warm['track_activity_query_size_bytes'],100,1048576),'ActualSettingBeforeHold');need(exact(warm['warm_before'],warm['warm_after'])and exact(warm['warm_before'],v['before']),'MissingPKWarmNoEffects')
 need(len(warm['same_connection_exact_warm_SQL'])==len(warm['full_warm_SQL_sha256'])==6,'ExactWarmStatements');need(all(hashlib.sha256(s.encode()).hexdigest()==h for s,h in zip(warm['same_connection_exact_warm_SQL'],warm['full_warm_SQL_sha256'])),'ActualFullWarmFingerprint');need(len(set(warm['same_connection_exact_warm_SQL']))==6,'DistinctRealWarmStatements')
 stats=v['actual_relay_terminal'];need(stats['errors']==[]and stats['listener_alive']is False,'RealRelaySettledNoErrors');drain=v['relay_naturally_drained_before_stop'];need(all(c['closed']is True and c['up_joined']is True for c in drain['connections'])and len(drain['connections'])>0,'NaturalConnectionDrainBeforeStop')
 stages={1:[('update_ready','first_held','post_observer_loss_samples')],2:[('child_ready','first_held','post_socket_loss_samples'),('commit_ready','commit_held','commit_held_samples')],3:[('child_ready','first_held',None),('parent_error_ready','parent_57014_held','error_ready_samples'),('rollback_ready','rollback_held','rollback_ready_samples')]}[case]
 for stage,key,samples in stages:
  h=v[key]['held'];wire(h,contracts[stage],stage,warm['actual_backend_pid']);need(h['sql']in warm['same_connection_exact_warm_SQL'],'SameFullWarmSQLReachedExecuteReady')
  hold=duration(stats['events'],stage,.25 if samples else 0)
  if samples:sample_pair(v[samples],h,hold)
 if case==1:
  need(v['actual_direct_observer_abort']is True and v['observer_returned_before_update_ready_release']is False,'ActualObserverAbortNoEarlyHTTP');delta(v['before'],v['while_update_ready_held_committed_snapshot'],1)
  raw=bytes.fromhex(v['first_held']['held']['parameter_hex'][3]);stamp=datetime.datetime(2000,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=struct.unpack('!q',raw)[0]);need(clock(v['request_began_utc'])<=stamp<=clock(v['request_completed_utc'])and stamp==clock(keyrow(v['after'],'plugins_pluginrepo',801)['updated_at']),'ActualGeneratedBindTimestampOwnClockAndSavedRow')
 elif case==2:
  need(exact(v['while_child_ready_held_uncommitted_snapshot'],v['before']),'ChildSetNullUncommittedInvisible')
  for key in ('while_child_ready_held_HTTP_fate','while_commit_ready_held_HTTP_fate'):need(exact(v[key]['response_statuses'],[])and v[key]['capacity_exceeded']is False,'NoHTTPResponseBeforeActualReady:'+key)
  need(v['actual_client_fd_shutdown_drop']is True and v['client_loss_implies_handler_abort']is False,'ActualFDLossNotAssumedHandlerAbort');delta(v['before'],v['while_commit_ready_held_committed_snapshot'],2);f=v['http_fate'];need(integer(f['accepted'],1,1)and integer(f['handler_started'],1,1)and f['socket_dropped']is True and f['capacity_exceeded']is False,'ActualTransportFateRetained')
 else:
  need(v['observer_returned_before_rollback_ready_release']is False and exact(v['while_rollback_ready_held_snapshot'],v['before']),'RollbackRestoresChildParentBeforeHTTP')
  target=v['actual_parent_lock_before_cancel'];sql=contracts['parent_error_ready']['sql'];prefix=sql[:min(len(sql),warm['track_activity_query_size_bytes']-1)];need(target['known_SQL_prefix']==prefix and target['known_prefix_bytes']==len(prefix)and target['full_SQL_sha256']==hashlib.sha256(sql.encode()).hexdigest(),'KnownPrefixFromActualSettingNotFullQueryAssumption');rows=target['rows'];need(len(rows)==1 and rows[0]['pid']==warm['actual_backend_pid']and rows[0]['wait_event_type']=='Lock','SameActualTargetLockCancel');need(integer(v['actual_error_response']['status'],503,503)and v['actual_error_response']['Source_injected_error_HTTP_authority']is False,'NativeDBFaultPredictionScope')
 dumps(v['cache_before'],v['cache_after'],True)
 process=json.loads((Path(out)/'proxy-process.json').read_text());need(process['forced']is False and integer(process['exit'],0,0)and integer(process['wait'],0,0)and process['proc_PID_absent_after_wait']is True and process['stdout_reader_joined']is True and type(process['starttime'])is str and process['starttime'].isdigit()and int(process['starttime'])>0,'ActualProxyWaitPIDPipeSettlement')
 return {'passed':True,'case':case,'imported_SQL_timestamp_lexical_differences_same_UTC_instant':imported_clock_lexemes,'scope':'optin actual metadata SQL physical Ready ownership; no Main/Source SQL injected-error authority'}
