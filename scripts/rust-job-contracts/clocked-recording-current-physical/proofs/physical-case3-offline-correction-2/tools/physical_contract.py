"""Strict fixed Native ownership controls; not Source failure-result parity."""
import base64,datetime,math
MODES=('cache-preack','sql-preack-ready','sql-update-error-ready','capture-observer-loss','channels-reply','result-reply','preexisting-active')
def require(ok,label):
 if not ok:raise ValueError(label)
def number(v):return type(v)in(int,float)and math.isfinite(v)
def integer(v,lo,hi):return type(v)is int and lo<=v<=hi
def exact(a,b):
 if type(a)is not type(b):return False
 if type(a)is dict:return a.keys()==b.keys()and all(exact(v,b[k])for k,v in a.items())
 if type(a)is list:return len(a)==len(b)and all(exact(x,y)for x,y in zip(a,b))
 return a==b
def instant(raw):
 require(type(raw)is str,'ActualUTCString');v=datetime.datetime.fromisoformat(raw.replace('Z','+00:00'));require(v.tzinfo is not None,'ActualAwareClock');return v

def nodelay(receipt,role):
 require(receipt['role']==role and type(receipt['actual_TCP_NODELAY_before'])is int and receipt['actual_TCP_NODELAY_before']in(0,1) and type(receipt['actual_TCP_NODELAY_after'])is int and receipt['actual_TCP_NODELAY_after']==1 and receipt['local'][0]==receipt['peer'][0]=='127.0.0.1'and number(receipt['observed_monotonic']),'ActualOwnedLoopbackTCP_NODELAYReadback')

def validate_capture(exit_code,report,mode):
 require(type(exit_code)is int and exit_code==0,'ActualFixedGuest0')
 return validate_effects(report,mode)

def validate_effects(report,mode):
 require(mode in MODES,'FixedModeEffectsOnly')
 require(report['Source_verified']is report['performance_verified']is False,'NoSourceFaultOrPerformanceClaim')
 case=report['physical'];require(case['mode']==mode and case['capture_only']is True and case['capture_complete']is True and case['cleanup_errors']==[],'ActualCaptureAndAttemptAllSettlement')
 child=case['peer_collection'];require(type(child['actual_exit'])is int and child['actual_exit']==0 and child['physically_settled']is child['pipes_settled']is child['writer_settled']is child['original_process_absent']is True and child['invalid']is child['writer_error']is None,'ActualNativeWaitPipesAndIdentity')
 peer=case['actual_peer'];require(integer(peer['pid'],1,2**31-1)and type(peer['starttime'])is str and peer['starttime'].isdigit()and int(peer['starttime'])>0,'OriginalActualPeerIdentity')
 before=case['initial'];after=case['final'];task=report['recording_dispatch']['frame']['headers']['id'];record=report['seed']['Recording_id'];activekey=base64.b64encode(('dvr:recording-active:'+str(record)).encode()).decode();samples=case['samples'];require(type(samples)is list and 1<=len(samples)<=16,'FiniteActualSamples')
 previous=-1;elapsed=-1;began=instant(case['actual_command_began_utc']);ended=instant(case['actual_command_finished_utc']);require(began<=ended,'OwnActualCommandWindow')
 for sample in samples:
  require(sample['peer_pid']==peer['pid']and sample['peer_starttime']==sample['observed_peer_starttime']==peer['starttime'],'SameObservedPeerEverySample');clock=sample['received_monotonic'];require(number(clock)and clock>previous,'CausalReceivedClocks');previous=clock;reply=sample['reply'];require(integer(reply['id'],1,16)and number(reply['monotonic_elapsed_seconds'])and reply['monotonic_elapsed_seconds']>=elapsed,'TypedActualPeerClockAndID');elapsed=reply['monotonic_elapsed_seconds'];require(type(reply['same_original_caller_weak_alive'])is bool and integer(reply['available_slots'],0,1),'TypedRealOriginalWeakAndSlot');reply_utc=instant(reply['utc']);received_utc=instant(sample['received_utc']);snapshot_utc=instant(sample['snapshot']['utc']);require(began<=reply_utc<=received_utc<=snapshot_utc<=ended,'EveryActualSampleClockInOwnCommandWindow')
 native=case['actual_native'];require(integer(native['event_connections_before'],1,65536)and integer(native['event_bytes_before'],1,2**31-1)and native['event_connections_after']==native['event_connections_before']and native['event_bytes_after']==native['event_bytes_before'],'ActualSameRealtimeBudgetsRestoredAfterPhysicalSettlement');require(native['capture_complete']is native['passed']is True and native['Source_task_execution']is native['Main_execution']is False and native['actual_first_close_pool_size']==0,'ActualTestCaptureNotSourceOrMain')
 require(exact(before['queues']['celery'],after['queues']['celery']),'EveryStaticSiblingFIFOByteOrderPreserved')
 meta=after['backend'][task];preack=mode in('cache-preack','sql-preack-ready','preexisting-active')
 require(after['queues']['dvr']==before['queues']['dvr']if preack else after['queues']['dvr']==[],'ActualPreACKPreservationOrConfirmedClaim')
 if preack:
  for field in ('recordings','periodic','clocked','sentinel','system_events'):require(exact(before[field],after[field]),'ActualPreACKUnchanged:'+field)
  require(meta['metadata']is None and meta['raw_base64']is None,'NoPreACKTerminalMetadata')
  require(set(after['Channels_raw_base64'])<=set(before['Channels_raw_base64']),'NoNewRawChannelsMemberBeforeACKEvenAcrossNaturalExpiry')
  require(exact(before['files'],after['files']),'NoPreACKFilesystemEffect')
 held_samples=[s for s in samples if s['purpose']in('held-sample1','held-sample2')]
 if mode!='preexisting-active':
  require(len(held_samples)==2,'TwoSustainedActualHeldSamples');a,b=held_samples;gate=case['actual_gate'];anchor=gate['held_monotonic'];release=case['release_requested_monotonic'];require(number(anchor)and number(release)and release-anchor>=6.2 and release-anchor<=20,'ActualFixedHeldReleaseWindow')
  require(a['received_monotonic']>=anchor+2.2 and b['received_monotonic']-a['received_monotonic']>=.25 and b['reply']['monotonic_elapsed_seconds']-a['reply']['monotonic_elapsed_seconds']>=.25 and b['received_monotonic']<=release,'SustainedCausalTwoPeerSamplesWithinActualHold')
  require(all(s['reply']['same_original_caller_weak_alive']is True and s['reply']['available_slots']==0 for s in held_samples),'SameOriginalOwnerRetainedAcrossCallerOrDeadline')
  if mode in('sql-update-error-ready','channels-reply','result-reply'):require(all(s['reply']['observer_finished']is False for s in held_samples),'RealNonAbortedObserverStillPendingNotMerelyUncertainOwnerPin')
  drains=[s for s in samples if s['reply']['action']=='drain-probe'];require(len(drains)==1 and drains[0]['reply']['drained']is False,'ActualDrainCannotPassWhileHeld')
  if mode in('sql-preack-ready','sql-update-error-ready'):
   h=gate['held'];require(integer(h['backend_pid'],1,2**31-1)and h['sync_seen']is True and len(h['parameter_hex'])==(1 if preack else 3),'ActualBoundQueryAndBackend')
   for point in held_samples:
    back=point['actual_target_backend'];require(back['pid']==h['backend_pid']and back['connection']==h['connection']and back['sql']==h['sql']and len(back['rows'])==1 and back['rows'][0][0]==h['backend_pid']and back['rows'][0][3]==h['sql'],'SameActualPostgreSQLBackendStillObserved')
   pg=case['PG_close']
   for connection in pg['connections']:
    records=[e for e in pg['events']if e['kind']=='actual-owned-TCP_NODELAY'and e['connection']==connection['id']];require(len(records)==2 and {e['side']for e in records}=={'downstream','upstream'},'EveryActualPGSocketPairReadback')
    for e in records:nodelay(e['receipt'],'native-downstream'if e['side']=='downstream'else'postgres-upstream')
   require(pg['errors']==[]and pg['listener_alive']is False and all(r['closed']is r['up_joined']is True for r in pg['connections']),'EveryActualPGRelayPhysicalClose');forward=[e for e in pg['events']if e['kind']=='ready-forwarded'];require(len(forward)==1 and forward[0]['connection']==h['connection']and number(forward[0]['monotonic'])and forward[0]['monotonic']>=release,'ActualMatchingReadyForwardedAfterAllHeldSamples')
   require(h['errors']==(['P0001']if mode=='sql-update-error-ready'else[])and h['commands']==([]if mode=='sql-update-error-ready'else['SELECT 1'])and h['rows']==(0 if mode=='sql-update-error-ready'else 1),'ExactActualSuccessOrErrorBeforeReadyGate')
  if 'Redis_close'in case:
   relay=case['Redis_close']
   options=[x for x in relay['receipts']if x['stage']=='actual-owned-TCP_NODELAY'];require(len(options)>=1,'ActualRedisSocketPairReadback')
   for x in options:nodelay(x['downstream'],'redis-downstream');nodelay(x['upstream'],'redis-upstream')
   require(relay['errors']==[]and relay['physical_close_verified']is True and relay['active_connections']==relay['handler_threads_alive']==0 and relay['server_thread_alive']is False and relay['target_replies']==1,'ActualOwnedRedisRelaySettledOnce')
   holds=[x for x in relay['receipts']if x['stage']=='actual-target-reply-held'];forwards=[x for x in relay['receipts']if x['stage']=='actual-target-reply-forwarded'];require(len(holds)==len(forwards)==1 and number(forwards[0]['monotonic'])and forwards[0]['monotonic']>=release and holds[0]['reply_sha256']==forwards[0]['reply_sha256'],'ActualServerReplyHeldThenSuccessfullyForwarded')
  if mode=='channels-reply':
   late=[s for s in samples if s['purpose']in('after-3s-deadline-sample1','after-3s-deadline-sample2')];require(all(s['reply']['observer_finished']is False and integer(s['reply']['event_connections_available'],0,native['event_connections_before']-1)and integer(s['reply']['event_bytes_available'],0,native['event_bytes_before']-1)for s in late),'ActualRealtimeConnectionAndByteBudgetsStillHeldAfterLogicalDeadline');require(len(late)==2 and late[0]['received_monotonic']>=anchor+3.3 and late[1]['received_monotonic']-late[0]['received_monotonic']>=.25 and late[1]['reply']['monotonic_elapsed_seconds']-late[0]['reply']['monotonic_elapsed_seconds']>=.25 and all(s['reply']['same_original_caller_weak_alive']is True and s['reply']['available_slots']==0 for s in late),'ActualOriginalOwnerAfterConfigured3sRealtimeDeadline')
 finish=samples[-1]['reply'];require(finish['action']=='finish','ActualFinalPeerCommand')
 if mode in('cache-preack','sql-preack-ready','capture-observer-loss','preexisting-active'):
  require(finish['same_original_caller_weak_alive']is False and finish['available_slots']==1 and finish['drained']is True,'ActualOwnerReleasedOnlyAfterUnderlyingWork')
 else:
  require(finish['same_original_caller_weak_alive']is True and finish['available_slots']==0 and finish['drained']is False,'UnknownPostACKOwnerRemainsPinnedExplicitly')
  outcome=finish['actual_observer_result'];require(outcome['kind']=='Operational','NoFabricatedSourceTerminalFailureOrNormalAlternative');text=outcome['diagnostic']
  if mode=='sql-update-error-ready':require('MutationWrite'in text and 'P0001'in text and 'owned-dvr-physical-prime'in text,'ExactActualStoreErrorDisposition')
  elif mode=='channels-reply':require(text=='Events(Unavailable(Unavailable))','ExactKnownUnavailableSinkDisposition')
  elif mode=='result-reply':require(text=='Transport(AmbiguousResult('+task+'))','ExactResultAmbiguityNoRetryAuthority')
 if mode in('sql-update-error-ready','channels-reply'):require(meta['metadata']is None and meta['raw_base64']is None,'NoTerminalMetadataForOperationalBodyFailure')
 initial_raw=before['Channels_raw_base64'];final_raw=after['Channels_raw_base64'];require(len(final_raw)==len(after['Channels_decoded'])and len(initial_raw)==len(before['Channels_decoded']),'CompleteActualRawAndDecodedChannelsObservations')
 new_events=[value for raw,value in zip(final_raw,after['Channels_decoded'])if raw not in initial_raw]
 channel=next(row for row in before['channels']if row['id']==report['seed']['channel_id'])['name']
 start_event={'type':'update','data':{'success':True,'type':'recording_started','channel':channel},'__asgi_channel__':['specific.rust_owned_clocked!receiver']}
 update_event={'type':'update','data':{'success':True,'type':'recording_updated','recording_id':record},'__asgi_channel__':['specific.rust_owned_clocked!receiver']}
 end_event={'type':'update','data':{'success':True,'type':'recording_ended','channel':channel},'__asgi_channel__':['specific.rust_owned_clocked!receiver']}
 if mode in('sql-update-error-ready','channels-reply'):
  require(exact(before['recordings'],after['recordings']),'ActualPrimeFailureOrPreAuditErrorKeepsScheduledRow')
  periodic=[row for row in before['periodic']if row['name']=='dvr-recording-'+str(record)];require(len(periodic)==1,'ExactInitialOwnedPeriodic')
  require(exact(after['periodic'],[row for row in before['periodic']if row['id']!=periodic[0]['id']])and exact(after['clocked'],[row for row in before['clocked']if row['id']!=periodic[0]['clocked_id']]),'OnlyActualDVRPeriodicClockedCleanupBeforeFault')
  require(exact(new_events,[start_event]),'ActualStartedEventPersistsAcrossOperationalErrorNoRollbackFabrication')
  if mode=='channels-reply':require(exact(before['system_events'],after['system_events'])and exact(before['files'],after['files']),'SinkErrorBeforeAuditAndRootStage')
  else:
   audits=after['system_events'][len(before['system_events']):];require(exact(after['system_events'][:len(before['system_events'])],before['system_events'])and len(audits)==1 and audits[0]['event_type']=='recording_start'and after['files']['exists']is True and after['files']['files']==[],'ActualAuditAndFreshRootPartialEffectsBeforePrimeError')
  rows=[r for r in after['cache']if r['key_base64']==activekey];require(len(rows)==1 and base64.b64decode(rows[0]['value_base64'],validate=True)==b'1'and integer(rows[0]['ttl'],1,45),'ActualPostACKActiveMarkerNotGuessedDeletedOnUncertainty')
 if mode in('capture-observer-loss','result-reply'):
  require(exact(new_events,[start_event,update_event,end_event]),'ActualStartedUpdatedEndedEventOrder')
  rows=case['retained_media']['files'];require(len(rows)==1 and rows[0]['relative'].endswith('.mkv')and rows[0]['stable_across_this_read']is True and integer(rows[0]['bytes'],1,64*1024*1024),'RetainedWholePhysicalOutputFileNotPrefix')
  processes=finish['diagnostics']['processes']['rows'];require(len(processes)==2,'ActualCaptureAndRemuxOnly')
  for process,code in zip(processes,[255,0]):require(type(process['code'])is int and process['code']==code and process['signal']is None and process['settled_utc']is not None and process['stderr']['read_error']is None and all(process[k]is None for k in ['reader_error','command_error','wait_error','kill_error']),'ActualKernelWaitAndStderrReaderSettled')

 if mode in('capture-observer-loss','result-reply'):
  value=meta['metadata'];require(type(value)is dict and value['status']=='SUCCESS'and value['result']is value['traceback']is None and value['children']==[]and value['task_id']==task,'ActualNormalNullMetadata');require(integer(meta['ttl'],1,3600),'ActualStrictOneHourTTL');require(after['recordings'][0]['custom_properties']['status']=='completed','ActualCompletedRow');require(not any(r['key_base64']==activekey for r in after['cache']),'ActualNormalActiveKeyRemoved')
 if mode=='preexisting-active':
  rows=[r for r in after['cache']if r['key_base64']==activekey];require(len(rows)==1 and base64.b64decode(rows[0]['value_base64'],validate=True)==b'1'and integer(rows[0]['ttl'],1,45),'ForeignActiveLiteralRetainedNotDeleted');require(finish['actual_observer_result']=={'kind':'Operational','diagnostic':'Unsupported("preexisting DVR active key")'},'ExplicitKnownPreACKRefusal')
 for key in ('all_prerun','all_postrun'):require(len(report[key])==1 and report[key][0]['id']==report['artwork']['id'],'OnlyOriginalArtworkSignalNoNativeFabrication')
 require(report['all_HTTP_attempts']==[],'NoPythonRemoteProviderAttempt')
 return {'passed':True,'mode':mode,'scope':'Native physical ownership/refusal only; no Source fault, Main/defaultgraph/performance/duration parity','strict_result_TTL_seconds':3600,'raw_all_fields_retained':True}
