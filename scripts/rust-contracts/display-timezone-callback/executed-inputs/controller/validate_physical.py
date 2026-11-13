"""Finite callback gate validation; no Source/Main/full-endpoint equivalence."""
import json,math
from pathlib import Path
from validate_native import equal,require,dump_string,literal_pickle
ROOT=Path(__file__).resolve().parents[1]
SYSTEM_SQL=json.loads((ROOT/'inputs/pg-contracts.json').read_text())['system_ready']['sql']
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal'}
def validate(value,case,out):
 require(type(case)is int and 1<=case<=6 and value['case']==case,'FixedSixPhysicalCases')
 require(value['capture_complete']is True and value['passed']is True and value['capture_only']is True and value['Source_runs']==value['Main_runs']==0,'CompleteActualLimitedPhysicalCapture')
 require(value['IANA_ZoneInfo_catalogue_equivalent']is False and value['dummy_owner']is False and type(value['real_existing_writer_budget'])is int and value['real_existing_writer_budget']==4,'NoFalseCatalogueOrOwnerBudget')
 for key in ('tables','sequences'):require(set(value['before'][key])==TABLES,'Full11Snapshot')
 require(equal(value['before'],value['after'])and equal(value['credentials_before'],value['credentials_after']),'NoForeignSQLOrSequenceOrCredentialEffects')
 require(type(value['closed_pool_size'])is int and value['closed_pool_size']==0 and type(value['closed_pool_idle_diagnostic'])is int and 0<=value['closed_pool_idle_diagnostic']<=4,'FirstClosePhysicalSize0IdleDiagnosticWithinCompiledMax4')
 require(value['while_ELF_alive_other_owned_SQL_backends']==[] and value['writer_slots_after_settlement']==4,'WhileAliveBackendAndOriginalBudgetSettlement')
 before_cache=value['raw_cache_before']['raw_DUMP_PTTL'];after_cache=value['raw_cache_after']['raw_DUMP_PTTL'];system=':1:coresettings:group:system_settings'.encode().hex()
 require(set(after_cache)==set(before_cache)|({system}if case==3 else set()),'OnlyDeclaredSystemCacheFill')
 for key,row in before_cache.items():
  other=after_cache[key];require(row['dump_hex']==other['dump_hex'],'UnrelatedCacheLiteralDumpPreserved');x,y=row['pttl'],other['pttl'];require(type(x)is type(y)is int and(x==y==-1 or 0<y<=x),'OwnCacheTTLMonotonic')
 if case==3:
  row=after_cache[system];require(type(row['pttl'])is int and 0<row['pttl']<=300000,'SystemCacheFillFiniteTTL');require(equal(literal_pickle(dump_string(bytes.fromhex(row['dump_hex']))),{'time_zone':'Europe/London'}),'ActualSystemGroupFillBytesBound')

 if case<=4:
  require(value['weak_original_owner_while_held']is True and value['writer_slots_while_held']==3 and value['weak_original_owner_after_settlement']is False,'OriginalActualPermitWeakLifetime')
  samples=value['post_loss_or_logical_held_observations'];require(type(samples)is list and len(samples)==2,'ExactlyTwoSustainedHeldSamples')
  for sample in samples:
   elapsed=sample['elapsed_monotonic_seconds'];require(type(elapsed)in(int,float)and math.isfinite(elapsed)and 0<=elapsed<=25,'FiniteBoundedHeldSampleClock');require(sample['original_owner_weak_alive']is True and type(sample['writer_slots'])is int and sample['writer_slots']==3,'ActualOriginalPermitHeldForBothSamples');require(type(sample['actual_cache_physical_operations'])is int and(1<=sample['actual_cache_physical_operations']<=16 if case>=3 else sample['actual_cache_physical_operations']==0),'ActualExistingCachePhysicalBudgetSamples')
  require(samples[1]['elapsed_monotonic_seconds']-samples[0]['elapsed_monotonic_seconds']>=.25,'SustainedHeldWindow250ms')
  duration=value['actual_target_hold_seconds'];require(type(duration)in(int,float)and math.isfinite(duration)and 0<=duration<=25,'ActualHoldWithinPredeclared25')
  final=value['proxy_final'];drained=value['proxy_naturally_drained_before_stop']
  if case>=3:
   require(drained['connections']==final['connections']==0 and final['invalid']==[] and final['listener_joined']is True,'RedisPhysicalDrainAndRelayJoin')
   rows=final['rows'];starts=[r for r in rows if r['kind']=='backend-target-reply-read-held'];ends=[r for r in rows if r['kind']=='backend-target-reply-released'];require(len(starts)==len(ends)==1,'ExactlyOneActualLuaReply')
  else:
   require(final['errors']==[] and final['listener_alive']is False and all(r['closed']is True and r['up_joined']is True for r in drained['connections']),'PGPhysicalDrainAndRelayJoin')
   held=value['held']['held'];require(held['sql']==SYSTEM_SQL and held['parameter_hex']==['system_settings'.encode().hex()]and held['sync_seen']is True,'ActualCompiledSQLBindExecuteSyncReady')
   require(held['errors']==(['57014']if case==2 else[])and held['rows']==(0 if case==2 else 1),'ActualSuccessOrErrorReady')
  process=json.loads((Path(out)/'proxy-process.json').read_text());require(process['forced']is False and process['exit']==process['wait']==0 and process['proc_PID_absent_after_wait']is True and process['stdout_reader_joined']is True and type(process['starttime'])is str and process['starttime'].isdigit(),'ActualPeerWaitPipesPIDNoForcedCleanup')
 if case==1:require(value['actual_observer_abort']is True,'ActualObserverJoinHandleAbort')
 if case==2:
  require(value['error_preserved_zone_and_pretry_debounce']is True,'SwallowedErrorPreviousZoneCheckedBeforeTry');require(value['same_target_backend_before_cancel'][0]['pid']==value['held']['held']['backend_pid'],'ActualSameTargetBackend57014')
 if case==3:
  f=value['HTTP_fate'];require(value['HTTP_FDloss_is_not_assumed_handler_abort']is True and type(value['actual_client_FD'])is int and f['accepted']==f['handler_started']==1 and f['socket_dropped']is True and f['capacity_exceeded']is False,'ActualHTTPFDAndHandlerFateRetained')
 if case==4:require(value['logical_observer_returned_before_physical_reply']is True,'LogicalVsPhysicalSettlementDistinct')
 if case==5:
  require(value['four_original_weak_alive']is True and value['four_real_reused_tokens_settled']is True,'RealFourTokenReuseAndDrain');require(len(value['four_real_target_backends_held'])==len(set(value['four_prepared_actual_backend_PIDs']))==4,'ActualFourPreparedBackendIdentities')
 if case==6:require(value['setter_invalid_previous_zone_and_real_debounce']is True and value['exact_4999_5000_boundary_authority']=='compiled paused-clock unit test; no wallclock exactness claim','SetterAndClockBoundaryScope')
 return {'passed':True,'case':case,'Source_runs':0,'Main_runs':0,'actual_callback_physical_scope_only':True,'whole_endpoint_or_IANA_parity':False}
