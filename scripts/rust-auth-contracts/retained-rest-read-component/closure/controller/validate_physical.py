"""Exact two actual opt-in SQL physical predicates; no Source/Main transfer."""
import json,math
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def require(ok,label):
 if not ok:raise ValueError(label)
def exact(a,b):
 if type(a)is not type(b):return False
 if type(a)is dict:return set(a)==set(b)and all(exact(a[k],b[k])for k in a)
 if type(a)is list:return len(a)==len(b)and all(exact(x,y)for x,y in zip(a,b))
 return a==b
def integer(v,lo,hi):return type(v)is int and lo<=v<=hi
def finite(v,lo,hi):return type(v)in(int,float)and math.isfinite(v)and lo<=v<=hi
def validate(value,case,out):
 require(integer(case,1,2)and type(value['case'])is int and value['case']==case,'TwoFixedCases')
 require(type(value['Source_runs'])is type(value['Main_runs'])is int,'TypedZeroApplicationCounts')
 require(value['capture_only']is True and value['capture_complete']is True and value['passed']is True and value['Source_runs']==value['Main_runs']==0,'CompleteActualNativeOnly')
 require(value['dummy_owner']is False and type(value['real_existing_writer_budget'])is int and value['real_existing_writer_budget']==4 and value['pool_max_connections']==4,'OriginalFourOnly')
 names={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal'}
 for key in('tables','sequences'):require(set(value['before'][key])==names,'ExactlyElevenNamedSnapshots')
 expected=json.loads((ROOT/'inputs/imported-existing-version.json').read_text())['row'];cache=value['raw_cache_before']['raw_DUMP_PTTL'];require(set(cache)=={expected['key_hex']}and cache[expected['key_hex']]=={'dump_hex':expected['dump_hex'],'pttl':-1},'ExactOneImportedPersistentVersionNoWarmGroup')
 require(exact(value['before'],value['after'])and exact(value['credentials_before'],value['credentials_after'])and exact(value['raw_cache_before'],value['raw_cache_after']),'LiteralSQLCredentialsCacheNoEffects')
 require(integer(value['prepared_actual_backend_pid'],1,2**31-1),'ActualPreparedPID')
 pid=value['prepared_actual_backend_pid'];locked=value['same_target_backend_before_cancel'];require(type(locked)is list and len(locked)==1 and locked[0]['pid']==pid and locked[0]['wait_event_type']=='Lock','SameBoundPreparedBackendBeforeCancel')
 stage='api_key_error_ready'if case==1 else'network_error_ready';want=json.loads((ROOT/'inputs/pg-contracts.json').read_text())[stage];held=value['held']['held']
 require(held['stage']==stage and held['backend_pid']==pid and held['sql']==want['sql']and held['parameter_hex']==want['parameter_hex']and held['errors']==['57014']and type(held['rows'])is int and held['rows']==0 and held['commands']==[]and held['sync_seen']is True and held['tx']=='I','ActualExecuteBind57014SyncReady')
 if case==1:require(value['actual_direct_observer_abort']is True,'ActualDirectObserverAbort')
 else:require(value['actual_logical_deadline_returned_before_ready']is True,'ActualDeadlineFutureDroppedBeforeReady')
 samples=value['post_loss_or_deadline_held_observations'];require(type(samples)is list and len(samples)==2,'ExactlyTwoHeldSamples')
 for sample in samples:
  require(finite(sample['elapsed_monotonic_seconds'],0,25),'FiniteHeldSample');require(sample['original_owner_weak_alive']is True and type(sample['writer_slots'])is int and sample['writer_slots']==3 and sample['same_backend_still_exists']is True,'OriginalOwnerAndBackendHeld')
  require(exact(sample['held'],held),'SameActualReadyStillHeld')
 require(samples[1]['elapsed_monotonic_seconds']-samples[0]['elapsed_monotonic_seconds']>=.25,'Sustained250ms')
 require(value['original_owner_weak_after_settlement']is False and type(value['writer_slots_after_settlement'])is int and value['writer_slots_after_settlement']==4 and type(value['actual_pool_reuse_value'])is int and value['actual_pool_reuse_value']==424242,'ActualRetirementAndPoolReuse')
 require(type(value['closed_pool_size'])is int and value['closed_pool_size']==0 and integer(value['closed_pool_idle_diagnostic'],0,4),'FirstClosePhysicalSize0DiagnosticWithinMax4')
 require(value['while_ELF_alive_subject_SQL_backends']==[],'WhileELFSubjectBackendsAbsent')
 final=value['proxy_final'];drained=value['proxy_naturally_drained_before_stop'];require(final['errors']==[]and final['listener_alive']is False and all(r['closed']is True and r['up_joined']is True for r in drained['connections']),'NaturalRelayConnectionDrainBeforeStop')
 starts=[e for e in final['events']if e['kind']=='ready-held'];ends=[e for e in final['events']if e['kind']=='ready-forwarded'];require(len(starts)==len(ends)==1 and starts[0]['stage']==ends[0]['stage']==stage,'ExactlyOneTargetReadyHeldAndForwarded')
 start_clock=starts[0]['monotonic'];end_clock=ends[0]['monotonic'];require(finite(start_clock,0,float('inf'))and finite(end_clock,0,float('inf'))and end_clock>=start_clock,'TypedFiniteOrderedActualReadyClocks')
 duration=end_clock-start_clock;sample_span=samples[1]['elapsed_monotonic_seconds']-samples[0]['elapsed_monotonic_seconds'];require(finite(value['actual_target_hold_seconds'],.25,25)and finite(duration,.25,25)and duration>=sample_span and value['actual_target_hold_seconds']==duration,'ActualHoldCoversSustainedSamplesWithin25')
 process=json.loads((Path(out)/'proxy-process.json').read_text());require(process['forced']is False and type(process['exit'])is type(process['wait'])is int and process['exit']==process['wait']==0 and process['proc_PID_absent_after_wait']is True and process['stdout_reader_joined']is True and type(process['starttime'])is str and process['starttime'].isdigit()and int(process['starttime'])>0,'ActualRelayPIDWaitPipeNoForcedCleanup')
 return {'passed':True,'case':case,'Source_runs':0,'Main_runs':0,'scope':'two opt-in read stages physical Ready ownership only; duplicate Source rendering and failed-auth SQL remain open'}
