"""Fail closed before interpreting absent events or a vacuous wire join."""
import json,math
from attribution import join
from causal_probe import Liveness

def validate(trace,wire,status,proxy,registry,signals):
 if status.get('invalid')or status.get('marker_present')is not False or not status.get('trace_owned')or not status.get('future_thread_trace_owned'):raise ValueError('IncompleteTraceOwnershipOrStatus')
 if status.get('rows')!=len(trace)or status.get('bytes')!=sum(len((json.dumps(row,separators=(',',':'),allow_nan=False)+'\n').encode())for row in trace):raise ValueError('TraceJournalCountMismatch')
 if proxy.get('invalid')or proxy.get('journal',{}).get('invalid')or proxy.get('connections')!=0 or proxy.get('seen')!=1 or proxy.get('threads_settled')is not True:raise ValueError('IncompletePhysicalProxy')
 if proxy['journal'].get('rows')!=len(wire)or proxy['journal'].get('bytes')!=sum(len((json.dumps(row,separators=(',',':'),allow_nan=False)+'\n').encode())for row in wire):raise ValueError('WireJournalCountMismatch')
 actual=[row for row in wire if row['kind']=='actual_counter_commit']
 if not actual:raise ValueError('MissingActualBusyINCRReference')
 # Each new physical writer requires a full uniquely matched original call/socket.
 attributed=join(trace+wire)
 if not attributed['all_actual_counter_writes_attributed']or len(attributed['matched'])!=len(actual):raise ValueError('IncompleteWireCallerAttribution')
 effects={row['sequence']:row for row in actual}
 for family,value in[('INCR',2),('DECR',1)]:
  if not any(row.get('role')=='target'and effects[row['effect_sequence']]['family']==family and type(row['value'])is int and row['value']==value for row in attributed['matched']):raise ValueError('MissingActualTarget'+family+'Reference')
 by_code={(row['code_sha256'],row['firstline']):row for row in registry}
 hits={(row['code'],row['firstline'])for row in trace if row['kind']=='call'}
 for name in['reserve_profile_slot','release_profile_slot']:
  selected=[key for key,row in by_code.items()if row['qualname'].endswith(name)]
  if len(selected)!=1 or selected[0]not in hits:raise ValueError('RequiredCounterCodeCoverage '+name)
 for role in['initial','follower','other']:
  own=[row for row in trace if row['kind']=='generator_bound'and row.get('role')==role]
  if not own or not any(row['kind']=='yield'and row.get('generator')in{x['generator']for x in own}for row in trace):raise ValueError('RequiredGeneratorCoverage '+role)
 if not any(row['kind']=='finally_enter'for row in trace):raise ValueError('NoObservedFinallyEntryCoverage')
 starts={row['request']:row for row in trace if row['kind']=='request_started'};ends={row['request']:row for row in trace if row['kind']=='request_finalized'}
 if len(starts)!=4 or set(starts)!=set(ends)or sorted(row['role']for row in starts.values())!=['follower','initial','other','target']:raise ValueError('RequestFinalizationCoverage')
 for identity,begin in starts.items():
  end=ends[identity]
  if (begin['role'],begin['sid_sha256'])!=(end['role'],end['sid_sha256'])or end['at']<begin['at']:raise ValueError('RequestFinalizationIdentity')
 # This proves the tracing hook actually sees daemon callbacks; absent callbacks
 # beyond this observed set remain unknown, never generalized to all six branches.
 callbacks=[row for row in trace if row['kind']=='call'and row.get('rule_kind')=='callback']
 if not callbacks or not any(type(row.get('callback'))is int and type(row.get('creator_generator'))is int for row in callbacks):raise ValueError('CallbackThreadWeakIdentityCoverage')
 bound={row.get('callback'):row for row in trace if row['kind']=='callback_bound'}
 if not any(row.get('callback')in bound and row.get('logical_thread_id')!=bound[row['callback']].get('logical_thread_id')for row in callbacks):raise ValueError('NewCallbackThreadCoverage')
 if any(type(row.get('logical_thread_id'))is not int or type(row.get('native_thread_id'))is not int for row in callbacks):raise ValueError('CallbackThreadIdentityCoverage')
 admin_begin=[row for row in trace if row['kind']=='admin_action_started'];admin_end=[row for row in trace if row['kind']=='admin_action_finalized']
 if len(admin_begin)!=1 or len(admin_end)!=1 or admin_begin[0].get('request')!=admin_end[0].get('request')or any(row.get('role')!='admin_action'or row.get('action')!='stop_client'or'sid_sha256'in row for row in admin_begin+admin_end)or admin_end[0]['at']<admin_begin[0]['at']:raise ValueError('DistinctAdminActionCoverage')
 admin_codes=[key for key,row in by_code.items()if row['qualname'].endswith('stop_vod_client')]
 if len(admin_codes)!=1 or not any((row.get('code'),row.get('firstline'))==admin_codes[0]and row.get('role')=='admin_action'for row in trace if row['kind']=='call'):raise ValueError('AdminStopCodeCoverage')
 if signals.get('valid')is not True:raise ValueError('IndependentSignalCoverage')
 live=Liveness()
 for row in sorted(trace+wire,key=lambda row:row['at']):live.observe(row)
 if live.invalid:raise ValueError('IncompleteOtherGeneratorLiveness')
 return{'actual_zero_seen':any(row['value']==0 for row in actual),'other_generator_yields_after_zero':live.qualified,'valid':True,'actual_counter_effects':len(actual),'unique_attributions':len(attributed['matched']),'observed_callback_calls':len(callbacks),'request_finalizations':len(ends),'scope':'only observed exact code identities/contexts/window; no all-six callback absence, GC or successful-finally assumption','attribution':attributed}


def scalar_observations(trace,wire):
 live=Liveness()
 for row in sorted(trace+wire,key=lambda row:row['at']):live.observe(row)
 return{'zero_observed':any(row.get('kind')=='actual_counter_commit'and type(row.get('value'))is int and row['value']==0 for row in wire),'raw_other_generator_yields_after_zero':live.qualified,'liveness_invalid':live.invalid,'scope':'literal typed wire0 and raw traced yields; causal coverage validity reported separately'}

def finalize_capture(report):
 http=report.get('source',{}).get('observation_valid')is True
 finalized=report.get('Source_signal_terminal_valid')is True
 causal=report.get('causal_coverage',{}).get('valid')is True
 cleanup=bool(report.get('default_named_tables_sequences_unchanged')is True and not report.get('physical_cleanup_errors')and set(report.get('remaining',{}))=={'containers','networks','volumes'}and not any(report['remaining'].values()))
 valid=http and finalized and causal and cleanup and not report.get('error_type')
 return{'HTTP_cohort_valid':http,'request_finalization_valid':finalized,'owned_cleanup_valid':cleanup,'causal_coverage_valid':causal,'observation_valid':valid,'passed':valid and report.get('Source_reference_expectation_matched')is True,'full_M4_acceptance':False,'parity_passed':None}
