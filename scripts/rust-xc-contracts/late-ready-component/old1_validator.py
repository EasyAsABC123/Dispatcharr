"""Finite ownership predicates, not Source error-message or Main parity."""
import hashlib,json,math,re
from pathlib import Path

def need(value,label):
 if not value:raise ValueError(label)
def validate(value,out):
 need(value['declared_Native_gateway_prelude_calls']==value['declared_Native_gateway_fault_calls']==10,'DeclaredActualGatewayWork')
 need(value['Source_requests']==0 and value['Main']is False and value['SQL_timestamps_rewritten']is False,'NativeOnly')
 all_rows=value['cases'];need(type(all_rows)is list and [(r['case'],r['mode'])for r in all_rows]==[(1,'observer_abort'),(1,'deadline')],'ExactOldPositiveAndNegativeCoverage');rows=[all_rows[0]]
 need([(r['case'],r['mode'])for r in rows]==[(1,'observer_abort')],'OnlyHistoricalPositiveAbort')
 schema=json.loads((Path(__file__).resolve().parent/'support/schema.json').read_text())
 need('after'not in value and sorted(value['before']['tables'])==schema['tables']and sorted(value['before']['sequences'])==schema['sequences'],'HistoricalOwnedAfterUnmeasuredNotUpgraded')
 plan=json.loads((Path(__file__).resolve().parent/'support/old-plan.json').read_text());native=json.loads((Path(__file__).resolve().parent/'support/native-inputs.json').read_text())
 for row in rows:
  expected=plan['producers'][row['case']-1];need(row['producer']==expected['producer'],'ExactProducerIdentity')
  folder=Path(out)/(str(row['case'])+'-'+row['mode'])
  for kind in('stdout','stderr'):need(hashlib.sha256((folder/('peer.'+kind)).read_bytes()).hexdigest()==row['peer_'+kind+'_sha256'],'ExactPeerTranscript')
  stdout=(folder/'peer.stdout').read_text();need(stdout.count(native['filter']+' ... ok')==1 and '1 passed; 0 failed' in stdout,'ActualOnePeerTest')
  live=row['live_identity'];need(type(live['PID'])is int and live['PID']>1 and type(live['starttime'])is str and live['starttime'].isdigit()and live['exe_sha256']==native['binary_sha256']and row['original_PID_absent']is True,'LiveELFOriginalPIDGone')
  need(type(row['elapsed_seconds'])in(int,float)and math.isfinite(row['elapsed_seconds'])and 0<row['elapsed_seconds']<=75 and row['watchdog_expired']is False,'Fixed75')
  need(len(row['cleanup_steps'])==6 and all(x['settled']is True for x in row['cleanup_steps']),'EveryCleanupSettled')
  need(row['prelude_request_issued']is True and row['prelude']=={'kind':'response','status':200,'body_bytes':row['prelude']['body_bytes'],'Native_gateway_prelude_calls':1}and type(row['prelude']['body_bytes'])is int and 0<=row['prelude']['body_bytes']<=16*1024*1024,'OneActualNativePrelude200')
  warm_state=row['prelude_settled'];need(warm_state['active_requests']==warm_state['workers']==0 and warm_state['available_memory_bytes']==warm_state['maximum_memory_bytes']==192*1024*1024,'PreludeSameGrantsReturned')
  from warm_contract import target as warm_target
  first=value['declared_first_parameter_hex'][str(row['case'])];need(type(first)is str and 0<len(first)<=16384,'BoundedDeclaredFirstParameter');expected_warm=warm_target(row['warm_snapshot'],expected,first);need(expected_warm==row['warm_target']and expected_warm['sql_sha256']==row['actual_SQL_sha256'],'ActualWarmWireProof')
  need(row['warm_target']['backend_pid']==row['held']['backend_pid']and row['warm_target']['connection']==row['held']['connection']and row['warm_target']['execute_generation']<row['held']['execute_generation'],'SamePreparedBackendNoPoolSubstitution')
  need(row['prelude_began_monotonic']<=row['prelude_completed_monotonic']<=row['request_started_monotonic'],'PreludeBeforeFaultStart')
  held=row['held'];need(type(held['parameter_hex'])is list and len(held['parameter_hex'])==expected['parameter_count']and held['parameter_hex'][0]==first,'ActualFaultBoundParameters');need(held['backend_pid']==row['cancelled_backend_pid']and held['errors']==['57014']and held['rows']==0 and held['sync_seen']is True and type(held['execute_generation'])is int and held['execute_generation']>0 and held['tx']==expected['transaction_status'] and held['sql'].startswith(expected['sql_prefix']),'RealSameErrorReady')
  need(hashlib.sha256(held['sql'].encode()).hexdigest()==row['actual_SQL_sha256'],'ExactWireSQLMatchesBlockedBackend')
  active=row['while_held'];need(type(active['active_requests'])is int and type(active['workers'])is int and active['active_requests']==active['workers']==1 and type(active['available_memory_bytes'])is int and active['maximum_memory_bytes']==192*1024*1024 and 0<=active['available_memory_bytes']<active['maximum_memory_bytes'],'OriginalRequestMemoryWorker')
  need(row['shutdown_held']=={'shutdown_drained':False},'NoPrematureDrain')
  settled=row['settled'];need(settled['active_requests']==settled['workers']==0 and settled['available_memory_bytes']==settled['maximum_memory_bytes'],'AllSameGrantsReturn')
  need(type(row['actual_exit'])is int and row['actual_exit']==0 and row['pipes_settled']is True,'PeerWaitPipes0')
  terminal=row['terminal'];need(terminal['active_requests']==0 and terminal['available_memory_bytes']==terminal['maximum_memory_bytes']==192*1024*1024 and terminal['pool_closed']is True,'FinalClosedPoolAndSameMemory')
  final=row['proxy_terminal'];natural=row['natural_proxy_drain'];need(final['listener_alive']is False and final['errors']==[]and natural['connections']and all(c['closed']and c['up_joined']for c in natural['connections']),'NaturalPhysicalPGDrain')
  events=final['events'];arm=[e for e in events if e['kind']=='gate-armed'and e['arm_epoch']==held['arm_epoch']];execute=[e for e in events if e['kind']=='execute'and e['connection']==held['connection']and e['execute_generation']==held['execute_generation']]
  need(len(arm)==len(execute)==1 and row['warm_target']['ready_monotonic']<=arm[0]['monotonic']<=held['execute_monotonic']<=execute[0]['monotonic'],'PostArmFreshExecute')
  if row['mode']=='observer_abort':need(row['observer']=={'kind':'join_error','cancelled':True},'ExplicitObserverAbortNotTCPInference')
  else:
   need(row['observer']['kind']=='gateway_error','DeadlineStillError')
   need(row['observer_finished_seen_monotonic']-row['request_started_monotonic']>=15,'Actual15ObserverElapsed')
 return {'qualified_fault_cases':1,'Source_requests':0,'Main':False,'historical_owned_after_SQL':'unmeasured','Source_error_body_parity':False,'prior_26_or_12_execution_transferred':False}
