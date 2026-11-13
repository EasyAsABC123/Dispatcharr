"""Finite ownership predicates, not Source error-message or Main parity."""
import hashlib,json,math,re
from pathlib import Path

def need(value,label):
 if not value:raise ValueError(label)
def validate(value,out):
 need(value['declared_Native_gateway_prelude_calls']==value['declared_Native_gateway_fault_calls']==9,'DeclaredActualGatewayWork')
 need(value['Source_requests']==0 and value['Main']is False and value['SQL_timestamps_rewritten']is False,'NativeOnly')
 rows=value['cases'];need(type(rows)is list and all(type(r['case'])is int for r in rows),'TypedMandatoryCases')
 need(len(rows)==9 and [(r['case'],r['mode'])for r in rows]==[(i,m)for i in range(1,6)for m in('observer_abort','deadline')if(i,m)!=(1,'observer_abort')],'MandatoryNineRemaining')
 schema=json.loads((Path(__file__).resolve().parent/'support/schema.json').read_text())
 need(value['before']==value['after']and sorted(value['before']['tables'])==schema['tables']and sorted(value['before']['sequences'])==schema['sequences'],'All62And59Unchanged')
 plan=json.loads((Path(__file__).resolve().parent/'support/plan.json').read_text());native=json.loads((Path(__file__).resolve().parent/'support/native-inputs.json').read_text())
 for row in rows:
  expected=plan['producers'][row['case']-1];need(row['producer']==expected['producer'],'ExactProducerIdentity')
  folder=Path(out)/(str(row['case'])+'-'+row['mode'])
  for kind in('stdout','stderr'):need(hashlib.sha256((folder/('peer.'+kind)).read_bytes()).hexdigest()==row['peer_'+kind+'_sha256'],'ExactPeerTranscript')
  stdout=(folder/'peer.stdout').read_text();need(stdout.count(native['filter']+' ... ok')==1 and '1 passed; 0 failed' in stdout,'ActualOnePeerTest')
  live=row['live_identity'];need(type(live['PID'])is int and live['PID']>1 and type(live['starttime'])is str and live['starttime'].isdigit()and live['exe_sha256']==native['binary_sha256']and row['original_PID_absent']is True,'LiveELFOriginalPIDGone')
  need(type(row['elapsed_seconds'])in(int,float)and math.isfinite(row['elapsed_seconds'])and 0<row['elapsed_seconds']<=75 and row['watchdog_expired']is False,'Fixed75')
  lock=row['observer_lock_cleanup'];need(lock['rollback_idle']is True and lock['autocommit_restored']is True and lock['wrapper_close_attempted']is True and lock['before_pool_return_transaction_status']==0,'ObserverLockReturnedIdleAutocommit')
  need(len(row['cleanup_steps'])==6 and all(x['settled']is True for x in row['cleanup_steps']),'EveryCleanupSettled')
  need(row['prelude_request_issued']is True and row['prelude']=={'kind':'response','status':200,'body_bytes':row['prelude']['body_bytes'],'Native_gateway_prelude_calls':1}and type(row['prelude']['body_bytes'])is int and 0<=row['prelude']['body_bytes']<=16*1024*1024,'OneActualNativePrelude200')
  warm_state=row['prelude_settled'];need(warm_state['active_requests']==warm_state['workers']==0 and warm_state['available_memory_bytes']==warm_state['maximum_memory_bytes']==192*1024*1024,'PreludeSameGrantsReturned')
  from warm_contract import target as warm_target
  first=value['declared_first_parameter_hex'][str(row['case'])];need(type(first)is str and 0<len(first)<=16384,'BoundedDeclaredFirstParameter');expected_warm=warm_target(row['warm_snapshot'],expected,first);need(expected_warm==row['warm_target']and expected_warm['sql_sha256']==hashlib.sha256(row['held']['sql'].encode()).hexdigest(),'ActualWarmWireProof')
  need(row['warm_target']['backend_pid']==row['held']['backend_pid']and row['warm_target']['connection']==row['held']['connection']and row['warm_target']['execute_generation']<row['held']['execute_generation'],'SamePreparedBackendNoPoolSubstitution')
  need(row['prelude_began_monotonic']<=row['prelude_completed_monotonic']<=row['request_started_monotonic'],'PreludeBeforeFaultStart')
  held=row['held'];need(type(held['parameter_hex'])is list and len(held['parameter_hex'])==expected['parameter_count']and held['parameter_hex'][0]==first,'ActualFaultBoundParameters');need(held['backend_pid']==row['cancelled_backend_pid']and held['errors']==['57014']and held['rows']==0 and held['sync_seen']is True and type(held['execute_generation'])is int and held['execute_generation']>0 and held['tx']==expected['transaction_status'] and held['sql'].startswith(expected['sql_prefix']),'RealSameErrorReady')
  wire=held['sql'].encode();need(hashlib.sha256(wire if len(wire)<=1023 else wire[:1023]).hexdigest()==row['actual_SQL_sha256'],'ObservedFullOrExact1023BytePGStatHash');need(len(wire)<=1023 or row['case']in(4,5),'OnlyObservedLongInfoQueries')
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
 return {'qualified_fault_cases':9,'Source_requests':0,'Main':False,'Source_error_body_parity':False,'prior_26_or_12_execution_transferred':False}
