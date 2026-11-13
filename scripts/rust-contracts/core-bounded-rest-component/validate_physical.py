"""Fresh bounded Core predicates; no historical-result transfer."""
import hashlib,json,math
from pathlib import Path
from bounded_contracts import CASES,warm,USER_SQL,NETWORK_SQL
def need(v,label):
 if not v:raise ValueError(label)
def validate(value,out):
 need(value['Native_only']is True and value['Source_business_requests']==0 and value['same4_writer_permits']is True and value['Main_activated']is False,'NativeComponentSameBudget')
 need(value['new_source_binding']=='46b91d70f2282fdadd14a6fc8b7a2110825d1019'and value['admin_principal']['id']==1 and value['admin_principal']['is_active']is True and value['admin_principal']['user_level']==10,'ExactCurrentSourceAndOwnedAdmin')
 need(value['cache_binding']=={'Django_default_database':1,'direct_database':1,'peer_database':1},'SameActualCacheDatabase')
 rows=value['cases'];need(type(rows)is list and[(r['case'],r['name'])for r in rows]==CASES,'MandatorySixCurrentCases')
 root=Path(__file__).resolve().parent;schema=json.loads((root/'schema62-authority.json').read_text());native=json.loads((root/'native-inputs.json').read_text())
 for name in('before_SQL','after_SQL'):
  snap=value[name];need(sorted(snap['full62']['tables'])==sorted(schema['tables'])and sorted(snap['full62']['sequences'])==sorted(schema['sequences'])and len(snap['named11']['tables'])==len(snap['named11']['sequences'])==11,'Complete6259And11')
 need(value['before_SQL']==value['after_SQL'],'PhysicalOwnedSQLSequencesUnchanged')
 need(value['declared_prelude_calls']==6 and value['declared_fault_HTTP_calls']==3 and value['declared_error_hold_calls']==4 and value['declared_fifth_calls']==1 and value['declared_reuse_calls']==3 and value['declared_direct_capacity_calls']==value['declared_capacity_hold_calls']==2,'FixedCoreEntryCounts')
 for row in rows:
  n=row['case'];folder=Path(out)/('bounded-physical-'+str(n));need(row['prelude_HTTP_invoked']is True and row['prelude']['status']==200 and row['prelude_began_monotonic']<=row['prelude_completed_monotonic'],'OneActualSuccessfulPrelude')
  need(row['peer_actual_exit']==0 and row['peer_pipes_settled']is True and row['original_peer_PID_absent']is True,'ActualPeerExitAndIdentity')
  live=row['live_identity'];need(live['PID']==row['peer_PID']and live['starttime']==row['peer_starttime']and live['exe_sha256']==native['binary_sha256'],'ActualCurrent854ELF')
  for stream in('stdout','stderr'):need(hashlib.sha256((folder/('peer.'+stream)).read_bytes()).hexdigest()==row['peer_'+stream+'_sha256'],'ActualPeerTranscriptBytes')
  text=(folder/'peer.stdout').read_text();need(text.count('core_settings_writes::physical_tests::actual_core32_http_peer ... ok')==1 and'1 passed; 0 failed'in text,'OneRealCurrentPeerPassed')
  need(type(row['elapsed_seconds'])in(int,float)and math.isfinite(row['elapsed_seconds'])and 0<row['elapsed_seconds']<=75 and row['watchdog_joined']is True and row['watchdog_expired']is False,'Fixed75AndWatchdogSettled')
  need(len(row['cleanup_steps'])==9 and all(x['settled']is True for x in row['cleanup_steps']),'NineUnconditionalCleanupSteps')
  need(row['state_settled']['active']==0 and row['state_settled']['available_writer_permits']==4 and row['state_settled']['cache_physical_operations']==0,'SameFourOwnersRestored')
  need(row['terminal']=={'shutdown_drained':True,'active':0,'available_writer_permits':4,'cache_physical_operations':0,'pool_closed':True},'ActualPoolAndOwnerTerminal')
  if n<=3:
   held=row['held'];sql=NETWORK_SQL if n==3 else USER_SQL;need(held['sql']==sql and len(held['parameter_hex'])==2 and held['parameter_hex'][1]=='00010000'and held['errors']==['57014']and held['rows']==0 and held['tx']=='I'and held['sync_seen']is True,'ActualBoundedProducerErrorReady')
   if n==3:need(held['parameter_hex'][0]==b'network_access'.hex(),'ActualNetworkParameter')
   else:need(type(held['parameter_hex'][0])is dict and held['parameter_hex'][0].get('projection')=='credential parameter bytes evidence; not raw wire' and type(held['parameter_hex'][0].get('bytes'))is int and 0<held['parameter_hex'][0]['bytes']<=8192 and held['parameter_hex'][0]['sha256']==value['admin_principal']['api_key_sha256'],'BoundedActualPersistedAdminKeyParameter')
   expected=warm(row['warm_snapshot'],{'sql':sql,'parameter_hex':held['parameter_hex']});need(expected==row['warm_target'],'ExactSuccessfulWarmWire')
   need(expected['connection']==held['connection']and expected['backend_pid']==held['backend_pid']==row['cancelled_backend_pid']and expected['execute_generation']<held['execute_generation'],'SamePreparedBackendFreshExecute')
   need(row['pg_stat_actual_SQL_sha256']==hashlib.sha256(sql.encode()).hexdigest()and row['pg_stat_actual_SQL_bytes']==len(sql.encode())<1024,'FullShortPGStatQueryMatch')
   need(0<=row['request_started_monotonic']-row['prelude_completed_monotonic']<=1,'ActualCallbackDebounceWindow')
   need(row['state_held']['active']==1 and row['state_held']['available_writer_permits']==3 and row['drain_held']=={'shutdown_drained':False},'OriginalCoreOwnerUntilReady')
   natural=row['pg_natural_drain'];final=row['pg_terminal'];need(natural['connections']and all(c['closed']and c['up_joined']for c in natural['connections'])and final['listener_alive']is False and final['errors']==[],'PGPhysicalNaturalDrain')
   lock=row['observer_lock_cleanup'];need(lock['rollback_idle']is True and lock['autocommit_restored']is True and lock['before_pool_return_transaction_status']==0 and lock['wrapper_close_attempted']is True,'ObserverIdleAutocommitBeforePoolReturn')
   events=final['events'];arms=[e for e in events if e['kind']=='gate-armed'and e['arm_epoch']==held['arm_epoch']];execs=[e for e in events if e['kind']=='execute'and e['connection']==held['connection']and e['execute_generation']==held['execute_generation']]
   need(len(arms)==len(execs)==1 and expected['ready_monotonic']<=arms[0]['monotonic']<=held['execute_monotonic']<=execs[0]['monotonic'],'FreshPostArmExecute')
   if n==1:
    need(row['result']['error']is not None and row['caller_close']['fd_after']==-1 and row['caller_close']['original_inode_absent']is True and row['caller_close']['timer_joined']is True,'ActualClientPhysicalLoss')
    close=row['caller_close_details'];need(close['interruption']=={'interrupt_requested':True,'physical_close_claimed':False}and close['reader_joined']is True and close['final_close']==row['caller_close']==row['result']['socket']and close['join_completed_monotonic']<=close['final_close_completed_monotonic']<=row['release_monotonic'],'JoinCloseBeforeRelease')
   else:
    result=row['result'];need(result['error']is None and result['socket']['fd_after']==-1 and result['socket']['original_inode_absent']is True and result['socket']['timer_joined']is True and result['response']['status']==503 and 15<=result['elapsed_seconds']<=24 and result['completed_monotonic']<=row['release_monotonic'],'Actual15Observer503BeforeRelease')
  elif n==4:
   need(all(x['status']==200 for x in row['held_error_responses'])and[x['json']for x in row['held_error_responses']]==[{'held_count':i,'Core_status':401}for i in range(1,5)],'FourAdmittedErrorResponses')
   for name in('state_held','state_after_fifth'):need(row[name]['active']==4 and row[name]['available_writer_permits']==0 and row[name]['admission_closed']is False,'NoFifthAllowance')
   need(row['fifth']['status']==503 and row['fifth']['json']=={'detail':'CoreSettings resource capacity is unavailable.'}and row['history_before_fifth']==row['history_after_fifth'],'CapacityBeforeFailedLoginAccounting')
   need(row['release_frames']['json']=={'released':4}and row['reuse']['status']==200,'ActualRetirementAndReuse')
  else:
   setup=row['fixture_only_capacity_setup'];need(setup['timestamp_columns_modified']is False and setup['Source_write_claim']is False and setup['replacement_bytes']>65536,'NativeOnlyOwnedCapacitySetup')
   need(row['direct_capacity']['status']==503 and row['direct_capacity']['json']=={'detail':'REST authentication read capacity unavailable.'},'ActualBoundedCoreCapacitySurface')
   need(row['held_capacity']['json']=={'held_count':1,'Core_status':503}and row['state_held']['active']==1 and row['state_held']['available_writer_permits']==3,'CapacityErrorRetainsSameOwner')
   need(row['history_before_capacity']==row['history_after_capacity']and row['release_capacity']['json']=={'released':1}and row['fixture_only_capacity_restore']is True and row['reuse']['status']==200,'NoAuthPenaltyRestoreAndReuse')
 return {'qualified_physical_cases':6,'Source_requests':0,'Main_activated':False,'full_Source_error_or_timing_parity':False,'historical_execution_transferred':False}
