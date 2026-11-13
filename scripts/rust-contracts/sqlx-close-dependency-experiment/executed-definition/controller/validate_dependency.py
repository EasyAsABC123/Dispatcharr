"""Read-only actual-three-case SQLx contract validation; negatives stay negative."""
import json
from pathlib import Path
from gate_validation import raw_stage,post_close_terminal
ROOT=Path(__file__).resolve().parents[1]
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal'}
def require(ok,label):
 if not ok:raise ValueError(label)
def audit(value):
 require(set(value)=={'tables','sequences'}and set(value['tables'])==set(value['sequences'])==TABLES,'Exact11TablesSequences')
 for sequence in value['sequences'].values():require(set(sequence)=={'last_value','is_called'}and type(sequence['last_value'])is int and type(sequence['is_called'])is bool,'LiteralSequences')
 return value
def validate_case(row,ledger,peer_receipt):
 number=row['id'];require(type(number)is int and number in(1,2,3)and row['Source_Main_or_HTTP_runs']==0 and row['max_connections']==1 and row['subject_close_calls']==1 and row['application_statement_retries']==0,'FiniteDependencyCaseScope')
 gate=json.loads((ROOT/'inputs/gate-contracts.json').read_text())['dependency-backend-id-ready'];captured=row['held_actual_select_ready'];held=raw_stage({'stage':'dependency-backend-id-ready','proxy':captured['proxy'],'actual_SQLx_client_sockets':captured['actual_SQLx_socket'],'backend_activity':captured['actual_backend']},gate);require(row['actual_query_task_joined']is True,'ActualQueryTaskJoined')
 require(audit(row['before'])==audit(row['after'])and row['all11_tables_and_sequences_unchanged']is True,'All11RowsAndSequencesUnchanged')
 if number==1:require(row['before_first_close']=={'is_closed':False,'size':1,'num_idle':1,'after_release_calls':1,'healthy_idle_proven':True}and row['after_release_calls']==1,'HealthyIdleControl')
 elif number==2:require(row['before_connection_drop']=={'closed':True,'first_close_polled_pending':True,'after_release_calls':0}and row['after_release_calls']==0,'CloseBeforeDropControl')
 else:require(row['actual_after_release_barrier']=={'hook_entered_after_initial_closed_check':True,'first_close_marks_closed_before_hook_release':True,'same_first_close_future_polled_pending':True,'after_release_calls':1,'no_sleep_as_ordering_authority':True}and row['after_release_calls']==1,'ActualCallbackBarrierOrdering')
 observation=row['first_close_observation'];require(observation['window_seconds']==10 and observation['additional_close_calls']==0 and observation['application_statement_retries']==0 and len(ledger)==observation['observations']and 1<=len(ledger)<=128 and ledger[-1]==observation['terminal'],'ExactAppendOnlyFirstCloseLedger');require(observation['initial_metadata_before_observer_await']['is_closed']is True,'PoolClosedBeforeObserverAwait');require(audit(observation['SQL_after_first_close_return'])==row['before'],'SQLSnapshotAfterFirstClose')
 for index,sample in enumerate(ledger,1):
  require(sample['ordinal']==index and sample['metadata_after']['is_closed']is True and sample['old_SQLx_socket_identity']==captured['actual_SQLx_socket'],'ActualFirstCloseObservedIdentity');require(sample['accepted']is post_close_terminal(sample),'PhysicalEndpointPredicateMatchesRawFacts');require(len(sample['relay']['connections'])==1 and sample['relay']['connections'][0]['backend_pid']==held['backend_pid'],'NoUnobservedReplacement')
 terminal=ledger[-1];accepted=post_close_terminal(terminal);require(observation['passed']is accepted and row['first_close_contract_passed']is accepted and row['passed']is accepted,'RealContractOutcomeNotWaived')
 if not accepted:require(terminal['elapsed_millis']>=10000,'EntireNegativeWindowObserved')
 cleanup=row['cleanup_after_last_pool_handle_drop'];require(cleanup['original_socket_inodes_absent']is True and cleanup['actual_backend_absent']==[]and row['pool_drop_cleanup_cannot_waive_first_close_negative']is True,'SeparateLastHandleDropCleanup');settled=row['cleanup_relay_backend_absence'];require(settled['backend_absence']and all(r['state']==[]for r in settled['backend_absence'])and all(r['closed']is True and r['up_joined']is True for r in settled['proxy']['connections']),'ActualCleanupRelayBackendAbsence');require(peer_receipt['forced']is False and peer_receipt['exit']==peer_receipt['wait']==0 and peer_receipt['signal']is None and peer_receipt['proc_PID_absent_after_wait']is True and peer_receipt['stdout_reader_joined']is True,'ActualPeerStopWaitPipes0')
 return {'case':number,'passed':accepted,'SQL_rows_sequences_qualified':True,'actual_first_close_samples':len(ledger),'cleanup_after_last_handle_drop_qualified':True,'cleanup_does_not_waive_first_close_negative':True}
def validate_directory(directory,variant):
 directory=Path(directory);report=json.loads((directory/'dependency-report.json').read_text());require(variant in('baseline','candidate')and report['variant']==variant and report['Source_Main_API_runs']==0 and report['case_order']==[1,2,3]and[row['id']for row in report['cases']]==[1,2,3],'AllThreeActuallyCapturedInOrder');results=[]
 for row in report['cases']:
  folder=directory/('case-'+str(row['id']));literal=json.loads((folder/'case.json').read_text());require(literal==row,'RawCaseAggregateBinding');ledger=[json.loads(line)for line in(folder/'post-first-close-observations.jsonl').read_text().splitlines()];peer=json.loads((folder/'proxy-process.json').read_text());results.append(validate_case(row,ledger,peer))
 passed=all(r['passed']for r in results);require(report['passed']is passed,'ActualThreeCaseAcceptance');return {'passed':passed,'actual_cases':3,'all_three_observations_and_cleanup_validated':True,'cases':results,'Source_Main_API_or_general_SQLx_shutdown_qualification':False,'known_notification2_4_6_negatives_waived':False}
