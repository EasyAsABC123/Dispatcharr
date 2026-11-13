"""Independent finite predicate checks; no flag-only or empty-coverage pass."""
import hashlib,json,math
from pathlib import Path
SQL='SELECT id::bigint AS id,key,name,octet_length(value::text) AS bytes,CASE WHEN octet_length(value::text)<=65536 THEN value::text END AS value_text FROM core_coresettings WHERE id::bigint=$1'

def need(ok,label):
    if not ok:raise ValueError(label)
def exact(a,b):return type(a)is type(b)and json.dumps(a,sort_keys=True,separators=(',',':'))==json.dumps(b,sort_keys=True,separators=(',',':'))
def validate(value,out):
    need(value['Native_only']is True and value['Source_business_requests']==0,'ComponentOnly')
    need(value['same4_writer_permits']is True,'ExistingBudgetOnly')
    rows=value['cases'];need(type(rows)is list and len(rows)==1 and rows[0]['case']in range(1,8),'OnePredeterminedPositive')
    for row in rows:
        need(row['watchdog_joined']is True and row['watchdog_expired']is False,'InFlightCaseDeadlineSettled')
        need(len(row['cleanup_steps'])==9 and all(x['settled']is True for x in row['cleanup_steps']),'UnconditionalAllCleanupStepsSettled')
        n=row['case'];need(type(row['elapsed_seconds'])in(int,float)and math.isfinite(row['elapsed_seconds'])and 0<row['elapsed_seconds']<=75,'FixedCase75')
        need(type(row['peer_PID'])is int and row['peer_PID']>1 and type(row['peer_starttime'])is str and row['peer_starttime'].isdigit(),'PhysicalProcessIdentity')
        need(row['peer_actual_exit']==0 and row['peer_pipes_settled']is True and row['original_peer_PID_absent']is True,'ActualPeerWaitAndPipes')
        expected=json.loads((Path(__file__).resolve().parent/'native-inputs.json').read_text())['binary_sha256']
        live=row['live_identity'];need(live['PID']==row['peer_PID']and live['starttime']==row['peer_starttime']and live['exe_sha256']==expected,'ActualLiveELFIdentity')
        folder=Path(out)/('physical-'+str(n))
        for kind in('stdout','stderr'):
            need(hashlib.sha256((folder/('peer.'+kind)).read_bytes()).hexdigest()==row['peer_'+kind+'_sha256'],'ActualPeerTranscriptHash')
        stdout=(folder/'peer.stdout').read_text();need(stdout.count('core_settings_writes::physical_tests::actual_core32_http_peer ... ok')==1 and '1 passed; 0 failed' in stdout,'OneRealPeerTestPassed')
        need(exact(row['terminal'],{'shutdown_drained':True,'active':0,'available_writer_permits':4,'cache_physical_operations':0,'pool_closed':True}),'ActualTerminalOwnersZero')
        settled=row['state_settled'];need(settled['active']==0 and settled['available_writer_permits']==4 and settled['cache_physical_operations']==0,'ActualSettledSameBudget')
        if n!=6:
            held=row['state_held'];need(type(held['active'])is int and held['active']>=1 and held['available_writer_permits']<=3,'PhysicalOwnerNotReleased')
            need(row['drain_held']=={'shutdown_drained':False},'HeldShutdownFalse')
        if n<=4:
            held=row['held'];need(type(held['execute_generation'])is int and held['execute_generation']>0 and type(held['arm_epoch'])is int and held['arm_epoch']>0,'FreshExecuteGenerationAfterArm')
            need(type(held['backend_pid'])is int and held['backend_pid']>0,'ActualTargetBackendPID')
            if n==2:need(row['cancelled_backend_pid']==held['backend_pid'],'SameOwnedCancelledBackend')
            events=row['pg_terminal']['events'];arms=[e for e in events if e['kind']=='gate-armed'and e['arm_epoch']==held['arm_epoch']]
            starts=[e for e in events if e['kind']=='execute'and e['execute_generation']==held['execute_generation']and e['connection']==held['connection']]
            ends=[e for e in events if e['kind']=='ready-held'and e['execute_generation']==held['execute_generation']and e['connection']==held['connection']]
            need(len(arms)==len(starts)==len(ends)==1 and arms[0]['monotonic']<=held['execute_monotonic']<=starts[0]['monotonic']<=ends[0]['monotonic'],'ActualFreshExecuteArmReadyTimeline')
            need(held['sql']==SQL and held['parameter_hex']==[(1002).to_bytes(8,'big',signed=True).hex()]and held['sync_seen']is True,'ExactOwnedQueryBindSync')
            need(held['errors']==(['57014']if n==2 else[])and held['rows']==(0 if n==2 else 1),'ActualSuccessErrorSchema')
            terminal=row['pg_terminal'];natural=row['pg_natural_drain'];need(terminal['errors']==[]and terminal['listener_alive']is False and natural['connections']and all(c['closed']and c['up_joined']for c in natural['connections']),'PGActualNaturalDrain')
        if n in(1,2,5):need(row['result']['response']['status']==(503 if n==2 else 200),'ActualSuccessErrorSurface')
        if n==2:need(row['result']['response']['body_hex']==b'{"error":"Database unavailable"}'.hex(),'CompiledDatabase503Body')
        if n==3:
            need(row['result']['response']['status']==503,'ObserverDeadlineNotPhysicalCompletion')
            result=row['result'];need(type(result['elapsed_seconds'])in(int,float)and math.isfinite(result['elapsed_seconds'])and 15<=result['elapsed_seconds']<=24 and result['completed_monotonic']<=row['release_monotonic'],'ObserverReturnedWhileActualSQLReplyHeld')
        if n==4:
            need(row['result']['error']is not None and row['caller_close']['original_inode_absent']is True and row['caller_close_monotonic']<row['release_monotonic'],'ActualCallerSocketLoss')
            close=row['caller_close_details'];need(close['interruption']=={'interrupt_requested':True,'physical_close_claimed':False}and close['reader_joined']is True and close['join_completed_monotonic']<=close['final_close_completed_monotonic']and close['final_close']==row['caller_close']==row['result']['socket']and close['final_close']['fd_after']==-1 and close['final_close']['timer_joined']is True and close['final_close_completed_monotonic']<=row['release_monotonic'],'ReaderJoinBeforeFinalPhysicalClose')

        if n==5:
            terminal=row['resp_terminal'];need(terminal['connections']==0 and terminal['invalid']==[],'RESPNaturalDrain')
            starts=[r for r in terminal['rows']if r['kind']=='backend-target-reply-read-held'];ends=[r for r in terminal['rows']if r['kind']=='backend-target-reply-released']
            need(len(starts)==len(ends)==1 and starts[0]['reply_hex']in('3a300d0a','3a310d0a'),'ActualDELIntegerReply')
        if n==6:
            need(row['result']['status']==500,'ActualDeleteSignalErrorRollback')
            from acl_contract import untyped,snapshot,ok_reply,disabled_DEL,restored,restore_rule_tokens
            setup=row['ACL_setup'];need(ok_reply(untyped(setup['reply_typed'])),'TypedSetupOKReply')
            baseline=snapshot(untyped(setup['before_typed']));disabled_DEL(baseline,snapshot(untyped(setup['after_typed'])))
            restore=row['ACL_restore'];need(restore['command_tokens']==row['cleanup_ACL_command_tokens']==restore_rule_tokens(baseline),'CapturedOriginalACLRulesReapplied');need(ok_reply(untyped(restore['reply_typed'])),'TypedRestoreOKReply');restored(baseline,snapshot(untyped(restore['state_typed'])))
            need(ok_reply(untyped(row['cleanup_ACL_reply_typed'])),'TypedCleanupOKReply');restored(baseline,snapshot(untyped(row['cleanup_ACL_state_typed'])))

        if n==7:
            need([r['json']for r in row['held_frames']]==[{'held_count':i,'Core_status':200}for i in range(1,5)],'FourActualSQLResponseFrames')
            need(row['state_held']['active']==4 and row['state_held']['available_writer_permits']==0 and row['state_held']['admission_closed']is False,'NoExtraGrantForFifth')
            need(row['fifth']['status']==503 and row['fifth']['json']=={'detail':'CoreSettings resource capacity is unavailable.'},'TypedCapacityBeforeAuth')
            need(exact(row['history_before_fifth'],row['history_after_fifth']),'NoFailedLoginReclassification')
            need(row['state_after_fifth']['active']==4 and row['state_after_fifth']['available_writer_permits']==0 and row['state_after_fifth']['admission_closed']is False,'FifthPreservesAllFourFrames')
    return {'qualified_physical_cases':1,'Source_requests':0,'Main_activated':False,'whole_Source_error_or_timing_parity':False}
