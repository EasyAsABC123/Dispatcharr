#!/usr/bin/env python3
"""Portable finite evidence verification; no private fallback or app dispatch."""
import hashlib,json,math,re
from pathlib import Path
from phase_predicates import validate as phase
from bootstrap_context import validate as builtin
HERE=Path(__file__).resolve().parent
AUTHORITY_SHA='22e6bff7c6f487a5b0bdae033883ad98a749de976582e40f62d6f82473e211a6'
EMPTY=hashlib.sha256(b'').hexdigest()
STEPS=['discover_partial_role_ownership','stop_normal_Main','owned_backend_absence_Unix_observer','default_full_after_audit','stop_owned_origin','origin_listener_socket_handler_ack','stop_management_PG_Redis','compose_down_owned_volumes','fresh_project_and_exact_CID_absence','all_original_PID_start_wait_pipe_settlement']
def need(v,label):
 if not v:raise ValueError(label)
def hex64(s):return type(s)is str and re.fullmatch('[a-f0-9]{64}',s)is not None
def load(name):return json.loads((HERE/name).read_text())
def completion(v,n):
 commands=v['commands'];need(type(commands)is list and len(commands)==n and [c['ordinal']for c in commands]==list(range(1,n+1)),'ExactActualCommandCoverage')
 identities=[]
 for c in commands:
  need(c['actual_exit']==0 and c['collection_invalid']is None and c['physically_settled']is True and c['pipe_threads_settled']is True,'ActualWaitExitAndPipes')
  need(all(type(c[k])in(int,float)and math.isfinite(c[k])for k in ['began_epoch','completed_epoch'])and c['began_epoch']<=c['completed_epoch'],'FiniteActualCommandClocks')
  need(all(hex64(c[k])for k in ['process_identity_sha256','argv_sha256','cwd_sha256','stdout_sha256','stderr_sha256']),'HashBoundOmittedCommandIdentityAndStreams');identities.append(c['process_identity_sha256'])
 need(len(set(identities))==n,'DistinctActualCLIIdentities')
 absence=v['original_host_identity_absence'];need(len(absence)==n+(3 if n==45 else 2)and all(hex64(x['process_identity_sha256'])and(x['current_identity_sha256']is None or hex64(x['current_identity_sha256'])and x['current_identity_sha256']!=x['process_identity_sha256'])for x in absence),'CompleteOriginalIdentityAbsence')
 need(set(identities).issubset({x['process_identity_sha256']for x in absence}),'EveryActualCLIHasAbsenceReceipt')
 need([x['step']for x in v['cleanup_steps']]==STEPS and all(x['settled']is True for x in v['cleanup_steps']),'TenUnconditionalCleanupSteps')
 need(v['fresh_project_cleanup']=={'container':[],'network':[],'volume':[]},'EmptyExactProject')
 roles={'candidate','origin','baseline','data-init'}if n==45 else{'baseline','origin'};cids=v['fresh_exact_CID_cleanup'];need(len(cids)==len(roles)and{x['role']for x in cids}==roles and len({x['CID_sha256']for x in cids})==len(roles)and all(hex64(x['CID_sha256'])and x['absent']is True for x in cids),'EveryExactOwnedCIDAbsence')
 for k,limit in [('host_elapsed_seconds',600),('cleanup_elapsed_seconds',90)]:need(type(v[k])in(int,float)and math.isfinite(v[k])and 0<=v[k]<=limit,'DeclaredHostCleanupLimit')
 need(v['host_within600']is True and v['cleanup_within90']is True,'ActualHostCleanupFlags')
def qualify(v,negative,build,fresh,history):
 result=phase(v);builtin(v['migration_builtin_context'],v['default_before']);completion(v,45)
 need(v['passed']is True and v['controller_exit']==0 and v['owned_backends_after_stop']==0 and v['installed_index_sha256']==v['cases'][0]['body_sha256'],'ActualSuccessfulHostAndInstalledIndex')
 need(type(v['capture_phase_elapsed_seconds'])in(int,float)and math.isfinite(v['capture_phase_elapsed_seconds'])and 0<=v['capture_phase_elapsed_seconds']<=40,'ActualCapture40')
 need(build['passed']is True and build['exit_code']==0 and build['git_revision']=='2dd210cf37877d9e132c864a54889fc66d6dd3b1'and build['image_id']==v['candidate_image']and build['source_tree_sha256']==v['candidate_tree']and len(build['source_sha256'])==1353 and sum(k.startswith('rust/')for k in build['source_sha256'])==856 and sum(k.startswith('frontend/')for k in build['source_sha256'])==495,'ExactImage60Build1353AndRuntime856')
 need(negative['passed']is False and negative['controller_exit']==1 and negative['actual_business_request_invocations']==0 and negative['Source_requests']==0 and negative['first_failure']['message']=='NoUserMovieProviderSeedOrExistingRows','PreservedOriginalHost1ZeroBusinessNegative');completion(negative,26)
 need('owned_before'not in negative and 'cases'not in negative and not negative.get('owned_clone_created_without_rows_seeded')and negative['default_before']==negative['default_after'],'NoFabricatedOldMainOrOwnedEpoch')
 need(negative['default_before']['tables']['accounts_user']['rows']==negative['default_before']['tables']['vod_movie']['rows']==0 and negative['default_before']['tables']['m3u_m3uaccount']['rows']==negative['default_before']['tables']['m3u_m3uaccountprofile']['rows']==1,'LiteralOldMigrationCounts')
 q=fresh['queries'];need(fresh['passed']is True and len(q)==7 and {x['query']for x in q}=={'container','network','volume','CID:candidate','CID:origin','CID:baseline','CID:data-init'}and all(x['actual_exit']==0 and x['physically_settled']is True and x['pipe_threads_settled']is True and x['empty']is True and x['stdout_sha256']==EMPTY for x in q),'IndependentSevenOwnedAbsenceQueries')
 need(history['run1_actual_host_exit']==1 and history['run1_business_requests']==0 and history['run2_actual_host_exit']==0 and history['run2_business_requests']==1 and history['Source_requests_both_runs']==0 and history['row_or_task_deletions_or_disables']==0,'LiteralAttemptHistoryNoRetryOrSourceReplay')
 return {**result,'actual_controller_exit':0,'full_default_and_owned_request_SQL_tables':62,'full_default_and_owned_request_SQL_sequences':59,'actual_settled_CLI_receipts':45,'fresh_exact_owned_cleanup_queries':7,'preserved_setup_negative_host_exit':1,'public_hash_receipt_scope':'host argv/PID/nonces and stream contents omitted; exact private transformation independently reviewed, not publicly re-executed'}
def check_once(v,build,blocked_run):
 cue,claim=v['cue'],v['claim'];need(cue['authorized']is True and cue['Native_business_requests']==1 and cue['Source_requests']==0 and all(cue[k]is False for k in ['resource_sampling','full_M4_acceptance','default_worker_activation','Source_replay']),'ExactNativeOnlyCueScope')
 need(cue['candidate_image']==build['image_id']and cue['source_archive_sha256']==build['source_archive_sha256']and cue['candidate_manifest_sha256']=='b408966cacef65e372ef1074fa204acb77f6f9ef26e3416ca6a1eb661c5c993e','CueExactImmutableCandidate')
 need(claim['cue_sha256']==v['raw_cue']['sha256']and claim['seal_sha256']==cue['seal_sha256']and claim['execution_run_sha256']==cue['execution_run_sha256']and claim['nonce_sha256']==cue['nonce_sha256'],'ClaimRawCueSealRunNonceChain')
 need(all(type(x)in(int,float)and math.isfinite(x)for x in [cue['issued_epoch'],cue['expires_epoch'],claim['claimed_epoch']])and 0<cue['expires_epoch']-cue['issued_epoch']<=1200 and cue['issued_epoch']<=claim['claimed_epoch']<cue['expires_epoch'],'ActualFreshClaimWindow')
 need(hashlib.sha256(blocked_run.replace(b'\nHARD_BLOCKED=True\n',b'\nHARD_BLOCKED=False\n')).hexdigest()==cue['execution_run_sha256'],'ExactFlagOnlyActivatedRunBytes')
def verify():
 raw=(HERE/'trace-authority.json').read_bytes();need(hashlib.sha256(raw).hexdigest()==AUTHORITY_SHA,'ExactRetainedProjectionAuthority')
 for name,want in json.loads(raw).items():need(hashlib.sha256((HERE/name).read_bytes()).hexdigest()==want,'ExactRetainedInput:'+name)
 v=load('native-execution.json');result=qualify(v,load('setup-negative.json'),load('build.json'),load('agent-fresh-cleanup.json'),load('history.json'))
 for label,key in [('default-before','default_before'),('default-after','default_after'),('owned-before','owned_before'),('owned-after','owned_after')]:need(load(label+'.json')==v[key],'ExactSeparateFullAudit:'+label)
 capture=load('native-capture.json');need(all(capture[k]==v[k]for k in capture),'ExactRetainedPhysicalCapture')
 closure=load('blocked-closure.json');need(closure['full_historical_application_replay_public']is False and len(closure['exact32_code_files'])==32,'HistoricalBlockedCodeScope')
 for x in closure['exact32_code_files']:need(hashlib.sha256((HERE/'blocked-code'/x['path']).read_bytes()).hexdigest()==x['sha256'],'ExactBlockedCode:'+x['path'])
 need(b'\nHARD_BLOCKED=True\n'in(HERE/'blocked-code/run.py').read_bytes(),'OriginalLauncherStillBlocked');check_once(load('once.json'),load('build.json'),(HERE/'blocked-code/run.py').read_bytes())
 probe=load('Root-installed-probe.json');need(probe['passed']is True and probe['actual_exit']==0 and probe['installed_hashed_files']==114 and probe['installed_frontend_and_credits_byteexact_image57']is True,'ActualInstalledFrontendCreditProbe')
 return result
if __name__=='__main__':print(json.dumps(verify(),indent=2))
