#!/usr/bin/env python3
"""Standalone projected build/Main7 evidence checks; no app, Docker or private fallback."""
import hashlib,json,math,re
from pathlib import Path
from qualify import phase,media_total
from source_reference import validate as validate_source6
from origin_coverage import validate as origin
from identity import kernel,worker
HERE=Path(__file__).resolve().parent
IMAGE='sha256:7fed56b01c41c4657c25bad95f2b12eb0b6dc1726c8e874dc9c561afcf06cf27'
REVISION='c0b824e115665a17823ac6511d465f1aaed2a6f1'
TREE='ff08e3f61d8196bb73863ddb4a9df262732fa7b8f76f68f312686aa9af25bdd8'
def need(v,label):
 if not v:raise ValueError(label)
def canon(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def sha(v):return hashlib.sha256(v).hexdigest()
def load(name):
 p=HERE/name;need(p.is_file()and not p.is_symlink()and p.stat().st_size<=4194304,'BoundedPublicJSON');return json.loads(p.read_bytes())
def trace_authority():
 for name,want in load('trace-authority.json').items():need(sha(canon(load(name)))==want,'ExactRetainedProjection:'+name)
def validate_build(build,review,probe):
 need(build['passed']is True and build['exit_code']==0 and build['git_revision']==REVISION and build['image_id']==IMAGE and build['source_tree_sha256']==TREE,'ExactImage59Build')
 m=build['source_sha256'];need(len(m)==1351 and sum(k.startswith('rust/')for k in m)==854 and sum(k.startswith('frontend/')for k in m)==495 and set(k for k in m if not k.startswith(('rust/','frontend/')))=={'LICENSE','ATTRIBUTION.md'},'Exact1351App854Rust495React2Credits')
 need(sha(json.dumps(m,sort_keys=True,separators=(',',':')).encode())==TREE,'RecomputedApplicationTree')
 need(build['source_archive_sha256']=='f81dad4566c26cb10b7f31f086ceb0a64e7355782ffabe703d1862ec2eda652c'and build['source_input_mode']=='committed-revision-snapshot','ExactImmutableNormalizedArchive')
 need(review['passed']is True and review['actual_build_wait_exit']==0 and review['all1351archive_bytes_exact_Git']is True and review['git_revision']==REVISION and review['image_id']==IMAGE and review['source_tree_sha256']==TREE,'RootActualGitBuildReceipt')
 need(review['Rust_inputs']==854 and review['React_inputs']==495 and review['credit_inputs']==2 and review['probe_actual_UID']==65532 and review['installed_asset_credit_files']==114 and review['unchanged_installed_React_and_original_credits']is True,'RootUIDFrontendCreditReceipt')
 need(probe['passed']is True and probe['actual_exit']==0 and probe['installed_hashed_files']==114 and probe['installed_frontend_and_credits_byteexact_image57']is True,'RootInstalled114Receipt')
 return {'application_inputs':1351,'Rust_inputs':854,'React_inputs':495,'credit_inputs':2,'Root_installed_file_receipt':114,'SDK_or_package28_or_performance':False}
def head_source(source,observed):
 need(len(source['cases'])==1 and source['Source_functions_replaced']is False and source['Native_requests']==0,'SeparateActualSourceHEAD1')
 r=source['cases'][0];need({k:r[k]for k in observed}==observed,'ExactRetainedSourceHEADFields')
 need(r['method']=='HEAD'and r['status']==401 and r['body_hex']==''and r['body_bytes']==0 and r['body_sha256']==sha(b''),'SourceHEAD401EmptyWire')
 headers={}
 for k,v in r['headers']:headers.setdefault(k.lower(),[]).append(v)
 for k,v in [('content-length','172'),('allow','OPTIONS, GET'),('vary','Accept, origin'),('content-type','application/json'),('www-authenticate','Bearer realm="api"')]:need(headers.get(k)==[v],'SourceHEADField:'+k)
 need('x-worker-id'not in headers and r['before']==r['after']and source['before']==source['after'],'SourceHEADNoWorkerSQLUnchanged')
 need(r['physical_close']['fd_after']==-1 and r['physical_close']['original_inode_absent']is True and r['absolute_observation']['timer_joined']is True,'SourceHEADPhysicalClose')
 return {'Source_HEAD_status':401,'Source_HEAD_wire_body_bytes':0,'Source_advertised_length_not_decoded_body':172}
def validate(native,summary,auth,agent):
 policy=load('blocked-code/policy.json');ref=load('provenance.json');need(ref['candidate_revision']==REVISION and ref['candidate_image']==IMAGE and ref['candidate_tree']==TREE and ref['support_revision']=='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f','DistinctCandidateAndSupportBindings')
 source=validate_source6(load('source-six-phase.json'),load('source-six-failed-summary.json'),load('source-six-auth.json'),(HERE/'source-six.stderr').read_text(),policy)
 q=phase(native,policy,'native');need(q==native['qualification']and native==summary['native']and summary['retained_Source6_offline_reference']==source,'ActualNativeAndSource6Recomputed')
 headref=summary['retained_Source_HEAD1_offline_reference'];need(headref['actual_capture_sha256']=='e9db2918f98adae5ae1766c4b327349f8b94b30c726a406e336cfbb86f1a2891'and headref['status']==401 and headref['wire_body_bytes']==0 and headref['Source6_epoch_separate']is True and headref['Source_requests_rerun']is False and headref['full_header_or_method_parity']is False,'SeparateMeasuredSourceHEADReference')
 need(summary['passed']is True and summary['candidate']['image_id']==IMAGE and summary['Source_VOD_requests_rerun']is False and summary['Source_business_methods_replaced']is False and summary['resource_sampler']is False and summary['full_M4_or_header_parity']is False,'ActualOnlySevenScope')
 need([r['status']for r in native['cases']]==[405,401,503,200,206,500,401],'LiteralSevenStatuses')
 need(native['Source_HEAD_reference_is_separate_retained_epoch']is True and native['Source_HEAD_request_rerun']is False,'Source6HEAD1DistinctEpochNoReplay')
 raw=native['kernel_witness']['raw_witness'];w=kernel(*(bytes.fromhex(raw[k])for k in ['hostname_hex','stat_hex','status_hex','cgroup_hex']),1,native=True);need(w==native['kernel_witness']and w['uid']==65532 and w['comm']=='dispatcharr-rs'and w['kernel_hostname']!='owned-environment-spoof','ActualKernelHostnamePIDUID')
 need(auth['status']==200 and auth['expected_real_SQL_user_id']==auth['auth_response_user_id']and type(auth['auth_response_user_id'])is int and auth['auth_response_user_level']==10 and auth['auth_response_stream_limit']==0 and auth['raw_body_and_authentication_material_omitted']is True,'ActualSQLBearerTypedProjection')
 need(auth['physical_close']['fd_after']==-1 and auth['physical_close']['original_inode_absent']is True and auth['absolute_observation']['timer_joined']is True and native['phase_started']<=auth['request_started']<=auth['request_completed']<=native['cases'][0]['request_started'],'AuthClosedBeforeCases')
 before,after=summary['default_before'],summary['default_after'];need(before==after and summary['default_named_tables_sequences_unchanged']is True and len(before['tables'])==len(before['sequences'])==9,'DefaultNineLiteralEqualityNotFull6259')
 for value in [before,after,summary['template_fingerprint']]:need(sha(json.dumps({'tables':value['tables'],'sequences':value['sequences']},sort_keys=True).encode())==value['dataset_sha256'],'NamedFingerprintDigest')
 clone=summary['clone_binding'];need(clone['independent_names']is True and clone['named_tables_and_sequences_equal']is True and clone['full_database_hash_claimed']is False and clone['dataset_sha256']==summary['template_fingerprint']['dataset_sha256'],'OwnedCloneBeforeScope')
 pre=summary['origin_preflight'];need(pre['passed']is True and pre['status']==206 and pre['prefix_bytes']==8192 and pre['prefix_sha256']==policy['prefix_sha256']and pre['preflight_origin_GET_requests']==1,'OriginRange8192PreflightNotDecoder')
 o=origin(summary['origin_terminal'],summary['fixture_nonce_sha256'],terminal=True);need(o==summary['origin_terminal_validation']and summary['origin_terminal']['server_thread_settled']is True,'OriginPhysicalTerminal')
 need(len(summary['origin_terminal']['requests'])==20 and len(summary['origin_terminal']['connections'])==19,'ActualOrigin20Requests19Sockets')
 need(summary['remaining']=={'containers':'','networks':'','volumes':''},'RetainedOwnedProjectEmpty')
 need(agent['reviewer'].startswith('Agent /root/stream_http')and agent['passed']is True and agent['assertions']==71 and agent['actual_image']==IMAGE,'IndependentAgent71NotRoot')
 limits=agent['actual_scope_limits'];need(limits['decoder_run']is False and limits['default_full6259']is False and limits['default_audit_named_tables_sequences']==9 and limits['noSDK_package28_performance_or_fullM4']is True,'LiteralMissingScopeNotInvented')
 fresh=agent['fresh_cleanup'];need(len(fresh)==6 and[r['actual_exit']for r in fresh]==[0,0,0,1,1,1]and all(r['pipes_settled']is True for r in fresh),'FreshScopedThreeInventoriesThreeRoleNameAbsences')
 return {'Native_cases':7,'statuses':[r['status']for r in native['cases']],'worker_header200_206':True,'new_invalid_HEAD401_single_case':True,'Source6_epoch_and_HEAD1_epoch_separate':True,'decoder_run':False,'default_named_scope':9,'owned_full_after_SQL':'unmeasured','SDK_package28_performance_or_fullM4':False}
def once():
 value=load('once.json');cue,claim=value['cue'],value['claim'];need(cue['authorized']is True and cue['candidate_application_requests_authorized']is True and cue['Source_VOD_requests_authorized']is False and cue['action']==claim['action']=='image59-worker-header-native-routed-and-head401-once','ExactOnceAction')
 need(cue['cases']==load('blocked-code/policy.json')['Native_cases']and cue['build_manifest_sha256']==sha((HERE/'build.json').read_bytes())and cue['source_tree_sha256']==TREE and cue['source_archive_sha256']==load('build.json')['source_archive_sha256'],'ExactActualCueCasesAndBuild')
 need(cue['candidate_image']==IMAGE and cue['git_revision']==REVISION and cue['Root_reviewed_Rust_input_count']==854 and cue['nonce_sha256']==claim['nonce_sha256']and cue['boot_id_sha256']==claim['boot_id_sha256']and claim['cue_sha256']==value['raw_cue']['sha256'],'ActualCandidateCueClaim')
 need(0<cue['expires_epoch']-cue['issued_epoch']<=1800 and cue['issued_epoch']<=claim['claimed_epoch']<cue['expires_epoch'],'ActualOnceWindow')
 code=load('blocked-closure.json');need(len(code['original56_input_rows'])==56 and len(code['exact_code_files'])==46 and len(code['private_reference_files_not_published'])==10 and code['full_original_execution_closure_publicly_restored']is False and code['original_selection_sha256']==cue['selection_sha256']==claim['selection_sha256'],'ExplicitPartialHistoricalCodeClosure')
 for row in code['exact_code_files']:need(sha((HERE/'blocked-code'/row['path']).read_bytes())==row['sha256'],'ExactBlockedCode:'+row['path'])
 need(b'\nEXECUTION_BLOCKED=True\n'in(HERE/'blocked-code/launch_once.py').read_bytes()and b'\nHARD_BLOCKED = True\n'in(HERE/'blocked-code/tools/run.py').read_bytes(),'OriginalLauncherAndCLIBlocksPreserved')
 return {'once_projection_valid':True,'exact_blocked_code_files':46,'omitted_private_reference_inputs':10,'full_historical_application_replay_public':False}
def history():
 value=load('history.json');need(value['old_Native_HEAD405']['status']==405 and value['old_Native_HEAD405']['not_relabelled_or_rerun']is True and value['old_Source7_failure']['passed']is False and value['old_Source7_failure']['excluded_unrouted_SPA_status']==500 and value['old_Source7_failure']['missing_template_fixture_diagnostic_not_VOD_authority']is True,'Historical405AndSourceFailurePreserved')
 get=value['separate_Source_compiled_context_GET'];need(get['method']=='GET'and get['status']==200 and get['body_bytes']==858 and get['body_sha256']=='b48dbb556b1bbe0ac3af50a9afbc528fc438eac114f4c924c5699a366d19d10a'and get['not_in_Main7_or_full_route_parity']is True,'SeparateCompiledTemplateContextNotMain7')
 return {'old_Native405_and_Source7_failure_preserved':True,'Source_SPA_GET_context_separate':True}
def inventory():
 want=load('packet-inventory.json');actual={str(p.relative_to(HERE)):sha(p.read_bytes())for p in HERE.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name!='packet-inventory.json'};need(want==actual,'ExactPacketInventory');return len(actual)
if __name__=='__main__':trace_authority();print(json.dumps({'public_files':inventory(),**validate_build(load('build.json'),load('Root-build-review.json'),load('Root-installed-probe.json')),**head_source(load('source-head-one.json'),load('source-head-observation.json')),**validate(load('native-phase.json'),load('native-summary.json'),load('native-auth.json'),load('agent-review.json')),**once(),**history()},indent=2))
