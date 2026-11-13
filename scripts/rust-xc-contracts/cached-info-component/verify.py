"""Portable finite verifier. Public files only; never use artifacts/private fallback."""
import hashlib,json,math,re,urllib.parse
from pathlib import Path
HERE=Path(__file__).resolve().parent
PAYLOAD_SHA='edb2af18d28d0c981b896b303b3a5a958331b8639de0fbfa2258c23dc9c3d85e'
NAMES=[r+':'+a for r in ['admin','standard','basic','hide_adult','disabled']for a in ['get_vod_info','get_series_info']]+['movie-info-missing-id','series-info-missing-id']
FILTERS=['held_final_frame_keeps_exact_capacity_lease_after_shutdown', 'cancelled_sql_caller_retains_admission_until_owned_worker_finishes', 'epg_sql_cancellation_and_detached_body_keep_bounded_lifetimes', 'category_sql_after_observer_deadline_keeps_physical_owner', 'category_sql_after_caller_loss_keeps_physical_owner', 'auth_cache_logical_error_keeps_admission_until_actual_reply', 'auth_cache_caller_loss_and_shutdown_keep_admission_until_actual_reply', 'bounded_user_sql_after_deadline_keeps_existing_grants', 'bounded_user_sql_after_caller_loss_keeps_existing_grants', 'bounded_user_oversize_is_not_a_failed_login_mutation', 'bounded_list_final_frame_keeps_same_grant_through_shutdown', 'cached_info_parent_after_deadline_keeps_existing_owners', 'cached_info_parent_after_caller_loss_keeps_existing_owners', 'cached_info_episode_after_caller_loss_keeps_parent_and_existing_owners', 'cached_info_final_frame_keeps_existing_owners_through_shutdown']
PIPE=['stdout_pipe_closed','stderr_pipe_closed','pipe_reader_threads_settled','stdin_writer_settled','pipes_physically_settled']
def need(ok,label):
 if not ok:raise ValueError(label)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def number(v):return type(v)in[int,float]and math.isfinite(v)
def digest(v):return type(v)is str and re.fullmatch('[0-9a-f]{64}',v)is not None
def read(name):return json.loads((HERE/name).read_bytes())
def safe_public_body(raw):
 
 if raw.lstrip().startswith(b'<!doctype html>'):
  text=raw.decode('utf-8');need('<title>Not Found</title>'in text and 'eyJ'not in text and 'http:'not in text and 'https:'not in text,'FiniteLiteral404HTMLPrivacy');return True
 value=json.loads(raw);pending=[(value,0)];seen=0
 while pending:
  item,depth=pending.pop();seen+=1;need(seen<=100000 and depth<=32,'BoundedPublicPrivacyWalk')
  if type(item)is dict:
   for key,child in item.items():
    if re.search(r'password|username|authorization|api.?key|(?:access|refresh)_token|secret',key,re.I):need(not(type(child)is str and child),'NoCredentialFieldValue')
    pending.append((child,depth+1))
  elif type(item)is list:pending.extend((child,depth+1)for child in item)
  elif type(item)is str:
   need(re.search(r'eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+',item)is None,'NoRawJWT')
   decoded=item
   for _ in range(4):
    newer=urllib.parse.unquote(decoded)
    if newer==decoded:break
    decoded=newer
   try:url=urllib.parse.urlsplit(decoded)
   except ValueError:continue
   if url.scheme in ['http','https']:
    need(url.username is None and url.password is None,'NoURLUserinfo')
    for key,values in urllib.parse.parse_qs(url.query,keep_blank_values=True).items():
     if re.search(r'password|username|secret|token|api.?key|^key$|auth',key,re.I):need(not any(values),'NoSensitiveURLQueryValue')
     pending.extend((child,depth+1)for child in values)
 return True
def terminal(value,role):
 need(value['role']==role and value['terminal_state']=='absent'and value['physical_process_stop_observed']is True and type(value['same_owned_starttime'])is str and value['same_owned_starttime'].isdigit()and int(value['same_owned_starttime'])>0,'ActualOwnedProcessTerminal')
def sql(value,schema):
 need(set(value)=={'tables','sequences','database_identifier_sha256','literal_private_canonical_sha256'}and digest(value['database_identifier_sha256'])and digest(value['literal_private_canonical_sha256']),'TypedSQLProjection')
 need(sorted(value['tables'])==schema['tables']and len(schema['tables'])==62 and sorted(value['sequences'])==schema['sequences']and len(schema['sequences'])==59,'MandatoryFullSQLSchema')
 for row in value['tables'].values():need(set(row)=={'rows','sha256'}and type(row['rows'])is int and row['rows']>=0 and digest(row['sha256']),'TypedTableFingerprint')
 for row in value['sequences'].values():need(set(row)=={'last_value','is_called'}and type(row['last_value'])is int and type(row['is_called'])is bool,'TypedSequenceState')
def body(value):
 raw=bytes.fromhex(value['body_hex']);need(len(raw)<=1048576 and sha(raw)==value['body_sha256'],'ExactBodyDigest')
 safe_public_body(raw)
 need(type(value['status'])is int and value['status']in[200,404]and type(value['headers']['content_type'])is list and len(value['headers']['content_type'])==1 and all(type(x)is str for x in value['headers']['content_type']),'ExactScopedStatusAndContentType')
 return raw
def validate(data,schema,source38,root,privacy,build,attempts):
 need(data['actual_controller_exit']==0 and type(data['actual_controller_exit'])is int and data['overall_passed']is True,'ActualHostExit0')
 for key in ['Main_accepted','full_M7_accepted','performance_accepted','default_worker_activation']:need(data[key]is False,'NoScopeTransfer:'+key)
 need(type(data['Source_business_requests'])is int and data['Source_business_requests']==0,'NoSourceBusinessReplay')
 need(type(data['cases'])is list and [r['name']for r in data['cases']]==NAMES,'MandatoryExact12CaseOrder')
 source={r['name']:r for r in source38['cases']};need(len(source)==38 and len(source38['cases'])==38,'IndependentSource38Authority')
 phase=data['HTTP_phase'];start=phase['started'];end=phase['completed'];need(all(number(x)for x in[start,end])and 0<=start<=end<=start+90 and phase['limit_seconds']==90 and phase['request_seconds']==8,'FixedHTTPClocks')
 database=data['owned_before']['database_identifier_sha256']
 for row in data['cases']:
  expected=source[row['name']];a=row['Source_reference'];b=row['Native_actual']
  need(row['role']==expected['role']and row['action']==expected['action'],'ExactSourceRoleAction')
  need(a['status']==expected['status']and a['body_hex']==expected['body_hex']and a['body_sha256']==expected['body_sha256']and a['headers']['content_type']==[v for k,v in expected['headers']if k.casefold()=='content-type'],'ExactRetainedSourceAuthority')
  need(body(a)==body(b)and a['status']==b['status']and a['headers']['content_type']==b['headers']['content_type'],'LiteralBodyStatusContentTypeParity')
  begin=b['began_monotonic'];done=b['completed_monotonic'];obs=b['absolute_observation'];close=b['physical_close']
  need(all(number(x)for x in[begin,done,obs['completed'],obs['deadline']])and start<=begin<=obs['completed']<=obs['deadline']<=begin+8 and obs['deadline']<=start+90 and obs['completed']<=done<=end,'PerRequestClockAndDeadline')
  need(obs['timer_joined']is True and close['timer_joined']is True and type(close['fd_after'])is int and close['fd_after']==-1 and close['original_inode_absent']is True,'ActualHTTPPhysicalClose')
  sql(b['before'],schema);sql(b['after'],schema);need(b['before']==b['after']and b['before']['database_identifier_sha256']==database,'PerCaseAllSQLAndNamespaceUnchanged')
 for kind in ['owned','default']:
  sql(data[kind+'_before'],schema);sql(data[kind+'_after'],schema);need(data[kind+'_before']==data[kind+'_after'],'WholeFixtureSQLUnchanged:'+kind)
 need(data['origin']=={'initial_rows':[],'initial_invalid':[],'terminal_rows':[],'terminal_invalid':[]},'ActualOwnedOriginNoContact')
 need(len(data['gates'])==15 and [g['role']for g in data['gates']]==['native-'+str(i)for i in range(1,16)],'MandatoryElevenGates')
 ns=data['native_phase']['started'];ne=data['native_phase']['completed'];need(all(number(x)for x in[ns,ne])and 0<=ns<=ne<=ns+180 and data['native_phase']['limit_seconds']==180,'FixedNativePhase')
 previous=ns
 for gate,name in zip(data['gates'],FILTERS,strict=True):
  expected='gateway::tests::'+name;need(gate['filter']==expected and gate['stdout_path']=='native-stdout/'+gate['role']+'.txt','ExactPhysicalGateIdentity')
  need(gate['actual_exit']==0 and type(gate['actual_exit'])is int and gate['actual_passed_tests']==1 and gate['actual_filtered_tests']==84,'ExactlyOnePhysicalTest')
  text=(HERE/gate['stdout_path']).read_bytes();need(sha(text)==gate['stdout_sha256'],'PhysicalStdoutDigest');lines=text.decode().splitlines();matches=[re.fullmatch(r'test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; 84 filtered out; finished in ([0-9]+(?:\.[0-9]+)?)s',line)for line in lines if line.startswith('test result:')]
  need(len(matches)==1 and matches[0]is not None and lines.count('test '+expected+' ... ok')==1,'ActualPhysicalRustFilterResult')
  elapsed=float(matches[0].group(1));need(number(gate['Rust_reported_seconds'])and elapsed==gate['Rust_reported_seconds']and 0<=elapsed<=45,'FixedPhysicalTestBudget')
  begin=gate['started_monotonic'];done=gate['completed_monotonic'];need(all(number(x)for x in[begin,done])and previous<=begin<=done<=ne and done-begin<=45,'SequentialActualGateClocks');previous=done;terminal(gate['terminal'],gate['role'])
 need(len(data['commands'])==79 and [c['number']for c in data['commands']]==list(range(1,80)),'Mandatory79CommandReceipts')
 for command in data['commands']:
  need(command['actual_identity_observed']is True and command['physically_settled']is True and command['invalid']is None and type(command['actual_exit'])is int and command['actual_exit']in[0,1]and all(command[k]is True for k in PIPE),'ActualCommandPhysicalSettlement')
  need(number(command['started_monotonic'])and number(command['completed_monotonic'])and command['started_monotonic']<=command['completed_monotonic'],'ActualCommandClock')
 need(set(data['jobs'])=={'source-redis','native-http'},'BothOwnedLongJobs')
 for row in data['jobs'].values():need(row['actual_identity_observed']is True and row['actual_exit']==0 and all(row[k]is True for k in PIPE),'ActualLongJobSettlement')
 terminal(data['HTTP_peer_terminal'],'native-http');peer=data['HTTP_peer_result'];need(peer['actual_passed_tests']==1 and peer['actual_filtered_tests']==84 and number(peer['Rust_reported_seconds'])and 0<=peer['Rust_reported_seconds']<=180,'FiniteActualHTTPPeer')
 need(set(data['removed_CIDs'])=={'baseline','origin'}and data['remaining']=={'containers':'','networks':'','volumes':''},'ExactOwnedResourceClosure')
 for row in data['removed_CIDs'].values():need(row['actual_exit']==1 and row['stdout']=='\n'and row['actual_identity_observed']is True and row['pipes_physically_settled']is True and row['exact_CID_NotFound']is True,'ExactCIDNotFound')
 q=root['fresh_cleanup_queries'];need(root['passed']is True and len(q)==5 and [x['role']for x in q]==['containers','networks','volumes','baseline','origin'],'IndependentRootActualCoverage')
 need(all(x['actual_exit']==0 and x['stdout']==''and digest(x['stderr_sha256'])for x in q),'FiveFreshRootActualEmptyInventories')
 need(privacy['body_rows_checked']==24 and privacy['literal_private_body_comparisons']==12 and privacy['body_rows_with_sensitive_matches']==0 and privacy['raw_conservative_marker_hit_rows']==8 and privacy['known_marker'].startswith('! is the public Django unusable-password sentinel')and privacy['credential_values_checked']>=21 and privacy['raw_or_projected_fallback']is False,'HonestFinitePrivacyInventory')
 need(build['Rust_inputs']==825 and build['full_inputs']==830 and build['all_source_before_after_equal']is True and build['scoped_build_only']is True and build['wholeworkspace_execution_transferred']is False and build['focused_XC']=={'passed':61,'failed':0,'ignored':24},'ActualScoped825BuildNotTransferred')
 need([r['stage']for r in build['waits']]==['fmt','strict-XC','defaults-XC']and all(type(r['actual_exit'])is int and r['actual_exit']==0 and number(r['started_epoch'])and number(r['completed_epoch'])and r['started_epoch']<=r['completed_epoch']for r in build['waits']),'AllThreeScopedWaitsActualZero')
 need('test result: ok. 61 passed; 0 failed; 24 ignored;'in(HERE/'build-logs/defaults-XC.log').read_text(),'Actual61Defaults24Ignored')
 clock=read('eligibility-clock.json');need(clock['micros']==1794528562638980 and clock['required_relation_rows']==7 and clock['SQL_timestamp_rewrites']is False and clock['Source_clock_reexecution']is False,'FixedEligibilityOnlyClock')
 need(root['fixed_eligibility_micros']==clock['micros']and root['SQL_timestamp_rewrites']is False and root['HTTP_cases']==12 and root['Native_physical_gates']==15 and root['Main_wallclock_refresh_or_performance_acceptance']is False,'RootExactControlledScope')
 return {'component_qualified':True,'HTTP_cases':12,'physical_controls':15,'controlled_eligibility_micros':1794528562638980,'wall_clock_or_refresh_accepted':False,'Native_phase_seconds':ne-ns,'Source_business_requests':0,'Main_or_full_M7_or_performance':False}
def validate_setup(setup,schema):
 deps=setup['dependency_setup'];need(setup['setup_only']is True and setup['Source_business_requests']==0 and setup['build_command_calls']==0 and len(setup['tables'])==11,'ExactSetupOnlyScope')
 need(deps['complete_retained_FK_schema']==schema and deps['existing_task1_exact_unchanged']is True and deps['historical_Source_task_payloads_claimed']is False and deps['no_task_dispatch']is True and deps['no_FK_null_or_remap']is True and deps['task_sequence_forced']is False,'HonestSourceDefinedDependencySetup')
 parents=deps['created_disabled_task_parents'];need(len(parents)==3 and [row['id']for row in parents]==[2,3,4]and all(row['enabled']is False and row['interval']['every']==1 and row['interval']['period']=='hours'and row['task']=='apps.m3u.tasks.refresh_single_m3u_account'and row['kwargs']=={'account_id':row['id']}for row in parents),'OnlyThreeDisabledSetupParents')
 return True
def verify():
 raw=(HERE/'payloads.json').read_bytes();need(sha(raw)==PAYLOAD_SHA,'ExactPublicManifest')
 manifest=json.loads(raw);need(type(manifest['files'])is list and len(manifest['files'])==len({r['path']for r in manifest['files']}),'UniquePublicPayloads')
 for row in manifest['files']:
  path=Path(row['path']);need(not path.is_absolute()and'..'not in path.parts,'SafePublicPath');file=HERE/path;need(file.is_file()and not file.is_symlink()and sha(file.read_bytes())==row['sha256'],'ExactPublicPayload:'+str(path))
 import reconstruct
 reconstruct.load()
 return validate(read('retained.json'),read('schema.json'),read('source-tcp38-authority.json'),read('Root-review.json'),read('privacy.json'),read('build-proof.json'),{})
if __name__=='__main__':print(json.dumps(verify(),sort_keys=True))
