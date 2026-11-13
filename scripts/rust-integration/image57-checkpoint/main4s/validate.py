"""Offline Main4s retained observation derivation, no application or network execution."""
import hashlib,importlib.util,json,math,re
from pathlib import Path
HERE=Path(__file__).resolve().parent
CASES=['success','busy','clientloss'];MODES=['source','native'];LABELS=['actual_reply_held','absolute_3_1s','immediately_before_release','immediately_after_release','post_close_1.5','post_close_3','post_close_6','post_close_15']
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings'}
IMAGE='sha256:fc5ea2aba28f86a3ebf3672f6bf8d874a6f79b2d9dd4c8a1efb8993de680375a'
COVERAGE_SHA='566feedaf76cbbed7cfbc5fc0657b059492bda95ace0e2fcd2715f85f687d65b'
def require(ok,label):
 if not ok:raise ValueError(label)
def canonical(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def number(x):return type(x)in[int,float]and math.isfinite(x)
def integer(x,lo=0,hi=2**63-1):return type(x)is int and lo<=x<=hi
def hex64(x):return type(x)is str and re.fullmatch('[0-9a-f]{64}',x)is not None
def module(name):
 spec=importlib.util.spec_from_file_location('main4s_'+name,HERE/'blocked-inputs/tools'/(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
def audit(v,default=False):
 require(type(v)is dict and set(v)=={'database','tables','sequences','dataset_sha256'},'AuditShape')
 require(v['database']=='dispatcharr'if default else bool(re.fullmatch('rust_pending_[a-z0-9_]{1,50}',v['database'])),'AuditOwnedDatabase')
 require(type(v['tables'])is dict and set(v['tables'])==TABLES,'NineTables')
 for row in v['tables'].values():require(type(row)is dict and set(row)=={'rows','sha256'}and integer(row['rows'])and hex64(row['sha256']),'TableSchema')
 require(type(v['sequences'])is dict and set(v['sequences'])=={'public.'+name+'_id_seq'for name in TABLES},'NineSequences')
 for seq in v['sequences'].values():require(type(seq)is dict and set(seq)=={'last_value','is_called'}and integer(seq['last_value'],1)and type(seq['is_called'])is bool,'SequenceSchema')
 require(hashlib.sha256(json.dumps({'tables':v['tables'],'sequences':v['sequences']},sort_keys=True).encode()).hexdigest()==v['dataset_sha256'],'AuditDigest')
def validate(data,root_cleanup,coverage):
 require(data.get('owned_process_PID_namespace_scope')=='container namespace PID/starttime; no host /proc absence inference; namespace disposal independently proven by project/container cleanup','ContainerNamespaceOnly')
 require(data.get('actual_exit_code')==0,'ActualExit');require(data.get('omitted_fields')==[],'AllFourResponseMapsRetained')
 require(coverage['raw_summary_sha256']==data['raw_sha256']['summary.json'],'RawSummaryBinding')
 redactions=data.get('private_seed_or_process_field_redactions');require(type(redactions)is list and len(redactions)==18 and len({x['private_path']for x in redactions})==18,'ExactPrivateRedactionCoverage')
 for redaction in redactions:
  path,field=redaction['private_path'].split(':');require(redaction['raw_file_sha256']==data['raw_sha256'][path]and hex64(redaction['field_sha256'])and(field,redaction['replacement'])in [('access_token','not published'),('nonce','nonce_sha256')],'PrivateRedactionBinding')
 summary=data['summary'];require(type(summary.get('cases'))is list and[r.get('case')for r in summary['cases']]==CASES,'MandatoryThreeCases');results=[]
 require(type(root_cleanup)is dict and root_cleanup.get('passed')is True and root_cleanup.get('summary_sha256')==data['raw_sha256']['summary.json'],'RootCleanupBinding')
 observation=module('observation');guards=module('fixture_guards');signals=module('request_observer')
 for r in summary['cases']:
  case=r['case'];ref=coverage['cases'][case];seed=data['seed_non_auth_projection'][case];require(r['project']==ref['project'],'OwnedProjectBinding');require(r['actual_Main_image_id']==IMAGE,'Image57Binding')
  require(root_cleanup['projects'].get(r['project'])=={'containers':'','networks':'','volumes':''}and r['remaining']==root_cleanup['projects'][r['project']],'IndependentOwnedCleanup')
  audit(r['default_database_before'],True);audit(r['default_database_after'],True);require(r['default_database_before']==r['default_database_after'],'DefaultDatabaseEqual')
  before=[r['template_fingerprint'],r['source_before'],r['native_before']]
  for a in before:audit(a)
  clone=guards.clones(*before,[a['database']for a in before]);require(r['DB_clone_binding']==clone and r['independent_DB_clones_verified']is True,'CloneBinding')
  owned=data['owned_process_record_projection'][case];require(type(owned)is list and [a['role']for a in owned]==ref['owned_roles']and len(owned)==5,'FiveOwnedProcessRecords')
  require(canonical(owned)==ref['owned_records_sha256'],'RetainedOwnedProcessCoverage')
  for p in owned:require(integer(p.get('pid'),2,2**31-1)and type(p.get('starttime'))is str and p['starttime'].isdigit()and hex64(p.get('nonce_sha256')),'OwnedProcessIdentity')
  rows=data['Source_signals'][case];require(canonical(rows)==ref['Source_signal_rows_sha256'],'RetainedSignalCoverage');roles=['initial','target','follower']+(['other']if case=='busy'else[]);signal=observation.signal_evidence(r['Source_signal_terminal_status'],rows,roles,signals.validate);require(signal==r['Source_signal_terminal_coverage']and r['Source_signal_terminal_valid']is True,'SourceTerminalCoverage')
  phase_results={}
  for mode in MODES:
   phase=r[mode];require(phase.get('backend')==mode and phase.get('case')==case and phase.get('observation_valid')is True and not phase.get('observation_error'),'PhaseInput')
   require(phase.get('actual_SQL_Bearer_user_verified')is True and phase.get('early_follower_valid')is True,'SQLBearerFollowerWitness')
   follower=phase['follower'];require(follower['status']==200 and follower['prefix_bytes']==8192 and follower['prefix_sha256']=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f','Early8192')
   require(phase['admin_stop']['status']==200,'AdminStop200');guards.early_stop(follower['state'])
   checkpoints=phase['checkpoints'];require(type(checkpoints)is list and[x['label']for x in checkpoints]==LABELS,'AllEightCheckpoints')
   require({x['label']:canonical(x['state'])for x in checkpoints}==ref['checkpoints'][mode],'CapturedSnapshotCoverage')
   for x in checkpoints:
    s=x['state'];require(s.get('atomic')is False and number(s['began_monotonic'])and number(s['completed_monotonic'])and s['began_monotonic']<=s['completed_monotonic']and type(x.get('target_headers_already_observed'))is bool,'SnapshotIntervals')
   proxy=r['Source_proxy_terminal'if mode=='source'else'proxy_terminal'];require(proxy['invalid']==[]and type(proxy['seen'])is int and proxy['seen']==1 and type(proxy['connections'])is int and proxy['connections']==0,'TerminalPhysicalProxy')
   trace=proxy['rows'];require(type(trace)is list and[x['kind']for x in trace]==['backend-target-reply-read-held','backend-target-reply-released']and all(type(x['seen'])is int and x['seen']==1 for x in trace),'ActualReplyTrace')
   held,released=[x['monotonic']for x in trace];require(number(held)and number(released)and phase['held_boundary_monotonic']==held,'PhysicalHoldClock')
   h=phase['hold_observation'];require(number(h.get('control_seconds'))and 0<=h['control_seconds']<=3.1,'ControlDuration')
   derived=observation.hold(held,held+h['control_seconds'],released);require(derived==h,'HoldRecomputed')
   point=checkpoints[1]['state'];timing=observation.at_checkpoint(point['began_monotonic'],held,3.1);observation.at_checkpoint(point['completed_monotonic'],held,3.1);require(phase['checkpoint_3_1_timing']==timing,'AbsoluteCheckpoint')
   for value in [checkpoints[3]['state']['began_monotonic'],checkpoints[3]['state']['completed_monotonic']]:observation.at_checkpoint(value,released,0)
   close=phase['consumer_close_boundary'];require(number(close),'CloseBoundary')
   for checkpoint,offset in zip(checkpoints[4:],[1.5,3,6,15]):
    for key in ['began_monotonic','completed_monotonic']:observation.at_checkpoint(checkpoint['state'][key],close,offset)
   closes=phase['client_closes'];require(type(closes)is list and len(closes)==len(roles)and{c['role']for c in closes}==set(roles),'ClientCloseRoles')
   for c in closes:require(number(c['close_begin'])and number(c.get('close_complete',c.get('reader_joined_monotonic'))),'ClientCloseClock')
   target=phase['target_outcome']
   if case=='clientloss':require(observation.client_loss(phase)['matched'],'ActualClientLoss')
   else:
    require(target['status']==(200 if case=='success' else 429),'ActualTargetStatus')
    headers=target.get('headers');require(type(headers)is dict and all(type(k)is str and type(v)is str for k,v in headers.items()),'ActualResponseHeaderMap')
    lower={k.lower():v for k,v in headers.items()};require({'content-type','content-length','vary','allow'}<=set(lower),'SemanticResponseHeaders')
    require(not any(k in lower for k in ['set-cookie','authorization','proxy-authorization']),'NoCredentialResponseHeaders')
    require(number(target['headers_received'])and target['state_at_outcome']['began_monotonic']>=target['headers_received'],'ImmediateOutcomeState')
   if case=='busy'and mode=='native':
    bound=guards.other_reservation(phase['other_private_before'],phase['other_private_after'],'profile_connections:'+str(seed['provider_profile_id']),seed['other_movie_uuid'],seed['real_user_id'],phase['other_global_after_close']);require(bound==phase['other_ownership_observation'],'OtherOwnerFence')
   phase_results[mode]={'status':target.get('status'),'hold_seconds':derived['actual_seconds'],'control_seconds':derived['control_seconds'],'actual_close_or_loss':True,'semantic_response_headers_retained':case!='clientloss'}
  comparison=observation.compare(r['source'],r['native']);require(comparison==r['comparison']and comparison['parity_passed']is True,'DerivedParityAgreement');final=observation.finalize(r);require(all(final[k]is True and r.get(k)is True for k in ['passed','observation_valid','parity_passed']),'DerivedCaseFinalAgreement');require(r['full_parity_accepted']is False and r['Source_reference_expectation_matched']is True,'ScopedExpectation')
  results.append({'case':case,'phases':phase_results,'Source_signal_rows':len(rows),'clones_equal':True,'default9tables9sequences_equal':True,'owned_project_empty':True})
 require(all(summary.get(k)is True for k in ['passed','observation_valid','parity_passed'])and summary['performance_verified']is False,'SummaryAgreement')
 return {'passed':True,'actual_exit_code':0,'cases':results,'all3_observation_and_scoped_parity_passed':True,'response_header_maps_retained':4,'header_redactions':0,'control_timestamp_limit':'Only elapsed control duration retained, not separate absolute control_done timestamp; physical hold clocks are independently retained in proxy traces.','Source_signal_scope':'request_finished after resource-closer attempts, not guaranteed generator-finally/providerEOF/TCP or server cancellation','full_M4_acceptance':False,'performance_claim':False,'application_rerun':False,'default_recovery_enabled':False}
def main():
 authority=HERE/'coverage-authority.json';require(hashlib.sha256(authority.read_bytes()).hexdigest()==COVERAGE_SHA,'ExactCoverageAuthority');data=json.loads((HERE/'retained.json').read_text());result=validate(data,json.loads((HERE/'independent-cleanup-projection.json').read_text()),json.loads(authority.read_text()));print(json.dumps(result,indent=2))
if __name__=='__main__':main()
