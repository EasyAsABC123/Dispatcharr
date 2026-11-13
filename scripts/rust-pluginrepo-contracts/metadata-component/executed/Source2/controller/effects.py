"""Two fresh Source rendered-error capture qualification; HTTP outcomes unmeasured."""
import hashlib,json,math,re
from pathlib import Path
from retained_effect_helpers import equal,clock,manifest_pair,stable_redis,channels,state_continuity
ROOT=Path(__file__).resolve().parents[1]
def need(ok,label):
 if not ok:raise ValueError(label)
def raw_exception(value):
 need(type(value)is dict and value['complete']is True and value['truncated']is False,'FullLiteralExceptionNotSilentlyTruncated')
 for field in('message','traceback'):
  raw=bytes.fromhex(value[field+'_hex']);need(len(raw)==value[field+'_bytes']==value[field+'_original_bytes']and hashlib.sha256(raw).hexdigest()==value[field+'_sha256']==value[field+'_original_sha256']and value[field+'_truncated']is False,'ExactCompleteExceptionBytes')
def validate_seed(report,plan):
 nonce=report['database'].removeprefix('rust_probe_pluginrepo_');seed=report['seed'];users=seed['users'];need(len(users)==3,'ExactlyThreeFreshUsers')
 for spec,row in zip(plan['roles'],users):
  need(equal(row['id'],spec['id'])and row['username']=='owned-pluginrepo-'+nonce+'-'+spec['alias']and equal(row['user_level'],spec['user_level'])and row['is_active']is True and equal(row['custom_properties'],spec['custom_properties'])and row['api_key']=='owned-rendered-errors-'+nonce+'-'+spec['api_key_alias'].replace('_','-'),'ExactRoleIdFieldsAndPersistedKeyAlias')
 key='owned-rendered-errors-'+nonce+'-duplicate-pair';need([r['id']for r in users if r['api_key']==key]==[201,202],'ExactlyTwoAmbiguousPersistedKeys');need([r['id']for r in users if r['api_key']=='owned-rendered-errors-'+nonce+'-unique-admin']==[101],'ExactlyOneUniqueAdminKey')
 need([r['id']for r in seed['repos']]==[1,802]and seed['configs']==[],'OnlyPinnedOfficialAndFreshTargetNoPassedPrefix')
 target=seed['repos'][1]
 for key,value in plan['seed']['custom_repos'][0].items():need(equal(target[key],value),'ExactSeedRepoField:'+key)
 for spec in plan['seed']['CoreSettings']:
  matches=[r for r in seed['settings']if r['key']==spec['key']];need(len(matches)==1 and equal(matches[0]['value'],spec['value'])and matches[0]['name']==spec['name'],'ExactSeedSetting')
 for key,last in plan['seed']['three_called_sequence_states'].items():need(equal(seed['public_manifest']['sequences'][key],[last,True]),'ExactSeedSequenceState')
 return {'passed':True,'fresh_users':3,'duplicate_matching_user_ids':[201,202],'unique_admin_matching_user_ids':[101],'model_seed_scope':'real Source ORM saves; no Native signal claim'}
def validate(report,plan,schema):
 need(report['definition_sha256']==hashlib.sha256((ROOT/'inputs/plan.json').read_bytes()).hexdigest(),'ExactPlanReference');need(report['capture_only']is True and report['passed']is False and report['capture_complete']is True and report['actual_request_count']==report['completed_capture_count']==len(report['requests'])==2,'TwoActualCapturesRecorderFalse')
 need(report['Source_framework_APIClient_only']is True and report['Source_TCP_or_Native']is False and report['force_authenticate_used']is False and report['client_raise_request_exception']is False,'ExactRenderedObserverScope')
 need(report.get('outer_fixture_catch')is None and report.get('probe_cleanup_exception')is None and report['connections_close_called']is True,'NoOuterCollectorFailure')
 validate_seed(report,plan);previous=report['seed'];outcomes=[];nonce=report['database'].removeprefix('rust_probe_pluginrepo_')
 for row,spec in zip(report['requests'],plan['cases']):
  need((row['index'],row['name'],row['method'],row['actor'],row['target_alias'],row['path'])==(spec['index'],spec['name'],spec['method'],spec['actor_key_alias'],spec['target'],spec['path']),'ExactTwoCaseOrderBodyActorTarget');need(row['resolver']['class']=='apps.plugins.api_views.'+('PluginRepoListCreateAPIView'if spec['target']=='collection'else'PluginRepoDetailAPIView')and equal(row['resolver']['kwargs'],{}if spec['target']=='collection'else{'pk':802}),'ActualResolvedClassAndTargetNotSelfConsistentWrongAlias')
  raw=bytes.fromhex(row['body_hex']);need(raw==bytes.fromhex(spec['body_hex'])and type(row['body_bytes'])is int and len(raw)==row['body_bytes']==spec['body_bytes']and hashlib.sha256(raw).hexdigest()==row['body_sha256']==spec['body_sha256'],'ExactLiteralRequestBody')
  header=('owned-rendered-errors-'+nonce+'-'+spec['actor_key_alias'].replace('_','-')).encode().hex();need(equal(row['raw_headers_private'],{'HTTP_X_API_KEY':header}),'ActualPersistedUniqueOrDuplicateKeyHeader')
  need(row['actual_request_invoked']is True and row['capture_complete']is True and 0<=(clock(row['completed_utc'])-clock(row['began_utc'])).total_seconds()<=8,'ActualRequestClockAndCapture')
  before,after=row['before'],row['after'];state_continuity(previous,before);previous=after;manifest_pair(before['public_manifest'],after['public_manifest'],set(),schema)
  for name in('repos','configs','settings','system_notifications','notification_dismissals','users','events'):need(equal(before[name],after[name]),'NoPredictedModelEffects:'+name)
  channels(before['Channels'],nonce);channels(after['Channels'],nonce);need(equal(before['Channels']['frames'],after['Channels']['frames']),'NoErrorRequestChannelsConsumptionOrMutation');stable_redis(before['raw_Redis'],after['raw_Redis'],True)
  if row['transport']=='http_response':
   need(type(row['status'])is int and 100<=row['status']<=599,'ActualReturnedHTTPStatusNotInvented');body=bytes.fromhex(row['response_body_hex']);need(len(body)==row['response_body_bytes']<=1048576 and hashlib.sha256(body).hexdigest()==row['response_body_sha256'],'LiteralHTTPBody');need(row['framework_exception']is None,'ReturnedHTTPAndOuterExceptionSeparate')
   if row['response_exc_info']is not None:raw_exception(row['response_exc_info'])
   outcomes.append({'index':row['index'],'transport':'http_response','actual_status':row['status'],'actual_content_type':{k.lower():v for k,v in row['headers'].items()}.get('content-type'),'response_exc_info_class':None if row['response_exc_info']is None else row['response_exc_info']['type'],'Native_parity_claimed':False})
  else:
   need(row['transport']=='framework_exception'and row.get('status')is None,'NoFakeHTTP500FromRaisedException');raw_exception(row['framework_exception']);outcomes.append({'index':row['index'],'transport':'framework_exception','literal_exception_class':row['framework_exception']['type'],'actual_status':None,'rendered_HTTP_witness':'NOT_OBSERVED'})
 need(type(report['phase_elapsed_seconds'])in(int,float)and math.isfinite(report['phase_elapsed_seconds'])and 0<=report['phase_elapsed_seconds']<=120,'FinitePhase120')
 need(equal(report['probe_cleanup'],{'own_membership_discarded':True,'frames_consumed':0,'mailbox_deleted_by_fixture':False}),'OwnedProbeCleanupNoFrameConsumption')
 return {'passed':True,'capture_and_no_model_effect_scope_only':True,'Source_requests':2,'actual_outcomes':outcomes,'returned_HTTP_witness_count':sum(x['transport']=='http_response'for x in outcomes),'Source_framework_not_TCP':True,'Native_status_body_or_duplicate_key_parity':False,'full_endpoint_or_M8':False}
