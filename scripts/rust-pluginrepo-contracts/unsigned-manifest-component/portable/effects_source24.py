"""NEW projected-reference qualifier. Original executed effects bytes stay in closure blobs. Only expected API-key spelling changes to explicit public alias; entrypoint binds strict public SHA."""
"""Bounded Source-observation validator; no invented fetch response/error data.
Predictions remain separate from actual status/body/SQL/cache observations.
"""
import datetime,hashlib,json,re

def need(ok,label):
 if not ok:raise ValueError(label)
def exact(a,b):
 if type(a)is not type(b):return False
 if type(a)is dict:return set(a)==set(b)and all(exact(a[k],b[k])for k in a)
 if type(a)is list:return len(a)==len(b)and all(exact(x,y)for x,y in zip(a,b))
 return a==b
def clock(value):
 need(type(value)is str,'ClockString');v=datetime.datetime.fromisoformat(value.replace('Z','+00:00'));need(v.utcoffset()==datetime.timedelta(0),'UTCClock');return v

def validate_seed(report,plan):
 seed=report['seed'];need(all(type(row['id'])is int for key in ('users','repos','configs')for row in seed[key]),'ExactIntegerSeedIdentities');nonce=report['database'].removeprefix('rust_probe_pluginrepo_');actors={u['id']:u for u in seed['users']};need(set(actors)=={r['id']for r in plan['roles']},'ExactlyFourDeclaredActors')
 for role in plan['roles']:
  u=actors[role['id']];need(u['username']=='owned-pluginrepo-'+nonce+'-'+role['alias']and u['api_key']=='public-unsigned-source24-'+role['alias'].replace('_','-')and u['password']=='!'and u['is_active']is True and type(u['user_level'])is int and u['user_level']==role['user_level']and exact(u['custom_properties'],role['custom_properties']),'ExactPersistedActorIdentityAndRole')
 repos={r['id']:r for r in seed['repos']};need(set(repos)=={1,801,802}and repos[1]['is_official']is True and repos[1]['url']=='https://dispatcharr.github.io/Plugins/manifest.json','ExactOfficialAndTwoOwnedRepositories')
 for spec in plan['seed']['custom_repos']:
  r=repos[spec['id']]
  for key,value in spec.items():need(exact(r[key],value),'ExactOwnedRepoSeed:'+key)
 configs={r['id']:r for r in seed['configs']};need(set(configs)=={951,952,953},'ExactThreeConfigIDs')
 for spec in plan['seed']['configs']:
  r=configs[spec['id']]
  for key,value in spec.items():need(exact(r[key],value),'ExactSourceConfigSeed:'+key)
  need(r['enabled']is False and r['ever_enabled']is False and r['owned_tasks']==[]and r['suspended_schedules']==[],'InertConfigsNoPluginExecution')
 for sequence,last in plan['seed']['sequence_last_values_called'].items():need(exact(seed['public_manifest']['sequences'][sequence],[last,True]),'ExactDeclaredExistingSequence')
 need(len(seed['public_manifest']['tables'])==62 and len(seed['public_manifest']['sequences'])==59,'Full6259Observed')
 need(exact(report['seed_cache_binding'],[{'key':key,'django_key':':1:'+key,'decoded_value':{'owned_cache_sentinel':key}}for key in plan['cache_detail_seed']]),'ExactDeclaredFourDetailCacheSeeds')
 return {'passed':True,'real_persisted_APIkeys':True,'no_force_auth':True,'exact_four_roles_two_custom_repos_three_configs':True,'no_new_Source_status_prediction_qualification':True}

def full_manifest(value,schema):
 need(set(value['tables'])==set(schema['tables'])and set(value['sequences'])==set(schema['sequences']),'ExactManifestIdentifiersNotOnlyCounts')
 for key in ('columns','indexes','constraints'):need(exact(value[key],schema[key]),'ExactManifestSchema:'+key)
 for row in value['tables'].values():need(type(row['count'])is int and 0<=row['count']<=10000 and type(row['sha_like_md5'])is str and re.fullmatch('[0-9a-f]{32}',row['sha_like_md5'])is not None,'TypedBoundedTableDigest')
 for row in value['sequences'].values():need(type(row)is list and len(row)==2 and type(row[0])is int and type(row[1])is bool,'TypedSequenceState')

def changed_rows(before,after,key):
 a={r['id']:r for r in before[key]};b={r['id']:r for r in after[key]};return {'added_ids':sorted(set(b)-set(a)),'deleted_ids':sorted(set(a)-set(b)),'changed_fields':{str(id):[field for field in a[id]if not exact(a[id][field],b[id].get(field))]for id in sorted(set(a)&set(b))if not exact(a[id],b[id])}}

def validate(report,plan,schema):
 need(report.get('_synthetic_only')is not True and report['capture_only']is True and report['passed']is False and report['capture_complete']is True,'LiteralCaptureOnlyCompleteSource');need(report['force_authenticate_used']is False and report['client_raise_request_exception']is False and report['Source_framework_APIClient_only']is True and report['Source_TCP_or_Native']is False,'ActualRealAuthAPIClientFalseScope');need(type(report['actual_request_count'])is int and report['actual_request_count']==report['completed_capture_count']==len(report['requests'])==len(plan['cases'])==24,'ExactlyNew24NoReplay')
 seed=validate_seed(report,plan);full_manifest(report['seed']['public_manifest'],schema);outcomes=[];previous=report['seed'];previous_end=clock(report['phase_started_utc'])
 for row,spec in zip(report['requests'],plan['cases']):
  need(type(row['index'])is int and row['index']==spec['index']and row['name']==spec['name']and row['actor']==spec['actor']and row['method']==spec['method']and row['path']==spec['path'],'ExactLiteralRequestOrderRoleRoute');need(row['content_type']==spec['content_type'],'ExactRequestContentType');expected_headers={}if spec['actor']=='anonymous'else{'HTTP_X_API_KEY':('public-unsigned-source24-'+spec['actor'].replace('_','-')).encode().hex()};need(exact(row['raw_headers_private'],expected_headers),'ActualPersistedAPIKeyOrAnonymousHeaderBinding');raw=bytes.fromhex(row['body_hex']);need(row['body_hex']==spec['body_hex']and len(raw)==spec['body_bytes']==row['body_bytes']and hashlib.sha256(raw).hexdigest()==row['body_sha256']==spec['body_sha256'],'ExactPresealedBodyBytesNoOutcomeFitting');need(row['capture_complete']is True and exact(row['before']['repos'],previous['repos'])and exact(row['before']['configs'],previous['configs'])and exact(row['before']['public_manifest'],previous['public_manifest']),'AdjacentObservedSQLStateNoReset')
  begin,end=clock(row['began_utc']),clock(row['completed_utc']);need(previous_end<=begin<=end,'ActualOwnAndCrossRequestClockOrder');previous_end=end;full_manifest(row['before']['public_manifest'],schema);full_manifest(row['after']['public_manifest'],schema)
  if row['transport']=='http_response':
   body=bytes.fromhex(row['response_body_hex']);need(len(body)==row['response_body_bytes']<=1048576 and hashlib.sha256(body).hexdigest()==row['response_body_sha256'],'ActualReturnedBodySHA');need(type(row['status'])is int and 100<=row['status']<=599,'LiteralActualStatus');need(row['framework_exception']is None,'ReturnedHTTPTransportNotNoExceptionClaim')
   if row.get('response_exc_info')is not None:need(row['response_exc_info']['complete']is True,'CompleteActualResponseExceptionMetadata')
  else:need(row['transport']=='framework_exception'and row['framework_exception']['complete']is True,'LiteralCompleteRaisedTransportException')
  need(len(row['after']['public_manifest']['tables'])==62 and len(row['after']['public_manifest']['sequences'])==59,'EveryRequestFull6259Observation');need(row['after']['users']==row['before']['users'],'NoCredentialIdentityRewrite')
  outcomes.append({'index':row['index'],'name':row['name'],'transport':row['transport'],'actual_status':row.get('status'),'actual_body_sha256':row.get('response_body_sha256'),'actual_response_exc_info_type':None if row.get('response_exc_info')is None else row['response_exc_info']['type'],'repos':changed_rows(row['before'],row['after'],'repos'),'configs':changed_rows(row['before'],row['after'],'configs'),'actual_repo_sequence_before':row['before']['public_manifest']['sequences']['plugins_pluginrepo_id_seq'],'actual_repo_sequence_after':row['after']['public_manifest']['sequences']['plugins_pluginrepo_id_seq'],'Source_status_body_fetch_errors_are_observed_not_fabricated':True});previous=row['after']
 return {'passed_observation_integrity':True,'new_Source_framework_requests':24,'seed':seed,'observed_outcomes':outcomes,'prediction_notes_are_not_measured_results':True,'Source_TCP_or_Native_Main_or_plugin_execution':False,'network_origin_loopback_and_default_audits':'separate controller acceptors must independently pass'}
