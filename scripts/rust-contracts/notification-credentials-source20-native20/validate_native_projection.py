"""Portable Native20 qualification against retained real-credential Source20.
Raw evidence is never rewritten; generated clock/nonce identity policy is explicit.
"""
import base64,datetime,hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parent
from credential_projection import evidence
from cache_binding import validate as bind_cache
N='core_systemnotification';D='core_notificationdismissal'
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings',N,D}
def require(ok,label):
 if not ok:raise ValueError(label)
def clock(value):
 require(type(value)is str,'LiteralClock');d=datetime.datetime.fromisoformat(value.replace('Z','+00:00'));require(d.utcoffset()==datetime.timedelta(),'UTCClock');return d
def own(row,value):require(clock(row['began_utc'])<=clock(value)<=clock(row['completed_utc']),'OwnNativeRequestClock')
class Clocks:
 def __init__(self,dynamic):self.dynamic={clock(v)for v in dynamic};self.mapping={}
 def compare(self,source,native,row):
  s,n=clock(source),clock(native)
  if s in self.mapping:require(n==self.mapping[s],'GeneratedClockCrossRequestContinuity')
  elif s in self.dynamic:own(row,native);self.mapping[s]=n
  else:require(n==s,'ExactSeedOrUnchangedInstant')
def compare_value(source,native,clocks,row,key=None):
 if key in('created_at','updated_at','dismissed_at','expires_at')and source is not None:clocks.compare(source,native,row);return
 require(type(source)is type(native),'LiteralJSONType:'+str(key))
 if isinstance(source,dict):
  require(set(source)==set(native),'ExactObjectKeys:'+str(key))
  for k,v in source.items():compare_value(v,native[k],clocks,row,k)
 elif isinstance(source,list):
  require(len(source)==len(native),'ExactArrayLength:'+str(key))
  for s,n in zip(source,native):compare_value(s,n,clocks,row,key)
 else:require(source==native,'StableLiteralValue:'+str(key))
def graph(state):return {'notifications':state['tables'][N],'dismissals':state['tables'][D]}
def check_sql(state):
 require(set(state)=={'tables','sequences'}and set(state['tables'])==set(state['sequences'])==TABLES,'ExactRaw11SQLScope')
 for rows in state['tables'].values():require(type(rows)is list and len(rows)<=10000,'BoundedRawSQLRows')
 for sequence in state['sequences'].values():require(set(sequence)=={'last_value','is_called'}and type(sequence['last_value'])is int and type(sequence['is_called'])is bool,'LiteralSQLSequences')
def expected_headers(spec,users,tokens):
 out={}
 for kind,value in spec['auth'].items():
  if kind=='bearer':out['authorization']=tokens[value]['authorization_header_evidence']
  elif kind=='bearer_literal':out['authorization']='Bearer '+value
  elif kind=='x_api_key':out['x-api-key']=users[value]['api_key']
  elif kind=='x_api_key_literal':out['x-api-key']=value
  elif kind=='authorization_literal':out['authorization']=value
  else:raise ValueError('FixedAuthSelector')
 return {k:(v if isinstance(v,dict)else evidence(v.encode()))for k,v in out.items()}
def validate(native,source=None,plan=None,*,reference_mode=None):
 if source is None:source=json.loads((ROOT/'Source20-projection.json').read_text())
 if plan is None:plan=json.loads((ROOT/'plan.json').read_text())
 require(reference_mode in('retained-original-observation','new-projected-reference'),'ExplicitReferenceModeRequired')
 require(native['passed']is False,'CaptureOnlyRecorderFalsePreserved')
 require(native['Native_only']is True and native['Source_application_runs']==0 and native['capture_complete']is True and native['actual_request_count']==len(native['requests'])==20,'Native20CompleteCapture')
 require(native['actual_auth_backend_observed']is False,'NoInventedBackendIntrospection')
 require(native['Source20_sha256']==(json.loads((ROOT/'Source20-projection-manifest.json').read_text())['original_sha256']if reference_mode=='retained-original-observation'else hashlib.sha256((ROOT/'Source20-projection.json').read_bytes()).hexdigest())and native['plan_sha256']==hashlib.sha256((ROOT/'plan.json').read_bytes()).hexdigest(),'ExactRetainedAuthorityHash')
 nonce=native['nonce'];require(re.fullmatch('[a-f0-9]{10}',nonce)is not None and native['database']=='rust_probe_notifcred_native_'+nonce,'OwnedNativeDatabase')
 users={};seed=native['seed_SQL'];check_sql(seed);require(seed['tables']['accounts_user']==native['seed_users'],'FullRawUserSeedBinding')
 for role,s,n in zip(plan['roles'],source['seed_users'],native['seed_users']):
  alias=role['alias'];wanted=dict(s,username='rust-probe-notifcred-'+nonce+'-'+alias,api_key=evidence(('fixture-key-'+nonce+'-'+alias).encode()),date_joined=n['date_joined']);require(n==wanted,'AllSeedUserFieldsAndDeclaredNonce:'+alias);require(clock(native['seed_started_utc'])<=clock(n['date_joined'])<=clock(native['seed_completed_utc']),'OwnNativeUserSeedClock');users[alias]=n
 require(len(native['seed_users'])==6 and set(users)=={r['alias']for r in plan['roles']},'ExactlySixActors');require(set(native['tokens_private'])=={r['alias']for r in plan['roles']if r['is_active']},'FiveActiveActorTokens')
 for alias,token in native['tokens_private'].items():
  require(token['raw_evidence']['sha256']==token['sha256']and 100<=token['raw_evidence']['bytes']<=4096,'ProjectedNativeTokenHash');claims=token['claim_summary'];require(str(claims['user_id'])==str(users[alias]['id'])and claims['token_type']=='access'and claims['lifetime_seconds']==1800,'ProjectedNativeTokenClaimSummary')
 require(seed['sequences']['accounts_user']=={'last_value':6,'is_called':True}and seed['sequences'][N]=={'last_value':2,'is_called':True}and seed['sequences'][D]=={'last_value':1,'is_called':False},'ExactSeedSequences')
 dynamic=[]
 for s in source['requests']:
  for group in('notifications','dismissals'):
   old={r['id']:r for r in s['before_notifications'][group]}
   for item in s['after_notifications'][group]:
    for field in('created_at','updated_at','dismissed_at'):
     if field in item and item[field]!=old.get(item['id'],{}).get(field):dynamic.append(item[field])
 clocks=Clocks(dynamic);seedrow={'began_utc':native['seed_started_utc'],'completed_utc':native['seed_completed_utc']};compare_value(source['seed_notifications'],graph(seed),clocks,seedrow)
 require(native['cache_after_seed']['api_key_failed_auth_history']is None,'NoSeedBudget');previous_sql=seed;previous_history=None;new_results=[]
 for n,s,spec in zip(native['requests'],source['requests'],plan['cases']):
  number=spec['ordinal'];require(all(n[k]==spec[k]for k in('ordinal','name','method','path'))and n['auth_selectors']==spec['auth']and n['request_payload']==spec['payload'],'ExactFixedRequestIdentity')
  require(n['raw_headers_private']==expected_headers(spec,users,native['tokens_private']),'ExactNativeCredentialMaterial');require(n['status']==s['status']==spec['predicted_status_UNMEASURED'],'ActualSourceStatus')
  raw=bytes.fromhex(n['body_hex']);require(len(raw)==n['body_bytes']<=65536 and hashlib.sha256(raw).hexdigest()==n['body_sha256']and json.loads(raw)==n['json'],'RawNativeBodyBinding')
  before,after=n['before_SQL'],n['after_SQL'];check_sql(before);check_sql(after);require(before==previous_sql,'All11CrossRequestContinuity');previous_sql=after
  for table in TABLES-{N,D}:require(before['tables'][table]==after['tables'][table]and before['sequences'][table]==after['sequences'][table],'UnrelatedTableSequence:'+table)
  require(before['tables']['accounts_user']==native['seed_users']==after['tables']['accounts_user'],'NoCredentialUserMutationOrNormalization');require(n['before_events']==n['after_events']==native['initialized_events'],'NoCredentialEventMutationOrNormalization')
  compare_value(s['before_notifications'],graph(before),clocks,n);compare_value(s['after_notifications'],graph(after),clocks,n)
  for table in(N,D):require(before['sequences'][table]==s['before_audit']['sequences']['public.'+table+'_id_seq']and after['sequences'][table]==s['after_audit']['sequences']['public.'+table+'_id_seq'],'ExactSourceNaturalSequence:'+table)
  compare_value(s['json'],n['json'],clocks,n)
  source_auth={k.lower():v for k,v in s['headers'].items()}.get('www-authenticate');native_auth=n['headers_hex'].get('www-authenticate');require(source_auth==(None if native_auth is None else bytes.fromhex(native_auth).decode()),'ActualAuthenticationChallenge')
  bind_cache(n['before_cache'],native=True);bind_cache(n['after_cache'],native=True)
  bh=n['before_cache']['api_key_failed_auth_history'];ah=n['after_cache']['api_key_failed_auth_history'];require(bh==previous_history,'NoHistoryResetBetweenRequests')
  if number in(15,16):
   old=[]if bh is None else bh;require(type(ah)is list and len(ah)==len(old)+1 and ah[1:]==old,'ExactlyOneRealFailedKeyEntry');own(n,datetime.datetime.fromtimestamp(ah[0],datetime.timezone.utc).isoformat())
  else:require(bh==ah,'NoOtherCredentialBudgetConsumption')
  require((0 if ah is None else len(ah))==(0 if number<15 else 1 if number==15 else 2),'ExactFiniteFailureBudget');previous_history=ah
  new_results.append({'ordinal':number,'status':n['status'],'response_SQL_and_sequence_match':True,'credential_effect_field_hashes_preserved':True})
 require(native['HTTP_listener_joined']is True,'ActualHTTPListenerJoined')
 return {'passed':True,'actual_Native_requests':20,'retained_Source_requests':20,'requests':new_results,'generated_Source_instant_mappings':len(clocks.mapping),'raw_clocks_events_cache_and_user_field_hashes_preserved':True,'Source_TCP_or_login_endpoint_or_complete_credential_grammar_parity':False,'physical8_closedrain_negatives_waived':False,'actual_backend_introspection':False}
