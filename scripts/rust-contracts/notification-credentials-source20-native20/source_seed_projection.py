"""Pure exact Source seed/actor/route binding; never imports application code."""
import datetime,hashlib,json,base64
from urllib.parse import urlsplit
from credential_projection import evidence
ACTIONS=['list','retrieve','unread_count','dismiss','list','create','partial_update','destroy','create','partial_update','list','list','list','list','list','list','list','dismiss','create','dismiss_all']
AUTH_CONFIGURATION={'middleware':['django.middleware.security.SecurityMiddleware','django.contrib.sessions.middleware.SessionMiddleware','django.middleware.common.CommonMiddleware','django.middleware.csrf.CsrfViewMiddleware','django.contrib.auth.middleware.AuthenticationMiddleware','django.contrib.messages.middleware.MessageMiddleware','django.middleware.clickjacking.XFrameOptionsMiddleware','corsheaders.middleware.CorsMiddleware'],'default_authentication_classes':['rest_framework_simplejwt.authentication.JWTAuthentication','apps.accounts.authentication.ApiKeyAuthentication'],'JWT_algorithm':'HS256','access_lifetime_seconds':1800.0,'user_id_field':'id','user_id_claim':'user_id','auth_header_types':['Bearer'],'signing_key_sha256':hashlib.sha256(b'notification-credential-fixture-only').hexdigest()}
def require(ok,label):
 if not ok:raise ValueError(label)
def utc(v):
 d=datetime.datetime.fromisoformat(v.replace('Z','+00:00'));require(d.utcoffset()==datetime.timedelta(),'UTCClock');return d
def expected_actor(spec,plan):
 if spec['ordinal']in(11,12,15,16):return None
 alias=spec['auth'].get('bearer')or spec['auth'].get('x_api_key');return next(r['id']for r in plan['roles']if r['alias']==alias)
def validate_seed(report,plan):
 require(report['actual_auth_configuration']==AUTH_CONFIGURATION,'ActualPinnedMiddlewareAndAuthenticationConfiguration')
 nonce=report['database'].removeprefix('rust_probe_notifcred_');users=report['seed_users'];require([u['id']for u in users]==[r['id']for r in plan['roles']],'ExactSixUserIDs')
 for user,role in zip(users,plan['roles']):
  alias=role['alias'];wanted={'id':role['id'],'username':'rust-probe-notifcred-'+nonce+'-'+alias,'password':'!','api_key':evidence(('fixture-key-'+nonce+'-'+alias).encode()),'is_active':role['is_active'],'user_level':role['user_level'],'custom_properties':role['custom_properties'],'first_name':'','last_name':'','email':'','is_staff':False,'is_superuser':False,'last_login':None,'stream_limit':0,'avatar_config':{}}
  require(set(user)==set(wanted)|{'date_joined'}and all(user[k]==v for k,v in wanted.items()),'ExactSourceActorFields:'+alias);require(utc(report['seed_started_utc'])<=utc(user['date_joined'])<=utc(report['seed_completed_utc']),'OwnUserSeedClock')
 rows=report['seed_notifications'];require(rows['dismissals']==[]and len(rows['notifications'])==2,'ExactInitialGraph')
 for got,spec in zip(rows['notifications'],plan['seed_notices']):
  wanted={**spec,'title':'Credential notice '+str(spec['id']),'message':'Owned fixture only','notification_type':'info','priority':'normal','action_data':{},'expires_at':None};require(set(got)==set(wanted),'ExactNoticeKeys')
  for k,v in wanted.items():require(utc(got[k])==utc(v)if k in('created_at','updated_at')else got[k]==v,'ExactNoticeSeedField:'+k)
 sequences=report['seed_audit']['sequences'];require(sequences['public.accounts_user_id_seq']=={'last_value':6,'is_called':True}and sequences['public.core_systemnotification_id_seq']=={'last_value':2,'is_called':True}and sequences['public.core_notificationdismissal_id_seq']=={'last_value':1,'is_called':False},'ExactOwnedSeedSequences')
 require(set(report['tokens_private'])=={r['alias']for r in plan['roles']if r['is_active']},'ActiveActorTokenCoverage')
 for alias,row in report['tokens_private'].items():
  require(row['raw_evidence']['sha256']==row['sha256']and 100<=row['raw_evidence']['bytes']<=4096,'ProjectedSourceTokenHash');claims=row['claim_summary'];wanted=next(r['id']for r in plan['roles']if r['alias']==alias);require(str(claims['user_id'])==str(wanted)and claims['token_type']=='access'and claims['lifetime_seconds']==1800,'ProjectedSourceTokenOwnActorBinding')
 return {'passed':True,'actors':6,'notices':2,'dismissals':0,'tokens':5}
def validate_row_binding(row,spec):
 require(row['ordinal']==spec['ordinal']and row['name']==spec['name']and row['method']==spec['method']and row['path']==spec['path']and row['auth_selectors']==spec['auth']and row['request_payload']==spec['payload'],'ExactRequestAuthTargetDefinition')
 require(row['resolver']['class']=='core.api_views.SystemNotificationViewSet'and row['resolver']['action']==ACTIONS[spec['ordinal']-1],'ExactActualResolvedAction')
 expected_kwargs={'pk':spec['path'].split('/')[4]}if ACTIONS[spec['ordinal']-1]in('retrieve','partial_update','destroy','dismiss')else{}
 require(row['resolver']['kwargs']==expected_kwargs and row['resolver']['actions'].get(spec['method'].lower())==ACTIONS[spec['ordinal']-1],'ExactResolvedTargetAndMethod')
 return True
def validate_auth_headers(row,spec,report):
 actors={r['username'].rsplit('-',1)[-1]:r for r in report['seed_users']};expected={}
 for kind,value in spec['auth'].items():
  if kind=='bearer':expected['HTTP_AUTHORIZATION']=report['tokens_private'][value]['authorization_header_evidence']
  elif kind=='bearer_literal':expected['HTTP_AUTHORIZATION']='Bearer '+value
  elif kind=='x_api_key':expected['HTTP_X_API_KEY']=actors[value]['api_key']
  elif kind=='x_api_key_literal':expected['HTTP_X_API_KEY']=value
  elif kind=='authorization_literal':expected['HTTP_AUTHORIZATION']=value
  else:raise ValueError('UnknownAuthSelector')
 require(row['raw_headers_private']=={k:(v if isinstance(v,dict)else evidence(v.encode()))for k,v in expected.items()},'ExactProjectedCredentialHeaders')
