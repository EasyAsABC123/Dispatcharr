"""Unexecuted20 real-auth Source requests; invoked only by reviewed owned shell.
No force authentication, permission patch, Source TCP or Native acceptance.
"""
import hashlib,json,os,re,sys
from pathlib import Path
from capture_common import audit,cache_snapshot,connected_database,exception_record,literal,notice_rows,require,runtime_snapshot,save
ROOT=Path('/credential-inputs');REPORT=Path('/evidence/notification-credentials-private.json')
def owned(name):require(type(name)is str and re.fullmatch('rust_probe_notifcred_[a-f0-9]{10}',name),'OwnedCredentialDatabase');return name
def headers(spec,actors,tokens):
 out={}
 for kind,value in spec.items():
  if kind=='bearer':out['HTTP_AUTHORIZATION']='Bearer '+tokens[value]
  elif kind=='bearer_literal':out['HTTP_AUTHORIZATION']='Bearer '+value
  elif kind=='x_api_key':out['HTTP_X_API_KEY']=actors[value].api_key
  elif kind=='x_api_key_literal':out['HTTP_X_API_KEY']=value
  elif kind=='authorization_literal':out['HTTP_AUTHORIZATION']=value
  else:raise ValueError('ExactAuthMaterializer')
 return out
def model_rows(model):
 values=literal(list(model.objects.order_by('id').values()[:10001]));require(len(values)<=10000,'RawModelRowsCapacity');used=0
 for row in values:
  encoded=json.dumps(row).encode();require(len(encoded)<=131072,'RawModelRowCapacity');used+=len(encoded);require(used<=8388608,'RawModelTotalCapacity')
 return values
def main():
 require(os.environ.get('NOTIFICATION_CREDENTIAL_AUTHORITY')=='owned-source-credentials-only','CredentialAuthority');stage=os.environ['NOTIFICATION_FIXTURE_STAGE'];database=os.environ['NOTIFICATION_FIXTURE_DATABASE'];require(stage in('audit','exercise'),'KnownStage')
 if stage=='exercise':owned(database)
 else:require(database=='dispatcharr','DefaultReadOnlyAudit')
 from django.db import connection,connections,transaction
 from django.utils import timezone
 from django.contrib.auth import get_user_model
 from django.urls import resolve
 from django.core.cache import cache
 from dispatcharr.app_initialization import should_skip_initialization
 from rest_framework.test import APIClient
 from rest_framework_simplejwt.tokens import AccessToken
 from core.models import CoreSettings,SystemNotification,NotificationDismissal
 from core.api_views import SystemNotificationViewSet
 plan=json.loads((ROOT/'plan.json').read_text());report={'stage':stage,'database':database,'Original':plan['Original'],'capture_valid':False,'actual_request_count':0,'requests':[],'force_authenticate_used':False,'real_default_JWT_and_APIKey_backends':True,'Source_TCP_or_Native':False,'client_raise_request_exception':False,'enforce_csrf_checks':True,'startup_receiver_scope':'real Original management shell initialization skip; snapshot retained; not web producer acceptance','phase':'startup'}
 def flush():
  if stage=='exercise':save(REPORT,report)
 def guard():require(connected_database(connection)==database and should_skip_initialization()is True,'OwnedDatabaseAndStartupScope')
 try:
  require('shell'in sys.argv and should_skip_initialization()is True,'RealManagementShellSkip');require(connected_database(connection)==database,'ActualDatabase')
  if stage=='audit':
   with transaction.atomic():
    with connection.cursor()as cursor:cursor.execute('SET TRANSACTION READ ONLY')
    report['audit']=audit(connection)
   report.update(capture_valid=True,passed=True);return
  report['phase']='actual-dependency-preflight';flush();proof=json.loads((ROOT/'dependency-proof.json').read_text());require(proof['state']=='EXACT_NEVER_STARTED_IMAGE_BYTES_CAPTURED'and proof['container_started']is False,'RealAuthDependencyClosure')
  for row in proof['files']:
   actual=Path(row['runtime_path']);require(hashlib.sha256(actual.read_bytes()).hexdigest()==row['sha256'],'RuntimeAuthDependencyByteBinding')
  import importlib.metadata
  from django.conf import settings
  from rest_framework.settings import api_settings as drf_settings
  from rest_framework_simplejwt.settings import api_settings as jwt_settings
  report['actual_auth_configuration']={'middleware':list(settings.MIDDLEWARE),'default_authentication_classes':[c.__module__+'.'+c.__qualname__ for c in drf_settings.DEFAULT_AUTHENTICATION_CLASSES],'JWT_algorithm':jwt_settings.ALGORITHM,'access_lifetime_seconds':jwt_settings.ACCESS_TOKEN_LIFETIME.total_seconds(),'user_id_field':jwt_settings.USER_ID_FIELD,'user_id_claim':jwt_settings.USER_ID_CLAIM,'auth_header_types':list(jwt_settings.AUTH_HEADER_TYPES),'signing_key_sha256':hashlib.sha256(str(jwt_settings.SIGNING_KEY).encode()).hexdigest()}
  from seed_binding import AUTH_CONFIGURATION
  require(report['actual_auth_configuration']==AUTH_CONFIGURATION,'ExactOriginalConfiguredAuthMiddlewareBeforeSeed')
  report['actual_auth_dependency_versions']={'SimpleJWT':importlib.metadata.version('djangorestframework-simplejwt'),'PyJWT':importlib.metadata.version('PyJWT')}
  report['runtime_before_seed']=runtime_snapshot();report['before_owned_isolation']=notice_rows(SystemNotification,NotificationDismissal);report['before_setup_audit']=audit(connection);flush();User=get_user_model();actors={};tokens={};nonce=database.removeprefix('rust_probe_notifcred_')
  report['phase']='owned-isolation-and-seed';report['seed_started_utc']=timezone.now().isoformat();flush()
  with transaction.atomic():
   guard();CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Network Access','value':plan['global_network_settings']});report['owned_notice_rows_after_settings_save_before_isolation']=notice_rows(SystemNotification,NotificationDismissal);flush();NotificationDismissal.objects.all().delete();SystemNotification.objects.all().delete();require(User.objects.count()==0,'NoExistingOwnedUsersBeforeSeed')
   for role in plan['roles']:
    alias=role['alias'];actors[alias]=User.objects.create(id=role['id'],username='rust-probe-notifcred-'+nonce+'-'+alias,password='!',api_key='fixture-key-'+nonce+'-'+alias,is_active=role['is_active'],user_level=role['user_level'],custom_properties=role['custom_properties'])
   for seed in plan['seed_notices']:
    data={k:v for k,v in seed.items()if k not in('created_at','updated_at')};obj=SystemNotification.objects.create(**data,title='Credential notice '+str(seed['id']),message='Owned fixture only',notification_type='info',priority='normal',action_data={},expires_at=None);SystemNotification.objects.filter(pk=obj.pk).update(created_at=seed['created_at'],updated_at=seed['updated_at'])
   with connection.cursor()as cursor:
    cursor.execute("SELECT setval('accounts_user_id_seq',6,true)");cursor.execute("SELECT setval('core_systemnotification_id_seq',2,true)");cursor.execute("SELECT setval('core_notificationdismissal_id_seq',1,false)")
  for alias,user in actors.items():
   if user.is_active:tokens[alias]=str(AccessToken.for_user(user))
  report['phase']='seed-binding';report['seed_users']=model_rows(User);report['seed_notifications']=notice_rows(SystemNotification,NotificationDismissal);report['seed_audit']=audit(connection);report['tokens_private']={alias:{'raw':token,'sha256':hashlib.sha256(token.encode()).hexdigest()}for alias,token in tokens.items()};report['seed_completed_utc']=timezone.now().isoformat();from seed_binding import validate_seed
  report['seed_binding']=validate_seed(report,plan);report['runtime_after_seed']=runtime_snapshot();report['cache_after_seed']=cache_snapshot();flush()
  # No cache/throttle reset between requests; capture legitimate auth effects.
  report['phase']='requests';flush()
  for spec in plan['cases']:
   guard();path=spec['path'];matched=resolve(path.split('?',1)[0]);method=spec['method'].lower();from seed_binding import ACTIONS
   require(matched.func.cls is SystemNotificationViewSet and matched.func.actions.get(method)==ACTIONS[spec['ordinal']-1],'CorrectRealResolvedNotificationMethod');material=headers(spec['auth'],actors,tokens);client=APIClient(enforce_csrf_checks=True);client.raise_request_exception=False;client.credentials(**material)
   row={'ordinal':spec['ordinal'],'name':spec['name'],'method':spec['method'],'path':path,'auth_selectors':spec['auth'],'raw_headers_private':{k:v.encode().hex()for k,v in material.items()},'request_payload':spec['payload'],'resolver':{'class':matched.func.cls.__module__+'.'+matched.func.cls.__qualname__,'action':matched.func.actions[method],'actions':matched.func.actions,'url_name':matched.url_name,'kwargs':matched.kwargs},'before_audit':audit(connection),'before_notifications':notice_rows(SystemNotification,NotificationDismissal),'before_cache':cache_snapshot(),'before_users':model_rows(User),'before_events':model_rows(__import__('core.models',fromlist=['SystemEvent']).SystemEvent)}
   report['requests'].append(row);flush();row['began_utc']=timezone.now().isoformat()
   try:
    options={'HTTP_ACCEPT':'application/json','REMOTE_ADDR':'127.0.0.1','follow':False}
    if spec['payload']is not None:options.update(data=spec['payload'],format='json')
    response=getattr(client,method)(path,**options);actual_request=(getattr(response,'renderer_context',None)or{}).get('request');actual_user=getattr(actual_request,'user',None);backend=getattr(actual_request,'successful_authenticator',None);row['actual_auth_observation']={'user_id':getattr(actual_user,'pk',None),'is_authenticated':bool(getattr(actual_user,'is_authenticated',False)),'backend_class':None if backend is None else type(backend).__module__+'.'+type(backend).__qualname__};raw=bytes(response.content);require(len(raw)<=65536,'BodyCaptureCapacity');head=dict(response.items());require(sum(len(k.encode())+len(v.encode())for k,v in head.items())<=65536,'HeaderCaptureCapacity')
    try:parsed=json.loads(raw)
    except(ValueError,UnicodeDecodeError):parsed=None
    row.update(transport='http_response',status=response.status_code,headers=head,body_hex=raw.hex(),body_sha256=hashlib.sha256(raw).hexdigest(),body_bytes=len(raw),json=parsed,framework_exception=exception_record(getattr(response,'exc_info',None)))
   except BaseException:
    row.update(transport='framework_exception',framework_exception=exception_record(sys.exc_info()))
    if sys.exc_info()[0]is ValueError and str(sys.exc_info()[1])in('BodyCaptureCapacity','HeaderCaptureCapacity'):raise
   finally:row['completed_utc']=timezone.now().isoformat()
   row.update(after_audit=audit(connection),after_notifications=notice_rows(SystemNotification,NotificationDismissal),after_cache=cache_snapshot(),after_users=model_rows(User),after_events=model_rows(__import__('core.models',fromlist=['SystemEvent']).SystemEvent),capture_complete=row.get('framework_exception')is None or row['framework_exception']['complete']);report['actual_request_count']+=1;flush();require(row['capture_complete'],'LiteralExceptionCaptureCapacity')
  report.update(capture_valid=True,phase='complete');flush()
 except BaseException:report['outer_fixture_catch']={'phase':report['phase'],'exception':exception_record(sys.exc_info())};flush()
 finally:
  connections.close_all();report['connections_close_called']=True
  if stage=='exercise':save(REPORT,report)
  else:save(Path('/evidence/audit-'+os.environ['NOTIFICATION_AUDIT_LABEL']+'-private.json'),report)
 require(report['capture_valid']is True and(stage=='audit'or report['actual_request_count']==20),'CredentialCaptureCompleteNoRetry')
if __name__=='__main__':main()
