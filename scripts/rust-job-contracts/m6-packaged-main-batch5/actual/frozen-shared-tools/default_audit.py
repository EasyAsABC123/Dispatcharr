"""New special-key framework Source recorder; no app imports on module load."""
import hashlib,json,os,re,sys,time
from pathlib import Path
from capture_common import audit,cache_snapshot,connected_database,exception_record,literal,require,runtime_snapshot,save
ROOT=Path('/special-root');EVIDENCE=Path('/evidence')
def rows(model):
 value=literal(list(model.objects.order_by('id').values()[:10001]));require(len(value)<=10000 and len(json.dumps(value).encode())<=8388608,'BoundedLiteralModelRows');return value
def main():
 require(os.environ.get('SPECIAL_SOURCE_AUTHORITY')=='owned-new-special-only','ExplicitChildAuthority');require('shell'in sys.argv,'ActualManagementShellMarker')
 from django.conf import settings
 from django.db import connection,connections,transaction
 from django.core.cache import cache
 from django.contrib.auth import get_user_model
 from django.urls import resolve
 from django.utils import timezone
 from rest_framework.test import APIClient
 from core.models import CoreSettings,SystemEvent,SystemNotification,NotificationDismissal
 from core.api_views import CoreSettingsViewSet
 from dispatcharr.app_initialization import should_skip_initialization
 from public_manifest import manifest
 from redis_channels import Probe
 from observables import local_state,brokers,bindings
 plan=json.loads((ROOT/'definition/plan.json').read_text());schema=json.loads((ROOT/'inputs/schema62-authority.json').read_text());stage=os.environ['SPECIAL_STAGE'];group=os.environ.get('SPECIAL_GROUP');database=os.environ['POSTGRES_DB'];nonce=os.environ['SPECIAL_NONCE'];require(re.fullmatch('[a-f0-9]{10}',nonce)is not None,'OwnedNonce')
 require(stage in ['audit','exercise'],'OnlyFixedStages');cohort=None if stage=='audit'else next(c for c in plan['cohorts']if c['group']==group);require(database=='dispatcharr'if stage=='audit'else database=='rust_special_'+nonce+'_'+group,'ExactSourceDBBinding')
 report={'passed':False,'capture_valid':False,'recorder_capture_only':True,'stage':stage,'group':group,'database':database,'Original_git':plan['Original_git'],'actual_request_count':0,'completed_capture_count':0,'Source_TCP_or_Native_or_Main':False,'force_authenticate':False,'requests':[],'phase':'startup'};probe=None;path=EVIDENCE/('default-'+os.environ.get('SPECIAL_AUDIT_LABEL','')+'.json'if stage=='audit'else group+'-source-private.json')
 def flush():save(path,report)
 def guard():require(should_skip_initialization()is True and connected_database(connection)==database,'ActualSourceManagementSkipAndDB')
 def snapshot():
  with transaction.atomic():
   with connection.cursor()as c:c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
   sql={'named11':audit(connection),'full6259':manifest(connection,schema),'settings':rows(CoreSettings),'users':rows(get_user_model()),'events':rows(SystemEvent),'notices':rows(SystemNotification),'dismissals':rows(NotificationDismissal)}
  raw=probe.raw();named={}
  for key in [group,'owned.special.renamed.'+group]:
   for name in [CoreSettings.group_cache_key(key),CoreSettings.group_cache_ver_key(key)]:named[name]=literal(cache.get(name))
  return {'SQL':sql,'Django_cache':cache_snapshot(),'named_group_cache':named,'raw_Redis':raw,'Channels':probe.snapshot(),'broker':brokers(raw),'local':local_state(),'runtime':runtime_snapshot(),'observed_UTC':timezone.now().isoformat(),'observed_monotonic':time.monotonic()}
 try:
  guard();report['startup']={'skip_initialization':should_skip_initialization(),'no_Source_web_worker_beat_media_started':True,'runtime':runtime_snapshot(),'actual_DB_driver_module':connection.Database.__name__,'actual_DB_driver_version':str(getattr(connection.Database,'__version__',None))}
  if stage=='audit':
   with transaction.atomic():
    with connection.cursor()as c:c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
    report['default_full6259']=manifest(connection,schema);report['default_named11']=audit(connection)
   import redis
   from redis_channels import inventory
   default_client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=7,socket_timeout=2,socket_connect_timeout=2,max_connections=2)
   try:report['default_cache_DB7']=inventory(default_client,7)
   finally:default_client.close()
   report.update(capture_valid=True,passed=True);return
  require(int(settings.REDIS_DB)==cohort['Redis_database'],'DeclaredSettingsRedisDB');probe=Probe(nonce,cohort['Redis_database']);report['before_receiver_Redis']=probe.raw();report['receiver_context']=probe.register();report['before_setup']=snapshot();report['phase']='owned_fixture_setup';flush()
  actors={};User=get_user_model()
  with transaction.atomic():
   require(User.objects.count()==0,'FreshOwnedUsersOnly')
   for key,value in plan['baseline_groups'].items():CoreSettings.objects.update_or_create(key=key,defaults={'name':'Owned baseline '+key,'value':value})
   for role,info in plan['auth']['roles'].items():
    if info['user_level']is None:continue
    properties={}if'custom_UI'not in info else{'allowed_networks':{'UI':info['custom_UI']}}
    actors[role]=User.objects.create(username='owned-special-'+nonce+'-'+role,password='!',api_key='special-'+nonce+'-'+role,user_level=info['user_level'],is_active=True,custom_properties=properties)
  report['actual_bindings']=bindings(cohort['Redis_database']);require(report['actual_bindings']['Celery_eager']is False,'RealBrokerNotEager');report['after_setup']=snapshot();target_id=CoreSettings.objects.get(key=group).id;report['initial_actual_target_id']=target_id;report['phase']='requests';flush()
  for spec in [c for c in plan['cases']if c['cohort']==group]:
   guard();info=plan['auth']['roles'][spec['role']];body=(ROOT/'definition'/spec['body_spec']['body']).read_bytes();require(len(body)==spec['body_spec']['body_bytes']and hashlib.sha256(body).hexdigest()==spec['body_spec']['body_sha256'],'ExactPredeterminedBody')
   url=plan['routes']['collection']if spec['target']=='collection'else plan['routes']['detail'].format(actual_id=target_id);match=resolve(url);action={'GET':'list'if spec['target']=='collection'else'retrieve','PUT':'update','PATCH':'partial_update','POST':'create','DELETE':'destroy'}[spec['method']];require(match.func.cls is CoreSettingsViewSet and match.func.actions.get(spec['method'].lower())==action,'LiteralResolvedSpecialAPIAction')
   warm={'before':snapshot(),'declared':'normal group helper; proxy/defaultUA helper when applicable; owned condition sentinel'}
   CoreSettings._get_group(group)
   if group=='proxy_settings':
    from apps.proxy.config import BaseConfig
    BaseConfig.get_proxy_settings()
   if group=='stream_settings':CoreSettings.get_default_user_agent()
   if group=='network_access':cache.set('dev_notif_condition_owned_special_'+nonce,{'owned':True},timeout=300)
   warm['after']=snapshot()
   client=APIClient(enforce_csrf_checks=True);client.raise_request_exception=False;headers={}if spec['role']=='anonymous'else{'HTTP_X_API_KEY':actors[spec['role']].api_key};client.credentials(**headers)
   row={'id':spec['id'],'name':spec['name'],'group':group,'role':spec['role'],'method':spec['method'],'path':url,'target_id':None if spec['target']=='collection'else target_id,'request_body_hex':body.hex(),'body_sha256':spec['body_spec']['body_sha256'],'private_header_hex':{k:v.encode().hex()for k,v in headers.items()},'REMOTE_ADDR':info['REMOTE_ADDR'],'resolved_class':match.func.cls.__module__+'.'+match.func.cls.__qualname__,'resolved_action':action,'declared_pre_request_warm':warm,'before':snapshot(),'began_utc':timezone.now().isoformat(),'began_monotonic':time.monotonic(),'actual_invoked':True};report['requests'].append(row);report['actual_request_count']+=1;flush()
   try:
    response=client.generic(spec['method'],url,data=body,content_type=spec['body_spec']['content_type'],HTTP_ACCEPT='application/json',REMOTE_ADDR=info['REMOTE_ADDR']);raw=bytes(response.content);require(len(raw)<=65536,'BoundedFullSourceResponse');reply_headers=dict(response.items());require(sum(len(k.encode())+len(v.encode())for k,v in reply_headers.items())<=65536,'BoundedResponseHeaders')
    try:parsed=json.loads(raw)
    except(ValueError,UnicodeDecodeError):parsed=None
    request=(getattr(response,'renderer_context',None)or{}).get('request');user=getattr(request,'user',None);auth=getattr(request,'successful_authenticator',None);row.update(transport='http_response',status=response.status_code,response_body_hex=raw.hex(),response_body_bytes=len(raw),response_body_sha256=hashlib.sha256(raw).hexdigest(),headers=reply_headers,json=parsed,framework_exception=exception_record(getattr(response,'exc_info',None)),actual_auth={'user_id':getattr(user,'pk',None),'is_authenticated':bool(getattr(user,'is_authenticated',False)),'backend':None if auth is None else type(auth).__module__+'.'+type(auth).__qualname__,'actual_REMOTE_ADDR':None if request is None else request.META.get('REMOTE_ADDR')});response.close()
   except BaseException:row.update(transport='framework_exception',framework_exception=exception_record(sys.exc_info()))
   finally:row.update(completed_utc=timezone.now().isoformat(),completed_monotonic=time.monotonic())
   row['after']=snapshot();row['capture_complete']=row.get('framework_exception')is None or row['framework_exception']['complete'];require(row['capture_complete'],'WholeActualExceptionCapture');report['completed_capture_count']+=1;flush()
   if spec.get('bind_actual_created_id'):
    value=row.get('json');require(type(value)is dict and type(value.get('id'))is int and value['id']>0,'ActualReturnedPOSTIdentityNoFallback');target_id=value['id']
  require(report['actual_request_count']==len(cohort['case_ids'])==report['completed_capture_count'],'FixedCompleteCohort');report.update(capture_valid=True,phase='complete')
 except BaseException:report['outer_fixture_exception']=exception_record(sys.exc_info())
 finally:
  if probe is not None:
   from observer_cleanup import retire
   retired,errors=retire(probe,snapshot);report.update(retired)
   if errors:report['observer_cleanup_errors']=errors;report['capture_valid']=False
  try:connections.close_all();report['connections_close_called']=True
  except BaseException:report['connection_cleanup_exception']=exception_record(sys.exc_info());report['capture_valid']=False
  finally:flush()
 require(report['capture_valid']is True,'CaptureCompletenessSeparateFromOutcomes')
if __name__=='__main__':main()
