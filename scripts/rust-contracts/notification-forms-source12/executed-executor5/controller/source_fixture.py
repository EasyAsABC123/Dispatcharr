"""UNEXECUTED real-resolver/API-key Source12 forms capture; no forced auth or parser patch."""
import hashlib,json,os,re,sys
from pathlib import Path
from capture_common import audit,cache_snapshot,connected_database,exception_record,literal,notice_rows,require,runtime_snapshot,save
from definition import target,validate,validate_row
ROOT=Path('/form-inputs');REPORT=Path('/evidence/notification-forms-private.json')
ACTIONS=['create','partial_update','partial_update','update','retrieve','partial_update','create','partial_update','partial_update','partial_update','partial_update','destroy']
def model_rows(model):
 values=literal(list(model.objects.order_by('id').values()[:10001]));require(len(values)<=10000 and len(json.dumps(values).encode())<=8388608,'BoundedRawModelRows');return values

def media_snapshot():
 root=Path('/app/media');rows=[];used=0
 for f in sorted(root.rglob('*')):
  require(not f.is_symlink(),'NoOwnedMediaSymlinks')
  if f.is_file():
   require(len(rows)<32 and f.stat().st_size<=1048576,'MediaFileCapacity');raw=f.read_bytes();used+=len(raw);require(used<=1048576,'MediaTotalCapacity');rows.append({'path':str(f.relative_to(root)),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
 return rows

def parsed_metadata(request):
 """Observe only already-cached fields. Never call request.data/FILES or read files."""
 if request is None:return {'request_available':False,'forced_parse':False}
 attrs=vars(request);out={'request_available':True,'request_class':type(request).__module__+'.'+type(request).__qualname__,'forced_parse':False,'cached':{}}
 for name in ('_data','_files','_full_data'):
  if name not in attrs:out['cached'][name]={'present':False};continue
  value=attrs[name];row={'present':True,'class':type(value).__module__+'.'+type(value).__qualname__}
  if isinstance(value,type):row['class_value']=type.__getattribute__(value,'__module__')+'.'+type.__getattribute__(value,'__qualname__')
  if isinstance(value,dict):
   pairs=list(value.lists())if hasattr(value,'lists')else [(k,[v])for k,v in value.items()];require(len(pairs)<=64,'ParserFieldCapacity');fields=[]
   for key,values in pairs:
    require(type(key)is str and len(key.encode())<=1024 and len(values)<=32,'ParserFieldAndValuesCapacity');items=[]
    for item in values:
     if type(item)in(str,int,bool,float)or item is None:require(len(json.dumps(item).encode())<=65536,'ParserScalarCapacity');items.append({'kind':'literal','value':item})
     else:
      # UploadedFile metadata only. Reading/rewinding would change a request-owned stream.
      require(hasattr(item,'name')and hasattr(item,'size')and hasattr(item,'content_type'),'KnownCachedUploadedFileMetadata');items.append({'kind':'file_metadata','class':type(item).__module__+'.'+type(item).__qualname__,'name':item.name,'size':item.size,'content_type':item.content_type,'charset':getattr(item,'charset',None),'file_content_read':False})
    fields.append({'name':key,'values':items})
   row['fields']=fields
  out['cached'][name]=row
 return out

def main():
 require(os.environ.get('NOTIFICATION_FORMS_AUTHORITY')=='owned-source-forms-only','FormsAuthority');stage=os.environ['NOTIFICATION_FIXTURE_STAGE'];database=os.environ['NOTIFICATION_FIXTURE_DATABASE'];require(stage in('audit','exercise'),'KnownStage')
 require(database=='dispatcharr'if stage=='audit'else re.fullmatch(r'rust_probe_notifforms_[a-f0-9]{10}',database)is not None,'ExactOwnedOrDefaultDatabase')
 from django.db import connection,connections,transaction
 from django.utils import timezone
 from django.contrib.auth import get_user_model
 from django.urls import resolve
 from dispatcharr.app_initialization import should_skip_initialization
 from rest_framework.test import APIClient
 from rest_framework.settings import api_settings
 from core.models import CoreSettings,SystemNotification,NotificationDismissal,SystemEvent
 from core.api_views import SystemNotificationViewSet
 plan=json.loads((ROOT/'plan.json').read_text());validate(plan,Path('/form-root'));schema_expected=json.loads((ROOT/'schema62-authority.json').read_text());probe=None;report={'stage':stage,'database':database,'Original':plan['Original'],'capture_valid':False,'actual_request_count':0,'completed_capture_count':0,'requests':[],'force_authenticate_used':False,'Source_TCP_or_Native':False,'client_raise_request_exception':False,'enforce_csrf_checks':True,'phase':'startup','startup_scope':'actual Original management shell before setup, not deployed startup acceptance'}
 def flush():
  if stage=='exercise':save(REPORT,report)
 def guard():require(connected_database(connection)==database and should_skip_initialization()is True,'OwnedDBAndRealStartupSkip')
 def snapshot():
  from public_manifest import manifest
  with transaction.atomic():
   with connection.cursor()as cursor:cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
   result={'audit':audit(connection),'public_manifest':manifest(connection,schema_expected),'notifications':notice_rows(SystemNotification,NotificationDismissal),'users':model_rows(get_user_model()),'events':model_rows(SystemEvent),'cache':cache_snapshot(),'media':media_snapshot(),'raw_Redis':probe.raw(),'Channels':probe.snapshot()}
  return result
 try:
  require('shell'in sys.argv and should_skip_initialization()is True,'RealManagementShellBeforeSetup');guard()
  if stage=='audit':
   with transaction.atomic():
    with connection.cursor()as cursor:cursor.execute('SET TRANSACTION READ ONLY')
    report['audit']=audit(connection)
    from public_manifest import manifest
    report['public_manifest']=manifest(connection,schema_expected)
   report.update(capture_valid=True,passed=True);return
  report['phase']='actual-input-preflight';flush()
  from redis_channels import Probe
  probe=Probe(database.removeprefix('rust_probe_notifforms_'));report['initial_raw_Redis_before_probe']=probe.raw();report['Channels_probe_context']=probe.register();flush()
  for row in plan['actual_dependencies']:require(hashlib.sha256(Path(row['runtime_path']).read_bytes()).hexdigest()==row['sha256'],'Actual55RuntimeDependencyBytes')
  report['parser_classes']=[p.__module__+'.'+p.__qualname__ for p in api_settings.DEFAULT_PARSER_CLASSES];require(report['parser_classes']==['rest_framework.parsers.JSONParser','rest_framework.parsers.FormParser','rest_framework.parsers.MultiPartParser'],'ActualPinnedDefaultParsers');report['runtime_before_seed']=runtime_snapshot();report['before_owned_isolation']=snapshot();report['phase']='owned-seed';report['seed_started_utc']=timezone.now().isoformat();flush();User=get_user_model();actors={};nonce=database.removeprefix('rust_probe_notifforms_')
  with transaction.atomic():
   guard();CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Network Access','value':plan['global_network_settings']});report['after_network_save_before_notice_isolation']=notice_rows(SystemNotification,NotificationDismissal);flush();NotificationDismissal.objects.all().delete();SystemNotification.objects.all().delete();require(User.objects.count()==0,'FreshOwnedUsers')
   for role in plan['roles']:
    alias=role['alias'];actors[alias]=User.objects.create(id=role['id'],username='rust-probe-notifforms-'+nonce+'-'+alias,password='!',api_key='fixture-forms-key-'+nonce+'-'+alias,is_active=role['is_active'],user_level=role['user_level'],custom_properties=role['custom_properties'])
   for seed in plan['seed_notifications']:
    data={k:v for k,v in seed.items()if k not in('created_at','updated_at')};obj=SystemNotification.objects.create(**data);SystemNotification.objects.filter(pk=obj.pk).update(created_at=seed['created_at'],updated_at=seed['updated_at'])
   for seed in plan['seed_dismissals']:
    data={k:v for k,v in seed.items()if k!='dismissed_at'};obj=NotificationDismissal.objects.create(**data);NotificationDismissal.objects.filter(pk=obj.pk).update(dismissed_at=seed['dismissed_at'])
   with connection.cursor()as cursor:
    for table,row in plan['seed_sequences'].items():require(table in('accounts_user','core_systemnotification','core_notificationdismissal'),'ThreeOwnedSequences');cursor.execute("SELECT setval(%s,%s,%s)",[table+'_id_seq',row['last_value'],row['is_called']])
  report['seed_completed_utc']=timezone.now().isoformat();report['seed']=snapshot();report['aliases']=dict(plan['seed_aliases']);report['runtime_after_seed']=runtime_snapshot();from effects import validate_seed
  report['seed_binding']=validate_seed(report,plan);flush();report['phase']='requests'
  for spec in plan['cases']:
   guard();path=target(spec,report['aliases']);matched=resolve(path);method=spec['method'].lower();require(matched.func.cls is SystemNotificationViewSet and matched.func.actions.get(method)==ACTIONS[spec['index']-1],'ActualCorrectResolvedClassAction');raw=(ROOT/spec['body_path']).read_bytes();require(len(raw)==spec['body_bytes']and hashlib.sha256(raw).hexdigest()==spec['body_sha256'],'FrozenRawBodyInput');material={'HTTP_X_API_KEY':actors[spec['actor']].api_key};client=APIClient(enforce_csrf_checks=True);client.raise_request_exception=False;client.credentials(**material)
   row={'index':spec['index'],'method':spec['method'],'actor':spec['actor'],'target_alias':spec['target_alias'],'path':path,'body_sha256':hashlib.sha256(raw).hexdigest(),'body_bytes':len(raw),'content_type':spec['content_type'],'raw_headers_private':{k:v.encode().hex()for k,v in material.items()},'resolver':{'class':matched.func.cls.__module__+'.'+matched.func.cls.__qualname__,'action':matched.func.actions[method],'actions':matched.func.actions,'kwargs':matched.kwargs,'url_name':matched.url_name},'before':snapshot()};validate_row(row,spec,report['aliases']);report['requests'].append(row);flush();row['began_utc']=timezone.now().isoformat();row['actual_request_invoked']=True;report['actual_request_count']+=1;flush()
   try:
    response=client.generic(spec['method'],path,data=raw,content_type=spec['content_type']or'application/octet-stream',HTTP_ACCEPT='application/json',REMOTE_ADDR='127.0.0.1')
   except BaseException:
    row.update(transport='framework_exception',framework_exception=exception_record(sys.exc_info()))
   else:
    row['transport']='http_response';row['status']=response.status_code;flush()
    try:
     body=bytes(response.content);require(len(body)<=65536,'BodyCaptureCapacity');headers=dict(response.items());require(sum(len(k.encode())+len(v.encode())for k,v in headers.items())<=65536,'HeaderCaptureCapacity')
     try:parsed=json.loads(body)
     except(ValueError,UnicodeDecodeError):parsed=None
     row.update(headers=headers,response_body_hex=body.hex(),response_body_bytes=len(body),response_body_sha256=hashlib.sha256(body).hexdigest(),json=parsed,framework_exception=exception_record(getattr(response,'exc_info',None)));flush()
     actual_request=(getattr(response,'renderer_context',None)or{}).get('request');actor=getattr(actual_request,'user',None);backend=getattr(actual_request,'successful_authenticator',None)
     row.update(parsed_request_metadata=parsed_metadata(actual_request),actual_auth_observation={'user_id':getattr(actor,'pk',None),'is_authenticated':bool(getattr(actor,'is_authenticated',False)),'backend_class':None if backend is None else type(backend).__module__+'.'+type(backend).__qualname__})
    except BaseException:
     row['capture_observer_exception']=exception_record(sys.exc_info());flush();raise
   finally:row['completed_utc']=timezone.now().isoformat()
   row['after']=snapshot();row['capture_complete']=row.get('framework_exception')is None or row['framework_exception']['complete'];report['completed_capture_count']+=1;flush();require(row['capture_complete'],'LiteralCompleteExceptionCapacity')
   if spec['index']in(1,7):
    name='FormA'if spec['index']==1 else'FormB';value=row.get('json');require(isinstance(value,dict)and type(value.get('id'))is int and value['id']>0,'ActualNaturalAliasUnavailableStopsFutureNotInvented');report['aliases'][name]=value['id'];flush()
  report.update(capture_valid=True,phase='complete');flush()
 except BaseException:report['outer_fixture_catch']={'phase':report['phase'],'exception':exception_record(sys.exc_info())};flush()
 finally:
  if probe is not None:
   try:report['final_before_probe_cleanup']=snapshot();report['Channels_probe_cleanup']=probe.discard();report['final_raw_Redis_after_probe_cleanup']=probe.raw();probe.close_clients()
   except BaseException:report['Channels_probe_cleanup_exception']=exception_record(sys.exc_info());report['capture_valid']=False
  connections.close_all();report['connections_close_called']=True
  save(REPORT if stage=='exercise'else Path('/evidence/audit-'+os.environ['NOTIFICATION_AUDIT_LABEL']+'-private.json'),report)
 require(report['capture_valid']is True and(stage=='audit'or report['actual_request_count']==12),'Source12CaptureCompleteNoRetry')
if __name__=='__main__':main()
