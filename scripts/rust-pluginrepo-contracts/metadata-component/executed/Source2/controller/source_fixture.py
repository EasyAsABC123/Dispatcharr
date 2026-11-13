"""UNEXECUTED fresh2 rendered-error PluginRepo real-resolver/API-key Source capture.
Entry is only the real pinned management shell; no framework/business patch.
"""
import hashlib,json,os,re,sys,traceback,time
from pathlib import Path
ROOT=Path('/plugin-inputs');REPORT=Path('/evidence/plugin-repositories-private.json')
def need(ok,label):
 if not ok:raise ValueError(label)
require=need
def save(path,value):
 raw=(json.dumps(value,indent=2,allow_nan=False)+'\n').encode();need(len(raw)<=67108864,'CaptureTotal64MiB');temp=path.with_name(path.name+'.tmp');temp.write_bytes(raw);temp.replace(path)
def exception(info):
 typ,value,tb=info;out={'type':typ.__module__+'.'+typ.__qualname__};complete=True
 for field,text,limit in [('message',str(value),65536),('traceback',''.join(traceback.format_exception(typ,value,tb)),4194304)]:
  raw=text.encode();captured=raw[:limit];out.update({field+'_hex':captured.hex(),field+'_bytes':len(captured),field+'_sha256':hashlib.sha256(captured).hexdigest(),field+'_original_bytes':len(raw),field+'_original_sha256':hashlib.sha256(raw).hexdigest(),field+'_truncated':len(raw)>limit});complete=complete and len(raw)<=limit
 out.update(complete=complete,truncated=not complete);return out

def rows(model):
 from capture_common import literal
 value=literal(list(model.objects.order_by('id').values()[:10001]));need(len(value)<=10000 and len(json.dumps(value).encode())<=8388608,'RawModelCapacity');return value

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
     elif type(item)in(dict,list):
      stack=[(item,0)];nodes=0
      while stack:
       node,depth=stack.pop();nodes+=1;require(nodes<=256 and depth<=8,'CachedJSONMetadataNodeDepth')
       if type(node)is dict:require(all(type(key)is str and len(key.encode())<=1024 for key in node),'CachedJSONMetadataKeys');stack.extend((child,depth+1)for child in node.values())
       elif type(node)is list:require(len(node)<=64,'CachedJSONMetadataItems');stack.extend((child,depth+1)for child in node)
       else:require(type(node)in(str,int,float,bool)or node is None,'CachedJSONMetadataLiteralTypes')
      require(len(json.dumps(item,allow_nan=False).encode())<=65536,'CachedJSONMetadataBytes');items.append({'kind':'json_literal','value':item})
     else:
      # UploadedFile metadata only. Reading/rewinding would change a request-owned stream.
      require(hasattr(item,'name')and hasattr(item,'size')and hasattr(item,'content_type'),'KnownCachedUploadedFileMetadata');items.append({'kind':'file_metadata','class':type(item).__module__+'.'+type(item).__qualname__,'name':item.name,'size':item.size,'content_type':item.content_type,'charset':getattr(item,'charset',None),'file_content_read':False})
    fields.append({'name':key,'values':items})
   row['fields']=fields
  out['cached'][name]=row
 return out

def files():
 result=[];used=0
 for root in [Path('/data/logs'),Path('/app/media')]:
  if not root.exists():continue
  for path in sorted(root.rglob('*')):
   need(not path.is_symlink(),'OwnedFilesNoSymlinks')
   if not path.is_file():continue
   need(len(result)<32 and path.stat().st_size<=1048576,'OwnedFileCountAndSize');raw=path.read_bytes();used+=len(raw);need(used<=4194304,'OwnedFiles4MiB');result.append({'root':str(root),'path':str(path.relative_to(root)),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'data_hex':raw.hex()})
 return result

def main():
 need(os.environ.get('PLUGIN_REPO_AUTHORITY')=='owned-source-repository-only','ExplicitOwnedAuthority');database=os.environ['PLUGIN_REPO_DATABASE'];need(re.fullmatch('rust_probe_pluginrepo_[a-f0-9]{10}',database)is not None,'ExactOwnedDatabase');need(len(sys.argv)>1 and sys.argv[1]=='shell','RealManagementShellBeforeDjangoSetup')
 from django.db import connection,connections,transaction
 from django.utils import timezone
 from django.urls import resolve
 from django.contrib.auth import get_user_model
 from dispatcharr.app_initialization import should_skip_initialization
 from rest_framework.test import APIClient
 from core.models import CoreSettings,SystemEvent,SystemNotification,NotificationDismissal
 from apps.plugins.models import PluginRepo,PluginConfig
 from apps.plugins.loader import PluginManager
 from apps.plugins.api_views import PluginRepoListCreateAPIView,PluginRepoDetailAPIView
 from public_manifest import manifest
 from redis_channels import Probe
 from capture_common import connected_database,runtime_snapshot,cache_snapshot
 need(should_skip_initialization()is True and connected_database(connection)==database,'OwnedDBAndPinnedManagementSkip');plan=json.loads((ROOT/'plan.json').read_text());dependencies=json.loads((ROOT/'actual-dependencies.json').read_text());need(len(dependencies)==67,'Exact55ReusedPlus12ActualRendererFiles')
 for dependency in dependencies:need(hashlib.sha256(Path(dependency['runtime_path']).read_bytes()).hexdigest()==dependency['sha256'],'PinnedActualRuntimeDependency')
 for original in json.loads((ROOT/'original-inputs.json').read_text())['files']:need(hashlib.sha256((Path('/app')/original['path']).read_bytes()).hexdigest()==original['sha256'],'PinnedOriginalRuntimeSource')
 from django.conf import settings
 schema=json.loads((ROOT/'schema62-authority.json').read_text());nonce=database.removeprefix('rust_probe_pluginrepo_');probe=Probe(nonce);User=get_user_model();actors={};report={'capture_only':True,'passed':False,'capture_complete':False,'actual_request_count':0,'completed_capture_count':0,'requests':[],'database':database,'Source_framework_APIClient_only':True,'Source_TCP_or_Native':False,'force_authenticate_used':False,'client_raise_request_exception':False,'enforce_csrf_checks':True,'startup_argv':list(sys.argv),'PluginManager_instance_must_remain_None':True,'phase':'startup','definition_sha256':hashlib.sha256((ROOT/'plan.json').read_bytes()).hexdigest()}
 report['observed_renderer_settings']={'DEBUG':settings.DEBUG,'DEBUG_PROPAGATE_EXCEPTIONS':settings.DEBUG_PROPAGATE_EXCEPTIONS,'ROOT_URLCONF':settings.ROOT_URLCONF,'DEFAULT_EXCEPTION_REPORTER':settings.DEFAULT_EXCEPTION_REPORTER};report['returned_response_exc_info_policy']='captured separately without rethrow or dynamic reporter invocation'
 def flush():save(REPORT,report)
 def guard():
  need(should_skip_initialization()is True and connected_database(connection)==database,'StillOwnedAndManagementSkip');need(vars(PluginManager).get('_instance')is None,'NoPluginManagerInstantiatedByMetadataPath')
 def snapshot():
  with transaction.atomic():
   with connection.cursor()as cursor:cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
   return {'public_manifest':manifest(connection,schema),'repos':rows(PluginRepo),'configs':rows(PluginConfig),'settings':rows(CoreSettings),'system_notifications':rows(SystemNotification),'notification_dismissals':rows(NotificationDismissal),'users':rows(User),'events':rows(SystemEvent),'owned_log_and_media_files':files(),'cache':cache_snapshot(),'raw_Redis':probe.raw(),'Channels':probe.snapshot()}
 try:
  report['initial_raw_Redis_before_probe']=probe.raw();report['probe_registration']=probe.register();report['runtime_before_seed']=runtime_snapshot();report['before_owned_seed']=snapshot();need(report['before_owned_seed']['public_manifest']==json.loads(Path('/evidence/default-before-private.json').read_text())['public62'],'ExactDefaultTemplateCopyBeforeOwnedSeed');flush();report['phase']='seed';report['seed_started_utc']=timezone.now().isoformat()
  with transaction.atomic():
   guard();need(User.objects.count()==0 and PluginConfig.objects.count()==0,'NoUnknownActorOrPluginConfig');official=PluginRepo.objects.get(is_official=True);need(official.pk==1 and official.url=='https://dispatcharr.github.io/Plugins/manifest.json','ActualPinnedOfficialMigrationRow');need(PluginRepo.objects.count()==1,'NoUnknownRepositories');
   for setting in plan['seed']['CoreSettings']:CoreSettings.objects.update_or_create(key=setting['key'],defaults={'name':setting['name'],'value':setting['value']})
   for spec in plan['roles']:
    alias=spec['alias'];actors[alias]=User.objects.create(id=spec['id'],username='owned-pluginrepo-'+nonce+'-'+alias,password='!',api_key='owned-rendered-errors-'+nonce+'-'+spec['api_key_alias'].replace('_','-'),user_level=spec['user_level'],is_active=True,custom_properties=spec['custom_properties'])
   for spec in plan['seed']['custom_repos']:PluginRepo.objects.create(**spec)
   need(plan['seed']['PluginConfig_rows']==[]and PluginConfig.objects.count()==0,'NoManagedConfigSeedOrPassedPrefix')
   with connection.cursor()as cursor:
    for name,last in plan['seed']['three_called_sequence_states'].items():
     need(name in ['plugins_pluginrepo_id_seq','plugins_pluginconfig_id_seq','accounts_user_id_seq'],'ThreeExactSeedSequences');cursor.execute('SELECT setval(%s,%s,true)',[name,last])
  report['seed_completed_utc']=timezone.now().isoformat();report['seed']=snapshot();report['runtime_after_seed']=runtime_snapshot();need(User._meta.get_field('api_key').unique is False and User._meta.get_field('api_key').db_index is True,'ActualNonuniqueIndexedApiKeyField');duplicate_key='owned-rendered-errors-'+nonce+'-duplicate-pair';need(list(User.objects.filter(api_key=duplicate_key).order_by('id').values_list('id',flat=True))==[201,202],'ExactTwoPersistedDuplicateKeyRowsBeforeAnyRequest');from effects import validate_seed
  report['seed_binding']=validate_seed(report,plan);report['phase']='requests';report['phase_started_utc']=timezone.now().isoformat();phase_started=time.monotonic();flush()
  for spec in plan['cases']:
   need(time.monotonic()-phase_started<=120,'SourcePhase120BeforeNextRequest');guard();path=plan['method_route_bindings'][spec['target']];matched=resolve(path);expected=PluginRepoListCreateAPIView if spec['target']=='collection'else PluginRepoDetailAPIView;need(matched.func.cls is expected,'ExactResolvedOriginalClass');need(matched.kwargs==({}if spec['target']=='collection'else{'pk':int(path.rstrip('/').split('/')[-1])}),'ExactTargetPKBinding');raw=bytes.fromhex(spec['body_hex']);need(len(raw)==spec['body_bytes']and hashlib.sha256(raw).hexdigest()==spec['body_sha256'],'ExactLiteralBodyBinding');client=APIClient(enforce_csrf_checks=True);client.raise_request_exception=False;headers={'HTTP_X_API_KEY':'owned-rendered-errors-'+nonce+'-'+spec['actor_key_alias'].replace('_','-')};client.credentials(**headers);row={'index':spec['index'],'name':spec['name'],'actor':spec['actor_key_alias'],'method':spec['method'],'target_alias':spec['target'],'path':path,'body_hex':raw.hex(),'body_bytes':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest(),'raw_headers_private':{k:v.encode().hex()for k,v in headers.items()},'content_type':spec['content_type'],'resolver':{'class':matched.func.cls.__module__+'.'+matched.func.cls.__qualname__,'kwargs':matched.kwargs,'url_name':matched.url_name},'before':snapshot()};report['requests'].append(row);row['began_utc']=timezone.now().isoformat();row['actual_request_invoked']=True;report['actual_request_count']+=1;flush()
   try:response=client.generic(spec['method'],path,data=raw,content_type=spec['content_type'],HTTP_ACCEPT='application/json',REMOTE_ADDR='127.0.0.1')
   except BaseException:row.update(transport='framework_exception',framework_exception=exception(sys.exc_info()))
   else:
    body=bytes(response.content);need(len(body)<=1048576,'ResponseBody1MiB');response_headers=dict(response.items());need(sum(len(k.encode())+len(v.encode())for k,v in response_headers.items())<=65536,'Headers64KiB')
    try:parsed=json.loads(body)
    except(ValueError,UnicodeDecodeError):parsed=None
    row['response_exc_info']=None if getattr(response,'exc_info',None)is None else exception(response.exc_info);request=(getattr(response,'renderer_context',None)or{}).get('request');actor=getattr(request,'user',None);auth=getattr(request,'successful_authenticator',None);row.update(transport='http_response',status=response.status_code,headers=response_headers,response_body_hex=body.hex(),response_body_bytes=len(body),response_body_sha256=hashlib.sha256(body).hexdigest(),json=parsed,framework_exception=None,parsed_request_metadata=parsed_metadata(request),actual_auth_observation={'user_id':getattr(actor,'pk',None),'is_authenticated':bool(getattr(actor,'is_authenticated',False)),'backend_class':None if auth is None else type(auth).__module__+'.'+type(auth).__qualname__})
   finally:row['completed_utc']=timezone.now().isoformat()
   guard();row['after']=snapshot();row['capture_complete']=all(row.get(field)is None or row[field]['complete']for field in('framework_exception','response_exc_info'));report['completed_capture_count']+=1;flush();need(row['capture_complete'],'ExceptionCapacityIsCaptureFailure')
  report['capture_complete']=report['actual_request_count']==report['completed_capture_count']==2;report['phase']='complete';report['phase_completed_utc']=timezone.now().isoformat();report['phase_elapsed_seconds']=time.monotonic()-phase_started;need(report['phase_elapsed_seconds']<=120,'SourcePhase120AfterFinalCapture');flush()
 except BaseException:report['outer_fixture_catch']={'phase':report['phase'],'exception':exception(sys.exc_info())};report['capture_complete']=False;flush()
 finally:
  try:report['final_before_probe_cleanup']=snapshot();report['probe_cleanup']=probe.discard();report['final_raw_Redis_after_probe_cleanup']=probe.raw();probe.close_clients()
  except BaseException:report['probe_cleanup_exception']=exception(sys.exc_info());report['capture_complete']=False
  connections.close_all();report['connections_close_called']=True;flush()
 need(report['capture_complete']is True,'Exact2CaptureCompleteNoRetry')
if __name__=='__main__':main()
