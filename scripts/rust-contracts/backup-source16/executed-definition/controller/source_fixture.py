"""Fixed unexecuted Source16 real resolver, credentials, worker and archive fixture."""
import hashlib,json,os,sys,zipfile
from pathlib import Path
# Real Original management-shell startup marker precedes django.setup in ALL stages.
STAGE=sys.argv[1];sys.argv.append('shell');sys.argv[1:]=['migrate' if STAGE=='migrate' else 'shell',STAGE,'shell'];os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from backup_common import *
from binding import initial_aliases,absorb_created,absorb_completed,path_for,validate_row,FUNCTIONS
PLAN=json.loads((ROOT/'plan.json').read_text());REPORT=EVIDENCE/'backup-job-private.json'
def main():
 from django.db import connection,connections,transaction
 from dispatcharr.app_initialization import should_skip_initialization
 require('shell'in sys.argv and should_skip_initialization()is True,'RealShellMarkerBeforeSetup')
 if STAGE=='migrate':
  require(os.environ['POSTGRES_DB']=='dispatcharr','DefaultBootstrapOnly');from django.core.management import call_command;call_command('migrate',interactive=False,verbosity=1);connections.close_all();return
 if STAGE=='audit':
  require(os.environ['POSTGRES_DB']=='dispatcharr'and connected_database(connection)=='dispatcharr','DefaultReadOnlyOnly')
  with transaction.atomic():
   with connection.cursor()as cursor:cursor.execute('SET TRANSACTION READ ONLY')
   value=audit(connection)
  connections.close_all();save(EVIDENCE/('audit-'+os.environ['BACKUP_AUDIT_LABEL']+'-private.json'),{'passed':True,'capture_valid':True,'audit':value,'connections_close_called':True,'startup_skip':True});return
 db=ownership();require(STAGE in('before','after','poll','snapshot','stop-worker'),'KnownOwnedStage')
 if STAGE=='stop-worker':
  import signal
  record=json.loads((EVIDENCE/'worker-PID.json').read_text());pid=record['pid'];stat=Path('/proc',str(pid),'stat');require(record['nonce']==nonce()and stat.read_text().rsplit(')',1)[1].split()[19]==record['starttime']and b'/backup-tools/worker_fixture.py'in Path('/proc',str(pid),'cmdline').read_bytes(),'ExactWorkerPIDBeforeTERM');os.kill(pid,signal.SIGTERM);connections.close_all();return
 if STAGE=='poll':
  from dispatcharr.celery import app
  report=json.loads(REPORT.read_text());key=app.backend.get_key_for_task(report['aliases']['created_task']);raw=redis_client(3).get(key);save(EVIDENCE/'backend-poll.json',{'key_hex':key.hex(),'raw_hex':None if raw is None else raw.hex(),'pttl':redis_client(3).pttl(key),'locks':locks()});connections.close_all();return
 if STAGE=='snapshot':save(EVIDENCE/'post-worker-snapshot.json',snapshot());connections.close_all();return
 from django.contrib.auth import get_user_model
 from django.utils import timezone
 from django.urls import resolve
 from rest_framework.test import APIClient
 from core.models import CoreSettings
 User=get_user_model();report={'Original':PLAN['Original'],'nonce':nonce(),'capture_valid':False,'actual_request_count':0,'requests':[],'aliases':initial_aliases(nonce()),'phase':'startup','force_authenticate_used':False,'Source_TCP_or_Native':False,'startup_marker':'shell','startup_skip':True,'APIClient_exception_policy':{'raise_request_exception':False,'enforce_csrf_checks':True}}
 if STAGE=='after':report=json.loads(REPORT.read_text());require(report['actual_request_count']==12,'ExactFirstTwelveAlreadyCaptured')
 def flush():save(REPORT,report)
 try:
  report['phase']=STAGE+'-dependency-check';report['actual_dependency_paths']=dependencies();flush();app,config=configured_app(producer=STAGE=='before');report[STAGE+'_Celery_configuration']=config;flush()
  if STAGE=='before':
   from apps.plugins.models import PluginConfig
   from apps.channels.models import Recording
   report['owned_startup_scope']={'plugin_configs':model_rows(PluginConfig),'recordings':model_rows(Recording),'plugin_directory':os.environ['PLUGINS_DIR'],'backup_root':os.environ['BACKUP_ROOT']}
   require(report['owned_startup_scope']['plugin_configs']==[]and report['owned_startup_scope']['recordings']==[],'NoPluginsOrRecordingsInOwnedFixture')
   report['initial_owned_snapshot']=snapshot();require(User.objects.count()==0,'OwnedUsersInitiallyEmpty');require(not any(redis_client(db).dbsize()for db in(2,3)),'OwnedBrokerResultInitiallyEmpty');Path(os.environ['BACKUP_ROOT']).mkdir(parents=True,exist_ok=False);Path(os.environ['PLUGINS_DIR']).mkdir(parents=True,exist_ok=False)
   with transaction.atomic():
    ownership();CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Network Access','value':{'UI':'127.0.0.0/8'}})
    for role in PLAN['roles']:User.objects.create(id=role['id'],username='rust-probe-backupjob-'+nonce()+'-'+role['alias'],password='!',api_key='backup-fixture-'+nonce()+'-'+role['alias'],is_active=role['is_active'],user_level=role['user_level'],custom_properties=role['custom_properties'])
    with connection.cursor()as cursor:cursor.execute("SELECT setval('accounts_user_id_seq',3,true)")
   for lock in LOCKS:require(redis_client(1).set(lock,'1',ex=300,nx=True)is True,'ExactlyOwnedStartupLockPreclaim')
   report['seed_users']=model_rows(User);report['seed_snapshot']=snapshot();report['locks_after_preclaim']=locks();report['Original_receivers_after_seed']=signal_snapshot();flush()
  else:
   backend=json.loads((EVIDENCE/'backend-poll.json').read_text());raw=bytes.fromhex(backend['raw_hex']);meta=json.loads(raw);require(meta['status']=='SUCCESS'and meta['task_id']==report['aliases']['created_task'],'ActualOwnBackendSUCCESS');absorb_completed(report['aliases'],meta['task_id'],meta['result']);report['actual_completed_backend']=backend;flush()
  actors={role['alias']:User.objects.get(pk=role['id'])for role in PLAN['roles']}
  for spec in [x for x in PLAN['cases']if(x['ordinal']<=12)==(STAGE=='before')]:
   ownership();path=path_for(spec,report['aliases']);matched=resolve(path.split('?',1)[0]);func=matched.func;module=func.__module__;name=FUNCTIONS[spec['ordinal']-1];from apps.backups import api_views
   require(func is getattr(api_views,name),'ActualResolverExportedCallableIdentity');headers={}
   if 'x_api_key'in spec['auth']:headers['HTTP_X_API_KEY']=actors[spec['auth']['x_api_key']].api_key
   if 'bearer_literal'in spec['auth']:headers['HTTP_AUTHORIZATION']='Bearer '+spec['auth']['bearer_literal']
   row={'ordinal':spec['ordinal'],'name':spec['name'],'method':spec['method'],'path':path,'auth_selectors':spec['auth'],'raw_headers_private':{k:v.encode().hex()for k,v in headers.items()},'raw_body_ascii':spec['raw_body_ascii'],'content_type':spec['content_type'],'resolver':{'module':module,'function':name,'actual_wrapper_name':func.__name__,'exported_callable_identity_matches':True,'kwargs':matched.kwargs,'url_name':matched.url_name},'before':snapshot(),'began_utc':timezone.now().isoformat()};validate_row(row,spec,report['aliases']);report['requests'].append(row);flush();client=APIClient(enforce_csrf_checks=True);client.raise_request_exception=False;client.credentials(**headers)
   try:
    response=client.generic(spec['method'],path,data=spec['raw_body_ascii']or'',content_type=spec['content_type']or'application/octet-stream',HTTP_ACCEPT='application/json',REMOTE_ADDR='127.0.0.1');streaming=bool(response.streaming);chunks=[];used=0
    if streaming:
     try:
      for chunk in response.streaming_content:
       used+=len(chunk);require(used<=8388608,'StreamBodyCapacity');chunks.append(bytes(chunk))
     finally:response.close()
     raw=b''.join(chunks)
    else:raw=bytes(response.content);require(len(raw)<=65536,'BodyCapacity')
    try:parsed=json.loads(raw)
    except(ValueError,UnicodeDecodeError):parsed=None
    row.update(transport='http_response',status=response.status_code,headers=dict(response.items()),streaming=streaming,body_hex=raw.hex(),body_bytes=len(raw),body_sha256=hashlib.sha256(raw).hexdigest(),json=parsed,framework_exception=exception_record(getattr(response,'exc_info',None)))
    if spec['ordinal']==4:absorb_created(report['aliases'],parsed)
   except BaseException:row.update(transport='framework_exception',framework_exception=exception_record(sys.exc_info()))
   finally:row['completed_utc']=timezone.now().isoformat();row['after']=snapshot();row['capture_complete']=row.get('framework_exception')is None or row['framework_exception']['complete'];report['actual_request_count']+=1;flush()
   require(row['capture_complete'],'LiteralExceptionCapacity')
   if spec['ordinal']==4:require('created_task'in report['aliases'],'CannotInventCreatedAliasAfterFailure')
  report['phase']=STAGE+'-complete';report['capture_valid']=True;flush()
 except BaseException:report['outer_fixture_catch']={'phase':report['phase'],'exception':exception_record(sys.exc_info())};report['capture_valid']=False;flush()
 finally:connections.close_all();report['connections_close_called']=True;flush()
 require(report['capture_valid']and report['actual_request_count']==(12 if STAGE=='before'else 16),'ExactPhaseCaptureNoRetry')
if __name__=='__main__':main()
