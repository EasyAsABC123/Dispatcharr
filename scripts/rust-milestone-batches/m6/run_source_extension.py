"""Common-container Source batch guest entrypoint. Root dispatches it only.
No Docker bootstrap, Source replay, task patch or owner concurrency pool.
"""
import argparse,base64,hashlib,json,os,sys,traceback
from pathlib import Path

def main(input_path):
 inputs=json.loads(Path(input_path).read_text())
 if inputs.get('root_authorized')is not True or inputs.get('phase')!='m6-missing-Source-domains':raise ValueError('RootSourceBatchAuthorization')
 database=inputs['database'];root=Path(inputs['output'])
 required={'apps/epg/tasks.py','apps/epg/utils.py','apps/epg/models.py','apps/channels/models.py','apps/channels/signals.py','core/tasks.py','core/models.py','core/utils.py','dispatcharr/celery.py','dispatcharr/settings.py','version.py'}
 if set(inputs['Original_module_sha256'])!=required:raise ValueError('Exact11PrimaryModuleBindings')
 if not database.startswith('rust_probe_backup_jobs_')or os.environ.get('POSTGRES_DB')!=database or os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise ValueError('CopiedOwnedSourceDatabase')
 if not root.is_absolute()or not str(root).startswith('/evidence/')or root.exists()or root.is_symlink():raise ValueError('FreshOwnedSourceOutput')
 plugin=Path(os.environ['DISPATCHARR_PLUGINS_DIR'])
 if not plugin.is_dir()or any(plugin.iterdir()):raise ValueError('ActualPluginDirEmptyBeforeApp')
 # Root provides the common fresh copied schema. Check real rows BEFORE Django
 # can initialize application hooks; no application/model method replacement.
 import psycopg
 with psycopg.connect(host='127.0.0.1',dbname=database,user=os.environ.get('POSTGRES_USER','postgres'),password=os.environ.get('POSTGRES_PASSWORD','playback-fixture-only'))as c:
  with c.cursor()as cur:
   for table in ['plugins_pluginconfig','plugins_pluginrepo','dispatcharr_connect_integration','dispatcharr_connect_eventsubscription','dispatcharr_connect_deliverylog']:
    cur.execute('SELECT COUNT(*) FROM '+table)
    if cur.fetchone()[0]!=0:raise ValueError('ActualFreshPluginIntegrationRows:'+table)
 import redis
 broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0)
 if broker.dbsize()!=0 or backend.dbsize()!=0:raise ValueError('FreshOwnedSourceBrokerBackend')
 # Same declared startup-dispatch guards as the retained Source default task
 # fixture. This missing-domain Source phase is NOT full Main/startup parity.
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600,nx=True)
 root.mkdir();os.environ['M6_SOURCE_EVIDENCE']=str(root);sys.path[:0]=['/app',str(Path(__file__).resolve().parent)];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
 from source_runtime import Observer,VersionOrigin,WorkerFactory
 origin=VersionOrigin(root);factory=WorkerFactory(root,Path(__file__).resolve().parent)
 error=None;report=None;ctx=None
 try:
  os.environ['DEFAULT_TASK_HTTP_ORIGIN']=origin.url
  import m6_source_observer
  import django;django.setup()
  # Verify actual app code BEFORE model setup/task publication.
  for name,expected in inputs['Original_module_sha256'].items():
   data=(Path('/app')/name).read_bytes()
   if hashlib.sha256(data).hexdigest()!=expected:raise ValueError('ActualOriginalModule:'+name)
  from source_context import Context
  from source_extension import execute
  from source_accept import validate,validate_versions
  ctx=Context(root=root,prefix=inputs['prefix'],app=m6_source_observer.app,broker=broker,backend=backend,cache=cache,worker_factory=factory,observer=Observer(root),owned_version_origin=origin,predeclared_xmltv_bytes=base64.b64decode(inputs['XMLTV_base64'],validate=True))
  versions_only=inputs.get('Source_cases')=='versions-only'
  report=execute(ctx,versions_only=versions_only);ctx.persist_partial('source-cases-before-acceptance',report);accepted=validate_versions(report)if versions_only else validate(report);report['acceptance']=accepted;report['passed']=True
 except BaseException as exception:error={'type':type(exception).__name__,'message':str(exception),'traceback':traceback.format_exc()}
 finally:
  cleanup=[]
  for job in factory.jobs:
   try:cleanup.append(factory.stop_and_collect(job))
   except BaseException as exception:cleanup.append({'error':type(exception).__name__,'message':str(exception),'settled':False})
  try:origin_closed=origin.close()
  except BaseException as exception:origin_closed={'error':type(exception).__name__,'message':str(exception),'thread_settled':False}
  from source_settlement import valid_worker
  cleanup_ok=bool(cleanup)and all(valid_worker(row)for row in cleanup)and origin_closed.get('thread_settled')is True
  result={'passed':bool(report and report.get('passed'))and error is None and cleanup_ok,'report':report,'error':error,'all_actual_worker_settlements':cleanup,'origin_close':origin_closed,'outer_host_default6259_Docker_CID_PID_cleanup_still_required':True,'performance_verified':False,'fullM6':False}
  data=(json.dumps(result,indent=2,default=str,allow_nan=False)+'\n').encode()
  if len(data)>32*1024*1024:raise ValueError('SourceReport32MiB; partial files retained')
  with (root/'Source-extension.json').open('xb')as out:out.write(data);out.flush();os.fsync(out.fileno())
 return 0 if result['passed']else 1
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--inputs',type=Path,required=True);a=p.parse_args();raise SystemExit(main(a.inputs))
