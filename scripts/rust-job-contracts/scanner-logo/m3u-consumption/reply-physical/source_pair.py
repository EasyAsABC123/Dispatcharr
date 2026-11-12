"""Native-only actual held Redis replies through production M3U worker; Original producer.
No Original-handler timing/performance claim. Each fresh parent is produced by the real Original scanner.
"""
import json,os,signal,subprocess,sys,time,uuid,select
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.db import connection
from dispatcharr.celery import app
from core.models import CoreSettings,SYSTEM_SETTINGS_KEY
from apps.m3u.models import M3UAccount
from owned_reply_proxy import Proxy
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9 and broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});worker=None;native=None;proxy=None;role=None
report={'passed':False,'native_verified':True,'scope':__doc__,'checks':[],'cases':[],'predeclared':{'modes':['parent','claim','events','result','cache'],'shutdown_probe_seconds':.03}}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn,seconds=30):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  v=fn()
  if v:return v
  time.sleep(.01)
 raise TimeoutError('Owned physical control deadline')
try:
 for root in ['/data/m3us','/data/epgs','/data/logos']:Path(root).mkdir(exist_ok=True);check('fresh-empty-'+root,not list(Path(root).iterdir()))
 for mode in ['parent','claim','events','result','cache']:
  channel='specific.rust_owned_m3u_replies!receiver';channel_key='asgispecific.rust_owned_m3u_replies!';cache.delete(channel_key);async_to_sync(get_channel_layer().group_add)('updates',channel)
  for old in Path('/data/m3us').iterdir():old.unlink()
  broker.delete('celery');cache.set('epg_content_generation',700)
  path=Path('/data/m3us/rust-owned-children-physical-'+mode+'.m3u');path.write_text('#EXTM3U\n#EXTINF:-1 group-title="Owned Group",Owned\nhttp://127.0.0.1:9/owned.ts\n'if mode=='cache'else'#EXTM3U\n');os.utime(path,(1699999993,1699999993))
  tid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':tid,'now':1700000000.125}))
  log=open('/evidence/scanner-worker-private.log','ab');worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q','rust-owned-scanner-parents','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close()
  parent=app.tasks['core.tasks.scan_and_process_files'].apply_async(args=[],task_id=tid,queue='rust-owned-scanner-parents');wait(lambda:any(v['id']==tid for v in records('postrun')));check(mode+'-real-parent-null',parent.get(timeout=2)is None);os.killpg(worker.pid,signal.SIGTERM);worker.wait(timeout=15);worker=None
  account=M3UAccount.objects.get(file_path=str(path));head=broker.lindex('celery',-1);ident=json.loads(head)['headers']['id'];proxy=Proxy(mode,tid,ident)
  stderr=open('/evidence/native-'+mode+'-private.log','wb');native=subprocess.Popen(['/native-m3u-peer','scanner_m3u_peer::native_scanner_m3u_peer','--exact','--ignored','--nocapture','--test-threads=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,bufsize=1)
  def response():
   end=time.monotonic()+35
   while time.monotonic()<end:
    assert native.poll()is None,'Native peer exited'
    if select.select([native.stdout],[],[],.1)[0]:
     line=native.stdout.readline();stderr.write(line.encode());stderr.flush()
     if 'NATIVE_SCANNER_M3U_JSON='in line:return json.loads(line.split('NATIVE_SCANNER_M3U_JSON=',1)[1])
   raise TimeoutError('Native physical reply response')
  def command(v):native.stdin.write(json.dumps(v)+'\n');native.stdin.flush();return response()
  config={'parent_id':tid,'account_id':account.id}
  field={'parent':'result_url','claim':'broker_url','events':'events_url','result':'result_url','cache':'cache_url'}[mode]
  config[field]='redis://:playback-fixture-only@127.0.0.1:'+str(proxy.server.server_address[1])+'/'+('8'if mode=='claim'else'9'if mode in ['parent','result']else'0')
  check(mode+'-normal-path-prepared',command(config)=={'prepared':True})
  if mode=='cache':
   check(mode+'-groups-first-complete',command({'action':'run'})['run'].startswith('Ok(Completed('));head=broker.lindex('celery',-1);ident=json.loads(head)['headers']['id'];proxy.child=ident.encode()
  check(mode+'-real-frame-before-start',json.loads(head)['headers']['parent_id']==tid)
  event_count_before=cache.zcard(channel_key)
  check(mode+'-owned-run-started',command({'action':'start'})=={'started':True});check(mode+'-actual-Redis-reply-held',proxy.held.wait(20))
  if mode=='parent':check(mode+'-no-ACK-result',broker.lindex('celery',-1)==head and backend.get(app.backend.get_key_for_task(ident))is None)
  elif mode=='result':check(mode+'-result-physically-committed',backend.get(app.backend.get_key_for_task(ident))is not None)
  else:check(mode+'-ACK-but-no-premature-result',broker.lindex('celery',-1)!=head and backend.get(app.backend.get_key_for_task(ident))is None)
  if mode=='events':check(mode+'-actual-Channels-message-committed',cache.zcard(channel_key)==event_count_before+1)
  if mode=='cache':check(mode+'-actual-generation-increment',cache.get('epg_content_generation')==b'701')
  answer=command({'action':'abort'});check(mode+'-caller-slot-shutdown-retained',answer=={'caller_retained':True,'slots':0,'admission':'Err(Capacity)','drain':'Err(DrainTimeout)'})
  proxy.release.set();check(mode+'-actual-completion-releases-owner',command({'action':'settled'})=={'caller_released':True,'slots':1})
  result=app.AsyncResult(ident);check(mode+'-Source-decodes-native-result',result.ready()and result.successful());result.get(timeout=2)
  check(mode+'-drain',command({'action':'shutdown'})=={'drained':True});native.stdin.close();native.wait(timeout=10);check(mode+'-native-exit0',native.returncode==0);native=None;stderr.close();proxy.close();check(mode+'-proxy-clean',not proxy.errors);report['cases'].append({'name':mode,'task_id':ident,'observation':answer,'proxy_commands':proxy.count,'event_count_before':event_count_before,'event_count_after':cache.zcard(channel_key)});proxy=None
 report['passed']=True
finally:
 if proxy:proxy.close()
 if native and native.poll()is None:native.kill();native.wait(timeout=5)
 if worker and worker.poll()is None:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5)
 Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'cases':len(report['cases'])}))
