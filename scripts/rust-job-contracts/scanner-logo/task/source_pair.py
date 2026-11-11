"""Actual pinned Original logo file scan; separately observed real descendants.
Owned disposable volume/DB only. No provider/network/file content processing.
Original Dispatcharr/Django/Celery/Channels/Redis authors retain credit/licenses.
"""
import base64,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db import connection
from django.db.models import Q
from celery.signals import before_task_publish
from dispatcharr.celery import app
from apps.channels.models import Logo
from apps.epg.models import EPGSource,EPGSourceIndex
from core.utils import send_websocket_update
import core.tasks as source
NOW=1700000000.125
TASK='core.tasks.scan_and_process_files';PARENT_QUEUE='rust-owned-scanner-parents';ROOT=Path('/data/logos')
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
assert broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();layer=get_channel_layer();channel='specific.rust_owned_scanner!receiver';channel_key=layer.prefix+channel.split('!')[0]+'!';group_key=layer._group_key('updates');sentinel='rust:owned:scanner:sentinel';cache.set(sentinel,'preserve',ex=3600)
report={'passed':False,'native_verified':False,'checks':[],'cases':[],'scope':'Actual unchanged Original scanner logo rows/markers/events/partial errors; separately observed unconsumed Index children with real default routing. Parent queue is an explicit fixture control. No child execution or native/whole-family claim.','fixed_scanner_clock':NOW};worker=None;made=set();owned_markers=set();published={}
@before_task_publish.connect
def parent_publication(sender=None,body=None,headers=None,properties=None,exchange=None,routing_key=None,**extra):
 if sender==TASK:published[headers['id']]={'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn):
 end=time.monotonic()+40
 while time.monotonic()<end:
  result=fn()
  if result:return result
  if worker and worker.poll()is not None:raise RuntimeError('Owned worker exited')
  time.sleep(.025)
 raise TimeoutError('Owned scanner task deadline')
def start():
 log=open('/evidence/scanner-worker-private.log','ab');p=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','-Q',PARENT_QUEUE,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-scanner@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/pair-tools:/app'),stdout=log,stderr=log,start_new_session=True);log.close();return p
def stop():
 global worker
 if worker and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 worker=None
def drop_trigger():
 with connection.cursor()as c:c.execute('DROP TRIGGER IF EXISTS rust_owned_logo_fail ON dispatcharr_channels_logo');c.execute('DROP FUNCTION IF EXISTS rust_owned_logo_fail()')
def rows():return list(Logo.objects.order_by('id').values('id','name','url'))
def files():
 return [{'path':str(p),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in sorted(made)if p.exists()]
def markers():
 return {key:{'value_base64':None if cache.get(key)is None else base64.b64encode(cache.get(key)).decode(),'value_ascii':None if cache.get(key)is None else cache.get(key).decode('ascii'),'ttl':cache.ttl(key)}for key in sorted(owned_markers)}
def queued():
 result=[]
 for raw in broker.lrange('celery',0,127):
  msg=json.loads(raw);result.append({'raw_base64':base64.b64encode(raw).decode(),'sha256':hashlib.sha256(raw).hexdigest(),'message':msg,'body':json.loads(base64.b64decode(msg['body']))})
 return result
def reset():
 drop_trigger()
 for p in made:
  if p.exists():p.unlink()
 made.clear()
 for key in owned_markers:cache.delete(key)
 Logo.objects.filter(url__startswith=str(ROOT)+'/rust-owned-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub/rust-owned-').delete()
 cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel)
def make(name,age):
 p=ROOT/name;p.parent.mkdir(parents=True,exist_ok=True);assert not p.exists();p.write_bytes(b'owned synthetic metadata-only logo fixture\n');os.utime(p,(NOW-age,NOW-age));made.add(p);owned_markers.add('processed_file:'+str(p));return p
def execute(label,expected_counts=None,acl=None,expected_failure=False):
 before={'rows':rows(),'files':files(),'markers':markers(),'queue':queued()};taskid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':taskid,'now':NOW,**({'marker_acl_user':acl}if acl else{})}));result=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=taskid,queue=PARENT_QUEUE)
 post=wait(lambda:next((v for v in records('postrun')if v['id']==taskid),None));metadata=json.loads(backend.get(app.backend.get_key_for_task(taskid)));events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,15)];after={'rows':rows(),'files':files(),'markers':markers(),'queue':queued()};children=[v for v in records('children')if v['headers']['parent_id']==taskid];case={'name':label,'parent_id':taskid,'parent_publication':published[taskid],'before':before,'after':after,'postrun':post,'prerun':next(v for v in records('prerun')if v['id']==taskid),'metadata':metadata,'events':events,'children':children,'sql':next(v for v in records('sql')if v['id']==taskid)};report['cases'].append(case)
 check(label+'-actual-result',post['state']==metadata['status']==('FAILURE'if expected_failure else'SUCCESS'))
 if not expected_failure:check(label+'-typed-null',metadata['result']is None and metadata['traceback']is None and result.get(timeout=2)is None)
 check(label+'-unchanged-owned-files',before['files']==after['files'])
 check(label+'-parent-early-ack-and-no-child-consumption',broker.llen(PARENT_QUEUE)==broker.hlen('unacked')==0 and all(backend.get(app.backend.get_key_for_task(v['headers']['id']))is None for v in children))
 check(label+'-result-ttl',3580<=backend.ttl(app.backend.get_key_for_task(taskid))<=3600)
 check(label+'-sentinel',cache.get(sentinel)==b'preserve'and cache.ttl(sentinel)>3300)
 if expected_counts is not None:
  processed,skipped,errors,total=expected_counts
  expected=[]if processed==errors==0 else[{'type':'update','data':{'success':True,'type':'logo_processing_summary','processed':processed,'skipped':skipped,'errors':errors,'total_files':total,'message':f'Logo processing complete: {processed} processed, {skipped} skipped, {errors} errors'}}]
  actual=[{k:v for k,v in e.items()if k!='__asgi_channel__'}for e in events];check(label+'-exact-summary',actual==expected)
 return case
from django.db import transaction
import datetime as dt
PARENT_QUEUE='celery';peer=None;errorlog=None
reference=json.loads(Path('/evidence/task-reference.json').read_text());assert reference['passed']and len(reference['checks'])==24
report['scope']='Real Original producer→same normal native library/scopedworker/application→Original resultconsumer; logo-only knownconditions and ownValueError, no Main/defaultenable/fullscanner claim.'
def response():
 while True:
  line=peer.stdout.readline()
  if not line:raise RuntimeError('Native task peer EOF')
  if line.startswith('NATIVE_LOGO_TASK_JSON='):return json.loads(line.split('=',1)[1])
def send(value):peer.stdin.write(json.dumps(value)+'\n');peer.stdin.flush()
def ask(value):send(value);return response()
def native_start(config=None):
 global peer,errorlog
 errorlog=open('/evidence/native-task-peer-private.log','a');peer=subprocess.Popen(['/native-task-peer','logo_task_peer::native_logo_task_peer','--ignored','--nocapture'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errorlog,text=True,bufsize=1,start_new_session=True);send({'now':NOW,**(config or{})});return response()
def native_stop():
 global peer,errorlog
 if peer is None:return
 check('native-family-drain',ask({'action':'shutdown'})['drained']is True);peer.stdin.close();check('native-host-exit-zero',peer.wait(timeout=10)==0);peer=None;errorlog.close()
def produce(ident=None):
 ident=ident or str(uuid.uuid4());app.tasks[TASK].apply_async(args=[],kwargs={},task_id=ident,headers={'periodic_task_name':'rust-owned-scanner-source-protocol'});return ident

def metadata(ident):return json.loads(backend.get(app.backend.get_key_for_task(ident)))
def validate_meta(ident,status,normal=True):
 meta=metadata(ident);check(ident+'-metadata-fields',set(meta)=={'status','result','traceback','children','date_done','task_id'}and meta['task_id']==ident and meta['children']==[]and dt.datetime.fromisoformat(meta['date_done']).utcoffset()==dt.timedelta(0));check(ident+'-state-and-TTL',meta['status']==status and 3580<=backend.ttl(app.backend.get_key_for_task(ident))<=3600);client=app.AsyncResult(ident);check(ident+'-actual-Source-ready',client.ready()is True)
 if normal:check(ident+'-actual-Source-null',client.get(timeout=2)is None and meta['result']is None and meta['traceback']is None)
 else:
  expected="could not convert string to float: 'owned-invalid-float'";check(ident+'-typed-ValueError',meta['result']=={'exc_type':'ValueError','exc_module':'builtins','exc_message':[expected]}and meta['traceback'].startswith('Native Dispatcharr logo scanner diagnostic'))
  try:client.get(timeout=2)
  except ValueError as error:check(ident+'-actual-Source-raises-args',type(error)is ValueError and error.args==(expected,))
  else:raise AssertionError('Source consumer did not raise ValueError')
 return meta

def blocked():
 with connection.cursor()as c:c.execute('SELECT pg_stat_clear_snapshot()');c.execute("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%dispatcharr_channels_logo%')");return c.fetchone()[0]
def await_block():
 end=time.monotonic()+10
 while time.monotonic()<end:
  if blocked():return
  time.sleep(.01)
 raise TimeoutError('Owned native SQL-body block')
try:
 for root in [Path('/data/m3us'),Path('/data/epgs'),ROOT]:root.mkdir(exist_ok=True);check('fresh-empty-'+root.name,list(root.iterdir())==[])
 EPGSource.objects.all().update(is_active=False);reset();path=make('rust-owned-task-primary.png',7.0);os.utime(path,ns=(1699999993123456789,1699999993123456789));first=produce();before=queued();before_rows=rows();before_files=files();before_events=cache.zrange(channel_key,0,15);legacy=native_start({'legacy_notifications':True});check('legacy-notification-prepared',legacy.get('legacy_notifications')is True);check('legacy-notification-poller-start',ask({'action':'start'})['started']is True);end=time.monotonic()+5
 while True:
  observed=ask({'action':'state'})
  if observed['state']=='UnsupportedHead':break
  if time.monotonic()>end:raise TimeoutError('Legacy notification scanner refusal')
  time.sleep(.01)
 check('legacy-notification-refuses-real-scanner-before-ACK',queued()==before and broker.hlen('unacked')==0 and backend.get(app.backend.get_key_for_task(first))is None and rows()==before_rows and files()==before_files and cache.zrange(channel_key,0,15)==before_events);native_stop();ready=native_start();check('prepare-no-intake',ready=={'ready':True,'state':'Prepared','first':False}and queued()==before and backend.get(app.backend.get_key_for_task(first))is None)
 with transaction.atomic():
  with connection.cursor()as c:c.execute('LOCK TABLE dispatcharr_channels_logo IN ACCESS EXCLUSIVE MODE')
  send({'action':'run'});await_block();check('native-earlyACK-before-SQL-reply',broker.llen('celery')==broker.hlen('unacked')==broker.zcard('unacked_index')==0);check('native-PENDING-while-blocked',backend.get(app.backend.get_key_for_task(first))is None and app.AsyncResult(first).ready()is False)
 answer=response();meta=validate_meta(first,'SUCCESS');check('initial-completed-first-state',answer['run']=='Completed('+first+')'and answer['first_before']is False and answer['first_after']is True);check('normal-file-marker-row',Logo.objects.get(url=str(path)).name==path.stem and cache.get('processed_file:'+str(path))==str(path.stat().st_mtime).encode());report['cases'].append({'name':'normal-held-SQL','id':first,'response':answer,'metadata':meta,'files':files(),'markers':markers(),'events':[layer.deserialize(v)for v in cache.zrange(channel_key,0,15)]})
 raw=backend.get(app.backend.get_key_for_task(first));backend.expire(app.backend.get_key_for_task(first),120);Logo.objects.filter(url=str(path)).delete();cache.delete('processed_file:'+str(path));cache.delete(channel_key);produce(first);answer=ask({'action':'run'});check('sameID-body-reexecution-oldSUCCESS',answer['run']=='AlreadySucceeded('+first+')'and Logo.objects.filter(url=str(path)).exists()and backend.get(app.backend.get_key_for_task(first))==raw and 110<=backend.ttl(app.backend.get_key_for_task(first))<=120)
 Logo.objects.filter(url=str(path)).delete();cache.set('processed_file:'+str(path),'owned-invalid-float');cache.delete(channel_key);produce(first);answer=ask({'action':'run'});check('sameID-failure-oldSUCCESS-preserved',answer['run']=='AlreadySucceeded('+first+')'and backend.get(app.backend.get_key_for_task(first))==raw and app.AsyncResult(first).get(timeout=2)is None and not Logo.objects.filter(url=str(path)).exists()and cache.zcard(channel_key)==0 and 110<=backend.ttl(app.backend.get_key_for_task(first))<=120)
 fresh_failure=produce();answer=ask({'action':'run'});meta=validate_meta(fresh_failure,'FAILURE',normal=False);check('known-failure-terminal',answer['run']=='Failed('+fresh_failure+')'and cache.zcard(channel_key)==0);report['cases'].append({'name':'known-invalid-marker','id':fresh_failure,'response':answer,'metadata':meta})
 reset();path=make('rust-owned-task-SQL-catch.png',7.0)
 with connection.cursor()as c:c.execute("CREATE FUNCTION rust_owned_logo_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned logo insert failure' USING ERRCODE='P0001'; END $$");c.execute('CREATE TRIGGER rust_owned_logo_fail BEFORE INSERT ON dispatcharr_channels_logo FOR EACH ROW EXECUTE FUNCTION rust_owned_logo_fail()')
 ident=produce();answer=ask({'action':'run'});meta=validate_meta(ident,'SUCCESS');check('caughtSQL-no-ProgrammingFailure',answer['run']=='Completed('+ident+')'and not Logo.objects.filter(url=str(path)).exists()and cache.get('processed_file:'+str(path))is None);events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,15)];check('caughtSQL-summary',len(events)==1 and events[0]['data']['errors']==1 and events[0]['data']['processed']==0);report['cases'].append({'name':'caught-P0001-normal-null','id':ident,'metadata':meta,'events':events})
 reset();path=make('rust-owned-task-summary-catch.png',7.0);cache.delete(group_key);cache.set(group_key,'owned-wrongtype');ident=produce();answer=ask({'action':'run'});validate_meta(ident,'SUCCESS');check('caughtChannels-normal-with-committed-effects',answer['run']=='Completed('+ident+')'and Logo.objects.filter(url=str(path)).exists()and cache.get('processed_file:'+str(path))is not None and cache.zcard(channel_key)==0);native_stop()
 reset();path=make('rust-owned-task-marker-catch.png',7.0);cache.execute_command('ACL','SETUSER','rust-owned-scanner-no-set','on','>playback-fixture-only','~processed_file:*','+@all','-set');check('ACL-configured-prepare',native_start({'marker_acl_user':'rust-owned-scanner-no-set'})['ready']is True);ident=produce();answer=ask({'action':'run'});validate_meta(ident,'SUCCESS');check('caughtSET-after-Logo-commit-normal-null',answer['run']=='Completed('+ident+')'and Logo.objects.filter(url=str(path)).exists()and cache.get('processed_file:'+str(path))is None);native_stop();cache.execute_command('ACL','DELUSER','rust-owned-scanner-no-set')
 # Refuse scoped unsupported state without ACK, then repair ONLY owned input
 # and consume that same unchanged Source message through normal authority.
 reset();path=make('rust-owned-task-refusal.png',7.0);cache.set('processed_file:'+str(path),'NaN');check('refusal-prepare',native_start()['ready']is True);ident=produce();before=queued();answer=ask({'action':'run'});check('unqualified-marker-head-preserved',answer['run']=='Unsupported(LogoScanner)'and queued()==before and backend.get(app.backend.get_key_for_task(ident))is None and not Logo.objects.filter(url=str(path)).exists());cache.delete('processed_file:'+str(path));answer=ask({'action':'run'});validate_meta(ident,'SUCCESS');check('same-message-after-owned-repair',answer['run']=='Completed('+ident+')')
 reset();path=make('rust-owned-task-unsupported-watch.png',7.0);extra=Path('/data/m3us/rust-owned-unported.m3u');extra.write_bytes(b'owned unported input');made.add(extra);ident=produce();before=queued();answer=ask({'action':'run'});check('unported-M3U-head-preserved',answer['run']=='Unsupported(LogoScanner)'and queued()==before and backend.get(app.backend.get_key_for_task(ident))is None);extra.unlink();made.remove(extra);check('M3U-repair-same-message',ask({'action':'run'})['run']=='Completed('+ident+')')
 reset();path=make('rust-owned-task-helper-refusal.png',7.0);EPGSource.objects.bulk_create([EPGSource(name='rust-owned-task-eligible-index',source_type='xmltv',is_active=True,file_path='/evidence/owned-not-read.xml')]);ident=produce();before=queued();answer=ask({'action':'run'});check('eligible-Index-head-preserved',answer['run']=='Unsupported(LogoScanner)'and queued()==before and backend.get(app.backend.get_key_for_task(ident))is None);EPGSource.objects.filter(name='rust-owned-task-eligible-index').update(is_active=False);check('Index-repair-same-message',ask({'action':'run'})['run']=='Completed('+ident+')');native_stop()
 # Actual prepared application poller handles the real Source-produced task.
 reset();path=make('rust-owned-task-poller.png',7.0);check('poller-prepare',native_start()['ready']is True);ident=produce();check('explicit-start-only',ask({'action':'start'})['started']is True);end=time.monotonic()+10
 while backend.get(app.backend.get_key_for_task(ident))is None:
  if time.monotonic()>end:raise TimeoutError('Native prepared poller')
  time.sleep(.01)
 validate_meta(ident,'SUCCESS');check('poller-created-row',Logo.objects.filter(url=str(path)).exists());native_stop()
 # New peer process loss after actual early ACK before any SQL body effect.
 reset();path=make('rust-owned-task-hardloss.png',7.0);check('hardloss-prepare',native_start()['ready']is True);lost=produce()
 with transaction.atomic():
  with connection.cursor()as c:c.execute('LOCK TABLE dispatcharr_channels_logo IN ACCESS EXCLUSIVE MODE')
  send({'action':'run'});await_block();check('hardloss-native-earlyACK',broker.llen('celery')==broker.hlen('unacked')==0 and backend.get(app.backend.get_key_for_task(lost))is None);os.killpg(peer.pid,signal.SIGKILL);peer.wait(timeout=10);peer=None;errorlog.close()
 check('hardloss-native-no-effects',not Logo.objects.filter(url=str(path)).exists()and cache.get('processed_file:'+str(path))is None and backend.get(app.backend.get_key_for_task(lost))is None);check('replacement-fresh-first-state',native_start()=={'ready':True,'state':'Prepared','first':False});time.sleep(1);check('bounded-no-native-replay',broker.llen('celery')==0 and backend.get(app.backend.get_key_for_task(lost))is None);ident=produce();answer=ask({'action':'run'});validate_meta(ident,'SUCCESS');check('replacement-completes-fresh',answer['first_before']is False and answer['first_after']is True and Logo.objects.filter(url=str(path)).exists());native_stop()
 reset();invalid=native_start({'index_only':True});check('index-only-authority-rejects-logo-assembly',invalid['ready']is False and 'Configuration'in invalid['error']);peer.stdin.close();check('index-only-peer-exit0',peer.wait(timeout=10)==0);peer=None;errorlog.close()
 physical=subprocess.run(['/native-task-peer','logo_task_resources::','--ignored','--nocapture','--test-threads=1'],text=True,capture_output=True,timeout=90);Path('/evidence/physical-private.log').write_text(physical.stdout+physical.stderr);check('actual-task-physical-exit0',physical.returncode==0);check('actual-task-physical5-complete','5 passed; 0 failed'in physical.stdout);report['physical_tests']=5
 check('foreign-expired-head-retained',broker.llen('celery')==1 and queued()[0]['message']['headers']['task']=='celery.backend_cleanup');report['passed']=True
finally:
 if peer and peer.poll()is None:os.killpg(peer.pid,signal.SIGKILL);peer.wait(timeout=5)
 drop_trigger();report['retained_default_queue']=queued();report['remaining_queue_count']=broker.llen('celery');report['descendants_not_deleted_before_owned_project_cleanup']=True
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
