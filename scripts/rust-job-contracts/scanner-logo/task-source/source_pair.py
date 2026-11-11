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
PARENT_QUEUE='celery';NOW=1700000000.125
report['scope']='Actual scanner default queue protocol, retained actual SQL block/earlyACK, duplicate execution+oldSUCCESS and hardloss/restart; logo-only inactive-source fixture. No Native claim.'
def publish(ident):
 Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':ident,'now':NOW}));app.tasks[TASK].apply_async(args=[],kwargs={},task_id=ident,headers={'periodic_task_name':'rust-owned-scanner-source-protocol'})
 frame=next((row for row in queued()if row['message']['headers']['id']==ident),{'producer_signal':published[ident],'queue_observation':'already consumed before passive LRANGE'});report.setdefault('publications',[]).append(frame);return frame
def posts(ident):return [v for v in records('postrun')if v['id']==ident]
def done(ident,count=1):
 wait(lambda:len(posts(ident))>=count);post=posts(ident)[count-1];meta=json.loads(backend.get(app.backend.get_key_for_task(ident)));return post,meta
def blocked():
 with connection.cursor()as c:
  c.execute('SELECT pg_stat_clear_snapshot()');c.execute("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%dispatcharr_channels_logo%')");return c.fetchone()[0]
try:
 for root in [Path('/data/m3us'),Path('/data/epgs'),ROOT]:root.mkdir(exist_ok=True);check('fresh-empty-'+root.name,list(root.iterdir())==[])
 EPGSource.objects.all().update(is_active=False);reset();path=make('rust-owned-protocol.png',7.0);first=str(uuid.uuid4());frame=publish(first);check('default-celery-queue',frame['message']['properties']['delivery_info']=={'exchange':'','routing_key':'celery'});check('source-noarg-frame',frame['body'][:2]==[[],{}]and frame['message']['headers']['root_id']==first and frame['message']['headers']['parent_id']is None and frame['message']['headers']['expires']is None)
 with transaction.atomic():
  with connection.cursor()as c:c.execute('LOCK TABLE dispatcharr_channels_logo IN ACCESS EXCLUSIVE MODE')
  worker=start();wait(blocked);before=next(v for v in records('prerun')if v['id']==first);check('actual-earlyACK-before-SQL-reply',broker.llen('celery')==broker.hlen('unacked')==broker.zcard('unacked_index')==0);check('actual-pending-while-body-blocked',backend.get(app.backend.get_key_for_task(first))is None and app.AsyncResult(first).state=='PENDING'and app.AsyncResult(first).ready()is False);check('no-postrun-before-SQL-release',posts(first)==[]);report['held_sql']={'prerun':before,'queue_count':broker.llen('celery'),'unacked_count':broker.hlen('unacked'),'backend_bytes':None,'state':'PENDING'}
 post,meta=done(first);check('initial-body-normal',post['state']=='SUCCESS'and meta['result']is None and Logo.objects.filter(url=str(path)).exists());check('task-attributes',before['task_attributes']['acks_late']is False and before['task_attributes']['ignore_result']is False and before['task_attributes']['track_started']is False);check('periodic-header-actual-handler',before['request_headers']=={'periodic_task_name':'rust-owned-scanner-source-protocol'});raw=backend.get(app.backend.get_key_for_task(first));backend.expire(app.backend.get_key_for_task(first),120);Logo.objects.filter(url=str(path)).delete();cache.delete('processed_file:'+str(path));cache.delete(channel_key)
 publish(first);post,meta=done(first,2);check('same-ID-body-reexecutes',post['state']=='SUCCESS'and Logo.objects.filter(url=str(path)).exists()and len([v for v in records('prerun')if v['id']==first])==2);check('same-ID-oldSUCCESS-bytes-TTL',backend.get(app.backend.get_key_for_task(first))==raw and 110<=backend.ttl(app.backend.get_key_for_task(first))<=120)
 Logo.objects.filter(url=str(path)).delete();cache.set('processed_file:'+str(path),'owned-invalid-float');cache.delete(channel_key);publish(first);post,meta=done(first,3);check('same-ID-failed-body-preserves-oldSUCCESS',post['state']=='FAILURE'and meta['status']=='SUCCESS'and backend.get(app.backend.get_key_for_task(first))==raw and 110<=backend.ttl(app.backend.get_key_for_task(first))<=120);check('same-ID-client-still-ready-null',app.AsyncResult(first).ready()is True and app.AsyncResult(first).get(timeout=1)is None);report['duplicates']={'task_id':first,'actual_postruns':posts(first),'preserved_metadata':meta,'raw_sha256':hashlib.sha256(raw).hexdigest(),'ttl':backend.ttl(app.backend.get_key_for_task(first))}
 reset();path=make('rust-owned-hardloss.png',7.0);lost=str(uuid.uuid4())
 with transaction.atomic():
  with connection.cursor()as c:c.execute('LOCK TABLE dispatcharr_channels_logo IN ACCESS EXCLUSIVE MODE')
  publish(lost);wait(blocked);check('hardloss-task-earlyACK-PENDING',broker.llen('celery')==broker.hlen('unacked')==0 and backend.get(app.backend.get_key_for_task(lost))is None);os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=10);worker=None
 check('hardloss-no-row-marker-result',not Logo.objects.filter(url=str(path)).exists()and cache.get('processed_file:'+str(path))is None and backend.get(app.backend.get_key_for_task(lost))is None and posts(lost)==[]);worker=start();time.sleep(1);check('replacement-no-bounded-replay',broker.llen('celery')==broker.hlen('unacked')==0 and backend.get(app.backend.get_key_for_task(lost))is None and posts(lost)==[])
 fresh=str(uuid.uuid4());publish(fresh);post,meta=done(fresh);prerun=next(v for v in records('prerun')if v['id']==fresh);check('replacement-completes-fresh-task',post['state']=='SUCCESS'and meta['result']is None and Logo.objects.filter(url=str(path)).exists()and cache.get('processed_file:'+str(path))is not None);check('replacement-resets-process-first-state',prerun['first_scan_before']is False and post['first_scan_after']is True);check('lost-still-PENDING-after-replacement',backend.get(app.backend.get_key_for_task(lost))is None and app.AsyncResult(lost).ready()is False);report['hardloss']={'lost_task_id':lost,'fresh_task_id':fresh,'bounded_replay_observation_seconds':1,'lost_state':'PENDING','fresh_metadata':meta,'fresh_prerun':prerun}
 check('only-five-owned-task-bodies',len(records('prerun'))==5 and len(records('postrun'))==4 and all(v['task']==TASK for v in records('prerun')));check('no-unrelated-or-descendant-publications',queued()==[]and records('children')==[]);check('source-data-files-unchanged',path.read_bytes()==b'owned synthetic metadata-only logo fixture\n');report['passed']=True
finally:
 stop();drop_trigger();report['remaining_queue_count']=broker.llen('celery');report['result_key_count']=backend.dbsize();report['descendants_not_deleted_before_owned_project_cleanup']=True
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
