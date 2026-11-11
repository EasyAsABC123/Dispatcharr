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
# UNEXECUTED vector extension. Use actual os.utime(ns), not float setup.
import math,struct
VECTORS=[(946684800,1),(1699999993,0),(1699999993,1),(1699999993,123456789),(1699999993,500000001),(1699999993,999999999),(2147483647,999999999),(4102444800,123456789)]
report['scope']='Actual Source finite file-stat/mtime marker extension only when executed; no native or general float claim.'
report['vector_plan']={'epoch_seconds_and_nanoseconds':VECTORS,'marker_states':['absent','empty','equal','greater','less'],'extra':['old-existing-absent','old-existing-empty','young-invalid-marker'],'case_count':70,'tie_effects':{'epochs':[100000000,100000001,268435455],'nanoseconds':[1953124,1953125,1953126],'marker_states':['absent','equal','greater']}}
def stat_fields(path):
 st=path.stat();value=st.st_mtime
 return {'st_mtime_ns':st.st_mtime_ns,'st_mtime_repr':repr(value),'st_mtime_str':str(value),'st_mtime_hex':value.hex(),'ieee754_big_endian_hex':struct.pack('>d',value).hex(),'finite':math.isfinite(value),'mtime_ns_from_float_rounded':round(value*1000000000)}
def exact_file(label,seconds,nanoseconds):
 path=make(label+'.png',7.0);requested=seconds*1000000000+nanoseconds;os.utime(path,ns=(requested,requested));return path,requested
try:
 for directory in [Path('/data/m3us'),Path('/data/epgs'),ROOT]:directory.mkdir(exist_ok=True);check('fresh-empty-owned-watch-'+directory.name,list(directory.iterdir())==[])
 EPGSource.objects.all().update(is_active=False);reset();worker=start()
 for ordinal,(seconds,nanoseconds) in enumerate(VECTORS):
  for marker_state in ['absent','empty','equal','greater','less']:
   reset();path,requested=exact_file('rust-owned-mtime-'+str(ordinal)+'-'+marker_state,seconds,nanoseconds);stat=stat_fields(path);value=path.stat().st_mtime;NOW=value+7.0;key='processed_file:'+str(path)
   check('vector-finite-'+str(ordinal)+'-'+marker_state,stat['finite'])
   # Record filesystem precision honestly. Do NOT assume requested ns survives.
   marker={'empty':'','equal':repr(value),'greater':repr(math.nextafter(value,math.inf)),'less':repr(math.nextafter(value,-math.inf))}.get(marker_state)
   if marker is not None:cache.set(key,marker,ex=600)
   skipped=marker_state in ['equal','greater'];case=execute('mtime-'+str(ordinal)+'-'+marker_state,expected_counts=(0,1,0,1)if skipped else(1,0,0,1));case['mtime_vector']={'requested_ns':requested,'stat_before':stat,'stat_after':stat_fields(path),'marker_state':marker_state,'input_marker_ascii':marker,'source_age':NOW-value,'source_clock_repr':repr(NOW),'source_written_marker_ascii':str(value)}
   check(case['name']+'-raw-marker-bytes',cache.get(key)==(marker if skipped else str(value)).encode('ascii'))
   check(case['name']+'-row',Logo.objects.filter(url=str(path)).exists()==(not skipped))
   check(case['name']+'-marker-ttl',560<=cache.ttl(key)<=600 if skipped else 259160<=cache.ttl(key)<=259200)
   check(case['name']+'-stat-stable',case['mtime_vector']['stat_before']==case['mtime_vector']['stat_after'])
 for marker_state in ['absent','empty']:
  reset();path,requested=exact_file('rust-owned-mtime-old-'+marker_state,1699999993,123456789);stat=stat_fields(path);NOW=path.stat().st_mtime+31.0;key='processed_file:'+str(path);Logo.objects.create(url=str(path),name='Owned mtime custom name')
  if marker_state=='empty':cache.set(key,'',ex=600)
  case=execute('old-existing-'+marker_state,expected_counts=(0,1,0,1));case['mtime_vector']={'requested_ns':requested,'stat_before':stat,'stat_after':stat_fields(path),'marker_state':marker_state,'source_clock_repr':repr(NOW)};check(case['name']+'-marker-and-name',cache.get(key)==str(path.stat().st_mtime).encode('ascii')and Logo.objects.get(url=str(path)).name=='Owned mtime custom name')
 reset();path,requested=exact_file('rust-owned-mtime-young-invalid',1699999993,500000001);stat=stat_fields(path);NOW=path.stat().st_mtime+5.875;key='processed_file:'+str(path);cache.set(key,'owned-invalid-float',ex=600);case=execute('young-invalid-marker',expected_counts=(0,1,0,1));case['mtime_vector']={'requested_ns':requested,'stat_before':stat,'stat_after':stat_fields(path),'source_clock_repr':repr(NOW)};check('young-before-float-parse',cache.get(key)==b'owned-invalid-float'and not Logo.objects.filter(url=str(path)).exists())
 for ordinal,(seconds,nanoseconds)in enumerate([(e,n)for e in [100000000,100000001,268435455]for n in [1953124,1953125,1953126]]):
  for marker_state in ['absent','equal','greater']:
   reset();path,requested=exact_file('rust-owned-decimal-tie-'+str(ordinal)+'-'+marker_state,seconds,nanoseconds);stat=stat_fields(path);value=path.stat().st_mtime;NOW=value+7.0;key='processed_file:'+str(path);marker=None if marker_state=='absent'else repr(value if marker_state=='equal'else math.nextafter(value,math.inf))
   if marker is not None:cache.set(key,marker,ex=600)
   skipped=marker_state!='absent';case=execute('decimal-tie-'+str(ordinal)+'-'+marker_state,expected_counts=(0,1,0,1)if skipped else(1,0,0,1));case['mtime_vector']={'requested_ns':requested,'stat_before':stat,'stat_after':stat_fields(path),'marker_state':marker_state,'input_marker_ascii':marker,'source_age':NOW-value,'source_clock_repr':repr(NOW),'source_written_marker_ascii':str(value)}
   check(case['name']+'-raw-marker-bytes',cache.get(key)==(marker if skipped else str(value)).encode('ascii'));check(case['name']+'-row',Logo.objects.filter(url=str(path)).exists()==(not skipped));check(case['name']+'-marker-ttl',560<=cache.ttl(key)<=600 if skipped else 259160<=cache.ttl(key)<=259200);check(case['name']+'-stat-stable',case['mtime_vector']['stat_before']==case['mtime_vector']['stat_after'])
 check('all70vectors-recorded',len(report['cases'])==70);check('no-descendant-publications',queued()==[]and all(c['children']==[]for c in report['cases']));report['passed']=True
finally:
 stop();drop_trigger();report['retained_default_queue']=queued();report['remaining_queue_count']=broker.llen('celery');report['result_key_count']=backend.dbsize();report['descendants_not_deleted_before_owned_project_cleanup']=True
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
