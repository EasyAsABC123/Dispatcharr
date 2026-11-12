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
PARENT_QUEUE='celery';peer=None;errorlog=None
reference=json.loads(Path('/evidence/task-reference.json').read_text());assert reference['passed']and len(reference['checks'])==85
report['scope']='Actual Source producer→helper-enabled normal native scanner→parent/children Original AsyncResult, joint real IndexApplication drains FIFO; no Main/M3U/EPG/large-native domain.'
def response():
 while True:
  line=peer.stdout.readline()
  if not line:raise RuntimeError('Native helper EOF')
  if line.startswith('NATIVE_SCANNER_INDEX_JSON='):return json.loads(line.split('=',1)[1])
def send(value):peer.stdin.write(json.dumps(value)+'\n');peer.stdin.flush()
def ask(value):send(value);return response()
def native_start():
 global peer,errorlog
 errorlog=open('/evidence/native-index-scanner-private.log','a');peer=subprocess.Popen(['/native-task-peer','scanner_index_peer::native_scanner_index_peer','--ignored','--nocapture'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errorlog,text=True,bufsize=1,start_new_session=True);send({'now':NOW,'spooled':True});return response()
def native_stop():
 global peer,errorlog
 if peer is None:return
 check('family-and-Index-transport-drain',ask({'action':'shutdown'})['drained']is True);peer.stdin.close();check('native-process-exit0',peer.wait(timeout=10)==0);peer=None;errorlog.close()
def produce():
 ident=str(uuid.uuid4());app.tasks[TASK].apply_async(args=[],kwargs={},task_id=ident,headers={'periodic_task_name':'scan-files'});return ident
def meta(ident):
 wait(lambda:backend.get(app.backend.get_key_for_task(ident)));value=json.loads(backend.get(app.backend.get_key_for_task(ident)));check(ident+'-SourceAsyncResult-null',app.AsyncResult(ident).ready()and app.AsyncResult(ident).get(timeout=2)is None);check(ident+'-metadata',value['status']=='SUCCESS'and value['result']is None and value['traceback']is None and value['task_id']==ident);check(ident+'-TTL',3580<=backend.ttl(app.backend.get_key_for_task(ident))<=3600);return value
try:
 for root in [Path('/data/m3us'),Path('/data/epgs'),ROOT]:root.mkdir(exist_ok=True);check('empty-owned-'+root.name,not list(root.iterdir()))
 EPGSource.objects.all().update(is_active=False);reset();specs=[('missing-z','xmltv',True),('sql-null-a','xmltv',True),('dict-m','xmltv',True),('json-null-b','xmltv',True),('inactive-x','xmltv',False),('dummy-d','dummy',True),('schedules-s','schedules_direct',True),('missing-y','xmltv',True)];sources=EPGSource.objects.bulk_create([EPGSource(name='rust-owned-helper-'+n,source_type=t,is_active=a)for n,t,a in specs]);EPGSourceIndex.objects.bulk_create([EPGSourceIndex(source=sources[i],data={})for i in [1,2,3]])
 with connection.cursor()as c:c.execute('UPDATE epg_epgsourceindex SET data=NULL WHERE source_id=%s',[sources[1].pk]);c.execute("UPDATE epg_epgsourceindex SET data='null'::jsonb WHERE source_id=%s",[sources[3].pk])
 by_id={s.pk:s.name for s in sources};gold_by_id={s['id']:s['name']for s in reference['helper_reference']['seed_order']};expected=[gold_by_id[i]for i in reference['helper_reference']['predeclared_queryset_result_order']]
 ready=native_start();check('prepared-no-intake',ready['state']=='Prepared'and ready['first']is False);one=produce();before=queued();time.sleep(.1);check('prepared-retains-frame',queued()==before);ask({'action':'start'});first=meta(one);frames=list(reversed(queued()));check('all-three-children-retained',len(frames)==3);check('Source-observed-SQL-iteration-exact',[by_id[v['body'][0][0]]for v in frames]==expected);ids=[v['message']['headers']['id']for v in frames];check('parent-metadata-successful-children-order',[v[0][0]for v in first['children']]==ids)
 for value in frames:
  headers=value['message']['headers'];check(headers['id']+'-typed-Source-lineage',headers['task']=='apps.epg.tasks.build_programme_index_task'and headers['root_id']==headers['parent_id']==one and headers['eta']is None and value['body'][1]=={}and value['message']['properties']['delivery_info']=={'exchange':'','routing_key':'celery'})
 check('first-flag-after-durable-publications',ask({'action':'state'})['first']is True)
 # A second parent must wait behind the real children; never delete/quarantine them.
 two=produce();time.sleep(.15);check('strict-FIFO-real-children-block-next-parent',backend.get(app.backend.get_key_for_task(two))is None and broker.llen('celery')==4);ask({'action':'start_index'});second=meta(two)
 for ident in ids:
  child=meta(ident);check(ident+'-Index-parent-field',child.get('parent_id')==one)
 wait(lambda:broker.llen('celery')==broker.hlen('unacked')==0);check('second-scan-no-repeated-helper',second['children']==[]);check('joint-index-no-operational-error',ask({'action':'state'})['index_error']=='None');native_stop()
 # Restart lifetime permits a new first helper; missing files left no index.
 native_start();three=produce();ask({'action':'start'});third=meta(three);restart=list(reversed(queued()));check('restart-republishes-all-eligible',[by_id[v['body'][0][0]]for v in restart]==expected and len(third['children'])==3);ask({'action':'start_index'});
 for value in restart:meta(value['message']['headers']['id'])
 wait(lambda:broker.llen('celery')==0);native_stop()
 # Actual physically replied helper SELECT P0001 is caught only after barrier+close.
 with connection.cursor()as c:c.execute("CREATE FUNCTION rust_owned_helper_select_fail() RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$BEGIN RAISE EXCEPTION 'rust-owned helper SELECT failure' USING ERRCODE='P0001'; END$$");c.execute('ALTER TABLE epg_epgsourceindex RENAME TO epg_epgsourceindex_rust_owned');c.execute('CREATE VIEW epg_epgsourceindex AS SELECT * FROM epg_epgsourceindex_rust_owned WHERE rust_owned_helper_select_fail()')
 native_start();four=produce();ask({'action':'start'});fault=meta(four);check('Source-caught-helper-server-error-null',fault['children']==[]and ask({'action':'state'})['first']is True);native_stop()
 with connection.cursor()as c:c.execute('DROP VIEW epg_epgsourceindex');c.execute('ALTER TABLE epg_epgsourceindex_rust_owned RENAME TO epg_epgsourceindex');c.execute('DROP FUNCTION rust_owned_helper_select_fail()')
 # Explicit native resource cap, not Source's unbounded helper claim.
 extra=EPGSource.objects.bulk_create([EPGSource(name='rust-owned-helper-cap-%02d'%i,source_type='xmltv',is_active=True)for i in range(33)]);native_start();capped=produce();raw_before=queued();ask({'action':'start'});wait(lambda:ask({'action':'state'})['state']=='UnsupportedDomain');check('bounded-helper-cap-refuses-beforeACK',queued()==raw_before and broker.hlen('unacked')==0 and backend.get(app.backend.get_key_for_task(capped))is None and ask({'action':'state'})['first']is False);native_stop();EPGSource.objects.filter(pk__in=[v.pk for v in extra]).update(is_active=False);native_start();ask({'action':'start'});repaired=meta(capped);check('same-cap-message-after-owned-repair',len(repaired['children'])==3);cap_children=list(reversed(queued()));ask({'action':'start_index'});
 for value in cap_children:meta(value['message']['headers']['id'])
 wait(lambda:broker.llen('celery')==0);native_stop();report['cap_control']={'admission_limit':32,'input_eligible':36,'before_raw_queue':raw_before,'repaired_parent':repaired,'no_Source_unbounded_parity_claim':True}
 partial=produce();physical=subprocess.run(['/native-task-peer','scanner_index_helper::resources::','--ignored','--nocapture','--test-threads=1'],text=True,capture_output=True,timeout=90,env=dict(os.environ,HELPER_PARTIAL_TASK_ID=partial));Path('/evidence/physical-private.log').write_text(physical.stdout+physical.stderr);check('physical-controls-exit0',physical.returncode==0);check('all-four-physical-controls-passed','4 passed; 0 failed'in physical.stdout)
 partial_meta=meta(partial);check('actual-Source-consumer-partial-child-null',len(partial_meta['children'])==1)
 report['cases']=[{'name':'first','metadata':first,'children':frames},{'name':'second-after-real-Index-drain','metadata':second},{'name':'restart','metadata':third,'children':restart},{'name':'caught-helper-server-P0001','metadata':fault}];report['physical_tests']=4;report['passed']=True;report['native_verified']=True
finally:
 if peer:
  try:native_stop()
  except Exception:
   if peer and peer.poll()is None:os.killpg(peer.pid,signal.SIGKILL);peer.wait(timeout=5)
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks'])}))
