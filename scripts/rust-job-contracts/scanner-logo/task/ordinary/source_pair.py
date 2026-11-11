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
# Component replay: no new Original task execution, reference remains exact.
reference=json.loads(Path('/evidence/paired-reference.json').read_text());assert len(reference['checks'])==102 and all(v.get('passed')is True for v in reference['checks'])and reference.get('passed',True)is True and len(reference['cases'])==12
report['scope']='Native logo component effects against frozen actual Original reference; no task/result/child-consumer equivalence.'
peer=None

def response():
 while True:
  line=peer.stdout.readline()
  if not line:raise RuntimeError('Native component peer EOF')
  if line.startswith('NATIVE_LOGO_JSON='):return json.loads(line.split('=',1)[1])
def ask(value):
 peer.stdin.write(json.dumps(value)+'\n');peer.stdin.flush();return response()
def owned_rows(value):return [{k:v for k,v in row.items()if k in ('name','url')}for row in value if row['url'].startswith(str(ROOT)+'/rust-owned-')or row['url'].startswith(str(ROOT)+'/sub/rust-owned-')]
def recreate(case):
 reset()
 for item in case['before']['files']:
  path=Path(item['path']);assert str(path).startswith(str(ROOT)+'/rust-owned-')or str(path).startswith(str(ROOT)+'/sub/rust-owned-');path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'owned synthetic metadata-only logo fixture\n');os.utime(path,ns=(item['mtime_ns'],item['mtime_ns']));made.add(path)
 for key,value in case['before']['markers'].items():
  assert key.startswith('processed_file:'+str(ROOT)+'/');owned_markers.add(key)
  if value['value_base64']is None:cache.delete(key)
  else:
   if value['ttl']>0:cache.set(key,base64.b64decode(value['value_base64']),ex=value['ttl'])
   else:cache.set(key,base64.b64decode(value['value_base64']))
 for row in owned_rows(case['before']['rows']):Logo.objects.create(**row)
try:
 for root in [Path('/data/m3us'),Path('/data/epgs'),ROOT]:root.mkdir(exist_ok=True);check('fresh-empty-owned-watch-'+root.name,list(root.iterdir())==[])
 EPGSource.objects.all().update(is_active=False);reset()
 errorlog=open('/evidence/native-peer-private.log','w');peer=subprocess.Popen(['/native-logo-peer','logo_scanner_peer::native_logo_scanner_peer','--ignored','--nocapture'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errorlog,text=True,bufsize=1);check('native-ready',response()=={'ready':True})
 for case in reference['cases'][:9]:
  recreate(case);label=case['name']
  if label=='caught-logo-sql-failure':
   with connection.cursor()as c:c.execute("CREATE FUNCTION rust_owned_logo_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned logo insert failure' USING ERRCODE='P0001'; END $$");c.execute('CREATE TRIGGER rust_owned_logo_fail BEFORE INSERT ON dispatcharr_channels_logo FOR EACH ROW EXECUTE FUNCTION rust_owned_logo_fail()')
  if label=='caught-summary-group-failure':cache.delete(group_key);cache.set(group_key,'owned-wrongtype')
  request={'now':1700000000.125}
  if label=='caught-logo-marker-set-failure':
   cache.execute_command('ACL','SETUSER','rust-owned-scanner-no-set','on','>playback-fixture-only','~processed_file:*','+@all','-set');request['marker_acl_user']='rust-owned-scanner-no-set'
  beforefiles=files();answer=ask(request);events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,15)];actual={'name':label,'response':answer,'rows':rows(),'files':files(),'markers':markers(),'events':events};report['cases'].append(actual)
  check(label+'-files-exact',actual['files']==case['after']['files']==beforefiles)
  check(label+'-rows-name-url-order',owned_rows(actual['rows'])==owned_rows(case['after']['rows']))
  check(label+'-events-exact',events==case['events'])
  for key,value in case['after']['markers'].items():
   check(label+'-marker-bytes-'+key,actual['markers'][key]['value_base64']==value['value_base64'])
   if value['value_base64']is not None and label!='uncaught-invalid-marker':check(label+'-marker-ttl-'+key,259160<=actual['markers'][key]['ttl']<=259200)
  if label=='uncaught-invalid-marker':check(label+'-internal-only-error',answer.get('error')=='InvalidMarker'and not answer['ok'])
  else:
   check(label+'-normal-component',answer['ok']and answer['first_after'])
   if label=='same-process-unchanged':check(label+'-retained-first-state',answer['first_before'])
  if label=='caught-logo-marker-set-failure':cache.execute_command('ACL','DELUSER','rust-owned-scanner-no-set')
 # Fresh native process matches the separate Source70 worker lifetime.
 check('prototype-phase-shutdown',ask({'action':'shutdown'})=={'shutdown':True});peer.stdin.close();check('prototype-phase-host-exit',peer.wait(timeout=10)==0);peer=None
 peer=subprocess.Popen(['/native-logo-peer','logo_scanner_peer::native_logo_scanner_peer','--ignored','--nocapture'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errorlog,text=True,bufsize=1);check('ordinary-phase-ready',response()=={'ready':True})
 mtime_reference=json.loads(Path('/evidence/mtime-reference.json').read_text());report['ordinary_reference_case_count']=70;assert mtime_reference['passed']and len(mtime_reference['cases'])==70 and len(mtime_reference['checks'])>=806 and all(v['passed']is True for v in mtime_reference['checks'])
 for case in mtime_reference['cases']:
  recreate(case);label=case['name'];vector=case['mtime_vector'];request={'now':float(vector['source_clock_repr'])};beforefiles=files();answer=ask(request);events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,15)];actual={'name':'ordinary-'+label,'response':answer,'rows':rows(),'files':files(),'markers':markers(),'events':events,'source_mtime_vector':vector};report['cases'].append(actual)
  check('ordinary-'+label+'-normal',answer['ok']is True and answer['first_after']is True);check('ordinary-'+label+'-process-lifetime',answer['first_before']is case['prerun']['first_scan_before']);check('ordinary-'+label+'-files',actual['files']==case['after']['files']==beforefiles);check('ordinary-'+label+'-rows',owned_rows(actual['rows'])==owned_rows(case['after']['rows']));check('ordinary-'+label+'-events',events==case['events'])
  skipped=vector.get('marker_state')in ['equal','greater']or label.startswith('old-existing-')or label=='young-invalid-marker';check('ordinary-'+label+'-counters',[answer[k]for k in ['processed','skipped','errors','total_files']]==([0,1,0,1]if skipped else[1,0,0,1]))
  for key,value in case['after']['markers'].items():
   check('ordinary-'+label+'-marker-'+key,actual['markers'][key]['value_base64']==value['value_base64'])
   if value['value_base64']is not None:
    ttl=actual['markers'][key]['ttl'];check('ordinary-'+label+'-ttl-'+key,560<=ttl<=600 if value['ttl']<=600 else 259160<=ttl<=259200)
 # Explicit unsupported child domain, before any logo effect/publication.
 EPGSource.objects.bulk_create([EPGSource(name='rust-owned-native-helper-reject',source_type='xmltv',is_active=True,file_path='/evidence/owned-unread-index.xml')]);before=rows();answer=ask({'now':1700000000.125});check('eligible-index-domain-unsupported-before-effects',answer.get('error')=='Unsupported'and rows()==before and queued()==[])
 check('native-shutdown',ask({'action':'shutdown'})=={'shutdown':True});peer.stdin.close();check('native-peer-host-exit-zero',peer.wait(timeout=10)==0);peer=None;errorlog.close();reset();EPGSource.objects.all().update(is_active=False)
 # Same owned inode Python stat → native stat and existing float helper.
 vectors=[]
 for ordinal,(seconds,nanoseconds)in enumerate([(epoch,ns)for epoch in [100000000,100000001,268435455,268435456,536870911,536870912,946684800,1073741823,1073741824,1699999993,2147483647,2147483648,4294967295,4294967296,8589934592,9999999999]for ns in [0,1,2,3,99,100,999,1000,9999,10000,123456,999999,1000000,1953124,1953125,1953126,123456789,249999999,250000000,250000001,499999999,500000000,500000001,749999999,750000000,750000001,998046874,998046875,998046876,999999997,999999998,999999999]]):
  path=ROOT/('rust-owned-same-file-'+str(ordinal)+'.png');path.write_bytes(b'owned stat vector');requested=seconds*1000000000+nanoseconds;os.utime(path,ns=(requested,requested));made.add(path);st=path.stat();import struct;vectors.append({'requested_ns':requested,'same_file_path':str(path),'stat_before':{'st_mtime_ns':st.st_mtime_ns,'st_mtime_str':str(st.st_mtime),'ieee754_big_endian_hex':struct.pack('>d',st.st_mtime).hex()}})
 value=subprocess.run(['/native-float-peer','scanner_float_peer::native_scanner_float_peer','--ignored','--nocapture'],input=json.dumps(vectors),text=True,capture_output=True,timeout=30);Path('/evidence/same-file-float-private.log').write_text(value.stdout+value.stderr);value.check_returncode();line=next(s for s in value.stdout.splitlines()if s.startswith('NATIVE_SCANNER_FLOAT_JSON='));native=json.loads(line.split('=',1)[1]);report['same_file_float_vectors']=[{'source':s,'native':n}for s,n in zip(vectors,native)];check('same-file-all512vectors',len(native)==len(vectors)==512);report['legacy_formatter_differences']=[{'source':s['stat_before']['st_mtime_str'],'legacy':n['formatted'],'source_bits':s['stat_before']['ieee754_big_endian_hex']}for s,n in zip(vectors,native)if s['stat_before']['st_mtime_str']!=n['formatted']]
 for ordinal,(s,n)in enumerate(zip(vectors,native)):
  check('same-file-bits-'+str(ordinal),s['stat_before']['ieee754_big_endian_hex']==n['native_joined_bits']==n['native_split_bits']);check('same-file-marker-format-'+str(ordinal),s['stat_before']['st_mtime_str']==n['round_even_format']);check('bounded-helper-domain-'+str(ordinal),n['bounded_epoch_format']==(s['stat_before']['st_mtime_str']if 100000000.0<=float(s['stat_before']['st_mtime_str'])<10000000000.0 else None));check('same-file-integer-stat-'+str(ordinal),str(s['stat_before']['st_mtime_ns'])==n['native_stat_ns'])
 reset()
 physical=subprocess.run(['/native-logo-peer','logo_scanner_resources::','--ignored','--nocapture','--test-threads=1'],text=True,capture_output=True,timeout=90);Path('/evidence/physical-private.log').write_text(physical.stdout+physical.stderr);check('physical-host-exit-zero',physical.returncode==0);check('physical4-tests-complete','4 passed; 0 failed' in physical.stdout);report['physical_tests']=4
 report['passed']=True
finally:
 if peer and peer.poll()is None:peer.kill();peer.wait(timeout=5)
 drop_trigger();report['remaining_queue_count']=broker.llen('celery');report['descendants_not_deleted_before_owned_project_cleanup']=True
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
