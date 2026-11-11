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
try:
 for root in [Path('/data/m3us'),Path('/data/epgs'),ROOT]:root.mkdir(exist_ok=True);check('fresh-empty-owned-watch-'+root.name,list(root.iterdir())==[])
 EPGSource.objects.all().update(is_active=False)
 for label,obj in [('scanner',source.scan_and_process_files.run),('first-index-helper',source._rebuild_programme_indices),('summary-helper',send_websocket_update),('logo-save',Logo.save)]:Path('/evidence/original-'+label+'.py').write_text(inspect.getsource(obj))
 reset();worker=start()
 a=make('rust-owned-a.PNG',7.125);b=make('sub/rust-owned-café-⏰.SVG',8.0);make('rust-owned-ignore.txt',9.0)
 case=execute('positive-nested-unicode',expected_counts=(2,1,0,3));check('positive-exact-logo-names',Logo.objects.get(url=str(a)).name==a.stem and Logo.objects.get(url=str(b)).name==b.stem);check('positive-exact-marker-bytes',all(cache.get('processed_file:'+str(p))==repr(p.stat().st_mtime).encode()for p in[a,b]));check('positive-marker-ttl3days',all(259160<=cache.ttl('processed_file:'+str(p))<=259200 for p in[a,b]));check('first-state-transition',case['prerun']['first_scan_before']is False and case['postrun']['first_scan_after']is True)
 cache.delete(channel_key);case=execute('same-process-unchanged',expected_counts=(0,3,0,3));check('unchanged-no-repeat-first-query',not any('epgsourceindex'in q['sql']for q in case['sql']['queries'])and case['prerun']['first_scan_before']is True)
 reset();p=make('rust-owned-young.png',5.875);case=execute('young-file',expected_counts=(0,1,0,1));check('young-no-row-or-marker',not Logo.objects.filter(url=str(p)).exists()and cache.get('processed_file:'+str(p))is None)
 reset();p=make('rust-owned-exact-six.png',6.0);case=execute('exact-age-six',expected_counts=(1,0,0,1));check('age-six-processed',Logo.objects.filter(url=str(p)).exists()and cache.get('processed_file:'+str(p))==repr(p.stat().st_mtime).encode())
 reset();p=make('rust-owned-old-existing.png',31.0);Logo.objects.create(url=str(p),name='Owned custom display');case=execute('startup-old-existing',expected_counts=(0,1,0,1));check('old-existing-marker-but-name-preserved',Logo.objects.get(url=str(p)).name=='Owned custom display'and cache.get('processed_file:'+str(p))==repr(p.stat().st_mtime).encode())
 reset();p=make('rust-owned-sql-failure.png',7.0)
 with connection.cursor()as c:c.execute("CREATE FUNCTION rust_owned_logo_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned logo insert failure' USING ERRCODE='P0001'; END $$");c.execute('CREATE TRIGGER rust_owned_logo_fail BEFORE INSERT ON dispatcharr_channels_logo FOR EACH ROW EXECUTE FUNCTION rust_owned_logo_fail()')
 case=execute('caught-logo-sql-failure',expected_counts=(0,0,1,1));check('caught-sql-no-row-no-marker',not Logo.objects.filter(url=str(p)).exists()and cache.get('processed_file:'+str(p))is None)
 reset();p=make('rust-owned-marker-failure.png',7.0);acl='rust-owned-scanner-no-set';cache.execute_command('ACL','SETUSER',acl,'on','>playback-fixture-only','~processed_file:*','+@all','-set');case=execute('caught-logo-marker-set-failure',expected_counts=(0,0,1,1),acl=acl);check('marker-failure-committed-logo',Logo.objects.filter(url=str(p)).exists()and cache.get('processed_file:'+str(p))is None);cache.execute_command('ACL','DELUSER',acl)
 reset();p=make('rust-owned-group-failure.png',7.0);cache.delete(group_key);cache.set(group_key,'owned-wrongtype');case=execute('caught-summary-group-failure',expected_counts=None);check('group-failure-normal-with-committed-effects',case['events']==[]and Logo.objects.filter(url=str(p)).exists()and cache.get('processed_file:'+str(p))is not None)
 reset();p=make('rust-owned-invalid-marker.png',7.0);cache.set('processed_file:'+str(p),'owned-invalid-float');case=execute('uncaught-invalid-marker',expected_failure=True);check('invalid-marker-real-valueerror',case['metadata']['result']['exc_module']=='builtins'and case['metadata']['result']['exc_type']=='ValueError'and not Logo.objects.filter(url=str(p)).exists()and case['events']==[])
 # Separately observe actual first-scan Index publications. Parent worker only
 # consumes the explicit parent queue; default-routed children remain untouched.
 stop();reset();source_rows=[]
 for n,kind,active in [('missing','xmltv',True),('sqlnull','xmltv',True),('jsonnull','xmltv',True),('empty','xmltv',True),('dummy','dummy',True),('sd','schedules_direct',True),('inactive','xmltv',False)]:
  EPGSource.objects.bulk_create([EPGSource(name='rust-owned-scan-index-'+n,source_type=kind,is_active=active,file_path='/evidence/owned-index-'+n+'.xml')]);source_rows.append(EPGSource.objects.get(name='rust-owned-scan-index-'+n))
 for row in source_rows[1:4]:EPGSourceIndex.objects.bulk_create([EPGSourceIndex(source=row,data={})])
 with connection.cursor()as c:c.execute('UPDATE epg_epgsourceindex SET data=NULL WHERE source_id=%s',[source_rows[1].pk]);c.execute("UPDATE epg_epgsourceindex SET data='null'::jsonb WHERE source_id=%s",[source_rows[2].pk])
 report['index_source_ids']={n.pk:n.name for n in source_rows};worker=start();first=execute('first-scan-index-lineage',expected_counts=(0,0,0,0));expected={source_rows[0].pk,source_rows[1].pk};observed=[v['body'][0][0]for v in first['children']];check('first-index-only-missing-and-sqlnull',set(observed)==expected and len(observed)==2);check('first-index-real-routing-root-parent',all(v['task']=='apps.epg.tasks.build_programme_index_task'and v['routing_key']=='celery'and v['headers']['root_id']==first['parent_id']and v['headers']['parent_id']==first['parent_id']and v['body'][1]=={}for v in first['children']));check('first-index-backend-children-recorded',len(first['metadata']['children'])==2)
 before=[v['sha256']for v in first['after']['queue']];second=execute('second-scan-retains-children',expected_counts=(0,0,0,0));check('second-no-new-child-or-deletion',second['children']==[]and[v['sha256']for v in second['after']['queue']]==before)
 stop();worker=start();third=execute('worker-restart-republishes-missing-index',expected_counts=(0,0,0,0));check('restart-actual-duplicate-source-publications',len(third['children'])==2 and set(v['body'][0][0]for v in third['children'])==expected and all(v in[v['sha256']for v in third['after']['queue']]for v in before));check('restart-preserves-all-four-descendants',len(third['after']['queue'])==len(first['before']['queue'])+4)
 report['passed']=True
finally:
 stop();drop_trigger();report['retained_default_queue']=queued();report['remaining_queue_count']=broker.llen('celery');report['result_key_count']=backend.dbsize();report['descendants_not_deleted_before_owned_project_cleanup']=True
 Path('/evidence/source-native-default-pair-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
