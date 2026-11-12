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
report={'predeclared_cases':['ordered-first-index','same-process-no-repeat','restart-repeat','second-index-LPUSH-denied','same-process-after-caught-publication-failure','helper-server-P0001','same-process-after-caught-SQL-failure','259-files-walk','259-files-one-caught-SQL-error'],'passed':False,'native_verified':False,'checks':[],'cases':[],'scope':'Actual unchanged Original scanner logo rows/markers/events/partial errors; separately observed unconsumed Index children with real default routing. Parent queue is an explicit fixture control. No child execution or native/whole-family claim.','fixed_scanner_clock':NOW};worker=None;made=set();owned_markers=set();published={}
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
 Logo.objects.filter(url__startswith=str(ROOT)+'/rust-owned-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub/rust-owned-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub1/rust-owned-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub2/rust-owned-').delete()
 cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel)
def make(name,age):
 p=ROOT/name;p.parent.mkdir(parents=True,exist_ok=True);assert not p.exists();p.write_bytes(b'owned synthetic metadata-only logo fixture\n');os.utime(p,(NOW-age,NOW-age));made.add(p);owned_markers.add('processed_file:'+str(p));return p
def execute(label,expected_counts=None,acl=None,expected_failure=False,extra=None):
 before={'rows':rows(),'files':files(),'markers':markers(),'queue':queued()};taskid=str(uuid.uuid4());Path('/evidence/owned-scanner-config.json').write_text(json.dumps({'task_id':taskid,'now':NOW,**({'marker_acl_user':acl}if acl else{}),**(extra or{})}));result=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=taskid,queue=PARENT_QUEUE)
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
def helper_query():
 return EPGSource.objects.filter(is_active=True).exclude(source_type__in=('dummy','schedules_direct')).filter(Q(index_record__isnull=True)|Q(index_record__data__isnull=True))
def IDs(case):return [v['body'][0][0]for v in case['children']]
def restore_index_relation():
 with connection.cursor()as c:
  c.execute("SELECT to_regclass('epg_epgsourceindex_rust_owned')")
  if c.fetchone()[0]:c.execute('DROP VIEW IF EXISTS epg_epgsourceindex');c.execute('ALTER TABLE epg_epgsourceindex_rust_owned RENAME TO epg_epgsourceindex')
  c.execute('DROP FUNCTION IF EXISTS rust_owned_helper_select_fail()')
try:
 for root in [Path('/data/m3us'),Path('/data/epgs'),ROOT]:root.mkdir(exist_ok=True);check('fresh-empty-owned-watch-'+root.name,list(root.iterdir())==[])
 EPGSource.objects.all().update(is_active=False)
 for label,obj in [('scanner',source.scan_and_process_files.run),('first-index-helper',source._rebuild_programme_indices),('summary-helper',send_websocket_update),('logo-save',Logo.save)]:Path('/evidence/original-'+label+'.py').write_text(inspect.getsource(obj))
 specs=[('missing-z','xmltv',True),('sql-null-a','xmltv',True),('dict-m','xmltv',True),('json-null-b','xmltv',True),('inactive-x','xmltv',False),('dummy-d','dummy',True),('schedules-s','schedules_direct',True),('missing-y','xmltv',True)]
 sources=EPGSource.objects.bulk_create([EPGSource(name='rust-owned-helper-'+n,source_type=t,is_active=a)for n,t,a in specs]);EPGSourceIndex.objects.bulk_create([EPGSourceIndex(source=sources[i],data={})for i in [1,2,3]])
 with connection.cursor()as c:c.execute('UPDATE epg_epgsourceindex SET data=NULL WHERE source_id=%s',[sources[1].pk]);c.execute("UPDATE epg_epgsourceindex SET data='null'::jsonb WHERE source_id=%s",[sources[3].pk])
 expected=list(helper_query().values_list('id',flat=True));report['helper_reference']={'seed_order':[{'id':s.pk,'name':s.name,'active':s.is_active,'type':s.source_type}for s in sources],'predeclared_queryset_result_order':expected,'query':str(helper_query().query),'ordering_contract':'Source query has no ORDER BY; retain observed original SQL iteration, no sort or global insertion-order guarantee'};check('eligible-missing-sqlnull-only',set(expected)=={sources[i].pk for i in [0,1,7]})
 reset();worker=start();one=execute('ordered-first-index',expected_counts=(0,0,0,0));check('exact-actual-helper-SQL-order',IDs(one)==expected);check('all-root-parent-and-default-route',all(v['headers']['root_id']==v['headers']['parent_id']==one['parent_id']and v['routing_key']=='celery'and v['body'][1]=={}for v in one['children']));check('parent-result-has-all-successful-children',len(one['metadata']['children'])==3)
 hashes=[v['sha256']for v in one['after']['queue']];two=execute('same-process-no-repeat',expected_counts=(0,0,0,0));check('no-repeat-no-child-removal',IDs(two)==[]and[v['sha256']for v in two['after']['queue']]==hashes)
 stop();worker=start();three=execute('restart-repeat',expected_counts=(0,0,0,0));check('restart-duplicates-with-actual-lineage',IDs(three)==expected and len(three['after']['queue'])==6 and all(v in[v['sha256']for v in three['after']['queue']]for v in hashes))
 stop();worker=start();partial=execute('second-index-LPUSH-denied',expected_counts=(0,0,0,0),extra={'deny_index_publish_number':2});check('actual-second-publication-attempt',IDs(partial)==expected[:2]);check('only-first-child-durable',len(partial['metadata']['children'])==1 and len(partial['after']['queue'])==len(partial['before']['queue'])+1);check('caught-publish-first-state-completed',partial['postrun']['first_scan_after']is True);again=execute('same-process-after-caught-publication-failure',expected_counts=(0,0,0,0));check('caught-publish-not-retried-in-same-process',again['children']==[]and again['after']['queue']==again['before']['queue'])
 stop()
 with connection.cursor()as c:
  c.execute("CREATE FUNCTION rust_owned_helper_select_fail() RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$BEGIN RAISE EXCEPTION 'rust-owned helper SELECT failure' USING ERRCODE='P0001'; END$$")
  c.execute('ALTER TABLE epg_epgsourceindex RENAME TO epg_epgsourceindex_rust_owned');c.execute('CREATE VIEW epg_epgsourceindex AS SELECT * FROM epg_epgsourceindex_rust_owned WHERE rust_owned_helper_select_fail()')
 worker=start();sql=execute('helper-server-P0001',expected_counts=(0,0,0,0));check('server-query-error-physically-observed',any(q.get('error_class')and 'epgsourceindex'in q['sql']for q in sql['sql']['queries']));check('helper-server-caught-normal-first-completed',sql['postrun']['first_scan_after']is True and sql['children']==[]);again=execute('same-process-after-caught-SQL-failure',expected_counts=(0,0,0,0));check('caught-SQL-not-retried',not any('epgsourceindex'in q['sql']for q in again['sql']['queries'])and again['after']['queue']==again['before']['queue']);stop();restore_index_relation()
 EPGSource.objects.all().update(is_active=False);reset()
 # Predeclared >128 domain: 257 supported files, two ignored extensions, four dirs.
 for i in range(257):make(('sub%d/'%(i%3)if i%3 else'')+'rust-owned-large-%03d.png'%i,7.125)
 make('rust-owned-large-ignore.txt',7.125);make('sub1/rust-owned-large-ignore.dat',7.125)
 walk=[str(Path(r)/n)for r,dirs,names in os.walk(ROOT)for n in names];report['large_pre_execution_walk']=walk;check('large-predeclared-count',len(walk)==259)
 worker=start();large=execute('259-files-walk',expected_counts=(257,2,0,259));check('large-257-exact-rows-and-markers',all(Logo.objects.filter(url=str(p),name=p.stem).exists()and cache.get('processed_file:'+str(p))==repr(p.stat().st_mtime).encode()for p in made if p.suffix=='.png'));check('large-source-not-128-capped',len(large['after']['rows'])>=257)
 reset()
 for i in range(257):make(('sub%d/'%(i%3)if i%3 else'')+'rust-owned-large-%03d.png'%i,7.125)
 make('rust-owned-large-ignore.txt',7.125);make('sub1/rust-owned-large-ignore.dat',7.125)
 with connection.cursor()as c:c.execute("CREATE FUNCTION rust_owned_logo_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.name='rust-owned-large-128' THEN RAISE EXCEPTION 'rust-owned per-file large failure' USING ERRCODE='P0001'; END IF; RETURN NEW; END$$");c.execute('CREATE TRIGGER rust_owned_logo_fail BEFORE INSERT ON dispatcharr_channels_logo FOR EACH ROW EXECUTE FUNCTION rust_owned_logo_fail()')
 caught=execute('259-files-one-caught-SQL-error',expected_counts=(256,2,1,259));bad=next(p for p in made if p.stem=='rust-owned-large-128');check('large-one-error-no-row-or-marker',not Logo.objects.filter(url=str(bad)).exists()and cache.get('processed_file:'+str(bad))is None);check('large-later-files-continue',sum(Logo.objects.filter(url=str(p)).exists()for p in made if p.suffix=='.png')==256);check('all-owned-descendants-retained',len(caught['after']['queue'])==7)
 report['passed']=True
finally:
 stop();cache.execute_command('ACL','SETUSER','default','+lpush');restore_index_relation();drop_trigger()
 Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks'])}))
