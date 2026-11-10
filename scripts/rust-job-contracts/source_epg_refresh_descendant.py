"""Actual Original refresh busy parent to actual ETA child and imported grandchildren.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
An explicit fixture router diverts children into an owned unconsumed queue.
"""
import base64,datetime as dt,hashlib,json,os,signal,subprocess,sys,time
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_')or len(database)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in database):raise RuntimeError('Copied database required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from dispatcharr.celery import app
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGSourceIndex,EPGData,ProgramData
from apps.channels.models import Channel
TASK=tasks.refresh_epg_data;CHILD_QUEUE='owned-refresh-children'
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned broker/result namespaces required')
root=Path('/evidence/refresh-import-files');root.mkdir();worker=None;owned=[];keys=[]
report={'passed':False,'scope':'Actual Original busy refresh parent to15second child execution/local import/index-SeriesRules grandchild publication; no native/remote/grandchild execution acceptance','task':TASK.name,'hard_time_limit':TASK.time_limit,'checks':[],'cases':[],'fixture_control':{'child_routes':'all tasks except refresh_epg_data → owned-refresh-children','worker_queues':['celery'],'purpose':'capture actual generated child envelopes without executing unrelated families','task_or_helper_replacement':False}}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def rows(name):
 p=Path('/evidence')/name;return[json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn,seconds=40):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  value=fn()
  if value:return value
  time.sleep(.025)
 raise TimeoutError('Owned refresh observation deadline')
def stop():
 global worker
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report.setdefault('forced_cleanup',[]).append(True)
 worker=None
def launch():
 global worker
 # Only parent refresh messages are allowed into the consumed queue.
 for raw in broker.lrange('celery',0,63):
  if len(raw)>65536 or json.loads(raw)['headers']['task']!=TASK.name:raise RuntimeError('Unrelated consumed queue head')
 broker.delete('_kombu.binding.celery.pidbox')
 with Path('/evidence/epg-refresh-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','epg_refresh_import_observer:app','worker','-Q','celery','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-refresh-import@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
def payload(envelope):
 raw=base64.b64decode(envelope['body'],validate=True)
 if len(raw)>16384:raise RuntimeError('Body bound')
 return json.loads(raw)
def send(kind,sid):
 if kind.endswith('default'):return TASK.delay(sid)
 if kind.endswith('force_kw'):return TASK.apply_async(args=[sid],kwargs={'force':True})
 if kind.endswith('force_pos'):return TASK.delay(sid,True)
 if kind.endswith('retry_pos'):return TASK.delay(sid,True,7)
 if kind.endswith('all_kw'):return TASK.apply_async(kwargs={'source_id':sid,'force':False,'_file_defer_retry':2})
 raise RuntimeError('Known case required')
try:
 broker.flushdb();backend.flushdb()
 for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
 path=root/'descendant.xml';start=dt.datetime.now(dt.timezone.utc).replace(microsecond=0)+dt.timedelta(hours=1);end=start+dt.timedelta(hours=1)
 xml=('<tv><channel id="mapped"><display-name>Owned descendant guide</display-name></channel><programme channel="mapped" start="'+start.strftime('%Y%m%d%H%M%S +0000')+'" stop="'+end.strftime('%Y%m%d%H%M%S +0000')+'"><title>Owned descendant programme</title></programme></tv>').encode();path.write_bytes(xml)
 source=EPGSource(name='rust-refresh-descendant',source_type='xmltv',is_active=True,file_path=str(path),custom_properties={});EPGSource.objects.bulk_create([source]);sid=source.pk;owned.append(sid)
 epg=EPGData.objects.create(epg_source=source,tvg_id='mapped',name='Before guide');Channel.objects.bulk_create([Channel(name='rust-refresh-descendant',channel_number=1999100,epg_data=epg)]);EPGSourceIndex.objects.create(source_id=sid,data={'owned':'before'})
 refresh='task_lock_refresh_epg_data_'+str(sid);file='task_lock_epg_source_file_'+str(sid);keys.extend([refresh,file]);cache.set(file,'owned-busy',ex=60)
 issued=TASK.delay(sid);envelope=json.loads(next(raw for raw in broker.lrange('celery',0,63)if json.loads(raw)['headers']['id']==issued.id));launch();parent_post=wait(lambda:next((r for r in rows('epg-refresh-postrun.jsonl')if r['id']==issued.id),None));children=wait(lambda:[r for r in rows('epg-refresh-child-envelopes.jsonl')if r.get('observed')and r['observed']['envelope']['headers'].get('parent_id')==issued.id]);check('one-parent-deferred-child',len(children)==1);child=children[0]['observed']['envelope'];child_id=child['headers']['id'];parent_raw=backend.get(app.backend.get_key_for_task(issued.id));parent_meta=json.loads(parent_raw)
 check('parent-success-null-with-child',parent_post['state']=='SUCCESS'and parent_meta['status']=='SUCCESS'and parent_meta['result']is None and parent_meta['children']==[[[child_id,None],None]])
 check('deferred-body-and-eta',payload(child)[0:2]==[[sid],{'force':False,'_file_defer_retry':1}]and child['headers']['eta']is not None and child['headers']['retries']==0)
 check('parent-lock-release-before-child',cache.get(refresh)is None and cache.get(file)==b'owned-busy'and EPGSourceIndex.objects.get(source_id=sid).data is None)
 # The only mutated dependency is our explicitly owned blocking file-lock key.
 deleted=cache.eval("if redis.call('GET',KEYS[1])==ARGV[1]then return redis.call('DEL',KEYS[1])end;return 0",1,file,'owned-busy');check('owned-file-lock-released-for-real-child',deleted==1)
 child_post=wait(lambda:next((r for r in rows('epg-refresh-postrun.jsonl')if r['id']==child_id),None),seconds=30);stop();child_raw=backend.get(app.backend.get_key_for_task(child_id));check('bounded-parent-child-metadata',len(parent_raw)<=262144 and child_raw is not None and len(child_raw)<=262144);child_meta=json.loads(child_raw)
 check('child-success-null-parent-id',child_post['state']=='SUCCESS'and child_meta['status']=='SUCCESS'and child_meta['result']is None and child_meta['parent_id']==issued.id)
 prerun=next(r for r in rows('epg-refresh-prerun.jsonl')if r['id']==child_id);check('actual-child-request-lineage',prerun['root_id']==issued.id and prerun['parent_id']==issued.id and prerun['args']==[sid]and prerun['kwargs']=={'force':False,'_file_defer_retry':1}and prerun['retries']==0)
 eta=dt.datetime.fromisoformat(child['headers']['eta']);actual=dt.datetime.fromisoformat(prerun['utc']);lateness=(actual-eta).total_seconds();check('actual-eta-child-not-early',-.1<=lateness<=5)
 grand=[r for r in rows('epg-refresh-child-envelopes.jsonl')if r.get('observed')and r['observed']['envelope']['headers'].get('parent_id')==child_id];check('two-imported-grandchild-families',[r['sender']for r in grand]==['apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules']);grand_envelopes=[r['observed']['envelope']for r in grand]
 check('grandchild-root-parent-lineage',all(e['headers']['root_id']==issued.id and e['headers']['parent_id']==child_id and e['headers']['retries']==0 for e in grand_envelopes));check('child-result-grandchild-identities',child_meta['children']==[[[e['headers']['id'],None],None]for e in grand_envelopes]);check('parent-result-not-rewritten',backend.get(app.backend.get_key_for_task(issued.id))==parent_raw)
 check('actual-imported-sql-and-index-invalidated',EPGData.objects.filter(pk=epg.pk,name='Owned descendant guide').exists()and ProgramData.objects.filter(epg_id=epg.pk,title='Owned descendant programme').count()==1 and EPGSourceIndex.objects.get(source_id=sid).data is None)
 check('refresh-file-leases-released',cache.get(refresh)is None and cache.get(file)is None);parent_ttl=backend.ttl(app.backend.get_key_for_task(issued.id));child_ttl=backend.ttl(app.backend.get_key_for_task(child_id));check('independent-result-ttls',3570<=parent_ttl<3595 and 3590<=child_ttl<=3600 and 10<=child_ttl-parent_ttl<=25)
 check('no-other-family-executed',not rows('epg-refresh-unexpected-tasks.jsonl'));check('grandchildren-unconsumed',broker.llen(CHILD_QUEUE)==2 and broker.llen('celery')==0 and broker.hlen('unacked')==0)
 report['cases']=[{'name':'busy-parent-actual-child-import','source_id':sid,'envelope':envelope,'metadata':parent_meta,'raw_metadata_sha256':hashlib.sha256(parent_raw).hexdigest(),'result_ttl':parent_ttl,'child_envelope':child,'child_metadata':child_meta,'child_raw_metadata_sha256':hashlib.sha256(child_raw).hexdigest(),'child_result_ttl':child_ttl,'child_request':prerun,'actual_eta_lateness_seconds':lateness,'grandchildren':grand,'xml_base64':base64.b64encode(xml).decode()}];report['passed']=True
finally:
 stop()
 for key in keys:cache.delete(key)
 Channel.objects.filter(epg_data__epg_source_id__in=owned).delete();EPGSource.objects.filter(pk__in=owned).delete();broker.flushdb();backend.flushdb();Path('/evidence/source-epg-refresh-descendant-private.json').write_text(json.dumps(report,indent=2)+'\n')
