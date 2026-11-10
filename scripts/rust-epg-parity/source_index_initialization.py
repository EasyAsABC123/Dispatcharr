"""Actual original client-initialization None and busy child-publication failures.
Original Dispatcharr authors retain all application/algorithm credit.
"""
import base64,hashlib,json,os,signal,socket,subprocess,time
from pathlib import Path
import source_index_failures as prior
import source_index_worker as f
from apps.epg.models import EPGSource,EPGSourceIndex
report=f.report;report['passed']=False;report['index_initialization_cases']=[];closed=socket.socket();closed.bind(('127.0.0.1',0));worker=None
try:
 f.broker.flushdb();f.backend.flushdb();sources={}
 for mode in ['cache-init-url','cache-init-network','busy-defer-publish']:
  sources[mode]=EPGSource.objects.create(name='rust-index-initialization-'+mode,source_type='xmltv',is_active=False,file_path=str(f.path))
 plan={'sources':{str(s.pk):mode for mode,s in sources.items()},'closed_port':closed.getsockname()[1]};Path('/evidence/index-failure-plan.json').write_text(json.dumps(plan))
 source=sources['busy-defer-publish'];lock='task_lock_epg_source_file_'+str(source.pk);marker=b'owned-initialization-busy';f.cache.set(lock,marker,ex=300)
 f.quarantine();f.broker.delete('_kombu.binding.celery.pidbox')
 with Path('/evidence/index-initialization-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','index_failure_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-index-initialization@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/index-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 f.wait(lambda:f.broker.exists('_kombu.binding.celery.pidbox'))
 for mode,source in sources.items():
  begin=time.monotonic();ident=f.task.delay(source.pk).id
  f.wait(lambda:f.finished(ident),timeout=120);key=f.app.backend.get_key_for_task(ident);raw=f.backend.get(key)
  if raw is None or len(raw)>262144:raise RuntimeError('Bounded result expected')
  result=json.loads(raw);ownedlock='task_lock_epg_source_file_'+str(source.pk)
  item={'name':mode,'task_id':ident,'source_id':source.pk,'elapsed_seconds':time.monotonic()-begin,'result':result,'result_raw_base64':base64.b64encode(raw).decode(),'result_raw_sha256':hashlib.sha256(raw).hexdigest(),'result_ttl':f.backend.ttl(key),'index_exists':EPGSourceIndex.objects.filter(source=source).exists(),'file_lock_value':None if f.cache.get(ownedlock)is None else f.cache.get(ownedlock).decode(),'file_lock_ttl':f.cache.ttl(ownedlock),'postrun':f.finished(ident),'queued':f.broker.llen('celery'),'unacked':f.broker.hlen('unacked')}
  report['index_initialization_cases'].append(item)
  f.check('original-initialization-'+mode,result['status']=='FAILURE'and result['result']['exc_type']==('OperationalError'if mode=='busy-defer-publish'else'AttributeError'))
  f.check('original-no-index-'+mode,not item['index_exists'])
 f.check('busy-publication-failure-keeps-foreign-lock',f.cache.get(lock)==marker)
 report.update(passed=True,scope='Actual Original cached-client initialization None vs busy-defer publication failure; dependency-only owned closed endpoint/settings',initialization_fault_scope={'cached_client_cleared':True,'original_init_function_unchanged':True,'original_retry_defaults_unchanged':True,'child_producer_pool_only_changed':True,'consumer_result_backend_unchanged':True,'producer_retry_policy':f.app.conf.task_publish_retry_policy})
finally:
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_initialization_worker_cleanup']=True
 if 'lock'in globals()and f.cache.get(lock)==marker:f.cache.delete(lock)
 closed.close();Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
