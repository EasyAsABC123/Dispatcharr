"""Original cached Redis SET/DEL ACL behavior and safe endpoint/user context.
Copied DB, per-key owned ACL users only; no native mapping or production writes.
Original Dispatcharr/Redis/redis-py authors retain full credit and licenses.
"""
import base64,hashlib,json,os,signal,subprocess,sys,uuid
from pathlib import Path
sys.path.insert(0,'/index-tools')
import source_index_worker as f
from apps.epg.models import EPGSource,EPGSourceIndex
import redis
report=f.report;report['passed']=False;worker=None;users=[];sources={};cases=[];probes=[]
report['cache_acl_cases']=cases;report['acl_script_probes']=probes
try:
 f.broker.flushdb();f.backend.flushdb();root=f.root/'cache-acl';root.mkdir();good=root/'good.xml';good.write_bytes(f.guide)
 plain=['+@connection','+get','+set','+del','+expire']
 specs=[('set_denied','file',['+@connection','+get','+del','+expire'],False),('set_denied_directory','directory',['+@connection','+get','+del','+expire'],False),('del_denied','file',['+@connection','+get','+set','+expire'],False),('key_denied','file',plain,True),('plain_without_eval','file',plain,False),('del_denied_eval_allowed','file',['+@all','-del'],False)]
 plan={'sources':{}}
 for name,kind,commands,wrongkey in specs:
  source=EPGSource.objects.create(name='rust-cache-acl-'+name,source_type='xmltv',is_active=False,file_path=str(root if kind=='directory'else good));sources[name]=source.pk;key='task_lock_epg_source_file_'+str(source.pk);username='owned_acl_'+uuid.uuid4().hex[:12];users.append(username)
  pattern='owned_unrelated_'+uuid.uuid4().hex if wrongkey else key
  f.cache.execute_command('ACL','SETUSER',username,'reset','on','>playback-fixture-only','~'+pattern,*commands)
  plan['sources'][str(source.pk)]={'username':username};client=redis.Redis(host='127.0.0.1',port=6379,username=username,password='playback-fixture-only',db=0)
  client.ping()
  if name in ('plain_without_eval','del_denied_eval_allowed'):
   f.cache.set(key,'owned-script-probe',ex=30)
   try:client.eval("return redis.call('DEL',KEYS[1])",1,key)
   except redis.exceptions.RedisError as error:
    probes.append({'name':name,'username':username,'module':type(error).__module__,'class':type(error).__name__,'arguments':list(error.args),'probe_value':f.cache.get(key).decode()})
   else:raise AssertionError('Owned eval or inner DEL must be denied')
   f.cache.delete(key)
  client.close()
 Path('/evidence/cache-acl-plan.json').write_text(json.dumps(plan));f.quarantine();f.broker.delete('_kombu.binding.celery.pidbox')
 with Path('/evidence/cache-acl-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','index_acl_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-cache-acl@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/index-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 f.wait(lambda:f.broker.exists('_kombu.binding.celery.pidbox'))
 for name,kind,commands,wrongkey in specs:
  sid=sources[name];ident=f.task.delay(sid).id;f.wait(lambda:any(row['id']==ident for row in f.rows('index-postrun.jsonl')));key=f.app.backend.get_key_for_task(ident);raw=f.backend.get(key)
  if raw is None or len(raw)>262144:raise RuntimeError('Owned metadata bound')
  result=json.loads(raw);lock='task_lock_epg_source_file_'+str(sid);value=f.cache.get(lock);index=EPGSourceIndex.objects.filter(source_id=sid).values_list('data',flat=True).first();post=[row for row in f.rows('index-postrun.jsonl')if row['id']==ident]
  item={'name':name,'task_id':ident,'source_id':sid,'safe_endpoint':{'host':'127.0.0.1','port':6379,'database':0,'username':plan['sources'][str(sid)]['username']},'commands':commands,'key_access_denied':wrongkey,'result':result,'raw_result_base64':base64.b64encode(raw).decode(),'raw_result_sha256':hashlib.sha256(raw).hexdigest(),'result_ttl':f.backend.ttl(key),'index':index,'lock_value':None if value is None else value.decode(),'lock_ttl':f.cache.ttl(lock),'postrun_state':post[-1]['state']};cases.append(item)
  success=name=='plain_without_eval';late=name.startswith('del_denied')
  f.check('original-terminal-'+name,result['status']==('SUCCESS'if success else'FAILURE')and item['postrun_state']==result['status'])
  if not success:f.check('permission-type-args-'+name,result['result']['exc_type']=='NoPermissionError'and result['result']['exc_module']=='redis.exceptions'and len(result['result']['exc_message'])==1 and isinstance(result['result']['exc_message'][0],str))
  f.check('index-effects-'+name,(index=={'channels':{'fixture&one':[4]},'interleaved_channels':[]})if success or late else index is None)
  f.check('lock-effects-'+name,(value==b'locked'and 0<item['lock_ttl']<=300)if late else value is None)
  f.check('result-geometry-'+name,3590<=item['result_ttl']<=3600 and result['task_id']==ident and result['children']==[]and 'parent_id'not in result)
  if name=='set_denied_directory':f.check('denied-acquire-prevents-directory-open','IsADirectoryError'not in result['traceback'])
 f.check('no-other-queue-work',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0)
 report.update(passed=True,scope='Actual Original cached-user SET/DEL/key ACL and source plain-command success without Lua; no native equivalence claim',cache_acl_cases=cases,acl_script_probes=probes,native_mapping_verified=False)
finally:
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_acl_worker_cleanup']=True
 for username in users:f.cache.execute_command('ACL','DELUSER',username)
 for sid in sources.values():
  key='task_lock_epg_source_file_'+str(sid);f.cache.eval("if redis.call('GET',KEYS[1])==ARGV[1]then return redis.call('DEL',KEYS[1])end;return 0",1,key,'locked')
 report['owned_acl_users_removed']=len(users);Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
