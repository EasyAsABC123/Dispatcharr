"""Original finally-release precedence with copiedDB/owned Redis ACL users only.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
"""
import base64,hashlib,json,os,signal,socket,subprocess,time,uuid,sys
from pathlib import Path
sys.path.insert(0,'/index-tools')
import source_index_worker as f
from apps.epg.models import EPGSource,EPGSourceIndex
from django.db import connection
report=f.report;report['passed']=False;worker=None;closed=socket.socket();closed.bind(('127.0.0.1',0));users=[];sources={};cases=[];trigger=False
report['double_failure_cases']=cases
try:
 f.broker.flushdb();f.backend.flushdb();root=f.root/'double-failures';root.mkdir();good=root/'good.xml';good.write_bytes(f.guide)
 specs=[('directory_release_network','directory','network'),('sql_release_network','sql','network'),('written_release_noperm','written','noperm'),('directory_release_noperm','directory','noperm'),('sql_release_noperm','sql','noperm')]
 plan={'sources':{},'closed_port':closed.getsockname()[1]}
 for name,execution,release in specs:
  path=root if execution=='directory'else good
  source=EPGSource.objects.create(name='rust-double-failure-'+name,source_type='xmltv',is_active=False,file_path=str(path));sources[name]=source.pk
  mode={'release':release}
  if release=='noperm':
   user='owned_release_'+uuid.uuid4().hex[:12];users.append(user);mode['acl_user']=user;key='task_lock_epg_source_file_'+str(source.pk)
   f.cache.execute_command('ACL','SETUSER',user,'reset','on','>playback-fixture-only','~'+key,'+@connection','+get')
   actual=f.cache.execute_command('ACL','GETUSER',user)
   if not actual:raise RuntimeError('Owned ACL creation failed')
   import redis
   probe=redis.Redis(host='127.0.0.1',port=6379,username=user,password='playback-fixture-only',db=0)
   probe.get(key)
   try:probe.delete(key)
   except redis.exceptions.NoPermissionError:pass
   else:raise AssertionError('Owned DEL must be denied')
   probe.close()
  plan['sources'][str(source.pk)]=mode
 Path('/evidence/double-failure-plan.json').write_text(json.dumps(plan))
 sql_ids=[sources[name]for name,execution,_ in specs if execution=='sql']
 with connection.cursor()as cursor:
  cursor.execute('CREATE FUNCTION owned_double_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_id IN ('+','.join(map(str,sql_ids))+') THEN RAISE EXCEPTION USING ERRCODE=\'P0001\', MESSAGE=\'owned-primary-sql-failure\', DETAIL=\'owned sql detail\'; END IF; RETURN NEW; END $$')
  cursor.execute('CREATE TRIGGER owned_double_failure BEFORE INSERT OR UPDATE ON epg_epgsourceindex FOR EACH ROW EXECUTE FUNCTION owned_double_failure()');trigger=True
 f.quarantine();f.broker.delete('_kombu.binding.celery.pidbox')
 with Path('/evidence/double-failure-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','index_double_failure_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-double-failure@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/index-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 f.wait(lambda:f.broker.exists('_kombu.binding.celery.pidbox'))
 for name,execution,release in specs:
  sid=sources[name];ident=f.task.delay(sid).id;f.wait(lambda:any(row['id']==ident for row in f.rows('index-postrun.jsonl')))
  key=f.app.backend.get_key_for_task(ident);raw=f.backend.get(key)
  if raw is None or len(raw)>262144:raise RuntimeError('Owned result bound')
  result=json.loads(raw);postrun=[row for row in f.rows('index-postrun.jsonl')if row['id']==ident];index=EPGSourceIndex.objects.filter(source_id=sid).values_list('data',flat=True).first();lock='task_lock_epg_source_file_'+str(sid);value=f.cache.get(lock);ttl=f.cache.ttl(lock)
  item={'name':name,'execution':execution,'release':release,'task_id':ident,'source_id':sid,'result':result,'result_raw_base64':base64.b64encode(raw).decode(),'result_raw_sha256':hashlib.sha256(raw).hexdigest(),'result_ttl':f.backend.ttl(key),'postrun':postrun,'index':index,'lock_value':None if value is None else value.decode(),'lock_ttl':ttl,'queue':f.broker.llen('celery'),'unacked':f.broker.hlen('unacked')};cases.append(item)
  expected='ConnectionError'if release=='network'else'NoPermissionError';failure=result.get('result')or{};trace=result.get('traceback')or''
  f.check('release-overrides-result-'+name,result['status']=='FAILURE'and failure.get('exc_type')==expected and failure.get('exc_module')=='redis.exceptions'and postrun[-1]['state']=='FAILURE')
  f.check('result-geometry-'+name,result['task_id']==ident and result['children']==[]and 'parent_id'not in result and isinstance(result['date_done'],str)and 3590<=item['result_ttl']<=3600)
  f.check('index-effects-'+name,(index=={'channels':{'fixture&one':[4]},'interleaved_channels':[]})if execution=='written'else index is None)
  f.check('lock-remains-'+name,value==b'locked'and 0<ttl<=300)
  if execution!='written':
   primary='IsADirectoryError'if execution=='directory'else'ProgrammingError'
   f.check('original-primary-in-trace-context-'+name,primary in trace and 'During handling of the above exception' in trace and expected in trace)
  f.check('no-other-queue-work-'+name,item['queue']==0 and item['unacked']==0)
 report.update(passed=True,scope='Actual Original finally-release precedence; directory/SQL plus network/NOPERM and normal Written plus NOPERM; Source-only',double_failure_cases=cases,dependency_faults={'cached_delete_only_wrapper':True,'original_task_helpers_and_tracing_unchanged':True,'actual_owned_acl_denies_del':True,'closed_loopback_port_held':True},native_classification_verified=False)
finally:
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_double_worker_cleanup']=True
 if trigger:
  with connection.cursor()as cursor:cursor.execute('DROP TRIGGER IF EXISTS owned_double_failure ON epg_epgsourceindex');cursor.execute('DROP FUNCTION IF EXISTS owned_double_failure()')
 for user in users:f.cache.execute_command('ACL','DELUSER',user)
 for sid in sources.values():
  key='task_lock_epg_source_file_'+str(sid)
  f.cache.eval("if redis.call('GET',KEYS[1])==ARGV[1]then return redis.call('DEL',KEYS[1])end;return 0",1,key,'locked')
 closed.close();report['owned_acl_users_removed']=len(users);Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
