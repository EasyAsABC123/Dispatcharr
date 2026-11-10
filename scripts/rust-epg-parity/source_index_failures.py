"""Actual original worker outcomes; controlled copiedDB/files/dependency faults.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import base64,hashlib,json,os,signal,socket,subprocess,time
from pathlib import Path
import source_index_worker as f
from apps.epg.models import EPGSource,EPGSourceIndex
from django.db import connection
report=f.report;report['passed']=False;worker=None;closed=socket.socket();closed.bind(('127.0.0.1',0))
try:
 f.broker.flushdb();f.backend.flushdb()
 root=f.root/'failures';root.mkdir();good=root/'good.xml';good.write_bytes(f.guide)
 definitions=[('missing-source',None,None),('missing-file',root/'missing.xml',None),('malformed-xml',root/'malformed.xml',b'<tv><programme channel="broken"><title>unterminated'),('invalid-encoding',root/'encoding.xml',b'<tv><programme channel="bad\xff&amp;one"></tv>'),('empty-file',root/'empty.xml',b''),('directory',root,None),('sql-write',good,None),('cache-network',good,None),('cache-auth',good,None),('release-network',good,None),('duplicate-success-then-error',good,None)]
 sources={};faults={};cases=[];report['index_failure_cases']=cases
 for name,path,content in definitions:
  if name=='missing-source':sources[name]=1000000+f.source.pk;continue
  if content is not None:path.write_bytes(content)
  source=EPGSource.objects.create(name='rust-index-failure-'+name,source_type='xmltv',is_active=False,file_path=str(path));sources[name]=source.pk
  if name in ('cache-network','cache-auth','release-network'):faults[str(source.pk)]=name
 plan={'sources':faults,'closed_port':closed.getsockname()[1]};Path('/evidence/index-failure-plan.json').write_text(json.dumps(plan))
 # A matching-source trigger affects only this disposable database's owned row.
 with connection.cursor()as cursor:
  cursor.execute("CREATE FUNCTION owned_index_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_id = "+str(sources['sql-write'])+" THEN RAISE EXCEPTION 'owned-index-sql-write-failure'; END IF; RETURN NEW; END $$")
  cursor.execute('CREATE TRIGGER owned_index_failure BEFORE INSERT OR UPDATE ON epg_epgsourceindex FOR EACH ROW EXECUTE FUNCTION owned_index_failure()')
 f.quarantine();f.broker.delete('_kombu.binding.celery.pidbox')
 with Path('/evidence/index-failure-worker-private.log').open('ab')as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','index_failure_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-index-failure@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/index-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 f.wait(lambda:f.broker.exists('_kombu.binding.celery.pidbox'))
 def capture(name,ident,source,count=1):
  f.wait(lambda:len([x for x in f.rows('index-postrun.jsonl')if x['id']==ident])>=count,timeout=30)
  key=f.app.backend.get_key_for_task(ident);raw=f.backend.get(key)
  if raw is None or len(raw)>262144:raise RuntimeError('Missing/oversized owned result')
  result=json.loads(raw);index=EPGSourceIndex.objects.filter(source_id=source).values_list('data',flat=True).first();lock='task_lock_epg_source_file_'+str(source)
  item={'name':name,'task_id':ident,'source_id':source,'postrun':[x for x in f.rows('index-postrun.jsonl')if x['id']==ident],'prerun':[x for x in f.rows('index-prerun.jsonl')if x['id']==ident],'result':result,'result_raw_base64':base64.b64encode(raw).decode(),'result_raw_sha256':hashlib.sha256(raw).hexdigest(),'result_ttl':f.backend.ttl(key),'index':index,'file_lock_value':None if f.cache.get(lock)is None else f.cache.get(lock).decode(),'file_lock_ttl':f.cache.ttl(lock),'queued_tasks':f.broker.llen('celery'),'unacked':f.broker.hlen('unacked')}
  cases.append(item);return item
 for name,_,_ in definitions:
  source=sources[name];ident=f.task.delay(source).id
  first=capture(name,ident,source)
  if name=='release-network':f.check('release-network-index-written-lock-remains',first['index']is not None and first['file_lock_value']=='locked');f.cache.delete('task_lock_epg_source_file_'+str(source))
  if name=='duplicate-success-then-error':
   before=first;EPGSource.objects.filter(pk=source).update(file_path=str(root));f.task.apply_async(args=[source],task_id=ident);second=capture('duplicate-later-directory-error',ident,source,2)
   f.check('duplicate-later-error-executes-but-prior-success-retained',second['postrun'][-1]['state']=='FAILURE'and second['result']['status']=='SUCCESS'and second['result_raw_sha256']==before['result_raw_sha256']and second['result_ttl']<=before['result_ttl'])
 for case in cases:
  if case['name']!='duplicate-later-directory-error':
   expected='FAILURE'if case['name']in('directory','sql-write','cache-network','cache-auth','release-network')else'SUCCESS'
   f.check('original-outcome-'+case['name'],case['result']['status']==expected and case['postrun'][-1]['state']==expected)
  f.check('result-geometry-'+case['name'],case['result']['task_id']==case['task_id']and 0<case['result_ttl']<=86400 and isinstance(case['result']['children'],list)and isinstance(case['result']['date_done'],str))
 report.update(passed=True,index_failure_cases=cases,scope='Actual Original index-task success/failure/result/duplicate contracts; controlled owned dependency fault endpoints only',dependency_faults={'implementation':'RedisClient cached dependency only; original task/helpers/tracing unchanged','loopback_closed_port_held':True,'sql_trigger_matching_owned_source_only':True})
finally:
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_failure_worker_cleanup']=True
 closed.close();Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
