"""Actual Original refresh ETA producer/consumer around native receipt scheduling.
Original Dispatcharr/Celery/Kombu authors retain code/algorithm/protocol credit.
No unrelated family is executed; Original descendant routing is a fixture control.
"""
import json,os,subprocess,uuid
from pathlib import Path
import source_refresh_eta_worker as f
report=f.report;report['passed']=False;owned=[]
try:
 native=[]
 for label in ['scheduled','warm_restore']:
  f.broker.flushdb();f.backend.flushdb()
  native_path=f.root/('native-'+label+'.xml');native_path.write_bytes(b'<tv></tv>')
  f.check(label+'-fresh-owned-input-after-Source-cleanup',native_path.exists()and native_path.read_bytes()==b'<tv></tv>')
  source=f.EPGSource(name='rust-native-refresh-eta-'+label,source_type='xmltv',is_active=True,file_path=str(native_path),custom_properties={});f.EPGSource.objects.bulk_create([source]);sid=source.pk;owned.append(sid)
  parent=str(uuid.uuid4())
  issued=f.task.apply_async(args=[sid],kwargs={'force':True,'_file_defer_retry':1},countdown=15,root_id=parent,parent_id=parent)
  ident=issued.id;before=json.loads(f.broker.lindex('celery',-1))
  value={'action':label,'task_id':ident,'source_id':sid,'database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+f.database,'cache_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND']}
  peer=subprocess.run(['/evidence/native-refresh-worker','--ignored','--exact','refresh_worker::peer::native_refresh_worker_peer','--nocapture'],input=json.dumps(value),capture_output=True,text=True,timeout=45)
  with Path('/evidence/native-refresh-eta.log').open('a')as log:log.write(peer.stdout+peer.stderr)
  if peer.returncode:
   report['native_failed_source']=list(f.EPGSource.objects.filter(pk=sid).values('status','last_message','custom_properties'));report['native_failed_backend']=f.backend.get(f.app.backend.get_key_for_task(ident)).decode()if f.backend.get(f.app.backend.get_key_for_task(ident))else None;report['native_failed_unacked']=f.broker.hlen('unacked');report['native_failed_queue']=f.broker.llen('celery')
  peer.check_returncode();marker='NATIVE_REFRESH_WORKER_JSON=';audit=json.loads(next(line.split(marker,1)[1]for line in peer.stdout.splitlines()if marker in line))
  f.check(label+'-actual-drain',audit['caller_released']and audit['worker_drained']and audit['service_drained']and audit['transport_drained']and audit['physical_jobs']==0 and audit['default_dispatch_calls']==0)
  f.check(label+'-scheduled-not-invented-return',audit['native_eta_consumer']and not audit['scheduled_return_observed'])
  if label=='warm_restore':
   f.check('native-warm-no-execution-or-fake-result',audit['calls']==0 and f.backend.get(f.app.backend.get_key_for_task(ident))is None and f.app.AsyncResult(ident).state=='PENDING')
   restored=json.loads(f.broker.lindex('celery',-1));expected=json.loads(json.dumps(before));expected['headers']['redelivered']=True;expected['properties']['delivery_info']['redelivered']=True
   f.check('native-warm-only-two-redelivery-fields',restored==expected and f.broker.hlen('unacked')==0 and f.broker.zcard('unacked_index')==0)
   f.launch();post=f.wait(lambda:next((row for row in f.rows('epg-refresh-postrun.jsonl')if row['id']==ident),None),seconds=35);f.stop()
   prerun=next(row for row in f.rows('epg-refresh-prerun.jsonl')if row['id']==ident)
   f.check('actual-Original-consumes-native-restored-frame',post['state']=='SUCCESS'and prerun['delivery_info'].get('redelivered')is True)
  else:f.check('actual-native-executor-called-once',audit['calls']==1)
  raw=f.backend.get(f.app.backend.get_key_for_task(ident));f.check(label+'-bounded-result',raw is not None and len(raw)<=262144);metadata=json.loads(raw);result=f.app.AsyncResult(ident)
  f.check(label+'-actual-Original-AsyncResult',result.ready()and result.successful()and result.get(timeout=2)is None and metadata['parent_id']==parent and metadata['traceback']is None)
  f.check(label+'-expiry',3580<=f.backend.ttl(f.app.backend.get_key_for_task(ident))<=3600)
  f.check(label+'-source-success',f.EPGSource.objects.get(pk=sid).status==f.EPGSource.STATUS_SUCCESS)
  f.check(label+'-no-owned-reservation-left',f.broker.hlen('unacked')==0 and f.broker.zcard('unacked_index')==0)
  descendants=[]
  queues=['celery']if label=='scheduled'else['rust-refresh-index-control','rust-refresh-series-control']
  for queue in queues:descendants.extend(json.loads(raw)for raw in reversed(f.broker.lrange(queue,0,63)))
  f.check(label+'-two-unconsumed-descendants',len(descendants)==2 and [row['headers']['task']for row in descendants]==['apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules'])
  f.check(label+'-descendant-lineage',all(row['headers']['root_id']==parent and row['headers']['parent_id']==ident for row in descendants))
  f.check(label+'-metadata-child-lineage',metadata['children']==[[[row['headers']['id'],None],None]for row in descendants])
  native.append({'name':label,'audit':audit,'result':metadata,'before':before,'descendants':descendants})
 report['native_cases']=native
 env=dict(os.environ,JOB_REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:6379/0')
 f.broker.flushdb();f.backend.flushdb()
 resources=subprocess.run(['/evidence/native-refresh-worker','--ignored','refresh_eta::physical::','--nocapture','--test-threads=1'],env=env,capture_output=True,text=True,timeout=45)
 Path('/evidence/native-refresh-eta-physical.log').write_text(resources.stdout+resources.stderr);resources.check_returncode()
 f.check('three-actualRedis-refresh-receipt-resource-gates','3 passed; 0 failed'in resources.stdout)
 report['native_cases']=native;report['scope']='Original15s reference33 plus actual Original producer→native ETA reservation/schedule/FileJob→Original AsyncResult and native warmrestore→realOriginal worker. Descendants unconsumed; no main/default intake/general visibility sweep.';report['passed']=True
finally:
 f.stop();f.EPGSource.objects.filter(pk__in=owned).delete();f.broker.flushdb();f.backend.flushdb()
 report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()]
 Path('/evidence/source-native-refresh-eta-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
