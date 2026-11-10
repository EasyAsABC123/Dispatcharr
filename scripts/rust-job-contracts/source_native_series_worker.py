"""Actual Original producer/AsyncResult around native zero-rule Series consumer.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
Configured dictionary rules/foreign tasks are explicitly not consumed.
"""
import json,os,subprocess,uuid
from pathlib import Path
import source_series_rules_worker as f
report=f.report;report['passed']=False
try:
 cases=[('absent',None,False),('empty-map',{},False),('empty-list',{'series_rules':[]},False),('nonlist',{'series_rules':'owned'},False),('nonmaps',{'series_rules':[0,None,[]]},False),('busy',{'series_rules':[{'title':'Never schedule'}]},True),('parented',{'series_rules':[]},False),('prepared',{'series_rules':[]},False),('held-sql',{'series_rules':[]},False),('configured-unsupported',{'series_rules':[{'title':'Never schedule'}]},False),('foreign',{'series_rules':[]},False)]
 native=[]
 for label,group,busy in cases:
  f.broker.flushdb();f.backend.flushdb();f.cache.delete(f.lock)
  f.CoreSettings.objects.filter(key='dvr_settings').delete()
  if group is not None:f.CoreSettings.objects.create(key='dvr_settings',name='Owned native Series worker',value=group)
  f.django_cache.delete(f.CoreSettings.group_cache_key('dvr_settings'))
  if busy:f.cache.set(f.lock,'owned-busy',ex=300)
  options={'root_id':str(uuid.uuid4()),'parent_id':str(uuid.uuid4())}if label=='parented'else{}
  issued=f.app.send_task('apps.backups.tasks.create_backup_task')if label=='foreign'else f.task.apply_async(args=[],**options)
  ident=issued.id;before=f.broker.lindex('celery',-1)
  expectation='foreign'if label=='foreign'else'unsupported-rules'if label=='configured-unsupported'else'completed'
  value={'task_id':ident,'expected':expectation,'held_sql':label=='held-sql','prepared':label=='prepared','database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+f.database,'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND']}
  peer=subprocess.run(['/evidence/native-refresh-worker','--ignored','--exact','series_worker::peer::native_series_worker_peer','--nocapture'],input=json.dumps(value),capture_output=True,text=True,timeout=35)
  with Path('/evidence/native-series-worker.log').open('a')as log:log.write(peer.stdout+peer.stderr)
  peer.check_returncode();marker='NATIVE_SERIES_WORKER_JSON=';audit=json.loads(next(line.split(marker,1)[1]for line in peer.stdout.splitlines()if marker in line))
  if label=='prepared':f.check('prepared-no-intake-then-started-and-drained',audit['prepared_no_intake_then_started'])
  if label=='held-sql':f.check('actual-SQL-callerabort-incomplete-drain',audit['held_actual_SQL']and audit['caller_aborted']and not audit['aborted_waiter_return_observed'])
  f.check(label+'-actual-owner-drain',audit['caller_released']and audit['physical_operations']==0 and audit['family_drained']and audit['transport_drained'])
  raw=f.backend.get(f.app.backend.get_key_for_task(ident));metadata=None
  if expectation=='completed':
   f.check(label+'-bounded-terminal',raw is not None and len(raw)<=262144);metadata=json.loads(raw)
   expected={'scheduled':0,'details':[{'status':'skipped','reason':'concurrent evaluation in progress'}]}if busy else{'scheduled':0,'details':[]}
   result=f.app.AsyncResult(ident)
   f.check(label+'-actual-Original-AsyncResult',result.ready()and result.successful()and result.get(timeout=2)==expected and metadata['result']==expected and metadata['traceback']is None and metadata['children']==[])
   f.check(label+'-actual-ACK',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0)
   f.check(label+'-expiry',3580<=f.backend.ttl(f.app.backend.get_key_for_task(ident))<=3600)
   if options:f.check('native-parented-lineage',metadata.get('parent_id')==options['parent_id'])
  else:
   f.check(label+'-original-bytes-unconsumed',f.broker.llen('celery')==1 and f.broker.lindex('celery',-1)==before)
   f.check(label+'-no-fake-result',raw is None and f.app.AsyncResult(ident).state=='PENDING'and f.broker.hlen('unacked')==0)
  f.check(label+'-global-lock-effect',f.cache.get(f.lock)==(b'owned-busy'if busy else None))
  f.check(label+'-no-recording-effects',list(f.Recording.objects.order_by('id').values())==f.before)
  native.append({'name':label,'metadata':metadata,'audit':audit,'expected':expectation})
 report['native_cases']=native
 report['scope']='Actual Original no-recording worker79 plus Sourceproducer→nativezero-ruleSeries→OriginalAsyncResult; configured dictionary rules/foreign families remain byte-identical in FIFO. No native preflight earlyACK parity, configured rule execution, main/default intake or scheduler acceptance.'
 report['passed']=True
finally:
 f.cache.delete(f.lock);f.broker.flushdb();f.backend.flushdb()
 report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()]
 Path('/evidence/source-native-series-worker-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
