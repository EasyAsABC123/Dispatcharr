"""Guarded Source/native typed exhaustion comparison and safety controls.
Original Dispatcharr authors retain full application/algorithm credit and AGPL.
"""
import hashlib,json,os,subprocess
from pathlib import Path

def native(context,sid,ident,operational=False):
 role=context.get('terminal_role')or'dispatch'
 value={'action':'run','source_id':sid,'task_id':ident,'database_url':'postgres://'+role+':playback-fixture-only@127.0.0.1:5432/'+os.environ['POSTGRES_DB'],'database_role':role,'close_reporter_before_run':context.get('native_reporter_closed',False),'cache_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND'],'expect_operational':operational}
 result=subprocess.run(['/evidence/native-refresh-worker','--ignored','--exact','refresh_worker::peer::native_refresh_worker_peer','--nocapture'],input=json.dumps(value),text=True,capture_output=True,timeout=40)
 with Path('/evidence/native-exhaustion-worker-private.log').open('a')as log:log.write(result.stdout+result.stderr)
 result.check_returncode();return json.loads(next(line.split('NATIVE_REFRESH_WORKER_JSON=',1)[1]for line in result.stdout.splitlines()if 'NATIVE_REFRESH_WORKER_JSON='in line))

def compare(context):
 sid=context['sid'];source=context['EPGSource'];index=context['EPGSourceIndex'];case=context['report']['cases'][-1];broker=context['broker'];backend=context['backend'];app=context['app'];cache=context['cache'];check=context['check'];label=context['label']
 source.objects.filter(pk=sid).update(status='idle',last_message=None)
 index.objects.filter(source_id=sid).update(data={'owned':'before'})
 broker.flushdb();backend.flushdb()
 issued=context['task'].apply_async(args=[sid],kwargs=context['kwargs'])
 audit=native(context,sid,issued.id)
 raw=backend.get(app.backend.get_key_for_task(issued.id));meta=json.loads(raw);actual_state=list(source.objects.filter(pk=sid).values('status','last_message','custom_properties'))
 check('native-exhaustion-Source-AsyncResult-'+label,app.AsyncResult(issued.id).get(timeout=2) is None and meta['status']=='SUCCESS'and meta['result']is None and meta['traceback']is None and meta['children']==[])
 check('native-exhaustion-exact-row-effects-'+label,actual_state==case['source_state']and index.objects.get(source_id=sid).data is None)
 check('native-exhaustion-original-busy-state-'+label,(cache.get(context['file_key']).decode()if cache.get(context['file_key'])else None)==case['file_lock']and(cache.get(context['parse_key']).decode()if cache.get(context['parse_key'])else None)==case['parser_counter']and cache.get(context['refresh_key'])is None)
 check('native-exhaustion-owner-drain-'+label,audit['caller_released']and audit['worker_drained']and audit['service_drained']and audit['transport_drained']and audit['physical_jobs']==0 and audit['default_dispatch_calls']==0 and audit['calls']==1)
 check('native-exhaustion-result-identity-TTL-'+label,meta['task_id']==issued.id and'parent_id'not in meta and 3590<=backend.ttl(app.backend.get_key_for_task(issued.id))<=3600 and 3590<=case['result_ttl']<=3600)
 context['report'].setdefault('native_cases',[]).append({'name':label,'source_id':sid,'source_task_id':case['metadata']['task_id'],'metadata':meta,'metadata_sha256':hashlib.sha256(raw).hexdigest(),'source_state':actual_state,'audit':audit})

def controls(context):
 # Reuse two owned normal fixtures, never production rows or arbitrary names.
 file_case=next(c for c in context['report']['cases']if c['name']=='exhausted_file');parser_case=next(c for c in context['report']['cases']if c['name']=='exhausted_parser')
 file_id=file_case['source_id'];parser_id=parser_case['source_id'];broker=context['broker'];backend=context['backend'];cache=context['cache'];app=context['app'];source=context['EPGSource'];check=context['check']
 broker.flushdb();backend.flushdb()
 resource=subprocess.run(['/evidence/native-exhaustion-resources','--ignored','--exact','native_file_job_exhaustion_resources','--nocapture'],input=json.dumps({'file_id':file_id,'parser_id':parser_id}),text=True,capture_output=True,timeout=60)
 Path('/evidence/native-exhaustion-resources.log').write_text(resource.stdout+resource.stderr);resource.check_returncode()
 audit=json.loads(next(line.split('NATIVE_EXHAUSTION_RESOURCE_JSON=',1)[1]for line in resource.stdout.splitlines()if 'NATIVE_EXHAUSTION_RESOURCE_JSON='in line))
 check('native-concurrent-reasons-and-physical-ownership-controls',audit['concurrent_reason_isolation']and audit['held_report_caller_abort']and audit['held_physical_release_reply_abort']and audit['replacement_release_typed_notowner']and audit['legacy_notowner_error_preserved']and audit['guarded_server_SQL_observed']and audit['reporter_stopping_no_typed_result']and audit['terminal_pool_acquire_no_typed_result']and not audit['aborted_waiter_return_observed']and audit['owned_jobs_drained'])
 context['report']['native_resources']=audit
 # Coarse failures and native stopped admission are not caught SQL authority.
 for label in ['coarse_counter','native_reporter_closed']:
  sid=file_id;source.objects.filter(pk=sid).update(status='idle',last_message=None);context['EPGSourceIndex'].objects.filter(source_id=sid).update(data={'owned':'before'});cache.delete('task_lock_refresh_epg_data_'+str(sid));cache.set('task_lock_epg_source_file_'+str(sid),'owned-busy',ex=300);cache.delete(context['tasks']._source_tvg_parse_count_key(sid))
  if label=='coarse_counter':cache.set(context['tasks']._source_tvg_parse_count_key(sid),'not-an-integer',ex=300)
  context['native_reporter_closed']=label=='native_reporter_closed'
  try:
   broker.flushdb();backend.flushdb();issued=context['task'].apply_async(args=[sid],kwargs={'force':True,'_file_defer_retry':480});audit=native(context,sid,issued.id,True)
   check('native-operational-control-no-fake-success-'+label,backend.get(app.backend.get_key_for_task(issued.id))is None and app.AsyncResult(issued.id).state=='PENDING'and cache.get('task_lock_refresh_epg_data_'+str(sid))is None and audit['physical_jobs']==0)
   context['report'].setdefault('native_operational_controls',[]).append({'name':label,'audit':audit,'result_state':'PENDING','source_id':sid})
  finally:
   context.pop('native_reporter_closed',None)
   for key in ['task_lock_epg_source_file_'+str(sid),context['tasks']._source_tvg_parse_count_key(sid)]:cache.delete(key)
