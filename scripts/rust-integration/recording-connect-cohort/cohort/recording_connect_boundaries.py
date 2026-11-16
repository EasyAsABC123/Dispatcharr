"""Actual Native-only Connect boundaries in the existing owned cohort.
Original Dispatcharr/dependency authors retain application/algorithm credit.
No application starts on import. Failed expectations stay failed.
"""
import hashlib,json,time,traceback,uuid
from pathlib import Path
import dvr_domain_cases as existing
CASE='recording-connect-boundaries'

def save(path,value):
 path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,indent=2,sort_keys=True,default=str)+'\n');path.chmod(0o600)

def prepare_context(f):
 assert f.backend=='Native'and f.case==CASE
 assert not any((f.case_data/'plugins').iterdir())
 f._plugin_environment={**getattr(f,'_plugin_environment',{}),'RUST_EXPERIMENTAL_SELECTED_BEAT':'0','RUST_PLUGIN_HOST_DIAGNOSTICS':'1'}
 f._connect_boundary_seed=f.sql("from apps.connect.models import Integration,EventSubscription;from apps.plugins.models import PluginConfig;assert not PluginConfig.objects.exists();i=Integration.objects.create(name='Owned boundary end Connect',type='webhook',enabled=True,config={'url':'http://origin:8080/control'});s=EventSubscription.objects.create(integration=i,event='recording_end',enabled=True,payload_template='{\"path\":\"/live/65499.ts\",\"action\":\"recover\"}');print(json.dumps({'integration':i.pk,'subscription':s.pk,'event':s.event}))",secret=True)

def state(f,backend_key=None):
 return {'SQL':existing.sql_state(f),'DVR_cache':existing.cache_state(f),'broker':existing.broker_state(f),'media':existing.media_state(f),'backend':None if backend_key is None else f.redis(2,"return redis.call('GET',ARGV[1])",backend_key)}

def ready(f):
 code,body=existing.health(f);w=body['selected_workers']
 assert code==200 and w['ready']is True and w['startup']=='Started'
 assert w['clocked_scheduler_enabled']is False and w['startup_recovery_deferred_unsupported_domain']is True
 assert w['dvr_plugin_domain']=='empty_directory_rechecked_before_recording_effects'
 assert f.redis(0,"return redis.call('GET','dvr:recover_dispatch_lock')")is None
 return {'actual_health_status':code,'selected_workers':w}

def run(f):
 assert f.backend=='Native'and f.case==CASE
 out=Path(f.output)/'recording-connect-boundaries';out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 report={'status':'running','cases':[],'scope':'Empty plugin directory with persisted end Connect. Negative API snapshots use isolated Beat0; no default Beat1/full parity inferred.','full_milestone_passed':False,'performance_verified':False}
 def record(name,value,passed):
  save(private/(name+'.json'),value);report['cases'].append({'name':name,'status':'passed'if passed else'failed'});save(out/'report.json',report)
 try:
  capability=ready(f);startup=f._recording_connect_boundary_startup_receipt
  assert startup['passed']and len(startup['actual_startup_frames'])==2
  assert {r['frame']['headers']['task']for r in startup['actual_startup_frames']}=={'apps.plugins.tasks.refresh_plugin_repos','core.tasks.check_for_version_update'}
  record('persisted-Connect-startup-two-roots-deferred',{'health':capability,'actual_startup':startup,'Source_three_root_parity':False},True)
  # Inert independent rows avoid production save signals before each baseline.
  seeded=f.sql("from django.utils import timezone;from datetime import timedelta;now=timezone.now();rows=Recording.objects.bulk_create([Recording(channel_id="+str(f.channel_id)+",start_time=now+timedelta(hours=1),end_time=now+timedelta(hours=2),custom_properties={'status':'recording'})for _ in range(2)]);print(json.dumps([r.pk for r in rows]))",secret=True)
  for label,method,path in [('inert-active-DELETE','DELETE','/api/channels/recordings/'+str(seeded[0])+'/')]:
   observer=existing.observer_start(f,label);before=state(f)
   try:
    response=f.api(method,path,{}if method=='POST'else None);after=state(f)
   finally:journal=existing.producer_observer_stop(f,observer)
   # Full raw journal preserved privately. Control PUBSUB/revokes are included
   # in addition to broker pushes/claims parsed by the existing observer.
   commands=[json.loads(line).get('command','')for line in Path(journal['raw_journal_path']).read_text().splitlines()]
   revoked=[c for c in commands if'revoke'in c.lower()and c.upper().startswith(('PUBLISH ','LPUSH ','RPUSH '))]
   passed=response['status']==501 and before==after and not journal['recording_recovery_offset_publications']and not journal['DVR_claim_or_ACK_commands']and not revoked
   record(label,{'before':before,'actual_HTTP':response,'after':after,'journal':journal,'actual_revoke_commands':revoked,'expected_status':501,'strict_equality':before==after,'actual_live_capture':False,'strict_inert_recording_status':True},passed)
  # Source stop emits no HTTP event. An inert row with no actual running task
  # legitimately returns200 and has no End to dispatch; never require501 here.
  inert_before=f.sql("print(json.dumps(list(Recording.objects.order_by('pk').values()),default=str))",secret=True)
  before_stop=state(f);stopped_inert=f.api('POST','/api/channels/recordings/'+str(seeded[1])+'/stop/',{});after_stop=state(f)
  inert_after=f.sql("print(json.dumps(list(Recording.objects.order_by('pk').values()),default=str))",secret=True)
  left={row['id']:row for row in inert_before};right={row['id']:row for row in inert_after};assert set(left)==set(right)
  expected=dict(left[seeded[1]]);props=dict(expected['custom_properties']);props.update(status='stopped',stopped_at=right[seeded[1]]['custom_properties'].get('stopped_at'));expected['custom_properties']=props
  valid=stopped_inert['status']==200 and isinstance(props.get('stopped_at'),str)and right[seeded[1]]==expected and all(right[k]==v for k,v in left.items()if k!=seeded[1])
  valid=valid and {k:v for k,v in before_stop['SQL'].items()if k!='dispatcharr_channels_recording'}=={k:v for k,v in after_stop['SQL'].items()if k!='dispatcharr_channels_recording'} and all(before_stop[k]==after_stop[k]for k in('DVR_cache','broker','media','backend'))
  record('inert-no-worker-stop-legacy200',{'actual_HTTP':stopped_inert,'before':before_stop,'after':after_stop,'rows_before':inert_before,'rows_after':inert_after,'Source_HTTP_event_expected':False,'real_running_Stop_End_parity':False},valid)
  pending=f.sql("from django.utils import timezone;from datetime import timedelta;now=timezone.now();rows=Recording.objects.bulk_create([Recording(channel_id="+str(f.channel_id)+",start_time=now+timedelta(hours=1),end_time=now+timedelta(hours=2),custom_properties={})for _ in range(2)]);print(json.dumps([r.pk for r in rows]))",secret=True)
  stopped=f.api('POST','/api/channels/recordings/'+str(pending[0])+'/stop/',{});deleted=f.api('DELETE','/api/channels/recordings/'+str(pending[1])+'/')
  logical=f.sql("print(json.dumps({'stop_status':Recording.objects.get(pk="+str(pending[0])+").custom_properties.get('status'),'deleted_absent':not Recording.objects.filter(pk="+str(pending[1])+").exists()}))",secret=True)
  record('pending-legacy-cleanup',{'actual_stop':stopped,'actual_DELETE':deleted,'rows':logical,'no_recording_end_invocation_expected':True},stopped['status']==200 and deleted['status']==204 and logical=={'stop_status':'stopped','deleted_absent':True})
  # Recovery is independent of inert-active-row API outcomes. Snapshot real current
  # state, not the pre-API baseline; never erase a failed producer observation.
  assert existing.broker_state(f)==[[],[],0,0]
  ident=str(uuid.uuid4());backend_key=f.sql("from celery import current_app;k=current_app.backend.get_key_for_task("+repr(ident)+");print(json.dumps(k.decode()if isinstance(k,bytes)else k))",secret=True);assert isinstance(backend_key,str)and backend_key.endswith(ident)
  observer=existing.observer_start(f,'recovery');before=state(f,backend_key);assert before['backend']is None
  made=f.sql("from apps.channels.tasks import recover_recordings_on_startup;t=recover_recordings_on_startup.apply_async(task_id="+repr(ident)+",queue='celery');print(json.dumps({'id':t.id}))",secret=True);assert made['id']==ident
  def blocked():
   code,body=existing.health(f);w=body.get('selected_workers',{})
   return {'status':code,'workers':w}if existing.domain_blocked(code,w,'celery')else False
  refusal=f.wait(blocked,20,'actual-Connect-recovery-beforeACK')
  raw=f.redis(1,"return redis.call('LRANGE','celery',0,64)");assert len(raw)==1 and json.loads(raw[0])['headers']['id']==ident
  after=state(f,backend_key);assert after['SQL']==before['SQL']and after['DVR_cache']==before['DVR_cache']and after['media']==before['media']and after['backend']==before['backend']and after['broker'][2:]==[0,0]
  for _ in range(3):time.sleep(min(1,f.remaining(1)));assert state(f,backend_key)==after
  stop=f.stop_native(expected=None);assert state(f,backend_key)==after
  terminal=existing.observer_stop(f,observer,'celery')
  removed=f.redis(1,"if redis.call('LLEN','celery')~=1 or redis.call('LINDEX','celery',0)~=ARGV[1]then return redis.error_reply('Owned recovery changed')end;return redis.call('LREM','celery',1,ARGV[1])",raw[0]);assert removed==1
  record('recovery-head-beforeACK',{'Source_producer':made,'actual_backend_key':backend_key,'before':before,'after':after,'raw_frame':raw[0],'raw_frame_sha256':hashlib.sha256(raw[0].encode()).hexdigest(),'typed_refusal':refusal,'stop':stop,'terminal_journal':terminal,'exact_owned_head_removed_after_stop':True},True)
  report['status']='passed'if all(c['status']=='passed'for c in report['cases'])else'failed'
 except BaseException as error:
  report.update(status='failed',error_type=type(error).__name__);save(private/'failure.json',{'traceback':traceback.format_exc()[-8192:]});raise
 finally:save(out/'report.json',report)
 return report

def active_delete_control(f,recording,*,actual_task_id):
 """One actual live-recording DELETE; critical fields only, media still grows.
 Called by ordinary event helper after actual recording status/file progress.
 """
 assert f.backend=='Native'
 assert isinstance(actual_task_id,str)and str(uuid.UUID(actual_task_id))==actual_task_id
 actual_backend_key=f.sql("from celery import current_app;k=current_app.backend.get_key_for_task("+repr(actual_task_id)+");print(json.dumps(k.decode()if isinstance(k,bytes)else k))",secret=True)
 assert isinstance(actual_backend_key,str)and actual_backend_key.endswith(actual_task_id)
 def current():
  return f.sql("r=Recording.objects.get(pk="+str(recording)+");print(json.dumps({'id':r.pk,'task_id':r.task_id,'channel_id':r.channel_id,'start_time':r.start_time,'end_time':r.end_time,'custom_properties':r.custom_properties},default=str))",secret=True)
 def files():
  result=[]
  for p in sorted((f.case_data/'recordings').rglob('*')):
   if p.is_file()and('.dvr_'+str(recording)+'_hls'in str(p)or p.name.startswith(str(recording))):result.append([str(p.relative_to(f.case_data)),p.stat().st_size])
  return result
 before=current();props=before['custom_properties'];assert props.get('status')=='recording'
 def event_rows():
  return f.sql("from core.models import SystemEvent;print(json.dumps(list(SystemEvent.objects.filter(event_type='recording_end',details__recording_id="+str(recording)+").order_by('id').values()),default=str))",secret=True)
 prior_events=event_rows();prior_backend=f.redis(2,"return redis.call('GET',ARGV[1])",actual_backend_key)
 media_before=files();assert media_before and sum(x[1]for x in media_before)>0
 response=f.api('DELETE','/api/channels/recordings/'+str(recording)+'/');after=current()
 save(Path(f.output)/'recording-connect-capture/private/active-DELETE-observed-before-assertion.json',{'before':before,'actual_HTTP':response,'after':after,'prior_end_events':prior_events,'prior_backend':prior_backend,'actual_claimed_task_id':actual_task_id,'actual_backend_key':actual_backend_key,'scheduled_row_marker':before['task_id']})
 assert response['status']==501
 assert event_rows()==prior_events
 assert f.redis(2,"return redis.call('GET',ARGV[1])",actual_backend_key)==prior_backend
 assert all(after[k]==before[k]for k in('id','task_id','channel_id','start_time','end_time'))
 assert after['custom_properties'].get('status')=='recording'
 assert after['custom_properties'].get('stopped_at')==props.get('stopped_at')
 def progressed():
  rows=files();return rows if sum(x[1]for x in rows)>sum(x[1]for x in media_before)else False
 media_after=f.wait(progressed,15,'actual-capture-continues-after-DELETE-refusal')
 value={'actual_HTTP':response,'actual_claimed_task_id':actual_task_id,'actual_backend_key':actual_backend_key,'scheduled_row_marker':before['task_id'],'before':before,'after':after,'media_before':media_before,'media_after':media_after,'whole_SQL_or_media_hash_equality_asserted':False,'continued_media_observed':True}
 save(Path(f.output)/'recording-connect-capture/private/active-DELETE-control.json',value);return value
