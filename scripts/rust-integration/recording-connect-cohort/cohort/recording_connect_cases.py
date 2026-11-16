"""Actual Source-first recording Connect evidence in the existing cohort.
Original Dispatcharr/dependency authors retain all application/algorithm credit.
No server, production helper replacement or runtime starts on import.
"""
import base64,datetime,hashlib,json,traceback
from pathlib import Path
import recording_plugin_cases as original
CASE=original.CASE
SUCCESS='Owned recording event webhook'
CAPTURE='Owned recording full payload capture'

def save(path,value):
 path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,indent=2,sort_keys=True,default=str)+'\n');path.chmod(0o600)

def obj(value):
 if isinstance(value,str):value=json.loads(value)
 if not isinstance(value,dict):raise TypeError('ActualPayloadObjectRequired')
 return value

def prepare_context(f):
 if f.case!=CASE or f.backend not in('Original','Native'):raise ValueError('OwnedRecordingConnectContextRequired')
 if f.backend=='Original':original.prepare_context(f)
 else:
  f._plugin_environment={**getattr(f,'_plugin_environment',{}),'RUST_PLUGIN_HOST_DIAGNOSTICS':'1'}
  assert not any((f.case_data/'plugins').iterdir())
  seeded=f.sql("from apps.plugins.models import PluginConfig;from apps.connect.models import Integration,EventSubscription;from core.models import CoreSettings;assert not PluginConfig.objects.exists();i=Integration.objects.create(name="+repr(SUCCESS)+",type='webhook',config={'url':'http://origin:8080/control'},enabled=True);rows=[]\nfor event in('recording_start','recording_end'):\n s=EventSubscription.objects.create(event=event,integration=i,enabled=True,payload_template='{\"path\":\"/live/65499.ts\",\"action\":\"recover\"}');rows.append({'id':s.pk,'event':event})\nd=dict(CoreSettings.get_dvr_settings());d['comskip_enabled']=False;s=CoreSettings.objects.get(key='dvr_settings');s.value=d;s.save();print(json.dumps(rows))",secret=True)
  f._recording_connect_success_seed=seeded
 # No-template request is an actual event dict. The same real Origin rejects
 # its missing control path/action with400; failure must not abort the business.
 f._recording_connect_capture_seed=f.sql("from apps.connect.models import Integration,EventSubscription;from core.models import CoreSettings,SystemEvent;i=Integration.objects.create(name="+repr(CAPTURE)+",type='webhook',config={'url':'http://origin:8080/control'},enabled=True);rows=[]\nfor event in('recording_start','recording_end'):\n s=EventSubscription.objects.create(event=event,integration=i,enabled=True,payload_template='');rows.append({'id':s.pk,'event':event})\ns=CoreSettings.objects.get(key='system_settings');s.value={**s.value,'max_system_events':3};s.save();old=SystemEvent.objects.bulk_create([SystemEvent(event_type='channel_start',details={'owned_history_sentinel':True,'ordinal':n})for n in range(6)]);print(json.dumps({'subscriptions':rows,'max_system_events':3,'old_history_sentinel_ids':[x.pk for x in old],'actual_caught_HTTP400_required':True}))",secret=True)
 save(Path(f.output)/'recording-connect-declared-inputs.json',{'backend':f.backend,'capture':f._recording_connect_capture_seed,'Native_plugin_installed':False,'Source_production_helpers_changed':False,'Native_outcomes':['stopped','completed'],'Source_outcomes':['stopped','completed'],'Origin_handler_changed':False})

def state(f):
 return f.sql("from apps.connect.models import DeliveryLog;from core.models import SystemEvent,CoreSettings;from apps.plugins.models import PluginConfig;print(json.dumps({'deliveries':list(DeliveryLog.objects.filter(subscription__integration__name__in="+repr([SUCCESS,CAPTURE])+").order_by('id').values('id','subscription__event','subscription__integration__name','status','request_payload','response_payload','created_at')),'audit':list(SystemEvent.objects.filter(event_type__in=['recording_start','recording_end']).order_by('id').values('id','event_type','channel_id','details')),'history_count':SystemEvent.objects.count(),'history_ids':list(SystemEvent.objects.order_by('id').values_list('id',flat=True)),'history_rows':list(SystemEvent.objects.order_by('-timestamp').values('id','timestamp','event_type','details')[:32]),'effective_max':CoreSettings.get_system_settings().get('max_system_events'),'recordings':list(Recording.objects.values()),'plugin_configs':PluginConfig.objects.count()},default=str))",secret=True)

def verify_history(f,s,*,require_old_removed=False):
 # Exact Source algorithm: cutoff queryset[max] remains because DELETE uses <,
 # so max3 retains FOUR rows. Do not silently change either implementation.
 maximum=s['effective_max'];assert maximum==3
 rows=s['history_rows'];ids=s['history_ids'];assert len(rows)==s['history_count']==len(ids)
 assert s['history_count']<=maximum+1
 if len(rows)>maximum:
  cutoff=rows[maximum]['id'];assert cutoff in ids and all(i>=cutoff for i in ids)
 if require_old_removed:
  assert not(set(f._recording_connect_capture_seed['old_history_sentinel_ids'])&set(ids))
  assert len(rows)==maximum+1
 return {'effective_max':maximum,'Source_offset_cutoff_retained':True,'observed_rows':rows,'old_sentinel_ids':f._recording_connect_capture_seed['old_history_sentinel_ids'],'old_removed':not(set(f._recording_connect_capture_seed['old_history_sentinel_ids'])&set(ids))}

def pg_sessions(f):
 return f.sql("with connection.cursor()as c:\n c.execute(\"SELECT pid,backend_start::text,client_addr::text,client_port,application_name,state FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid() ORDER BY pid\");rows=c.fetchall()\nprint(json.dumps([{'pid':r[0],'backend_start':r[1],'client_addr':r[2],'client_port':r[3],'application_name':r[4],'state':r[5]}for r in rows]))",secret=True)

def captures(s):
 rows=[r for r in s['deliveries']if r['subscription__integration__name']==CAPTURE]
 assert len(rows)<=4
 for r in rows:
  r['request_payload']=obj(r['request_payload']);response=obj(r['response_payload'])
  assert r['status']=='failed'and response.get('status_code')==400 and response.get('success')is False
  assert obj(response.get('body'))=={'error':'invalid fixture control'}
  assert len(json.dumps(r['request_payload'],default=str).encode())<=65536
 return rows

def successes(s):
 rows=[r for r in s['deliveries']if r['subscription__integration__name']==SUCCESS]
 for r in rows:
  assert r['status']=='success'and obj(r['response_payload']).get('status_code')==200
  assert obj(r['request_payload'])=={'path':'/live/65499.ts','action':'recover'}
 return rows

def normalized(payload,row,*,validate_files=None):
 p=dict(payload);rid=row['id'];assert p['recording_id']==rid and isinstance(p['channel_id'],str)
 p['recording_id']='<owned recording>';p['channel_id']='<owned channel UUID>'
 if 'stream_id'in p:
  assert isinstance(p['stream_id'],int)and not isinstance(p['stream_id'],bool);p['stream_id']='<owned selected stream>'
 if 'start_time'in p:
  for key in('start_time','end_time'):
   assert datetime.datetime.fromisoformat(p[key])==datetime.datetime.fromisoformat(row[key]);p[key]='<actual '+key+'>'
  props=row['custom_properties'];assert p['status']==props['status']and p['has_file']is True and p['remux_success']is True
  assert p['file_path']==props['file_path']and p['file_name']==Path(props['file_path']).name
  assert p['file_url']==f'/api/channels/recordings/{rid}/file/'
  assert p['bytes_written']==props['bytes_written']and isinstance(p['bytes_written'],int)and p['bytes_written']>0
  assert isinstance(p['file_size'],int)and p['file_size']>0
  if validate_files is not None:
   path=validate_files/Path(p['file_path']).relative_to('/data');assert path.is_file()and path.stat().st_size==p['file_size']
  for key in('file_path','file_name','file_url','file_size','bytes_written'):p[key]='<actual '+key+'>'
 return p

def verify_owned_projection(f,payload):
 # IDs are normalized only after resolving their actual current owned rows.
 observed=f.sql("from apps.channels.models import Channel,Stream;c=Channel.objects.get(pk="+str(f.channel_id)+");s=Stream.objects.filter(pk="+repr(payload.get('stream_id'))+").values('id','name','url','m3u_account__name').first()if "+repr(payload.get('stream_id'))+" is not None else None;print(json.dumps({'channel_uuid':str(c.uuid),'channel_name':c.name,'stream':s},default=str))",secret=True)
 assert payload['channel_id']==observed['channel_uuid']and payload['channel_name']==observed['channel_name']
 if 'stream_id'in payload:
  stream=observed['stream'];assert stream and payload['stream_name']==stream['name']and payload['stream_url']==stream['url']and payload['channel_url']==stream['url']
  assert payload.get('provider_name')==stream['m3u_account__name']
  cache=f.redis(0,"return {redis.call('GET',ARGV[1])or'',redis.call('GET',ARGV[2])or''}",'channel_stream:'+str(f.channel_id),'stream_profile:'+str(payload['stream_id']))
  save(Path(f.output)/'recording-connect-capture/private'/('actual-cache-'+str(payload['recording_id'])+('-end'if'status'in payload else'-start')+'.json'),cache)
  # This live observer runs after dispatch; expiry/retirement may already have
  # removed keys. Retain bytes as context, never fabricate a contemporaneous read.
  if cache[0]:assert int(cache[0])==payload['stream_id']
  if cache[1]:
   profile=f.sql("from core.models import StreamProfile;print(json.dumps(StreamProfile.objects.get(pk="+str(int(cache[1]))+").name))",secret=True);assert payload['profile_used']==profile
  else:
   assert f.sql("from core.models import StreamProfile;print(json.dumps(StreamProfile.objects.filter(name="+repr(payload['profile_used'])+").exists()))",secret=True)
 return observed

def source_reference(f):
 retained=f.binding.get('retained_source_recording_connect')
 if retained is not None:
  assert retained['reviewed_scope']=='Source44-extra-400-recording-event-capture'
  loaded={}
  for key in('report','payloads','execution','aggregate_report','cleanup'):
   reference=retained[key];path=Path(reference['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==reference['sha256'];loaded[key]=json.loads(path.read_text())
  e=loaded['execution'];assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
  assert loaded['aggregate_report']['exact_cleanup_passed']is True and loaded['cleanup']['all_owned_resources_absent']is True
  assert loaded['report']['status']=='passed'and loaded['report']['backend']=='Original'
  assert len(loaded['payloads'])==4
  return loaded['payloads'],{'retained':retained,'scope':'Actual qualified Source44 only; no Source replay/currentNative acceptance inferred'}
 candidates=list(Path(f.binding['fixture_output']).glob('Original-*/recording-connect-capture/report.json'))
 assert len(candidates)==1,'OneActualFreshSourceEventAuthorityRequired'
 report=json.loads(candidates[0].read_text());assert report['status']=='passed'and report['backend']=='Original'
 payloads=candidates[0].parent/'private/source-payloads.json';return json.loads(payloads.read_text()),{'path':str(payloads),'sha256':hashlib.sha256(payloads.read_bytes()).hexdigest()}

def source_contract_shape(payload):
 # Source42 dynamic owned values are separate from invariant payload semantics.
 p=dict(payload)
 for key in('recording_id','channel_id','stream_id','start_time','end_time','file_path','file_name','file_url','file_size','bytes_written'):
  if key in p:p[key]='<dynamic '+key+'>'
 return p

def public_shape_projection(payload):
 return {'keys':sorted(payload),'semantic_values':{k:payload[k]for k in('outcome','status','has_file','remux_success')if k in payload},'semantic_types':{k:type(payload[k]).__name__ for k in('outcome','status','has_file','remux_success')if k in payload}}

def source_run(f,out,report):
 result=original.run(f);assert result['status']=='passed'
 s=state(f);save(out/'private/source-final-SQL-before-validation.json',s);cap=captures(s);assert len(cap)==4 and len(successes(s))==4 and s['effective_max']==3
 verify_history(f,s)
 observed=original.observations(f);probe=observed['probe'];assert len(probe)==4
 payloads=[];contract=json.loads((Path(__file__).resolve().parents[1]/'public-shapes.json').read_text());assert len(contract['events'])==4
 for capture,witness,old in zip(cap,probe,contract['events']):
  p=capture['request_payload'];assert capture['subscription__event']==witness['payload']['event']==old['event']
  assert p==witness['payload']['actual_payload']
  row=next(r for r in s['recordings']if r['id']==p['recording_id']);verify_owned_projection(f,p);n=normalized(p,row,validate_files=f.case_data)
  assert public_shape_projection(p)=={'keys':old['keys'],'semantic_values':old['semantic_values'],'semantic_types':old['semantic_types']}
  payloads.append({'event':capture['subscription__event'],'recording_status':row['custom_properties']['status'],'normalized':n,'payload':p,'recording':row,'actual_action':witness})
 save(out/'private/history-cutoff.json',verify_history(f,s,require_old_removed=True))
 save(out/'private/source-payloads.json',payloads);save(out/'private/locally-collected-contract.json',{'events':[{'event':r['event'],'actual_payload':r['payload']}for r in payloads]});save(out/'private/final.json',{'SQL':s,'actual_actions':probe})
 report.update(status='passed',source_original_callback=result,capture400_count=4,original200_count=4,actual_actions=4,history_count=s['history_count'],Source42_common_contract_matched=True)


def broker_frames(host):
 p=host/'snapshot.json'
 if not p.exists():return []
 value=json.loads(p.read_text());assert not value['journal_error'];return value['frames']

def recording_frame(host,rid):
 matches=[]
 for row in broker_frames(host):
  frame=row['frame']
  if frame['headers']['task']!='apps.channels.tasks.run_recording':continue
  args=json.loads(base64.b64decode(frame['body']))[0]
  if args[0]==rid:matches.append(row)
 assert len(matches)<=1
 return matches[0]if matches else False

def _observe_claimed_start(f,flow,host,out,rid,ordinal):
 active=flow.wait(lambda:flow.active([rid]),30,'ActualNativeConnectRecordingActive')
 frame=f.wait(lambda:recording_frame(host,rid),20,'ActualClaimedRecordingPublication');task_id=frame['frame']['headers']['id']
 save(out/'private'/('publication-'+str(ordinal)+'.json'),frame)
 def start_capture():
  sample=state(f);save(out/'private'/('start-live-'+str(ordinal)+'.json'),sample)
  matches=[r for r in captures(sample)if r['request_payload']['recording_id']==rid and r['subscription__event']=='recording_start']
  assert len(matches)<=1
  if not matches:return False
  audit=[a for a in sample['audit']if a['event_type']=='recording_start'and a['details'].get('recording_id')==rid]
  assert len(audit)==1
  return {'capture':matches[0],'audit':audit[0],'history_ids':sample['history_ids']}
 start_event=f.wait(start_capture,20,'ActualNativeStartPayloadLive')
 save(out/'private'/('start-witness-'+str(ordinal)+'.json'),start_event)
 return {'rid':rid,'active':active,'frame':frame,'task_id':task_id,'start_event':start_event}

def _book_warm_successor(f,flow,out,first,observe):
 # A real end delivery/audit/row/backend barrier precedes this publication.
 assert first['end_capture']['subscription__event']=='recording_end'
 assert first['end_capture']['request_payload']['recording_id']==first['row']['id']
 assert first['end_audit']['details']['recording_id']==first['row']['id']
 assert first['backend']['status']=='SUCCESS'and first['backend']['task_id']==first['task_id']
 save(out/'private/first-pair-before-next-book.json',first)
 stream=first['end_capture']['request_payload']['stream_id']
 aliases=f.redis(0,"return {redis.call('GET',ARGV[1])or'',redis.call('GET',ARGV[2])or''}",'channel_stream:'+str(f.channel_id),'stream_profile:'+str(stream))
 save(out/'private/actual-warm-assignment-before-book.json',{'aliases':aliases,'source':'Actual owned raw cache; no writes or artificial grace','required_stream_id':stream})
 assert aliases[0]and int(aliases[0])==stream and aliases[1],'ActualWarmSuccessorAssignmentMissingBeforeBook'
 from workflow import utc
 start=utc()+datetime.timedelta(seconds=3)
 rid=flow.book(start,start+datetime.timedelta(seconds=18),'Owned recording event completed')
 following=observe(rid,1)
 save(out/'private/actual-second-start-before-first-validation.json',following)
 assert following['start_event']['capture']['request_payload'].get('stream_id')==stream,'ActualSecondStartWarmPreconditionMissing'
 return following

def native_run(f,out,report):
 from workflow import Workflow,utc
 from dvr_domain_cases import observer_start,producer_observer_stop
 assert not any((f.case_data/'plugins').iterdir())and state(f)['plugin_configs']==0
 code,raw=f.get(f.base_url+'/health/ready');ready=json.loads(raw)['selected_workers']
 assert code==200 and ready['ready']is True and ready['startup']=='Started'and ready['startup_recovery_deferred_unsupported_domain']is True
 assert ready['dvr_plugin_domain']=='empty_directory_rechecked_before_recording_effects'
 startup=f._recording_connect_startup_receipt
 assert startup['passed']is True and startup['actual_startup_root_count']==2
 assert startup['DVR_domain_capability']['recovery_root_published']is False
 prefix=f.sql("from celery import current_app;k=current_app.backend.get_key_for_task('owned-event-prefix-sentinel');k=k.decode()if isinstance(k,bytes)else k;print(json.dumps(k[:-len('owned-event-prefix-sentinel')]))",secret=True)
 save(out/'private/startup.json',{'ready':ready,'actual_startup_receipt':startup,'recovery_deferred_no_fake_root':True})
 try:expected,authority=source_reference(f);authority['qualified']=True
 except (AssertionError,OSError,ValueError,KeyError)as error:
  expected=None;authority={'qualified':False,'failure_type':type(error).__name__};save(out/'private/Source-authority-failure.json',{'traceback':traceback.format_exc()[-8192:]})
 report['fresh_Source_authority']=authority
 legacy_contract=json.loads(Path(f.binding['local_source_contract']).read_text())
 flow=Workflow(f,out/'native-flow');events=[];origin_before=json.loads(f.get(f.origin+'/stats')[1]);monitor=observer_start(f,'actual-recording-frame',database=1);name,host=monitor;primary_error=None
 try:
  following=None
  for ordinal,label in enumerate(('stopped','completed')):
   if following is None:
    start=utc()+datetime.timedelta(seconds=3);rid=flow.book(start,start+datetime.timedelta(seconds=90),'Owned recording event '+label)
    observed=_observe_claimed_start(f,flow,host,out,rid,ordinal)
   else:observed=following;following=None
   rid=observed['rid'];active=observed['active'];frame=observed['frame'];task_id=observed['task_id'];start_event=observed['start_event']
   verify_owned_projection(f,start_event['capture']['request_payload'])
   f.wait_for_hls_segments([rid],2,30)
   if label=='stopped':
    from recording_connect_boundaries import active_delete_control
    guard=active_delete_control(f,rid,actual_task_id=task_id);save(out/'private/active-DELETE-guard.json',guard)
    code,value=flow.request('POST','/api/channels/recordings/'+str(rid)+'/stop/',{});save(out/'private/claimed-Stop-response.json',{'status':code,'body':value,'actual_claimed_task_id':task_id})
    assert code==200
   flow.wait(lambda:flow.terminal(rid,label),90,'ActualNativeClaimedOutcome-'+label)
   def deliveries_complete():
    current=state(f);rows=[r for r in captures(current)if r['request_payload']['recording_id']==rid]
    return current if len(rows)==2 else False
   current=f.wait(deliveries_complete,20,'ActualNativeCapture400AndContinuation');save(out/'private'/('completion-SQL-before-validation-'+str(ordinal)+'.json'),current)
   pair=[r for r in captures(current)if r['request_payload']['recording_id']==rid];assert[r['subscription__event']for r in pair]==['recording_start','recording_end']
   end_audit=[r for r in current['audit']if r['details'].get('recording_id')==rid and r['event_type']=='recording_end'];assert len(end_audit)==1
   audit=[start_event['audit'],end_audit[0]];assert audit[0]['id']!=audit[1]['id']
   row=next(r for r in current['recordings']if r['id']==rid)
   def actual_terminal_backend():
    raw=f.redis(2,"return redis.call('GET',ARGV[1])",prefix+task_id)
    if not raw:return False
    value=json.loads(raw)
    return value if value.get('status')in('SUCCESS','FAILURE','REVOKED')else False
   meta=f.wait(actual_terminal_backend,30,'ActualNativeRecordingBackendResult');assert meta['status']=='SUCCESS'and meta['task_id']==task_id and meta['result']is None and meta['traceback']is None
   assert meta['children']==[]
   if ordinal==0:
    first={'row':row,'start_event':start_event,'end_capture':next(e for e in pair if e['subscription__event']=='recording_end'),'end_audit':end_audit[0],'task_id':task_id,'actual_request_frame':frame,'backend':meta}
    following=_book_warm_successor(f,flow,out,first,lambda next_rid,next_ordinal:_observe_claimed_start(f,flow,host,out,next_rid,next_ordinal))
   # Expensive validation/decoding follows the actual next start witness.
   flow.decode(rid)
   for event in pair:
    p=event['request_payload'];verify_owned_projection(f,p);value=normalized(p,row,validate_files=f.case_data)
    if expected is not None:
     source=next(e for e in expected if e['event']==event['subscription__event']and e['recording_status']==label)
     assert value==source['normalized']
    old_index=ordinal*2+(0 if event['subscription__event']=='recording_start'else 1)
    assert source_contract_shape(p)==source_contract_shape(legacy_contract['events'][old_index]['actual_payload'])
    events.append({'event':event['subscription__event'],'recording_id':rid,'task_id':task_id,'payload':p,'normalized':value})
   settled=state(f);save(out/'private'/('SQL-after-terminal-'+str(ordinal)+'.json'),settled)
   verify_history(f,settled)
   save(out/'private'/('completion-'+str(ordinal)+'.json'),{'SQL_before_terminal':current,'SQL_after_terminal':settled,'actual_request_frame':frame,'backend':meta,'events':events[-2:],'recording_serializer_marker':active[0]['task_id'],'observed_audit_pair_before_later_trim':audit})
  final=state(f);assert len(captures(final))==4 and len(successes(final))==4 and final['plugin_configs']==0
  witness=producer_observer_stop(f,monitor);monitor=None
  assert witness['terminal']['database_filter']==1 and witness['terminal']['complete_all_database_journal']is False
  task_ids={e['task_id']for e in events};assert len(task_ids)==2
  own=[r for r in witness['publications']if r['id']in task_ids];assert len(own)==2 and all(r['task']=='apps.channels.tasks.run_recording'for r in own)
  assert not[r for r in witness['publications']if r['raw_frame']['headers'].get('parent_id')in task_ids and r['id']not in task_ids]
  final_media=flow.wait(lambda:retired(flow),20,'ActualNativeRecordingConnectPhysicalRetirement')
  origin_after=json.loads(f.get(f.origin+'/stats')[1]);assert origin_after['fault_requests']-origin_before['fault_requests']==4
  history=verify_history(f,final,require_old_removed=True);before_pg=pg_sessions(f);save(out/'private/PG-before-Main-stop.json',before_pg)
  stop=f.stop_native(expected=0)
  host_log=f.cli[-2];save(out/'private/Main-stop-raw-log-receipt.json',host_log)
  remaining_pg=f.wait(lambda:{'rows':rows}if not(rows:=pg_sessions(f))else False,10,'ActualOwnedDatabaseClientSessionsAbsent')
  save(out/'private/PG-after-Main-stop.json',remaining_pg)
  save(out/'private/final.json',{'SQL':final,'events':events,'broker':witness,'media':final_media,'normal_Main_stop':stop,'history_cutoff':history,'PG_before':before_pg,'PG_after':remaining_pg,'PG_attribution':'Fresh owned DB excludes the observing management connection; no independent pool-size claim','per_Host_EOF_independently_qualified':False,'per_event_SQL_session_receipts_independently_qualified':False,'Host_raw_stderr_receipt':host_log})
  report.update(status='passed'if expected is not None else'failed',fresh_Source_capture_qualified=expected is not None,Native_own_claimed_outcome_subcells_passed=True,natural_completions=1,actual_claimed_Stop200=1,claimed_start_end_events=4,capture400_count=4,original200_count=4,history_count=final['history_count'],Native_empty_plugin_Connect_slice=True)
 except BaseException as error:
  primary_error=error
  save(out/'private/primary-event-failure.json',{'type':type(error).__name__,'traceback':traceback.format_exc()[-8192:]})
  raise
 finally:
  if monitor is not None:
   try:save(out/'private/failed-monitor-final.json',producer_observer_stop(f,monitor))
   except BaseException as error:
    save(out/'private/monitor-cleanup-failure.json',{'type':type(error).__name__,'traceback':traceback.format_exc()[-8192:],'primary_failure_preserved':primary_error is not None})
    if primary_error is None:raise

def retired(flow):
 s=flow.snapshot('recording-connect-physical-'+__import__('uuid').uuid4().hex[:6])
 return s if s['all_owned_recording_children_retired']and s['owned_hls_directories_absent']and s['actual_recording_ffmpeg_count']==0 and s['actual_origin']['active']==0 else False

def run(f):
 if f.case!=CASE:raise ValueError('OwnedEventContextRequired')
 out=Path(f.output)/'recording-connect-capture';out.mkdir();(out/'private').mkdir(mode=0o700)
 report={'status':'running','backend':f.backend,'helper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'full_milestone_passed':False,'performance_verified':False,'active_plugin_Native_recording_accepted':False}
 save(out/'report.json',report)
 try:
  if f.backend=='Original':source_run(f,out,report)
  elif f.backend=='Native':native_run(f,out,report)
  else:raise ValueError('ActualSourceOrNativeRequired')
 except BaseException as error:
  report.update(status='failed',error_type=type(error).__name__);save(out/'private/failure.json',{'traceback':traceback.format_exc()[-8192:]});raise
 finally:save(out/'report.json',report)
 return report
