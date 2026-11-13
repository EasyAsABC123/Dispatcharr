"""Predeclared finite normal DVR acceptance; every raw input remains retained.

Source3 is a one-case authority, never an old frame to replay. Exact own-model
IDs, task UUIDs and clocks below bind each fresh real model/Beat context. No
raw media hash equality or general cache decoder/stop/fault parity is inferred.
"""
import base64,datetime,hashlib,json,math,uuid
from pathlib import Path
def require(ok,label):
 if not ok:raise ValueError(label)
def exact(a,b):
 if type(a)is not type(b):return False
 if type(a)is dict:return set(a)==set(b)and all(exact(a[k],b[k])for k in a)
 if type(a)is list:return len(a)==len(b)and all(exact(x,y)for x,y in zip(a,b))
 return a==b
def utc(value):
 require(type(value)is str,'ActualClockString')
 parsed=datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
 if parsed.tzinfo is None:parsed=parsed.replace(tzinfo=datetime.timezone.utc)
 return parsed.astimezone(datetime.timezone.utc)
def integer(value,lo,hi):return type(value)is int and lo<=value<=hi
def by_id(rows,ident):
 found=[row for row in rows if row['id']==ident];require(len(found)==1,'OneActualModelIdentity');return found[0]
def cache_map(rows):
 require(type(rows)is list and len(rows)<=256,'BoundedOwnedCacheRows')
 result={}
 for row in rows:
  key=base64.b64decode(row['key_base64'],validate=True);require(key not in result,'UniqueRawCacheKey');result[key]=row
 return result
def cache_relation(before,after):
 a=cache_map(before);b=cache_map(after)
 return {'added':sorted(key.hex()for key in b.keys()-a.keys()),'removed':sorted(key.hex()for key in a.keys()-b.keys()),'existing':{key.hex():{'type_before':a[key]['type'],'type_after':b[key]['type'],'DUMP_changed':a[key]['DUMP_base64']!=b[key]['DUMP_base64']}for key in sorted(a.keys()&b.keys())}}
def clock_inventory(value,path='$'):
 """Retain, identify and parse every date-shaped string; do not strip fields."""
 result=[]
 if type(value)is dict:
  for key,item in value.items():result.extend(clock_inventory(item,path+'.'+key))
 elif type(value)is list:
  for index,item in enumerate(value):result.extend(clock_inventory(item,path+f'[{index}]'))
 elif type(value)is str and len(value)>=19 and value[4:5]=='-'and value[7:8]=='-'and value[10:11]in('T',' '):
  result.append({'path':path,'raw':value,'parsed_UTC':utc(value).isoformat()})
 return result
def validate_cleanup_sql(proxy,periodic,recording,clocked,begin,end):
 require(proxy['errors']==[]and proxy['held']is None and proxy['listener_alive']is False,'ActualNoHoldRelaySettled')
 require(0<len(proxy['connections'])<=32 and all(row['closed']is row['up_joined']is True for row in proxy['connections']),'EveryActualRelayConnectionNaturallyClosed')
 events=proxy['events'];require(len(events)<=6000 and not any(row['kind']in('ready-held','ready-forwarded','relay-exception')for row in events),'NormalObserverNoHoldOrError')
 require(all(type(row['epoch'])in(int,float)and type(row['monotonic'])in(int,float)and math.isfinite(row['epoch'])and math.isfinite(row['monotonic'])and row['epoch']>0 and row['monotonic']>=0 for row in events),'TypedFiniteActualProxyEventClocks')
 require([row['monotonic']for row in events]==sorted(row['monotonic']for row in events),'ActualProxyEventCausalOrder')
 lookup='SELECT ident::bigint FROM django_celery_beat_periodictasks WHERE ident=1 FOR UPDATE'
 touch='UPDATE django_celery_beat_periodictasks SET last_update=$1 WHERE ident=1'
 epg='UPDATE epg_epgsource SET refresh_task_id=NULL WHERE refresh_task_id=$1'
 m3u='UPDATE m3u_m3uaccount SET refresh_task_id=NULL WHERE refresh_task_id=$1'
 delete_periodic='DELETE FROM django_celery_beat_periodictask WHERE id=$1 AND name=$2 AND clocked_id=$3'
 delete_clocked='DELETE FROM django_celery_beat_clockedschedule WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE clocked_id=$1)'
 unused='SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE clocked_id=$1)'
 expected=['BEGIN','SAVEPOINT native_dvr_predelete',lookup,touch,'RELEASE SAVEPOINT native_dvr_predelete',epg,m3u,delete_periodic,'COMMIT','BEGIN',lookup,touch,'COMMIT',unused,'BEGIN',delete_clocked,'SAVEPOINT native_dvr_clocked_postdelete',lookup,touch,'RELEASE SAVEPOINT native_dvr_clocked_postdelete','COMMIT']
 selected_indices=[index for index,row in enumerate(events)if row['kind']=='execute'and row['sql']in set(expected)]
 selected=[events[index]for index in selected_indices]
 require([row['sql']for row in selected]==expected,'ThreeActualTransactionGroupsAndAllTwentyOneCleanupStatements')
 require(len({row['connection']for row in selected})==1,'SameActualCleanupConnection')
 connection=selected[0]['connection'];first=selected_indices[0];last=selected_indices[-1];require([row['sql']for row in events[first:last+1]if row['kind']=='execute'and row['connection']==connection]==expected,'NoUnlistedCleanupConnectionExecutionBetweenFirstBeginAndFinalCommit')
 def binary_int(value):
  raw=bytes.fromhex(value);require(len(raw)==8,'ActualSQLxBigintParameter');return int.from_bytes(raw,'big',signed=True)
 updates=[row for row in selected if row['sql']==touch]
 observed=[]
 for update in updates:
  parameters=update['parameter_hex'];require(type(parameters)is list and len(parameters)==1,'ActualTrackingTimestampParameter')
  stamp=datetime.datetime(2000,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=binary_int(parameters[0]));require(begin<=stamp<=end,'OwnThreeTrackingTouchClockWindows');observed.append(stamp)
 require(observed==sorted(observed),'ActualTrackingTouchOrder')
 for row in selected:
  if row['sql']in(epg,m3u,delete_clocked,unused):require([binary_int(v)for v in row['parameter_hex']]==[clocked if row['sql']in(delete_clocked,unused)else periodic],'ActualForeignKeyAndClockedIdentity')
  if row['sql']==delete_periodic:require(binary_int(row['parameter_hex'][0])==periodic and bytes.fromhex(row['parameter_hex'][1]).decode()=='dvr-recording-'+str(recording)and binary_int(row['parameter_hex'][2])==clocked,'ActualPeriodicDeleteAllParameters')
 for query,command in[(epg,'UPDATE 0'),(m3u,'UPDATE 0'),(delete_periodic,'DELETE 1'),(delete_clocked,'DELETE 1')]:
  frames=[row for row in events if row['kind']=='actual-target-backend-frame'and row['sql']==query and row['kind_hex']=='43'];require(len(frames)==1 and bytes.fromhex(frames[0]['body_hex'])==command.encode()+b'\0','ActualBackendCommandComplete:'+command)
 require(not any(row['kind']=='actual-target-backend-frame'and row['kind_hex']=='45'for row in events),'NoUnobservedNormalCleanupError')
 return {'actual_native_cleanup_groups':3,'actual_tracking_updates':3,'actual_FK_SET_NULL_updates':2,'actual_periodic_and_clocked_deletes':2,'actual_tracking_UTC':[x.isoformat()for x in observed],'Source_grouping_authority':'pinned models/signals code plus actual Source3 query order; Source3 wrapper did not capture BEGIN/COMMIT packets','raw_native_PG_events_retained':True}
def validate_native_capture(actual_exit,actual,reference_path):
 reference_bytes=Path(reference_path).read_bytes()
 require(hashlib.sha256(reference_bytes).hexdigest()=='323c396c00b88fa48459a61a63799c5f2529919554b529778aa3b7a221b0a7b8','FrozenWholeSource3Authority')
 source=json.loads(reference_bytes)
 require(type(actual_exit)is int and actual_exit==0,'ActualGuest0')
 require(actual['passed']is True and actual['Source_verified']is False and actual['Native_verified']is True and actual['performance_verified']is False,'ScopedMixedExecutionOnly')
 require(actual['application_task_model_signal_methods_replaced']is False and actual['actual_routing_never_overridden']is True,'RealSourceProducerNoMethodsReplaced')
 require(actual['all_HTTP_attempts']==[]and actual['finally_errors']==[],'NoPythonProviderAttemptOrFinallyError')
 require(all(row['passed']is True for row in actual['checks']),'AllActualChecksLiteralTrue')
 seed=actual['seed'];ident=seed['Recording_id'];channel=seed['channel_id'];task=actual['recording_dispatch']['frame']['headers']['id']
 require(str(uuid.UUID(task))==task and uuid.UUID(task).version==4,'ActualCanonicalTaskV4')
 headers=actual['recording_dispatch']['frame']['headers'];frame=actual['recording_dispatch']['frame']
 require(headers['task']=='apps.channels.tasks.run_recording'and headers['root_id']==task and headers['parent_id']is None,'ActualRootSelfParentNull')
 require(frame['properties']['delivery_info']=={'exchange':'','routing_key':'dvr'},'NaturalDVRRoute')
 require(actual['recording_dispatch']['decoded_body'][0]==[ident,channel,seed['start_time'],seed['end_time']]and actual['recording_dispatch']['decoded_body'][1]=={},'ActualFourModelArguments')
 require(actual['setup']['files']['exists']is actual['after_beat']['files']['exists']is False and actual['recordings_root_context_before_native']['preprovisioned_by_fixture']is False,'ActualSource3EquivalentInitialMissingRootNoFixtureMkdir')
 native=actual['native_dvr'];require(native['recording_root_before_admission']['exists']is False and actual['final']['files']['exists']is True,'ActualNativeFreshRootCreatedAtRuntime')
 require(native['capture_only']is native['capture_complete']is native['passed']is True and native['Source_task_execution']is native['Main_execution']is False,'CompleteActualNativePeer')
 require(native['actual_run']['kind']=='Completed'and native['actual_run']['task']==task and native['actual_run']['metadata_written']is True,'ActualNormalLibraryCompletion')
 begin=utc(native['command_began_utc']);end=utc(native['command_returned_utc']);require(begin<=end,'ActualNativeCommandInterval')
 require(utc(actual['native_command_began_utc'])<=begin<=end<=utc(actual['native_command_finished_utc']),'ActualOuterAndInnerCommandIntervals')
 require(native['drain']['ok']is True and native['diagnostics_after_drain']['available_slots']==1 and native['caller_weak_alive_after_run']is False,'ActualWorkerRetirement')
 require(native['event_connections_before']==native['event_connections_after']and native['event_bytes_before']==native['event_bytes_after']and native['pool_size_after_explicit_close']==0,'ActualSinkAndSQLRetirement')
 processes=native['actual_run']['processes'];require(type(processes)is list and len(processes)==2,'ActualCaptureAndRemuxOnly')
 for process,code in zip(processes,[255,0]):
  require(type(process['code'])is int and process['code']==code and process['signal']is None and process['emergency']is False,'ObservedNormalCaptureRemuxExitCodes')
  require(all(process[key]is None for key in ['reader_error','command_error','wait_error','kill_error']),'NoProcessSettlementError')
  identity=process['identity'];require(integer(process['pid'],1,2**31-1)and identity['pid']==identity['pgid']==process['pid']and integer(identity['start_ticks'],1,2**63-1),'ActualOwnedChildIdentity')
  require(begin<=utc(process['started_utc'])<=utc(process['settled_utc'])<=end,'ActualChildWaitCommandWindow')
  require(process['stderr']['read_error']is None and integer(process['stderr']['total_bytes'],0,2**63-1)and len(process['stderr']['tail'])<=65536,'DrainedBoundedStderrReceipt')
 source_processes=[row for row in source['all_subprocess']if row['argv'][0]=='ffmpeg']
 require(len(source_processes)==2,'TwoActualSourceFFmpegCommands')
 for process,expected_process in zip(processes,source_processes):
  expected_argv=[arg.replace(source['seed']['channel_uuid'],seed['channel_uuid']).replace('recording-'+str(source['seed']['Recording_id']),'recording-'+str(ident)).replace('.dvr_'+str(source['seed']['Recording_id'])+'_hls','.dvr_'+str(ident)+'_hls')for arg in expected_process['argv']]
  require(process['argv']==expected_argv,'ActualSourceFixedArgumentsWithOwnModelIdentity')
 before=by_id(actual['after_beat']['recordings'],ident);final=by_id(actual['final']['recordings'],ident);expected=source['final']['recordings'][0]
 require(set(final)==set(expected),'AllRecordingColumnsPresent')
 for key in final:
  if key!='custom_properties':require(exact(final[key],before[key]),'StableRecordingColumn:'+key)
 require(final['task_id']=='dvr-recording-'+str(ident),'PeriodicNameNotCeleryUUID')
 props=final['custom_properties'];source_props=expected['custom_properties']
 require(set(props)==set(source_props),'AllSourceNormalPropertyFields')
 variables={'started_at','ended_at','bytes_written','poster_logo_id','file_url','output_file_url'}
 for key in props:
  if key not in variables:require(exact(props[key],source_props[key]),'SourceNormalProperty:'+key)
 require(props['poster_logo_id']==seed['VOD_logo_id'],'ActualLocalPosterIdentity')
 require(props['file_url']==props['output_file_url']==f'/api/channels/recordings/{ident}/file/','ActualRecordingFileURL')
 require(integer(props['bytes_written'],1,64*1024*1024)and props['bytes_written']==native['actual_run']['bytes_written'],'ActualHLSSumDistinctFromMKV')
 started=utc(props['started_at']);ended=utc(props['ended_at']);scheduled_end=utc(seed['end_time'])
 require(begin<=started<=ended<=end and ended>=scheduled_end,'OwnWallClockStartedEndedWithinActualCommand')
 for key,value in before['custom_properties'].items():
  if key!='status':require(exact(props[key],value),'PreserveEveryPreviouslyOwnedProperty:'+key)
 metadata=actual['recording_task']['raw_result']['metadata'];require(set(metadata)==set(source['recording_task']['raw_result']['metadata']),'FullBackendFieldPresence')
 require(metadata['status']=='SUCCESS'and metadata['result']is None and metadata['traceback']is None and metadata['children']==[]and metadata['task_id']==task,'TypedSourceNormalBackendMetadata')
 require(begin<=utc(metadata['date_done'])<=end,'ActualBackendDateDoneCommandWindow')
 require(utc(metadata['date_done'])>=utc(seed['end_time']),'NoNormalBackendBeforeScheduledEnd')
 require(integer(actual['recording_task']['raw_result']['ttl'],1,86400),'ActualNormalBackendTTLWithinOneDay')
 backend_before=cache_map(actual['after_beat']['result_backend_raw']);backend_after=cache_map(actual['final']['result_backend_raw']);new_key=base64.b64decode(actual['recording_task']['raw_result']['key_base64'],validate=True)
 require(set(backend_after)-set(backend_before)=={new_key}and set(backend_before)<=set(backend_after),'OnlyActualDVRBackendKeyAdded')
 require(all(backend_before[key]['DUMP_base64']==backend_after[key]['DUMP_base64']and backend_before[key]['type']==backend_after[key]['type']for key in backend_before),'EveryPriorBackendValueUnchanged')
 result=actual['recording_task']['Original_AsyncResult_get'];require(result['executed']is True and result['actual_state']=='SUCCESS'and result['value']is None and result['type']=='builtins.NoneType','RealOriginalAsyncResultNullInterop')
 require(utc(result['began_utc'])>=end and utc(result['finished_utc'])>=utc(result['began_utc']),'ActualConsumerAfterNativeReturn')
 require(actual['recording_task']['prerun']==actual['recording_task']['postrun']==[],'NoFabricatedSourceNativeSignals')
 functional=actual['native_functional_observations'];require(type(functional)is list and 1<=len(functional)<=181,'BoundedActualFunctionalObservations')
 active=[]
 for observation in functional:
  require(type(observation['monotonic'])in(int,float)and math.isfinite(observation['monotonic']),'FiniteActualFunctionalObservationClock')
  for row in observation['active_rows']:
   require(base64.b64decode(row['key_base64'],validate=True)==('dvr:recording-active:'+str(ident)).encode()and row['type']=='string'and base64.b64decode(row['value_base64'],validate=True)==b'1'and integer(row['ttl'],1,45),'ActualLiteralOneAndBounded45SecondTTL')
   active.append(observation)
 require(len(active)>=2 and active[-1]['monotonic']-active[0]['monotonic']>=.25,'SustainedActualActiveKeyObservation')
 trace=native['actual_run']['active_observations'];kinds=[row['kind']for row in trace]
 require(kinds[:3]==['before-active','before-auxiliary','acquired']and kinds[-2:]==['after-auxiliary','removed']and all(kind=='renewed'for kind in kinds[3:-2]),'DeclaredActualActiveLifecycleOrder')
 require(exact(trace[0]['observation']['value'],[0,-2])and trace[2]['observation']['value']is True and integer(trace[-1]['observation']['value'],1,1),'ActualAbsentThenNXAcquireThenRemoveReplies')
 require(trace[1]['observation']['absent']is True and trace[-2]['observation']['absent']is True,'ActualViewerAndProxyAbsenceObservations')
 for row in trace:
  observed=row['observation'];require(begin<=utc(observed['before_utc'])<=utc(observed['after_utc'])<=end,'OwnActiveCommandClockIntervals')
  if row['kind']=='renewed':require(integer(observed['value'],1,1),'ActualRenewalReplyIntegerOne')
 capture_elapsed=(utc(processes[0]['settled_utc'])-utc(processes[0]['started_utc'])).total_seconds()
 if capture_elapsed>20:
  require('renewed'in kinds and any(later['active_rows'][0]['ttl']>earlier['active_rows'][0]['ttl']+5 for earlier,later in zip(active,active[1:])),'Actual45SecondTTLResetDuringCapturedLongerThan20SecondBody')
 art=actual['artwork'];require(len(actual['all_prerun'])==len(actual['all_postrun'])==1,'OnlyActualSourceArtworkSignals')
 require(actual['all_prerun'][0]['id']==actual['all_postrun'][0]['id']==art['id']and actual['all_prerun'][0]['task']==actual['all_postrun'][0]['task']=='apps.channels.tasks.prefetch_recording_artwork','ExactAllowedSourceSignalIdentity')
 require(art['Original_AsyncResult_get']['value']=='ok'and art['Original_AsyncResult_get']['actual_state']=='SUCCESS','OriginalLocalArtworkPrefix')
 receipts=actual['worker_exit_receipts'];require(len(receipts)==3 and {row['role']for row in receipts}=={'artwork','beat','native-dvr'},'ExactlyTwoOriginalAndOneNativeRole')
 for receipt in receipts:
  require(type(receipt['actual_exit'])is int and receipt['actual_exit']==0 and type(receipt['actual_poll_exit'])is int and receipt['actual_poll_exit']==0 and receipt['actual_wait_completed']is True and receipt['pipes_settled']is True and receipt['forced']is False and receipt['invalid']is None,'EveryActualRoleWaitAndPipeSettlement')
  require(type(receipt['starttime'])is str and receipt['starttime'].isdigit()and int(receipt['starttime'])>0 and receipt['pid']==receipt['pgid']==receipt['actual_initial_pgid'],'EveryObservedOriginalRoleIdentity')
  if receipt['role']=='native-dvr':require(receipt['writer_settled']is True and receipt['original_process_absent']is True,'ActualNativeWriterAndOriginalPIDSettlement')
  else:require(receipt['owned_PID_observations']and all(row['original_process_absent']is True and type(row['starttime'])is str and row['starttime'].isdigit()and int(row['starttime'])>0 for row in receipt['owned_PID_observations']),'EveryOriginalRoleObservedChildPIDAbsent')
 artwork_receipt=next(row for row in receipts if row['role']=='artwork');prerun=actual['all_prerun'][0];postrun=actual['all_postrun'][0]
 require(prerun['pid']==postrun['pid']and prerun['actual_process_starttime']==postrun['actual_process_starttime']and any(row['pid']==prerun['pid']and row['starttime']==prerun['actual_process_starttime']for row in artwork_receipt['owned_PID_observations']),'ActualSourceSignalPIDOwnedByArtworkRole')
 require(actual['final']['queues']['dvr']==[]and actual['final']['queues']['celery']==actual['after_beat']['queues']['celery']==actual['unconsumed_Beat_siblings']['raw_FIFO_base64'],'AllRawStaticSiblingFIFOUnconsumed')
 periodic=[row for row in actual['after_beat']['periodic']if row['name']==final['task_id']];require(len(periodic)==1,'ActualPeriodicIdentityBeforeClaim')
 removed=periodic[0]
 sql_acceptance=validate_cleanup_sql(actual['native_pg'],removed['id'],ident,removed['clocked_id'],begin,end)
 require(len(actual['final']['sentinel'])==1 and actual['final']['sentinel'][0]['ident']==1 and utc(actual['final']['sentinel'][0]['last_update'])==utc(sql_acceptance['actual_tracking_UTC'][-1]),'ActualFinalSingletonEqualsThirdWireTrackingTouch')
 before_stop=actual['native_pg_before_listener_stop'];require(before_stop['errors']==[]and all(row['closed']is row['up_joined']is True for row in before_stop['connections']),'NaturalSQLRetirementBeforeListenerStop')
 require(exact(actual['final']['periodic'],[row for row in actual['after_beat']['periodic']if row['id']!=removed['id']]),'OnlyOwnPeriodicRemovedNoOtherColumnChange')
 require(exact(actual['final']['clocked'],[row for row in actual['after_beat']['clocked']if row['id']!=removed['clocked_id']]),'OnlyOwnUnusedClockedRemoved')
 for key in ['channels','core_settings','notifications','notification_dismissals','integrations','event_subscriptions','delivery_logs','plugin_configs','plugin_repos','movies','logos','intervals','crontabs']:
  require(exact(actual['after_beat'][key],actual['final'][key]),'EveryUnrelatedCapturedRowUnchanged:'+key)
 require(exact([row for row in actual['after_beat']['recordings']if row['id']!=ident],[row for row in actual['final']['recordings']if row['id']!=ident]),'AllOtherRecordingRowsUnchanged')
 require(len(actual['final']['system_events'])==2,'ActualStartEndEventsOnly')
 events=actual['final']['system_events']
 for row,event_type in zip(events,['recording_start','recording_end']):
  expected_event=next(value for value in source['final']['system_events']if value['event_type']==event_type)
  require(set(row)==set(expected_event),'AllAuditEventColumnsPresent')
  require(row['event_type']==event_type and row['channel_id']==seed['channel_uuid']and row['channel_name']==source['final']['system_events'][0]['channel_name'],'ActualEventIdentity')
  require(begin<=utc(row['timestamp'])<=end,'OwnAuditClockWithinNativeCommand')
  require(set(row['details'])==set(expected_event['details']),'EverySourceAuditDetailField')
  expected_details=dict(expected_event['details']);expected_details['recording_id']=ident
  if event_type=='recording_end':expected_details.update(file_size=native['actual_run']['file_size'],bytes_written=props['bytes_written'],start_time=utc(seed['start_time']).isoformat(),end_time=utc(seed['end_time']).isoformat(),file_url=props['file_url'])
  require(exact(row['details'],expected_details),'AllSourceAuditDetailsExplicitOwnBindings')
 require(exact(actual['final']['Channels_decoded'],source['final']['Channels_decoded']),'SourceStartedUpdatedEndedChannelOrderAndFields')
 require(cache_relation(actual['after_beat']['cache'],actual['final']['cache'])==cache_relation(source['after_beat']['cache'],source['final']['cache']),'SourceObservedRawCacheKeyTypeChangedUnchangedRelations')
 require(actual['final']['origin']['invalid']==[],'OwnedOriginNoInvalidRequest')
 origin=actual['final']['origin'];require(origin['media_sha256']==source['final']['origin']['media_sha256']and origin['media_bytes']==source['final']['origin']['media_bytes'],'SameOwnedSyntheticInput')
 require(len(origin['rows'])==1 and origin['rows'][0]['method']=='GET'and origin['rows'][0]['path']=='/proxy/ts/stream/'+seed['channel_uuid']and origin['rows'][0]['status']==200,'OneActualOwnedFullGET')
 require(origin['rows'][0]['headers']==source['final']['origin']['rows'][0]['headers'],'ExactCapturedOriginHeaders')
 files=[row for row in actual['retained_media']['files']if row.get('relative','').endswith('.mkv')]
 require(len(files)==1 and files[0]['stable_across_this_read']is True and integer(files[0]['bytes'],1,64*1024*1024)and files[0]['bytes']==native['actual_run']['file_size'],'ActualStableMKVSize')
 require([row['relative']for row in actual['retained_media']['files']]==[files[0]['relative']],'NoRetainedHLSSegmentsOrUnknownOutput')
 video=[row for row in actual['ffprobe']['streams']if row['codec_type']=='video'];require(len(video)==1,'ExactlyOneVideoStream')
 expected_video=[row for row in source['ffprobe']['streams']if row['codec_type']=='video'][0]
 for key in ['codec_name','width','height','r_frame_rate']:require(exact(video[0][key],expected_video[key]),'SourceVideoWorkQuality:'+key)
 duration=float(actual['ffprobe']['format']['duration']);source_duration=float(source['ffprobe']['format']['duration']);require(math.isfinite(duration)and 0<duration<=30,'PositiveWholeDecodedVideoWithinScheduled30Seconds')
 require(len(actual['media_commands'])==2 and all(row['actual_exit']==0 and row['settled']is row['pipes_settled']is True and row['invalid']is None for row in actual['media_commands']),'ActualProbeAndFullVideoDecode0')
 source_path=source['retained_media']['files'][0]['retained_copy'];actual_path=files[0]['retained_copy']
 for observed,reference in zip(actual['media_commands'],source['media_commands']):require(observed['argv']==[actual_path if argument==source_path else argument for argument in reference['argv']],'ExactProbeAndWholeVideoDecodeArgumentsNoPrefix')
 inventory=clock_inventory(actual);require(len(inventory)>0,'AllRetainedDateStringsParsed')
 return {'passed':True,'scope':'one fresh Source model/artwork/Beat → inert Native normal DVR; no full graph/fault/cache-decoder/long lock claim','actual_Task_UUID':task,'actual_model_Recording_id':ident,'all_temporal_fields_retained':True,'temporal_inventory':inventory,'explicit_temporal_policy':'unchanged before/after fields exact; fresh Native started/ended/audit/backend clocks in own actual command windows; seed schedule binds actual four-argument producer','raw_cache_scope':'key/type/changed-unchanged relations only; full arbitrary cache-value decoding parity OPEN','Source_reference_sha256':hashlib.sha256(reference_bytes).hexdigest(),'media_policy':'same synthetic TS bytes/codec/dimensions/fps, complete decode0 and positive whole media duration within30s; own scheduling/start latency retained; no equal delivered duration/bytes or performance claim','actual_Source3_video_duration_seconds':source_duration,'actual_Native_video_duration_seconds':duration,'Native_duration_equals_Source_qualified':False,'actual_cleanup':sql_acceptance}
