"""Finite Source capture acceptance; retains raw task/clock/result fields.
Does not project this normal case into Native/error/recovery/full-graph parity.
"""
import base64,datetime,math,json
from sibling_frames import validate as validate_siblings,validate_intervals
from fresh_context import empty_effect_context
from seed_spec import ARTWORK,RECORD,LEAD_SECONDS,DURATION_SECONDS
CHECKS=['owned-output-initially-empty','actual-origin-generated','exact-one-known-celery','natural-dvr-empty-before-due','model-created-one-future-clocked-row','artwork-source-success','artwork-natural-queue-drained','future-window-not-consumed-by-setup','exact-one-known-dvr','actual-four-model-signal-arguments','actual-dvr-routing-key','known-static-Beat-siblings-retained-unconsumed','actual-recording-postrun-success','no-unexpected-Python-provider-attempts','dvr-drained-default-sibling-FIFO-byte-order-preserved','one-actual-stable-MKV','actual-video-stream']
def require(ok,name):
 if ok is not True:raise ValueError(name)
def actual_utc(value):
 require(type(value)is str,'ActualUTCString');value=datetime.datetime.fromisoformat(value);require(value.utcoffset()==datetime.timedelta(0),'ActualUTCOffset');return value
def finite(value):return type(value)in(int,float)and math.isfinite(value)
def validate_source_capture(exit_code,report):
 require(type(exit_code)is int and exit_code==0,'ActualGuestZero')
 require(report.get('passed')is True and report.get('Source_verified')is True and report.get('Native_verified')is False and report.get('performance_verified')is False,'ExactSourceOnlyScope')
 checks=report['checks'];require([row['name']for row in checks]==CHECKS and all(row['passed']is True for row in checks),'ExactCompleteOrderedChecks')
 require(report['finally_errors']==[] and report['all_HTTP_attempts']==[],'ActualFinallyAndProviderObservation')
 require(report['application_task_model_signal_methods_replaced']is False and report['actual_routing_never_overridden']is True and report['fixture_sequential_Beat_then_DVR']is True,'ActualImplementationAndSequencingScope')
 require(len(report['cases'])==1 and report['cases'][0]['name']=='future-clocked-normal','OneDeclaredCase')
 seed=report['seed'];anchor=actual_utc(seed['anchor_utc']);start=actual_utc(seed['start_time']);end=actual_utc(seed['end_time'])
 require((start-anchor).total_seconds()==LEAD_SECONDS and(end-start).total_seconds()==DURATION_SECONDS,'PredeclaredActualWindow')
 require(seed['internal_TS_base']=='http://origin:8080','OwnedOnlyOrigin')
 dispatch=report['recording_dispatch'];frame=dispatch['frame'];args,kwargs,embedded=dispatch['decoded_body']
 require(frame['headers']['task']==RECORD and args==[seed['Recording_id'],seed['channel_id'],seed['start_time'],seed['end_time']]and kwargs=={},'ActualFourArgumentFrame')
 require(frame['properties']['delivery_info']['routing_key']=='dvr','ActualNaturalDvrFIFO')
 tasks=[report['artwork'],report['recording_task']]
 for task,family in zip(tasks,[ARTWORK,RECORD]):
  require(type(task['id'])is str and task['id']!='','ActualObservedTaskID')
  pre=task['prerun'];post=task['postrun'];require(len(pre)==len(post)==1 and pre[0]['task']==post[0]['task']==family and pre[0]['id']==post[0]['id']==task['id']and post[0]['state']=='SUCCESS','ActualUniqueTaskExecution')
  require(actual_utc(pre[0]['utc'])<=actual_utc(post[0]['utc']),'ActualTaskUTCOrder')
  require(type(pre[0]['attrs']['ignore_result'])is bool and type(pre[0]['attrs']['acks_late'])is bool,'ActualTaskFlagsNotGuessed')
  raw=task['raw_result'];require('raw_base64'in raw and'metadata'in raw and'key_base64'in raw,'ActualConfiguredBackendKeyObservation')
  require(base64.b64decode(raw['key_base64'],validate=True).endswith(task['id'].encode()),'ActualTaskBackendKey')
  if raw['raw_base64']is None:require(raw['metadata']is None and task['Original_AsyncResult_get']['executed']is False,'NoInventedBackendValue')
  else:
   require(json.loads(base64.b64decode(raw['raw_base64'],validate=True))==raw['metadata'],'ExactRawBackendMetadata')
   require(raw['metadata']['status']=='SUCCESS'and raw['metadata']['result']==post[0]['result'],'BackendAndPostrunValueAgreement')
   get=task['Original_AsyncResult_get'];require(get['executed']is True and get['value']==post[0]['result']and get['actual_state']=='SUCCESS'and get['propagate']is False and get['timeout_seconds']==2 and actual_utc(get['began_utc'])<=actual_utc(get['finished_utc']),'ActualOriginalAsyncResultInterop')
 siblings=report['unconsumed_Beat_siblings'];beat_start=next(row for row in report['worker_starts']if row['role']=='beat');beat_end=next(row for row in report['worker_exit_receipts']if row['role']=='beat')['finished_utc'];derived=validate_siblings(report['after_beat']['queues']['celery'],report['all_publications'],report['after_beat']['periodic'],report['seed']['actual_static_beat_schedule'],beat_start,beat_end);require(derived==siblings,'IndependentExactSiblingProvenance');require(validate_intervals(report['after_beat']['periodic'],report['after_beat']['intervals'],siblings,report['seed']['actual_static_beat_schedule'])==report['sibling_interval_rows'],'ExactActualSiblingIntervalRows');publications=report['all_publications'];expected_ids={task['id']for task in tasks}|set(siblings['ids']);require(len(publications)==len(expected_ids)and{row['headers']['id']for row in publications}==expected_ids,'CompleteActualPublicationLedger')
 require(report['all_prerun']==[task['prerun'][0]for task in tasks]and report['all_postrun']==[task['postrun'][0]for task in tasks],'ExactAllPrerunPostrunKnownTwoNoDangling')
 for task,role,queue,frame in [(tasks[0],'artwork','celery',report['admission_celery']['frames'][0]['frame']),(tasks[1],'dvr','dvr',report['recording_dispatch']['frame'])]:
  header=frame['headers'];wire_body=json.loads(base64.b64decode(frame['body'],validate=True));pre=task['prerun'][0];post=task['postrun'][0]
  require(header['id']==task['id']and header['root_id']==task['id']and header['parent_id']is None,'ActualKnownFrameRootAndIdentity')
  require(pre['root_id']==post['root_id']==header['root_id']and pre['parent_id']==post['parent_id']==header['parent_id']and pre['args']==wire_body[0]and pre['kwargs']==wire_body[1]and pre['delivery']['routing_key']==queue,'ActualKnownPrerunFrameArgumentsLineageQueue')
  require(type(pre['pid'])is int and pre['pid']==post['pid']and type(pre['actual_process_starttime'])is str and pre['actual_process_starttime'].isdigit()and pre['actual_process_starttime']==post['actual_process_starttime'],'ActualSamePrePostProcessIdentity')
  role_exit=next(row for row in report['worker_exit_receipts']if row['role']==role);owned=[row for row in role_exit['owned_PID_observations']if row['pid']==pre['pid']and row['starttime']==pre['actual_process_starttime']];require(len(owned)==1 and owned[0]['original_process_absent']is True,'ActualKnownRoleOwnedIdentity')
  if role=='dvr':require(pre['pid']==role_exit['pid'],'ActualDvrThreadsRolePID')
 require(report['recording_dispatch']['frame']['headers']['periodic_task_name']=='dvr-recording-'+str(seed['Recording_id']),'ActualPeriodicRootMapping')
 periodic_name=report['recording_dispatch']['frame']['headers']['periodic_task_name'];periodic_rows=[row for row in report['after_beat']['periodic']if row['name']==periodic_name];recording_rows=[row for row in report['after_beat']['recordings']if row['id']==seed['Recording_id']];require(len(periodic_rows)==len(recording_rows)==1,'ActualUniqueClockedProducerRows');periodic=periodic_rows[0];recording=recording_rows[0];clocked_rows=[row for row in report['after_beat']['clocked']if row['id']==periodic['clocked_id']];require(recording['task_id']==periodic_name and recording['channel_id']==seed['channel_id']and actual_utc(recording['start_time'])==start and actual_utc(recording['end_time'])==end and periodic['task']==RECORD and periodic['one_off']is True and json.loads(periodic['args'])==args and json.loads(periodic['kwargs'])=={}and all(periodic[key]is None for key in ('interval_id','crontab_id','solar_id'))and len(clocked_rows)==1 and actual_utc(clocked_rows[0]['clocked_time'])==start,'ActualRecordingPeriodicClockedFrameMapping')
 require(report['fresh_effect_context_verified']is True and empty_effect_context(report['fresh_effect_context'])is True,'ActualFreshIntegrationPluginRows')
 require(report['configured_plugin_directory_before_app']['empty']is True and report['configured_plugin_directory_before_app']['configured']=='/data/plugins','ActualFreshPluginDirectoryBeforeSourceSetup')
 exits=report['worker_exit_receipts'];require([row['role']for row in exits]==['artwork','beat','dvr'],'ActualRolesCompletedInOrder')
 for row in exits:
  require(type(row['starttime'])is str and row['starttime'].isdigit()and type(row['pid'])is int,'ActualRoleOriginalIdentity')
  require(row['actual_exit']==0 and type(row['actual_exit'])is int and row['actual_wait_completed']is True and row['pipes_settled']is True and row['all_observed_owned_PIDs_absent']is True and row['forced']is False and row['invalid']is None,'ActualRolePhysicalSettlement')
 final=report['final'];require(final['configured_plugin_directory']['empty']is True and final['integrations']==[]and final['event_subscriptions']==[]and final['delivery_logs']==[]and final['plugin_configs']==[],'ActualNoIntegrationPluginEffects')
 for key in ('system_events','notifications','notification_dismissals','plugin_repos'):
  require(type(report['setup'][key])is list and type(final[key])is list,'ActualBeforeAfterRows:'+key)
 require(final['queues']=={'celery':siblings['raw_FIFO_base64'],'dvr':[]},'ActualDvrDrainedAndSiblingFIFOExact')
 recordings=[row for row in final['recordings']if row['id']==seed['Recording_id']];require(len(recordings)==1,'ActualFinalRecording')
 cp=recordings[0]['custom_properties'];require(cp.get('status')=='completed'and cp.get('owned_fixture_marker')=='preserve-normal-clocked','NormalCompletedStatusAndFreshPropertiesPreserved')
 require(not any(row['name']=='dvr-recording-'+str(seed['Recording_id'])for row in final['periodic']),'ActualOneoffRowCleanedByTask')
 origin=final['origin'];require(origin['invalid']==[] and 1<=len(origin['rows'])<=4,'ActualOwnedOriginRequestCount')
 require(all(row['status']==200 and row['bytes_sent']>0 and row['path']=='/proxy/ts/stream/'+seed['channel_uuid']for row in origin['rows']),'ActualOwnedTSRequests')
 processes=report['subprocess_postrun'];matching=[entry for entry in processes if entry['task_id']==report['recording_task']['id']];require(len(matching)==1,'ActualTaskProcessObserverPostrun')
 observation=matching[0]['observation'];require(observation['errors']==[] and observation['observer_never_waits_or_reaps']is True,'UnchangedApplicationWaitOwnership')
 require(any(row['argv'][0].split('/')[-1]=='ffmpeg'and row['actual_thread_task_id']==report['recording_task']['id']for row in observation['rows']),'ActualRecordingFFmpegIdentity')
 for row in observation['rows']:
  require(row['spawn_state']=='actual_child_started'and type(row['starttime'])is str and row['starttime'].isdigit(),'ActualSubprocessIdentity')
  require(row['original_process_absent']is True and type(row['actual_returncode_attribute'])is int,'ActualSourceSubprocessReapedByApplication')
 commands=report['media_commands'];require(len(commands)==2 and all(type(row['actual_exit'])is int and row['actual_exit']==0 and row['settled']is True and row['pipes_settled']is True and row['invalid']is None and type(row['starttime'])is str and row['starttime'].isdigit()for row in commands),'ActualProbeAndFullVideoDecode')
 for row in report['all_SQL']:
  require(row['actual_database']==report['actual_database']and finite(row['began_epoch'])and finite(row['finished_epoch'])and row['began_epoch']<=row['finished_epoch'],'ActualSQLClockAndDatabase')
 return {'passed':True,'unconsumed_Beat_sibling_count':len(siblings['ids']),'unconsumed_sibling_execution_verified':False,'scope':'One actual Source future-clocked normal recording; raw result/headers/flags/SQL/clock/files retained; no Native/default graph/recovery/error claim','cases':1,'checks':len(checks),'actual_tasks':2,'raw_time_fields_removed':0,'metadata_absence_not_invented_as_null_SUCCESS':True}
