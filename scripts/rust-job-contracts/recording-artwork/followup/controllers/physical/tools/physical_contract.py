"""Native-only ownership acceptor; not a new Source failure/concurrency oracle."""
import base64,json,copy,math
from pathlib import Path
from pg_reply_proxy import error_fields
SQL=json.loads(Path(__file__).with_name("sql-policy.json").read_text())
RESP=json.loads(Path(__file__).with_name("redis-policy.json").read_text())
from row_compare import equal,instant,require
from seed_spec import REFRESH,SERIES,ARTWORK,QUEUES,validate_role_frames
from contract import SNAPSHOT_FIELDS
MODES=['recording-read-Ready','update-Ready','update-error-Ready','Channels-reply','result-reply','oldSUCCESS-preserved','fresh-status-merge']
def actual_number(value):return type(value)in(int,float)and math.isfinite(value)
def sustained_error_owner(case,subject,release):
 samples=case['post_error_held_samples'];require(type(samples)is list and len(samples)==2,'ExactlyTwoActualErrorHeldSamples')
 gate=subject['monotonic'];end=release['monotonic'];require(actual_number(gate)and actual_number(end),'FiniteActualProxyGateReleaseClocks')
 require(subject['actual_error']is True and type(subject['backend_pid'])is int and subject['backend_pid']>0 and type(subject['connection'])is int and subject['connection']>0,'ActualMatchedErrorBackendIdentity')
 process=case['native_process'];require(type(process['pid'])is int and process['pid']>0 and type(process['starttime'])is str and process['starttime'].isdigit()and int(process['starttime'])>0,'ActualSamePeerIdentity')
 for index,sample in enumerate(samples):
  require(set(sample)=={'sampled_monotonic','sampled_utc','response','peer_pid','peer_starttime','gate_backend_pid','gate_connection','gate_monotonic','proxy_release_not_requested','backend_raw_base64'},'CompleteTypedOwnershipSampleFields')
  t=sample['sampled_monotonic'];require(actual_number(t)and gate+2.2<=t<end,'ActualPostErrorGateBoundedSampleClock')
  require(type(sample['gate_monotonic'])is float and sample['gate_monotonic']==gate,'SameActualErrorGateClock')
  require(type(sample['gate_backend_pid'])is int and sample['gate_backend_pid']==subject['backend_pid']and type(sample['gate_connection'])is int and sample['gate_connection']==subject['connection'],'SameActualErrorBackendAndConnection')
  require(type(sample['peer_pid'])is int and sample['peer_pid']==process['pid']and type(sample['peer_starttime'])is str and sample['peer_starttime']==process['starttime'],'SameActualPeerForBothSamples')
  response=sample['response'];require(set(response)=={'caller_pending','caller_released','available_slots','event_connections','event_publish_bytes'},'CompleteActualPollFields')
  require(response['caller_pending']is True and response['caller_released']is False and type(response['available_slots'])is int and response['available_slots']==0,'ActualOriginalWeakCallerAndAdmissionHeld')
  require(type(response['event_connections'])is int and response['event_connections']>=0 and type(response['event_publish_bytes'])is int and response['event_publish_bytes']>=0,'TypedActualEventBudgetDiagnostics')
  require(sample['proxy_release_not_requested']is True and sample['backend_raw_base64']is None,'ActualHeldNoEarlyTerminalMetadata')
  matches=[r for r in case['responses']if r['label']=='poll'and r['monotonic']==t and r['utc']==sample['sampled_utc']and equal(r['value'],response)];require(len(matches)==1,'ActualRetainedSamePeerResponseReceipt')
  observation=[o for o in case['observations']if o['label']=='post-error-held-ownership-'+str(index+1)];require(len(observation)==1,'ActualRetainedOwnershipSnapshot')
  observation=observation[0];require(actual_number(observation['monotonic'])and t<=observation['monotonic']<end and observation['backend']['raw_base64']is None,'RawSnapshotAfterPollBeforeReleaseWithoutMetadata')
  require(instant(case['began_utc'])<=instant(sample['sampled_utc'])<=instant(release['utc']),'ActualOwnSampleUTCWindow')
 require(samples[1]['sampled_monotonic']-samples[0]['sampled_monotonic']>=.25,'ActualSustainedOwnerAtLeast250ms')
 return True
def validate(code,report,reference):
 require(type(code)is int and code==0,'ActualGuest0');require(report['passed']is True and report['Native_verified']is True and report['Source_verified']is False and report['performance_verified']is False,'NativeOnlyPhysicalScope')
 require(len(report['cases'])==3,'FreshTwoOriginalParentsPlusOnePhysicalCase');first,second,case=report['cases'];mode=case['name'];require(mode in MODES and report['predeclared_cases']==[first['name'],second['name'],mode],'ExactFinitePhysicalMode')
 require(first['role']=='refresh'and second['role']=='series'and all(t['result']['metadata']['status']=='SUCCESS'for c in[first,second]for t in c['tasks']),'ActualCompletedOriginalParents')
 require([p['task']for p in report['all_prerun']]==[REFRESH,SERIES]and len(report['worker_exit_receipts'])==2,'NoSourceArtworkOrDefaultChildrenExecuted')
 for p in report['worker_exit_receipts']:require(p['actual_exit']==0 and p['actual_wait_completed']is True and p['pipes_settled']is True and p['all_observed_owned_PIDs_absent']is True,'OriginalParentWorkersSettled')
 require(case['passed']is True and case['finally_errors']==[]and case['case_elapsed_seconds']<=90 and report['finally_errors']==[],'CaseAndFinallySettlement')
 process=case['native_process'];require(process['spawn_state']=='actual_child_started'and process['exit']==0 and process['settled']is True and process['pipes_settled']is True and process['writer_settled']is True and process['collector_invalid']is None,'ActualNativePeerProcess0AndPipes')
 require(all(c['passed']is True for c in case['checks'])and len(case['checks'])>=4,'AllActualPredeclaredCaseChecks')
 require(all(c['passed']is True for c in report['checks']),'SourceSetupChecks')
 for value in case['proxy_close_receipts'].values():require(value['physical_close_verified']is True and value['errors']==[],'ActualProxySettlementNoHiddenError')
 before=case['before_native'];after=case['after_native'];own=before['queues'][QUEUES['artwork']];require(len(own)==2 and after['queues'][QUEUES['artwork']]==own[:-1],'ExactlyKnownFirstChildConsumedOtherRetained')
 parent=second['tasks'][0];root=first['tasks'][0];ids=[v[0][0]for v in parent['result']['metadata']['children']];validate_role_frames(own,'artwork',ids);require(ids[0]==case['id'],'ExactKnownFirstPublishedChild');frame=json.loads(base64.b64decode(own[-1],validate=True));require(frame['headers']['id']==case['id']and frame['headers']['task']==ARTWORK and frame['headers']['root_id']==root['id']and frame['headers']['parent_id']==parent['id'],'ActualParentedChildIdentityNotFixtureBool');require(parent['result']['metadata']['parent_id']==root['id'],'ActualCompletedParentResultLineage')
 require(before['backend'][ids[1]]['raw_base64']is None and after['backend'][ids[1]]['raw_base64']is None,'UnusedChildMetadataNeverInvented')
 for name in ['channels','sources','epg_data','programmes','source_indices','recurring_rules','periodic','clocked','sentinel','core_settings','VOD_movies','VOD_logos','files']:
  require(equal(before[name],after[name]),'NoUnrelatedCopiedRowOrFileMutation:'+name)
 require(set(before)==set(after)==SNAPSHOT_FIELDS,'CompleteRawSnapshotFieldPresence')
 require(len(before['recordings'])==len(after['recordings'])==1,'OneKnownRecordingRow')
 a=before['recordings'][0];b=after['recordings'][0];require(set(a)==set(b)and all(equal(a[k],b[k])for k in a if k!='custom_properties'),'EveryUnownedRecordingColumnUnchanged')
 expected=copy.deepcopy(a['custom_properties'])
 if mode!='update-error-Ready':expected.update(poster_logo_id=before['VOD_logos'][0]['id'],season=1,episode=1)
 if mode=='fresh-status-merge':expected.update(status='recording',owned_current_row_marker='preserved')
 require(equal(expected,b['custom_properties']),'ExactKnownMetadataEffectsAndCurrentMerge')
 count=len(after['Channels_decoded'])-len(before['Channels_decoded']);require(count==(0 if mode=='update-error-Ready'else 1),'ExactActualPartialEventCount')
 require(after['Channels_raw_base64'][:len(before['Channels_raw_base64'])]==before['Channels_raw_base64'],'PriorRawEventsUnchanged')
 expected_events=[]if mode=='update-error-Ready'else[{'type':'update','data':{'success':True,'type':'recording_updated','recording_id':a['id']},'__asgi_channel__':['specific.rust_owned_parented!receiver']}]
 require(equal(after['Channels_decoded'][len(before['Channels_decoded']):],expected_events),'ExactActualRecordingUpdateEventPayload')
 require(before['queues']['celery']==after['queues']['celery']==report['final']['queues']['celery'],'EveryDefaultDescendantRawFrameUnchanged')
 require(report['all_HTTP_attempts']==[],'NoProviderNetworkAttempt')
 if mode!='oldSUCCESS-preserved':
  release=case['actual_gate_release_request'];require(6.2<=release['monotonic']-release['gate_reached_monotonic']<=20,'ActualFixedHoldLowerBound')
  held=next(o for o in case['observations']if o['label']=='held-before-fixed-release');require(held['monotonic']<=release['monotonic'],'HeldSnapshotBeforeRelease')
  response=next(r['value']for r in case['responses']if r['label']=='poll');require(response['caller_released']is False and response['available_slots']==0,'RealPhysicalCallerOwnerAndSlotHeld')
  drained=next(r for r in case['responses']if r['label']=='drain-completed');require(drained['value']['drained']is True and drained['value']['caller_released']is True and instant(drained['utc'])>=instant(release['utc']),'DrainAfterActualRelease')
 if mode=='recording-read-Ready':require(next(o for o in case['observations']if o['label']=='held-before-fixed-release')['snapshot']['queues'][QUEUES['artwork']]==own,'ActualPreACKFramePreserved')
 if mode in ('update-error-Ready','Channels-reply'):require(case['result_after']['metadata']is None,'OperationalNotInventedTerminal')
 elif mode=='oldSUCCESS-preserved':require(case['result_after']['raw_base64']==case['old_metadata_before']['raw_base64']and 1<=case['result_after']['ttl']<=case['old_metadata_before']['ttl'],'ExactOldSuccessBytesAndMonotonicTTL')
 else:require(case['Original_get_value']=='ok'and case['Original_get_type']=='builtins.str','ActualOriginalGetNativeOk')
 if mode=='fresh-status-merge':require(after['recordings'][0]['custom_properties']['status']=='recording'and after['recordings'][0]['custom_properties']['owned_current_row_marker']=='preserved','CurrentRowPreserved')
 if'PG'in case['proxy_close_receipts']:
  value=case['proxy_close_receipts']['PG'];require(value['authentication_bytes_retained']is False and value['CancelKey_secret_retained']is False and value['handler_or_upstream_threads_alive']==0,'AuthProjectionAndActualAllThreadsClosed')
  held=[v for v in value['timeline']if v['stage']=='actual-matching-Ready-held'];require(len(held)==1 and type(held[0]['backend_pid'])is int and held[0]['backend_pid']>0,'ActualMatchingSubjectBackendPID')
  require(held[0]['monotonic']==case['actual_gate_release_request']['gate_reached_monotonic'],'GateClockIsActualProxyHeldTimestamp')
  subject=held[0];key={'recording-read-Ready':'recording_read','update-Ready':'update','update-error-Ready':'update','fresh-status-merge':'movie_read'}[mode];require(subject['identity']==SQL[key]and subject['executions']==1 and subject['frontend_Sync_or_simpleQuery_observed']is True,'ActualCompleteExecutedSQLIdentity')
  if mode in('recording-read-Ready','fresh-status-merge'):require(subject['matching_rows']==1 and subject['matching_commands_hex']==[b'SELECT 1\0'.hex()]and subject['actual_error']is False,'ActualSubjectDataRowAndCommandComplete')
  elif mode=='update-Ready':require(subject['matching_rows']==0 and subject['matching_commands_hex']==[b'UPDATE 1\0'.hex()]and subject['actual_error']is False,'ActualOneRowUPDATECommandComplete')
  else:
   require(subject['actual_error']is True and subject['matching_commands_hex']==[],'ActualUPDATEErrorWithoutFakeCommandComplete');fields=[error_fields(base64.b64decode(f['payload_base64'],validate=True))for f in value['frames']if f['connection']==subject['connection']and f['direction']=='backend'and f['kind_hex']==b'E'.hex()];require(any(f.get('C')==SQL['known_owned_trigger_error_code']and f.get('M')==SQL['known_owned_trigger_error_message']for f in fields),'ActualServerOwnedP0001Fields')
   sustained_error_owner(case,subject,case['actual_gate_release_request'])
  if mode=='fresh-status-merge':require(held[0]['subject_complete_occurrence']==2,'MovieExecutionNotPreflight')
 if 'RESP'in case['proxy_close_receipts']:
  held=[v for v in case['proxy_close_receipts']['RESP']['receipts']if v.get('stage')=='actual-target-reply-held'];require(len(held)==1 and held[0]['monotonic']==case['actual_gate_release_request']['gate_reached_monotonic'],'RESPGateClockActualProxyTimestamp')
  require(held[0]['actual_server_reply_observed']is True and held[0]['actual_selected_DB']==(0 if mode=='Channels-reply'else 9)and held[0]['script_sha1']==RESP['CHANNELS_PUBLISH_sha1'if mode=='Channels-reply'else'result_write_lua_sha1'],'ActualCompiledLuaReplyAndDBIdentity')
 return {'passed':True,'scope':'Native-only actual ownership/caller/refusal infrastructure; no Source fault/concurrency/timing or Main/defaultFIFO claim','mode':mode,'Original_setup_tasks':2,'Native_driven_children':1,'other_artwork_child_retained':1,'default_descendants_preserved':True,'physical_checks':len(case['checks']),'case_seconds':case['case_elapsed_seconds'],'all_raw_snapshot_timestamps_retained':True,'Source_failure_result_parity_verified':False,'PG_authentication_replay_verified':False,'Main_default_graph_verified':False,'performance_verified':False}
