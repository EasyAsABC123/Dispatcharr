"""Read-only qualification of actual packaged-Main scopes; no application replay."""
import base64,datetime,hashlib,json,sys
from pathlib import Path
ROOT=Path(sys.argv[1]).resolve();h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();report=json.loads((ROOT/'native-cohort.json').read_text());host=json.loads((ROOT/'execution-private.json').read_text());seed=json.loads((ROOT/'seed.json').read_text());checks=[]
def need(value,name):
 checks.append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def dt(v):return datetime.datetime.fromisoformat(v)
def programmes(row,phase):return row[phase]['tables']['epg_programdata']
def notices(row,phase):return [v for v in row[phase]['tables']['core_systemnotification']if v['notification_type']=='version_update']
need(host['capture_valid']is True and host['controller_exit']==0,'ActualHostCapture0NotFunctionalM6Pass');need(all(r['passed']is True for r in host['cleanup_steps']),'All10ActualCleanupSteps');need(host['passed']is False and report['fullM6']is False,'IncompleteMilestoneNotGreen');need(report['Original_API_or_task_replay']is False,'NoSourceAPIOrTaskReplay')
cases={row['name']:row for row in report['cases']};successes=[row for row in report['cases']if row.get('native_API_producer')is False and row['expected_backend_status']=='SUCCESS'];need(len(successes)==9,'NineRetainedConsumerSuccessCases')
for row in successes:
 meta=row['result']['metadata'];raw=json.loads(base64.b64decode(row['result']['raw_base64'],validate=True));expected='EPG not found'if row['name']=='tvg-missing'else None
 need(raw==meta and meta['task_id']==row['task_id']and meta['status']=='SUCCESS'and meta['result']==expected,'LiteralBackendAndResult:'+row['name'])
 consumer=row['result']['Original_configured_result_consumer'];need(consumer['state']=='SUCCESS'and consumer['value']==expected,'ActualOriginalResultConsumer:'+row['name']);need(1<=row['result']['ttl']<=3600,'ActualTTL:'+row['name'])
 need(dt(row['before']['utc'])<=dt(meta['date_done'])<=dt(row['after']['utc']),'FullBackendTimestamp:'+row['name'])
 need(row['native_API_producer']is False and row['entrypoint'].startswith('retained_Source_frame_projection'),'ConsumerScopeNotAPIProducer:'+row['name'])
 keys=[base64.b64decode(v['key_base64']).decode()for v in row['after']['cache_raw']]
 need(not any(k.startswith(('task_lock_parse_epg_programs_','epg_source_tvg_parse_'))for k in keys),'ActualLeaseAndCounterCleanup:'+row['name'])
first=cases['tvg-initial'];rows=programmes(first,'after');need(len(rows)==1 and rows[0]['title']=='Owned M6 Programme'and rows[0]['tvg_id']=='m6.batch.owned'and rows[0]['epg_id']==seed['tvg_epg'],'ActualTVGProgramme')
need(dt(rows[0]['start_time'])==dt(seed['tvg_start'])and dt(rows[0]['end_time'])==dt(seed['tvg_end']),'FullFreshInputTimestampsRetained')
repeat=cases['tvg-repeat'];need([{k:v for k,v in r.items()if k!='id'}for r in rows]==[{k:v for k,v in r.items()if k!='id'}for r in programmes(repeat,'after')],'TVGRepeatStableBusinessFieldsWithIDsRetainedRaw')
for row in [cases[name]for name in ['tvg-missing','tvg-dummy','tvg-unmapped']]:need(programmes(row,'before')==programmes(row,'after'),'NoopProgrammeRows:'+row['name'])
versions=[cases['version200-'+name]for name in ['no-update','update','repeat','malformed']];need(notices(versions[0],'after')==[],'VersionNoUpdateClearsCurrentNotice')
update=notices(versions[1],'after');again=notices(versions[2],'after');invalid=notices(versions[3],'after');need(len(update)==len(again)==len(invalid)==1,'OneVersionNotice')
u=update[0];a=again[0];need(u['notification_key']=='version-0.34.0'and u['admin_only']is u['is_active']is True and u['priority']=='high'and u['action_data']=={'version':'0.34.0','release_url':'https://owned.invalid/update','release_notes':None},'FullVersionNoticeFields')
need(u['id']==a['id']and u['created_at']==a['created_at']and dt(a['updated_at'])>dt(u['updated_at'])and u['source']==a['source']and u['expires_at']==a['expires_at'],'VersionRepeatPreservesIdentityCreationSourceExpiryUpdatesActualTime')
need(again==invalid,'MalformedVersionLeavesEveryNoticeFieldUnchanged')
for row in versions:
 need(dt(row['before']['utc'])<=dt(row['result']['metadata']['date_done'])<=dt(row['after']['utc']),'VersionTaskActualClock:'+row['name'])
events=versions[-1]['after']['Channels_decoded'];system=[r for r in events if r.get('data',{}).get('type')=='system_notification'];cleared=[r for r in events if r.get('data',{}).get('type')=='notifications_cleared'];need(len(system)==len(cleared)==1 and cleared[0]['data']['count']==1,'VersionExactlyOneClearAndOneCreateNoRepeatEvent')
need(dt(system[0]['data']['notification']['created_at'])==dt(u['created_at']),'EventUsesActualPersistedFullCreationTimestamp')
origin=[json.loads(x)for x in(ROOT/'origin-requests.jsonl').read_text().splitlines()];need(len(origin)==4 and all(r['completed']is True and r['path']=='/owned-version'and r['status']==200 for r in origin),'ExactlyFourActualOwnedHTTPCalls')
for r in origin:
 headers={k.lower():v for k,v in r['headers']};need(headers['user-agent']=='Dispatcharr/0.33.0'and headers['accept']=='application/vnd.github.v3+json'and headers['accept-encoding']=='gzip, deflate','ActualSourceHTTPHeaders')
failure=cases['tvg-insert-error'];meta=failure['result']['metadata'];need(failure['expected_backend_status']=='FAILURE'and meta is not None and failure['passed']is True,'ActualP0001FAILUREMetadata')
need(json.loads(base64.b64decode(failure['result']['raw_base64']))==meta and meta['task_id']==failure['task_id']and meta['status']=='FAILURE','ActualFailureRawMetadataIdentity')
need(meta['result']['exc_type']=='ProgrammingError'and meta['result']['exc_module']=='django.db.utils'and isinstance(meta['result']['exc_message'],list)and len(meta['result']['exc_message'])==1,'SourceFailureDTOClassModuleArity')
need('owned M6 programme insert'in meta['result']['exc_message'][0]and 'CONTEXT:'in meta['result']['exc_message'][0],'ActualPostgresMessageAndContext')
need(isinstance(meta['traceback'],str)and 'Native'in meta['traceback']and 'Traceback (most recent call last)'not in meta['traceback'],'TruthfulNativeDiagnosticNotPythonFrames')
need(meta['children']==[]and 1<=failure['result']['ttl']<=3600,'FailureEmptyChildrenTTL')
need(failure['result']['Original_configured_result_consumer']['state']=='FAILURE','ActualOriginalConfiguredFailureConsumer')
need(dt(failure['before']['utc'])<=dt(meta['date_done'])<=dt(failure['after']['utc']),'FailureActualFullTimestamp')
keys=[base64.b64decode(v['key_base64']).decode()for v in failure['after']['cache_raw']];need(not any(k.startswith(('task_lock_parse_epg_programs_','epg_source_tvg_parse_'))for k in keys),'FailureActualLeaseCounterCleanup')
need(dt(repeat['before']['utc'])>=dt(failure['after']['utc'])and repeat['passed']is True,'ActualSameTVGLaneContinuedAfterFailure')
need(programmes(failure,'before')==programmes(failure,'after'),'ActualP0001RollbackPreservesEveryProgrammeField')
server_logs=''.join((ROOT/f'command-{i:03}.stdout').read_text()for i,row in enumerate(host['commands'],1)if 'logs'in row['argv']and row['argv'][-1]==json.loads((ROOT/'owned-role.json').read_text())['baseline']['CID']);need('ERROR:  P0001: owned M6 programme insert'in server_logs,'ActualServerSQLSTATECapturedNotDeclared')
api=cases['Native-API-configured-parent'];need(api['native_API_producer']is True and api['response']['status']==202 and json.loads(base64.b64decode(api['response']['body_base64']))=={'success':True,'message':'EPG data refresh initiated.'},'ActualNativeAPI202Publication')
new={ident:value for ident,value in api['after']['backend'].items()if ident in api['new_backend_ids']and value['metadata']is not None};roots=[x['metadata']for x in new.values()if x['metadata'].get('parent_id')is None];need(len(roots)==1,'OneActualNativeRefreshRoot');parent=roots[0];children=[x[0][0]for x in parent['children']];need(parent['status']=='SUCCESS'and parent['result']is None and len(children)==2 and set(children).issubset(set(new)-{parent['task_id']}),'ActualParentBackendTwoCompletedChildren')
series=[x['metadata']for x in new.values()if isinstance(x['metadata']['result'],dict)];need(len(series)==1,'ActualConfiguredSeriesBackend');series=series[0];need(series['parent_id']==parent['task_id']and series['result']=={'scheduled':1,'details':[{'tvg_id':'rust.owned.parented.artwork','title':'Owned Parented Artwork','status':'ok','created':1}]},'ActualConfiguredSeriesInheritedParentAndResult')
need(len(new)==5 and len(api['new_backend_ids'])==5 and set(api['new_backend_ids'])==set(new),'ExactFiveActualNewAPIBackendKeys')
artwork=[x['metadata']for x in new.values()if x['metadata'].get('parent_id')==series['task_id']];need(len(artwork)==2 and {x['task_id']for x in artwork}=={x[0][0]for x in series['children']},'TwoActualCompletedArtworkChildren')
need(all(x['status']=='SUCCESS'and x['result']=='ok'and x['children']==[]and x['traceback']is None for x in artwork),'SourceArtworkReturnAndTerminalShape')
need(set(children)==set(new)-{parent['task_id']}-{x['task_id']for x in artwork},'ExactIndexSeriesDirectChildren')
need(api['after']['queues']['celery']==[],'ActualDefaultFIFOEmptyAfterArtwork')
recordings=api['after']['tables']['dispatcharr_channels_recording'];need(len(recordings)==1 and recordings[0]['channel_id']==seed['api_channel']and dt(recordings[0]['start_time'])==dt(seed['programme_start'])and dt(recordings[0]['end_time'])==dt(seed['programme_end']),'ActualOneRecordingFullTimes')
props=recordings[0]['custom_properties'];need(props['poster_logo_id']==seed['poster_logo_id'] and props['season']==1 and props['episode']==1 and props['program']['title']=='Owned Parented Artwork','ActualArtworkRecordingMetadata')
need('status'not in props,'RecordingSourceStatusAbsencePreserved')
before_events=api['before']['Channels_decoded'];after_events=api['after']['Channels_decoded'];need(after_events[:len(before_events)]==before_events,'PriorChannelsEventsPreserved')
updates=[r for r in after_events[len(before_events):]if r.get('data',{}).get('type')=='recording_updated'];need(len(updates)==1 and updates[0]['data']=={'success':True,'type':'recording_updated','recording_id':recordings[0]['id']},'ExactlyOneArtworkChangeEventNoRepeatEvent')
file='rust-owned-parented-artwork/logo.png';need(api['before']['media_files'][file]==api['after']['media_files'][file],'ActualExistingLogoFileUnchanged')
post=json.loads((ROOT/'native-poststop.json').read_text());need(post['native_stopped_before_capture']is True and post['queues']['celery']==[],'ActualPoststopFIFOEmpty')
need(post['recordings']==recordings and post['media_files']==report['final_before_native_stop']['media_files'],'PoststopRecordingAndOwnedFilesStable')
need(len(post['backend'])==18 and sum(v['metadata']['status']=='SUCCESS'for v in post['backend'].values())==17 and sum(v['metadata']['status']=='FAILURE'for v in post['backend'].values())==1,'ActualPoststop17SuccessPlusP0001Failure')
for ident,value in report['final_before_native_stop']['backend'].items():need(post['backend']['celery-tasks:celery-task-meta-'+ident]['metadata']==value['metadata'],'PoststopBackendMetadataUnchanged:'+ident)
monitor=[json.loads(x)for x in(ROOT/'redis-monitor.jsonl').read_text().splitlines()];publication=[]
for row in monitor:
 c=row.get('command','')
 if c.upper().startswith('LPUSH CELERY '):
  try:frame=json.loads(c.split(' ',2)[2])
  except ValueError:continue
  if frame['headers']['id']==parent['task_id']:publication.append((row,frame))
need(len(publication)==1 and publication[0][0]['client_address']=='lua'and publication[0][1]['headers']['task']=='apps.epg.tasks.refresh_epg_data','ActualNativeProducerLPUSHObserved')
body=json.loads(base64.b64decode(publication[0][1]['body']));need(body[0]==[seed['api_source']]and body[1]=={'force':False},'ActualNativeProducerSource37ForceFalseSpelling')
need(host['native_source_tree_mounted']is False and host['native_EPG_path_overrides']is False and host['native_no_source_tree_physical_probe']is True,'ActualNativeNoSourceTreeBindOrEPGPathOverrides')
empty=cases['Native-API-no-rules-parent'];need(empty['settings_patch']['response']['status']==200 and empty['response']['status']==202 and empty['passed']is True,'ActualNativeCorePATCHAndNoRulesImportAPI')
patch=json.loads(base64.b64decode(empty['settings_patch']['response']['body_base64']));need(patch['key']=='dvr_settings'and patch['value']['series_rules']==[]and patch['value']['pre_offset_minutes']==patch['value']['post_offset_minutes']==0,'ActualNativeDVRRulesClearedWithOffsetsPreserved')
need(empty['after_settings_patch']['queues']['celery']==[]and set(empty['after_settings_patch']['backend'])==set(empty['before']['backend']),'CorePATCHNoUnintendedTaskPublication')
new_empty={ident:value['metadata']for ident,value in empty['after']['backend'].items()if ident in empty['new_backend_ids']};need(len(new_empty)==3 and all(meta['status']=='SUCCESS'for meta in new_empty.values()),'ActualThreeNoRulesAPIBackends')
roots_empty=[meta for meta in new_empty.values()if meta.get('parent_id')is None];need(len(roots_empty)==1,'OneActualNoRulesRefreshRoot');root_empty=roots_empty[0]
need(root_empty['result']is None and {entry[0][0]for entry in root_empty['children']}==set(new_empty)-{root_empty['task_id']},'NoRulesRefreshListsExactIndexSeriesChildren')
series_empty=[meta for meta in new_empty.values()if isinstance(meta['result'],dict)];need(len(series_empty)==1 and series_empty[0]['result']=={'scheduled':0,'details':[]}and series_empty[0]['children']==[]and series_empty[0]['parent_id']==root_empty['task_id'],'ActualNoRulesSourceResultAndEmptyChildren')
need(empty['before']['tables']['dispatcharr_channels_recording']==empty['after']['tables']['dispatcharr_channels_recording']and not empty['after']['queues']['celery'],'NoRulesNoRecordingMutationOrArtworkQueue')
extra_events=empty['after']['Channels_decoded'][len(empty['before']['Channels_decoded']):];need(not any(event.get('data',{}).get('type')in ['recording_updated','recordings_refreshed']for event in extra_events),'NoRulesNoRecordingOrSeriesRefreshEvents')
keys=[base64.b64decode(item['key_base64']).decode()for item in empty['after']['cache_raw']];need(not any(key.startswith(('task_lock_evaluate_series_rules_','epg_source_tvg_parse_'))for key in keys),'NoRulesActualSeriesLeaseAndCounterClean')
need(empty['before']['media_files'][file]==empty['after']['media_files'][file],'NoRulesExistingLogoFileUnchanged')
need(report['monitor_close']['terminal_barrier_observed']is True and any(report['monitor_close']['terminal_marker']in str(row.get('command',''))for row in monitor),'FsyncedTerminalBusinessJournalBarrier')
queue=[];published={};popped=[]
for entry in monitor:
 command=entry.get('command','');upper=command.upper()
 if upper.startswith('LPUSH CELERY '):
  frame=json.loads(command.split(' ',2)[2]);ident=frame['headers']['id'];need(ident not in published,'UniqueActualQueuePublication:'+ident);published[ident]=(entry,frame);queue.insert(0,ident)
 elif upper=='RPOP CELERY':
  need(bool(queue)and entry['client_address']=='lua','ActualCASPopRequiresOldestPublishedFrame');ident=queue.pop();popped.append(ident)
  if ident in {r['task_id']for r in artwork}:need(entry['time']>=dt(published[ident][1]['headers']['eta']).timestamp(),'ArtworkActualClaimAtOrAfterExactPublishedETA:'+ident)
need(not queue and len(popped)==18 and set(popped)==set(published),'Exact18FIFOClaimsNoSkippedHead')
for value in artwork:
 entry,frame=published[value['task_id']];need(frame['headers']['root_id']==parent['task_id']and frame['headers']['parent_id']==series['task_id'],'ActualArtworkPublicationLineage:'+value['task_id']);need(json.loads(base64.b64decode(frame['body']))[0]==[recordings[0]['id']],'ActualPublishedRecordingID:'+value['task_id']);need(dt(value['date_done'])>=dt(frame['headers']['eta']),'ArtworkCompletionAfterActualETA:'+value['task_id'])
need(host['native_stop']['ExitCode']==host['origin_stop']['ExitCode']==0,'ActualNormalNativeOriginStops')
result={'qualified':True,'qualifier_source_sha256':h(Path(__file__)), 'checks':checks,'qualified_Main_consumer_successes':9,'consumer_entrypoint':'retained Source-template projection; NOT NativeAPIproducer proof','qualified_actual_Native_API_chain':8,'qualified_normal_no_rules_API_chain':True,'Native_source_tree_mounted':False,'Native_EPG_path_overrides':False,'qualified_P0001_failure':True,'full_Source37_API_contract':False,'fullM6':False,'performance_verified':False,'unpassed':['full default scheduler/restart/other required M6 matrix cells','grouped generic artwork manual/status/provider/rating/ETA/restart domains'],'evidence_hashes':{p:h(ROOT/p)for p in['execution-private.json','native-cohort.json','native-poststop.json','seed.json','redis-monitor.jsonl','origin-requests.jsonl','tool-source-sha256.json']},'observer_limits':['Terminal ECHO barrier includes completed business commands; later background worker polls are outside that journal scope.','No held-Ready proxy condition is exercised in this cohort; normal successful SQL physical closure and normal Main exit are separate from held-wire diagnostics.']}
(ROOT/'qualification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'qualified':True,'checks':len(checks),'Main_consumer_successes':9,'Native_API_completed_chain':8,'normal_no_rules_API_chain':True,'P0001_failure':True,'fullM6':False}))
