"""Pure bounded Source capture validator; synthetic controls are not authority.
Root/request values are observed, never collapsed to DirectTask assumptions.
"""
import base64,json,uuid
from datetime import datetime
from seed_spec import *
def need(value,label):
 if value is not True:raise ValueError(label)
def same(a,b):
 if type(a)is not type(b):return False
 if isinstance(a,dict):return set(a)==set(b)and all(same(v,b[k])for k,v in a.items())
 if isinstance(a,list):return len(a)==len(b)and all(same(x,y)for x,y in zip(a,b))
 return a==b
def when(s):
 need(type(s)is str,'AwareTimeString');d=datetime.fromisoformat(s);need(d.tzinfo is not None,'AwareTime');return d
def frame(raw):return json.loads(base64.b64decode(raw,validate=True))
def canonical(s):
 try:u=uuid.UUID(s)
 except (TypeError,ValueError,AttributeError):return False
 return str(u)==s and u.version==4 and u.variant==uuid.RFC_4122
SNAPSHOT_FIELDS={'utc','channels','sources','epg_data','programmes','source_indices','recordings','recurring_rules','periodic','clocked','sentinel','core_settings','VOD_movies','VOD_logos','queues','broker','backend','result_backend_raw','cache','Channels_raw_base64','Channels_decoded','files'}
def snapshot_present(value):
 need(set(value)==SNAPSHOT_FIELDS,'AllRawSnapshotFieldsPresent')
 need(set(value['queues'])=={'celery',*QUEUES.values()},'AllFourRawQueuesPresent')
 for k in SNAPSHOT_FIELDS-{'utc','queues','backend'}:need(type(value[k])is list,'SnapshotListType:'+k)
 need(type(value['backend'])is dict,'BackendMapPresent')
 for domain in ('broker','cache','result_backend_raw'):
  for row in value[domain]:need(all(k in row for k in ('key_base64','type','DUMP_base64','ttl')),'RawRedisTypeDUMPAndTTLNotProjectedAway')
CHECKS=['setup-exact-model-produced-refresh']
for index,name in enumerate(CASES):
 CHECKS.extend(name+'-'+suffix for suffix in ('all-results-SUCCESS','all-default-descendants-preserved','role-queue-empty','zero-HTTP-dependency-attempts'))
 if index==0:CHECKS.append('refresh-created-one-parented-series')
 if index==1:CHECKS.append('series-created-two-real-artwork-children')
 if index==3:CHECKS.append('repeat-refresh-created-one-parented-series')
def validate_source_capture(code,r):
 need(type(code)is int and code==0 and r.get('passed')is True,'ActualGuestExit0SemanticTrue')
 need(r.get('Native_verified')is False and r.get('performance_verified')is False,'SourceOnly')
 need(r.get('application_task_model_signal_methods_replaced')is False and r.get('HTTP_dependency_guard_installed')is True,'ActualTasksUnmodifiedHTTPGuardDeclared')
 need(same(r['fixture_routes'],ROUTES),'ExactThreeOwnedRoutes')
 need(r.get('finally_errors')==[],'AllFinallyResourcesSettled');need(r.get('task_always_eager')is False,'ActualBrokerWorkerMode')
 need(type(r.get('actual_database'))is str and r['actual_database'].startswith('rust_probe_backup_jobs_'),'ActualCopiedDatabaseRecorded')
 need(all(v.get('actual_database')==r['actual_database']and v.get('ownership_refused')is not True for v in r['all_sql']),'EveryObservedWorkerSQLBoundToCopiedDatabase')
 cases=r['cases'];need([c['name']for c in cases]==CASES,'ExactFivePhaseOrder')
 checks=r['checks'];need([v.get('name')for v in checks]==CHECKS and all(v.get('passed')is True for v in checks),'Exact24RetainedChecks')
 exits=r['worker_exit_receipts'];need(len(exits)==5,'FiveActualWorkerWaits')
 for e in exits:need(type(e['actual_exit'])is int and type(e['actual_poll_exit'])is int and e['actual_exit']==0 and e['actual_poll_exit']==0 and e['actual_wait_completed']is True and e['pipes_settled']is True and e['all_observed_owned_PIDs_absent']is True and e['forced']is False and e['invalid']is None,'CleanActualWorkerTreeAndPipes')
 need(r['all_HTTP_attempts']==[],'ZeroRealOrInterceptedProviderContact')
 need([v['task']for v in r['all_prerun']]==EXPECTED_TASKS and[v['task']for v in r['all_postrun']]==EXPECTED_TASKS,'PredeclaredSixActualTaskExecutionsOrder')
 setup=r['setup'];snapshot_present(setup);need(len(setup['recordings'])==0,'NoPreexistingOwnedRecordings')
 seed=r['seed'];need(seed['actual_VOD_namespace']=='vod_vodlogo','VODLogoNotChannelLogoNamespace')
 owned_channels=[v for v in setup['channels']if v['id']==seed['channel_id']];need(len(owned_channels)==1 and owned_channels[0]['logo_id']is None,'NoChannelLogoFallbackConfound')
 owned_movies=[v for v in setup['VOD_movies']if v['name']==TITLE];need(len(owned_movies)==1 and owned_movies[0]['logo_id']==seed['VOD_logo_id'],'ExactLocalMovieLogoBeforeProviderStages')
 need(when(seed['programme_start'])>when(seed['anchor_utc'])and when(seed['programme_end'])>when(seed['programme_start']),'FutureProgrammeInput')
 need(len(seed['files'])==2 and all(v['uid']==seed['uid']and v['readable']is True and v['bytes']>0 for v in seed['files']),'ActualOwnedFilesModesUIDReadability')
 need(all(v['path'].startswith(seed['settings_MEDIA_ROOT']+'/rust-owned-parented-artwork/')for v in seed['files']),'OnlyOwnedMediaPaths')
 pubs=r['all_publications'];by_id={v['headers']['id']:v for v in pubs};need(len(by_id)==len(pubs),'UniqueActualPublicationIDs')
 actual_ids=[];lineages=[]
 for index,c in enumerate(cases):
  expected_role=['refresh','series','artwork','refresh','series'][index];expected_name={'refresh':REFRESH,'series':SERIES,'artwork':ARTWORK}[expected_role];need(c['role']==expected_role and len(c['tasks'])==(2 if index==2 else 1),'ExactRoleMultiplicity')
  need(c['HTTP_attempts']==[],'NoProviderAttemptWithinPhase')
  before,after=c['before'],c['after'];snapshot_present(before);snapshot_present(after);validate_role_frames(before['queues'][QUEUES[expected_role]],expected_role,c['ids']);need(all(raw in after['queues']['celery']for raw in before['queues']['celery']),'DefaultFramesNeverRemoved')
  need(after['queues'][QUEUES[expected_role]]==[],'OwnedRoleQueueDrained')
  for task in c['tasks']:
   ident=task['id'];need(canonical(ident)and ident not in actual_ids,'UniqueCanonicalActualTaskID');actual_ids.append(ident);pub=by_id[ident];need(pub['task']==expected_name and pub['routing_key']==QUEUES[expected_role],'ActualPublisherAndRoleRoute')
   need(len(task['sql'])>0 and all(v['id']==ident for v in task['sql']),'EveryTaskSQLCapturedWithIdentity');need(same(task['publications'],[v for v in pubs if v['headers'].get('id')==ident or v['headers'].get('parent_id')==ident]),'EveryTaskPublicationProjectionComplete');need(len(task['prerun'])==len(task['postrun'])==1,'ExactlyOnePrerunPostrun');pre,post=task['prerun'][0],task['postrun'][0];need(pre['task']==post['task']==expected_name and post['state']=='SUCCESS','ActualSourceTaskSUCCESS')
   metadata=task['result']['metadata'];need(same(json.loads(base64.b64decode(task['result']['raw_base64'],validate=True)),metadata),'RawBackendMetadataEquality')
   need(metadata['task_id']==ident and metadata['status']=='SUCCESS'and metadata['traceback']is None and same(metadata['result'],task['Original_get_value']),'ActualOriginalAsyncResult')
   need(type(task['result']['ttl'])is int and 3580<=task['result']['ttl']<=3600,'CapturedResultTTL')
   need(when(c['began_utc'])<=when(metadata['date_done'])<=when(c['finished_utc']),'ActualTaskCompletionInOwnPhaseWindow')
   # Preserve raw root/header/request values independently, including NULL.
   # No identity normalization/assumed fallback is used as Source authority.
   lineages.append({'id':ident,'published_root':pub['headers']['root_id'],'published_parent':pub['headers']['parent_id'],'prerun_root':pre['root_id'],'prerun_parent':pre['parent_id'],'postrun_root':post['root_id'],'postrun_parent':post['parent_id'],'metadata_parent_present':'parent_id'in metadata,'metadata_parent':metadata.get('parent_id')})
   if pub['headers']['parent_id']is not None:need('parent_id'in metadata and metadata['parent_id']==pub['headers']['parent_id']and pre['parent_id']==post['parent_id']==pub['headers']['parent_id'],'ObservedParentNotCollapsed')
   if expected_role=='refresh':
    need(task['Original_get_value']is None,'ActualRefreshNormalNull')
    emitted=[v['headers']['id']for v in pubs if v['headers'].get('parent_id')==ident];need([v[0][0]for v in metadata['children']]==emitted and len(emitted)==2 and[by_id[x]['task']for x in emitted]==[INDEX,SERIES],'ActualRefreshIndexThenSeriesMetadataTrail')
   if expected_role=='series':need(type(task['Original_get_value'])is dict and type(task['Original_get_value']['scheduled'])is int and task['Original_get_value']['scheduled']==(1 if index==1 else 0),'ActualPositiveThenRepeatZero')
   if expected_role=='artwork':need(task['Original_get_value']=='ok'and task['Original_get_type']=='builtins.str','ActualArtworkNormalOKString');need(pub['headers']['eta']is not None,'RealCountdownETAWasNotRemoved')
  owned_programmes=[v for v in after['programmes']if v['epg_id']==seed['epg_id']];need(len(owned_programmes)==1 and owned_programmes[0]['tvg_id']==TVG and owned_programmes[0]['title']==TITLE and when(owned_programmes[0]['start_time'])==when(seed['programme_start'])and when(owned_programmes[0]['end_time'])==when(seed['programme_end']),'ActualXMLProgrammeMappedToOwnedChannel')
  need(same(before['files'],after['files']),'OwnedInputFilesUnchanged')
  if index in (0,3,4):need(same(before['recordings'],after['recordings']),'NoUnplannedRecordingMutation')
  count=len(after['recordings'])-len(before['recordings']);need(count==(1 if index==1 else 0),'OnlySeriesPositiveCreatesOneRecording')
  need(same(before['VOD_movies'],after['VOD_movies'])and same(before['VOD_logos'],after['VOD_logos']),'OwnedVODLookupRowsNotMutated')
  if index==2:
   need(len(before['recordings'])==len(after['recordings'])==1,'OneArtworkTargetRow')
   cp=after['recordings'][0]['custom_properties'];need(cp.get('poster_logo_id')==seed['VOD_logo_id'],'ActualLocalVODPosterID')
   first_updates=[q for q in c['tasks'][0]['sql']if q['sql'].lstrip().startswith('UPDATE "dispatcharr_channels_recording"')];second_updates=[q for q in c['tasks'][1]['sql']if q['sql'].lstrip().startswith('UPDATE "dispatcharr_channels_recording"')];need(len(first_updates)==1 and second_updates==[],'FirstArtworkMutationSecondDuplicateNoMutation')
   newevents=after['Channels_decoded'][len(before['Channels_decoded']):];recording_updates=[v for v in newevents if v.get('type')=='update'and v.get('data',{}).get('type')=='recording_updated'];need(len(recording_updates)==1 and recording_updates[0]['data']['recording_id']==after['recordings'][0]['id'],'OneActualArtworkUpdateEvent')
   need(same(before['clocked'],after['clocked'])and same(before['periodic'],after['periodic']),'MetadataSaveDidNotRescheduleOrRedispatch')
 need(same(r['all_sql'],[q for case in cases for task in case['tasks']for q in task['sql']]),'AllWorkerSQLInActualTaskOrder')
 root_a,series_a=cases[0]['ids'][0],cases[1]['ids'][0]
 root_b,series_b=cases[3]['ids'][0],cases[4]['ids'][0]
 need(by_id[series_a]['headers']['parent_id']==root_a and by_id[series_b]['headers']['parent_id']==root_b,'ActualRefreshProducedParentedSeries')
 art_ids=cases[2]['ids'];need(len(art_ids)==2 and all(by_id[v]['headers']['parent_id']==series_a for v in art_ids),'ActualSignalAndSeriesArtworkParents')
 need([v['headers']['id']for v in pubs if v['task']==ARTWORK]==art_ids,'ActualArtworkPublicationOrderNoSorting')
 need([v['id']for v in r['all_postrun']if v['task']==ARTWORK]==art_ids,'PredeclaredArtworkExecutionOrder')
 need([v[0][0]for v in cases[1]['tasks'][0]['result']['metadata']['children']]==art_ids,'SeriesResultChildrenExactPublicationOrder')
 need(cases[4]['tasks'][0]['result']['metadata']['children']==[],'RepeatProducedNoArtworkChildren')
 final=r['final'];snapshot_present(final);default=[frame(v)for v in final['queues']['celery']];need([v['headers']['task']for v in reversed(default)]==[SETUP,INDEX,INDEX],'ExactDefaultDescendantsPreservedFIFOObservation')
 need(len(final['recordings'])==1 and len(final['clocked'])==1,'OneFutureRecordingAndClockedRow')
 record=final['recordings'][0];schedule=[v for v in final['periodic']if v['name']==record['task_id']];need(len(schedule)==1,'ExactFutureRecordingBeatIdentity')
 period=schedule[0];need(period['task']=='apps.channels.tasks.run_recording'and period['one_off']is True and period['enabled']is True and period['clocked_id']==final['clocked'][0]['id'],'RealFutureRecordingBeatRowRetained')
 need(json.loads(period['args'])==[record['id'],record['channel_id'],record['start_time'],record['end_time']]and when(final['clocked'][0]['clocked_time'])==when(record['start_time']),'ExactFutureRecordingArgsAndInstant')
 need(not any(v['task']=='apps.channels.tasks.run_recording'for v in r['all_prerun']),'NoFutureRecordingTaskConsumed')
 return {'passed':True,'cases':5,'actual_tasks':6,'actual_task_order':EXPECTED_TASKS,'observed_lineages':lineages,'default_descendant_tasks':[SETUP,INDEX,INDEX],'root_inheritance_inferred_or_normalized':False,'Native_verified':False,'full_workflow':False,'artwork_domain':'one exact localVODLogo/title; no provider/HTTP/othertitle authority'}
