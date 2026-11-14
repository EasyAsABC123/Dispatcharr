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
for row in report['cases'][:9]:
 meta=row['result']['metadata'];raw=json.loads(base64.b64decode(row['result']['raw_base64'],validate=True));expected='EPG not found'if row['name']=='tvg-missing'else None
 need(raw==meta and meta['task_id']==row['task_id']and meta['status']=='SUCCESS'and meta['result']==expected,'LiteralBackendAndResult:'+row['name'])
 consumer=row['result']['Original_configured_result_consumer'];need(consumer['state']=='SUCCESS'and consumer['value']==expected,'ActualOriginalResultConsumer:'+row['name']);need(1<=row['result']['ttl']<=3600,'ActualTTL:'+row['name'])
 need(dt(row['before']['utc'])<=dt(meta['date_done'])<=dt(row['after']['utc']),'FullBackendTimestamp:'+row['name'])
 need(row['native_API_producer']is False and row['entrypoint'].startswith('retained_Source_frame_projection'),'ConsumerScopeNotAPIProducer:'+row['name'])
 keys=[base64.b64decode(v['key_base64']).decode()for v in row['after']['cache_raw']]
 need(not any(k.startswith(('task_lock_parse_epg_programs_','epg_source_tvg_parse_'))for k in keys),'ActualLeaseAndCounterCleanup:'+row['name'])
first=report['cases'][0];rows=programmes(first,'after');need(len(rows)==1 and rows[0]['title']=='Owned M6 Programme'and rows[0]['tvg_id']=='m6.batch.owned'and rows[0]['epg_id']==seed['tvg_epg'],'ActualTVGProgramme')
need(dt(rows[0]['start_time'])==dt(seed['tvg_start'])and dt(rows[0]['end_time'])==dt(seed['tvg_end']),'FullFreshInputTimestampsRetained')
repeat=report['cases'][1];need([{k:v for k,v in r.items()if k!='id'}for r in rows]==[{k:v for k,v in r.items()if k!='id'}for r in programmes(repeat,'after')],'TVGRepeatStableBusinessFieldsWithIDsRetainedRaw')
for row in report['cases'][2:5]:need(programmes(row,'before')==programmes(row,'after'),'NoopProgrammeRows:'+row['name'])
versions=report['cases'][5:9];need(notices(versions[0],'after')==[],'VersionNoUpdateClearsCurrentNotice')
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
failure=report['cases'][9];need(failure['expected_backend_status']=='FAILURE'and failure['result']['metadata']is None and failure['passed']is False,'NativeP0001FailureMetadataGapPreserved')
need(programmes(failure,'before')==programmes(failure,'after'),'ActualP0001RollbackPreservesEveryProgrammeField')
server_logs=''.join((ROOT/f'command-{i:03}.stdout').read_text()for i,row in enumerate(host['commands'],1)if 'logs'in row['argv']and row['argv'][-1]==json.loads((ROOT/'owned-role.json').read_text())['baseline']['CID']);need('ERROR:  P0001: owned M6 programme insert'in server_logs,'ActualServerSQLSTATECapturedNotDeclared')
api=report['cases'][-1];need(api['native_API_producer']is True and api['response']['status']==202 and json.loads(base64.b64decode(api['response']['body_base64']))=={'success':True,'message':'EPG data refresh initiated.'},'ActualNativeAPI202Publication')
new={ident:value for ident,value in api['after']['backend'].items()if ident in api['new_backend_ids']and value['metadata']is not None};roots=[x['metadata']for x in new.values()if x['metadata'].get('parent_id')is None];need(len(roots)==1,'OneActualNativeRefreshRoot');parent=roots[0];children=[x[0][0]for x in parent['children']];need(parent['status']=='SUCCESS'and parent['result']is None and len(children)==2 and set(children)==(set(new)-{parent['task_id']}),'ActualParentBackendTwoCompletedChildren')
series=[x['metadata']for x in new.values()if isinstance(x['metadata']['result'],dict)];need(len(series)==1,'ActualConfiguredSeriesBackend');series=series[0];need(series['parent_id']==parent['task_id']and series['result']=={'scheduled':1,'details':[{'tvg_id':'rust.owned.parented.artwork','title':'Owned Parented Artwork','status':'ok','created':1}]},'ActualConfiguredSeriesInheritedParentAndResult')
frames=[json.loads(base64.b64decode(v))for v in api['after']['queues']['celery']];need(len(frames)==2 and all(f['headers']['task']=='apps.channels.tasks.prefetch_recording_artwork'and f['headers']['parent_id']==series['task_id']and f['headers']['root_id']==parent['task_id']for f in frames),'TwoRealArtworkDescendantsRemainInFIFO')
need({f['headers']['id']for f in frames}=={x[0][0]for x in series['children']},'ExactArtworkMembershipNotFakeACK')
recordings=api['after']['tables']['dispatcharr_channels_recording'];need(len(recordings)==1 and recordings[0]['channel_id']==seed['api_channel']and dt(recordings[0]['start_time'])==dt(seed['programme_start'])and dt(recordings[0]['end_time'])==dt(seed['programme_end']),'ActualOneRecordingFullTimes')
monitor=[json.loads(x)for x in(ROOT/'redis-monitor.jsonl').read_text().splitlines()];publication=[]
for row in monitor:
 c=row.get('command','')
 if c.upper().startswith('LPUSH CELERY '):
  try:frame=json.loads(c.split(' ',2)[2])
  except ValueError:continue
  if frame['headers']['id']==parent['task_id']:publication.append((row,frame))
need(len(publication)==1 and publication[0][0]['client_address']=='lua'and publication[0][1]['headers']['task']=='apps.epg.tasks.refresh_epg_data','ActualNativeProducerLPUSHObserved')
body=json.loads(base64.b64decode(publication[0][1]['body']));need(body[0]==[seed['api_source']]and body[1]=={'force':False},'ActualNativeProducerSource37ForceFalseSpelling')
need(host['native_stop']['ExitCode']==host['origin_stop']['ExitCode']==0,'ActualNormalNativeOriginStops')
result={'qualified':True,'checks':checks,'qualified_Main_consumers':9,'consumer_entrypoint':'retained Source-template projection; NOT NativeAPIproducer proof','qualified_Native_API_positive_parent_scope':True,'full_Source37_API_contract':False,'fullM6':False,'performance_verified':False,'unpassed':['tvg-insert-error SourceFAILURE metadata absent','artwork heads unconsumed','full default scheduler/restart/other required M6 matrix cells'],'evidence_hashes':{p:h(ROOT/p)for p in['execution-private.json','native-cohort.json','seed.json','redis-monitor.jsonl','origin-requests.jsonl','tool-source-sha256.json']},'observer_limits':['Redis MONITOR observer was closed with unread tail; root Native LPUSH is retained but later child/result commands are not all observed. Backend/FIFO/SQL/Channels snapshots qualify their recorded scope; no complete command-journal claim.','new_backend_ids observer list also contains the earlier TVG ID with absent metadata; this qualification filters actual stored metadata and independently identifies the Native root/children.','No post-native-stop FIFO snapshot was captured; final pre-stop two artwork frames and normal stop are separate observations.']}
(ROOT/'qualification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'qualified':True,'checks':len(checks),'Main_consumer_cases':9,'Native_API_parent_scope':True,'fullM6':False}))
