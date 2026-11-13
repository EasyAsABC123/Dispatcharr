"""Pure parented component acceptor. All raw observations remain intact.
No default FIFO, native artwork, generic Redis-byte or SQL-query-count claim.
"""
import base64,copy,hashlib,json
from datetime import timedelta,datetime,timezone
from uuid import UUID
from contract import SNAPSHOT_FIELDS,snapshot_present,frame,same,validate_source_capture
from seed_spec import CASES,REFRESH,SERIES,ARTWORK,INDEX,SETUP,QUEUES,xmltv,PNG_BASE64
from row_compare import Comparison,require,equal,instant,TABLES

def uuid(s):
 require(type(s)is str,'UUIDString');u=UUID(s);require(str(u)==s and u.version==4 and u.variant=='specified in RFC 4122','CanonicalUUID4');return s

def metadata(t,case,window):
 r=t['result'];require(r['raw_base64']is not None,'RetainedActualBackend');m=json.loads(base64.b64decode(r['raw_base64'],validate=True));require(equal(m,r['metadata']),'ExactRawBackendProjection')
 require(m['status']=='SUCCESS'and m['traceback']is None and m['task_id']==t['id'],'KnownSuccessMetadataIdentity')
 require(type(m['children'])is list and type(r['ttl'])is int and 3580<=r['ttl']<=3600,'ActualChildrenAndFixedTTL')
 key=base64.b64decode(r['key_base64'],validate=True);require(key==('celery-tasks:celery-task-meta-'+t['id']).encode(),'ActualConfiguredBackendPrefix')
 require(instant(window[0])<=instant(m['date_done'])<=instant(window[1]),'ActualMetadataCompletionWindow')
 require(equal(t['Original_get_value'],m['result']),'ActualOriginalGetValue')
 wanted='builtins.NoneType'if m['result']is None else'builtins.dict'if isinstance(m['result'],dict)else'builtins.str'
 require(t['Original_get_type']==wanted,'ActualOriginalGetType')
 return m

def children(m):
 result=[]
 for c in m['children']:
  require(type(c)is list and len(c)==2 and c[1]is None and type(c[0])is list and len(c[0])==2 and c[0][1]is None,'ExactChildTupleShape');result.append(uuid(c[0][0]))
 require(len(result)==len(set(result)),'UniqueChildIDs');return result

def source_window(case):
 rows=[v for t in case['tasks']for v in t['prerun']];ends=[v for t in case['tasks']for v in t['postrun']]
 require(bool(rows)and bool(ends),'ActualSourcePrerunPostrunWindow');return [min((v['utc']for v in rows),key=instant),max((v['utc']for v in ends),key=instant)]

def native_task_window(task):
 p=task['native_process'];require(p['spawn_state']=='actual_child_started'and p['exit']==0 and p['settled']is True and p['pipes_settled']is True and p['writer_settled']is True and p['invalid']is None,'ActualNativeProcessAndPipes')
 rows=task['native_responses'];require(len(rows)==3 and rows[0]['prepared']is True and rows[0]['route']==QUEUES['artwork']and rows[0]['task_id']==task['id']and rows[0]['provider_fallback']is False and rows[2]=={'closed':True},'ActualFiniteOwnedArtworkPeerTranscript')
 raw=[frame(value)for value in task['before']['queues'][QUEUES['artwork']]if frame(value)['headers']['id']==task['id']];require(len(raw)==1,'OneActualRawArtworkForTranscript');h=raw[0]['headers'];require(rows[0]['root']==h['root_id']and rows[0]['parent']==h['parent_id'],'PreparedLineageMatchesActualRawFrame');require(instant(rows[0]['captured_ETA'])==instant(h['eta']),'PreparedETAIsActualRawETA')
 run=rows[1];require(run['action']=='run'and run['ok']is True and run['typed_result']['state']=='Completed'and run['typed_result']['task_id']==task['id']and type(run['typed_result']['changed'])is bool and run['caller_released']is True and type(run['available_slots'])is int and run['available_slots']==1,'ActualCompletedOwnedNativeArtworkTask')
 require(task['prerun']==[]and task['postrun']==[]and task['sql']==[],'NoFabricatedSourceObservationsForNative')
 require(instant(p['began_utc'])<=instant(run['began_utc'])<=instant(run['finished_utc'])<=instant(p['finished_utc']),'ActualNativeTranscriptOwnProcessWindow')
 require(instant(rows[0]['captured_ETA'])<=instant(run['began_utc']),'ActualCapturedETABeforeRun')
 return [run['began_utc'],run['finished_utc']]

def native_window(case):
 require(case['Source_task_prerun_fabricated']is False and len(case['tasks'])==2 and len(case['native_processes'])==2,'TwoActualNativeArtworkProcesses')
 windows=[native_task_window(t)for t in case['tasks']]
 require(all(equal(p,t['native_process'])for p,t in zip(case['native_processes'],case['tasks'])),'ActualCaseProcessReceiptMatchesTaskReceipt')
 require(instant(windows[0][1])<=instant(windows[1][0]),'ActualSerialArtworkCommandOrder')
 return [windows[0][0],windows[-1][-1]]

def sql_rows_and_times(reference,actual,source_begin,native_begin):
 scontext={'seed':reference['seed'],'lifecycle_began_utc':source_begin};ncontext={'seed':actual['seed'],'lifecycle_began_utc':native_begin};c=Comparison(scontext,ncontext)
 for s,n in zip(reference['cases'],actual['cases']):
  sw=source_window(s);nw=native_window(n)if n['role']=='artwork'else source_window(n)
  sc=dict(s,task_window=sw);nc=dict(n,task_window=nw)
  for phase in ['before','after']:c.rows(s[phase],n[phase],phase,sc,nc)
 return {'all_SQL_row_fields_compared':True,'row_id_bindings':c.ids,'opaque_model_UUID_bindings':c.opaque,'input_anchor_correspondence':{'Source':reference['seed']['anchor_utc'],'Native':actual['seed']['anchor_utc'],'delta_seconds':c.delta.total_seconds(),'SQL_clock_injection':False},'temporal_observations':c.observations}

def verify_files(report):
 seed=report['seed'];expected_xml=xmltv(instant(seed['programme_start']),instant(seed['programme_end']));expected_png=base64.b64decode(PNG_BASE64,validate=True)
 before=None
 for snapshot in [report['setup']]+[p for c in report['cases']for p in[c['before'],c['after']]]+[report['final']]:
  files=snapshot['files'];require(len(files)==2,'BothOwnedPositiveFiles')
  for row,blob,name in zip(files,[expected_xml,expected_png],['guide.xml','logo.png']):
   # Diagnostic enumeration is predeclared filename ordering, never outcome sort.
   require(row['path']==seed['settings_MEDIA_ROOT']+'/rust-owned-parented-artwork/'+name and row['bytes']==len(blob)and row['sha256']==hashlib.sha256(blob).hexdigest(),'ExactOwnedInputFileContent')
   require(row['mode']==33188 and row['uid']==seed['uid']==0 and row['gid']==0 and row['readable']is True and type(row['mtime_ns'])is int,'ActualSourceFixtureFileContext')
   anchor_ns=int(instant(seed['anchor_utc']).timestamp())*1000000000;setup=instant(report['setup']['utc']);setup_ns=int(setup.timestamp())*1000000000+setup.microsecond*1000;require(anchor_ns<=row['mtime_ns']<=setup_ns,'ActualSeedFileStatWindow')
  if before is None:before=files
  else:require(equal(before,files),'OwnedInputFilesLiteralUnchanged')

def raw_snapshot_consistency(snapshot):
 snapshot_present(snapshot)
 broker={base64.b64decode(v['key_base64'],validate=True):v for v in snapshot['broker']}
 for queue,raw in snapshot['queues'].items():
  entry=broker.get(queue.encode())
  if entry is None:require(raw==[],'AbsentRawQueueCannotContainFrames')
  else:require(entry['type']=='list'and entry['list_base64']==raw,'ActualRawBrokerListProjection')
 require(b'unacked'not in broker and b'unacked_index'not in broker,'NoHiddenOutstandingTaskReservations')
 backend={base64.b64decode(v['key_base64'],validate=True):v for v in snapshot['result_backend_raw']}
 expected={}
 for ident,row in snapshot['backend'].items():
  key=base64.b64decode(row['key_base64'],validate=True)
  require(key==('celery-tasks:celery-task-meta-'+ident).encode(),'EveryKnownActualBackendKey')
  if row['raw_base64']is not None:
   require(equal(json.loads(base64.b64decode(row['raw_base64'],validate=True)),row['metadata']),'EveryKnownRawMetadataProjection')
   expected[key]=row['raw_base64']
 require(set(backend)==set(expected),'NoMissingOrUnexpectedRawBackendKeys')
 for key,value in expected.items():require(backend[key]['type']=='string'and backend[key]['value_base64']==value,'EveryRawBackendValuePreserved')

def cache_and_event_effects(reference,r):
 from primitive_cache import cache_value
 def cache_map(snapshot):return {base64.b64decode(v['key_base64'],validate=True):v for v in snapshot['cache']}
 key=b':1:coresettings:group:dvr_settings';lock=b'task_lock_evaluate_series_rules_all';observations=[]
 for source,actual in zip(reference['cases'],r['cases']):
  for label in ['before','after']:
   old=cache_map(source[label]);new=cache_map(actual[label]);sr=old.get(key);nr=new.get(key)
   require((sr is None)==(nr is None),'ActualGroupedDVRCachePresence')
   if nr is not None:
    require(sr['type']==nr['type']=='string','ActualGroupedDVRCacheType')
    sv=cache_value(sr['value_base64']);nv=cache_value(nr['value_base64'])
    sco=next(v['value']for v in source[label]['core_settings']if v['key']=='dvr_settings');nco=next(v['value']for v in actual[label]['core_settings']if v['key']=='dvr_settings')
    require(equal(sv,sco)and equal(nv,nco),'ActualDataOnlyGroupedDVRValueMatchesOwnSQLSettings')
    require(type(sr['ttl'])is int and type(nr['ttl'])is int and 1<=sr['ttl']<=300 and 1<=nr['ttl']<=300,'ActualGroupedDVR300TTL')
   require(lock not in new and lock not in old,'NoResidualGlobalSeriesLease')
   require(len(actual[label]['Channels_raw_base64'])==len(actual[label]['Channels_decoded']),'AllRawEventFramesRetained')
  before,after=actual['before'],actual['after'];sb,sa=source['before'],source['after']
  require(after['Channels_raw_base64'][:len(before['Channels_raw_base64'])]==before['Channels_raw_base64'],'PriorRawEventsUnchanged')
  expected=sa['Channels_decoded'][len(sb['Channels_decoded']):];observed=after['Channels_decoded'][len(before['Channels_decoded']):]
  require(len(expected)==len(observed),'ExactSourceEventDeltaCount')
  # Every event field is retained and compared. The only execution-generated
  # field in this captured domain is EPG updated_at from actual Refresh code.
  for s_event,n_event in zip(expected,observed):
   es=copy.deepcopy(s_event);en=copy.deepcopy(n_event)
   if 'updated_at'in es.get('data',{}):
    require('updated_at'in en['data'],'EventClockFieldPresence')
    sw=source_window(source);nw=source_window(actual)
    for value,window,name in[(es['data']['updated_at'],sw,'Source'),(en['data']['updated_at'],nw,'Native')]:require(instant(window[0])<=instant(value)<=instant(window[1]),name+'EventClockOwnWindow')
    observations.append({'Source_raw':es['data']['updated_at'],'Native_raw':en['data']['updated_at'],'Source_window':sw,'Native_window':nw,'case':actual['name'],'path':'event.data.updated_at'})
    en['data']['updated_at']=es['data']['updated_at']
   require(equal(es,en),'EveryEventFieldAndSourceEffect')
  for ident,row in before['backend'].items():
   if row['raw_base64']is not None:require(ident in after['backend']and row['raw_base64']==after['backend'][ident]['raw_base64'],'PriorTaskMetadataLiteralUnchanged')
 return {'event_temporal_observations':observations,'DVR_cache_primitive_values_compared':True,'global_series_lease_absent':True,'universal_rawRedis_byte_parity_claim':False}

def validate(code,r,wrapper):
 require(type(code)is int and code==0,'ActualGuestExit0');require(r['passed']is True and r['Native_verified']is True and r['Source_verified']is False and r['Source_reference_verified']is True and r['performance_verified']is False,'ActualScopedStatus')
 require(r['predeclared_cases']==CASES and[r['cases'][i]['name']for i in range(len(r['cases']))]==CASES,'ExactFiveCasesInOrder')
 reference=wrapper['Source'];require(equal(validate_source_capture(0,reference),reference['source_acceptance']),'IndependentlyQualifiedSource24Reference');require(hashlib.sha256((json.dumps(reference,indent=2)+'\n').encode()).hexdigest()==wrapper['raw_Source_sha256'],'ExactRetainedSourceReference')
 require(len(r['cases'])==5 and len(r['checks'])==24 and all(v['passed']is True for v in r['checks']),'AllPredeclared24CaptureChecks')
 require(r['application_task_model_signal_methods_replaced']is False and r['all_HTTP_attempts']==[]and r['task_always_eager']is False and r['finally_errors']==[],'ActualApplicationPathAndNoProviderCalls')
 require(len(r['worker_exit_receipts'])==4 and all(v['actual_exit']==0 and v['actual_wait_completed']is True and v['pipes_settled']is True and v['all_observed_owned_PIDs_absent']is True and v['invalid']is None and v['forced']is False for v in r['worker_exit_receipts']),'FourRealOriginalWorkerClosures')
 ids={};native_windows={};semantic_checks=[]
 for s,n in zip(reference['cases'],r['cases']):
  require(set(n['before'])==set(s['before'])==SNAPSHOT_FIELDS and set(n['after'])==set(s['after'])==SNAPSHOT_FIELDS,'AllRawSnapshotFieldsRetained')
  snapshot_present(n['before']);snapshot_present(n['after']);require(n['role']==s['role']and len(n['tasks'])==len(s['tasks']),'ExactRoleAndTaskMultiplicity')
  window=native_window(n)if n['role']=='artwork'else source_window(n);native_windows[n['name']]=window
  require(instant(n['began_utc'])<=instant(n['before']['utc'])<=instant(window[0])<=instant(window[1])<=instant(n['finished_utc'])<=instant(n['after']['utc']),'ActualObservationAndCommandOrder')
  for a,b in zip(s['tasks'],n['tasks']):
   uuid(a['id']);uuid(b['id']);old=ids.setdefault(a['id'],b['id']);require(old==b['id'],'StableTaskIdentityCorrespondence')
  require(n['after']['queues'][QUEUES[n['role']]]==[],'RoleQueueActuallyConsumed')
  require(n['before']['queues']['celery']==n['after']['queues']['celery']if n['role']!='refresh'else all(v in n['after']['queues']['celery']for v in n['before']['queues']['celery']),'DefaultFramesLiteralPreservation')
 # Bind all task identities before traversing metadata/descendant relationships.
 for s,n in zip(reference['cases'],r['cases']):
  for a,b in zip(s['tasks'],n['tasks']):
   sm=a['result']['metadata'];nm=metadata(b,n,native_task_window(b)if n['role']=='artwork'else native_windows[n['name']]);require(set(sm)==set(nm),'MetadataFieldPresence')
   require(equal(sm['result'],nm['result']),'ExactSourceKnownResultValue')
   require(('parent_id'in sm)==('parent_id'in nm),'ParentFieldPresenceNotNullFallback')
   if 'parent_id'in sm:require(nm['parent_id']==ids[sm['parent_id']],'ExactObservedParentIdentity')
   sch=children(sm);nch=children(nm)
   # Refresh Index child remains default and is bound by its actual ordered tuple,
   # never silently assumed absent merely because it is not consumed.
   require(len(sch)==len(nch),'ExactOrderedChildMultiplicity')
   for old,new in zip(sch,nch):require(ids.setdefault(old,new)==new,'OrderedChildIdentity')
 require(len(set(ids.values()))==len(ids),'OneToOneTaskIdentity')
 # Both artwork frames are actual Original publications and raw queued bytes,
 # not synthesized Native before_task_publish events. Validate all headers and
 # own countdown windows against the actual Original Series command interval.
 require(r['native_publications']==[],'NoInventedNativePublications')
 first=r['cases'][1];parent=first['ids'][0];root=r['cases'][0]['ids'][0]
 source_pubs=[v for v in reference['all_publications']if v['task']==ARTWORK]
 pubs=[v for v in r['all_publications']if v['task']==ARTWORK]
 require(len(pubs)==len(source_pubs)==2,'ActualTwoOriginalArtworkPublications')
 artids=children(first['tasks'][0]['result']['metadata']);require([p['headers']['id']for p in pubs]==artids==r['cases'][2]['ids'],'ActualOrderedChildrenAndNativeConsumedIDs')
 for pub,source_pub in zip(pubs,source_pubs):
  require(set(pub)==set(source_pub)and set(pub['headers'])==set(source_pub['headers']),'AllOriginalPublicationFields')
  h=pub['headers'];require(h['root_id']==root and h['parent_id']==parent,'InheritedRefreshRootAndSeriesParent')
  for key,value in source_pub['headers'].items():
   if key not in['id','root_id','parent_id','origin','eta','argsrepr']:require(equal(value,h[key]),'ExactArtworkHeader:'+key)
  require(instant(native_windows[first['name']][0])+timedelta(seconds=1)<=instant(h['eta'])<=instant(native_windows[first['name']][1])+timedelta(seconds=1),'ActualOriginalCountdownOwnSeriesWindow')
  raw=[v for v in first['after']['queues'][QUEUES['artwork']]if frame(v)['headers']['id']==h['id']];require(len(raw)==1 and raw[0]in r['cases'][2]['before']['queues'][QUEUES['artwork']],'ActualRawOriginalArtworkTransferredToNative')
  queued=frame(raw[0]);require(equal(queued['headers'],h)and queued['properties']['delivery_info']=={'exchange':'','routing_key':QUEUES['artwork']},'ActualRawHeadersAndOwnedRoute')
  schema=frame(reference['cases'][1]['after']['queues'][QUEUES['artwork']][-1]);require(set(queued)==set(schema)and set(queued['properties'])==set(schema['properties']),'CompleteArtworkRawEnvelopeAndProperties');require(queued['content-type']=='application/json'and queued['content-encoding']=='utf-8','ArtworkEncoding');require(all(equal(queued['properties'][k],schema['properties'][k])for k in['delivery_mode','priority','body_encoding']),'ArtworkImmutableProperties');require(h['argsrepr']=='['+str(r['final']['recordings'][0]['id'])+']','RecordingIDArgsreprCorrespondence');require(pub['exchange']==''and pub['routing_key']==QUEUES['artwork'],'OriginalPublicationRoute');require(instant(native_windows[first['name']][0])<=instant(pub['utc'])<=instant(native_windows[first['name']][1]),'PublicationOwnActualCommandWindow')
  require(equal(json.loads(base64.b64decode(queued['body'],validate=True)),pub['body']),'ActualRawBodyProjection')
  require(equal(pub['body'],[[r['final']['recordings'][0]['id']],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}]),'ExactActualArtworkBody')
  uuid(queued['properties']['delivery_tag']);require(queued['properties']['correlation_id']==h['id'],'RawCorrelationIdentity')
 # Source production model/signal effects; duplicate artwork does not repeat save.
 require(len(r['final']['recordings'])==1 and len(r['final']['clocked'])==1,'OneActualRecordingAndClockedRow')
 for index,count in [(1,1),(4,0)]:require(r['cases'][index]['tasks'][0]['Original_get_value']=={'scheduled':count,'details':[{'tvg_id':'rust.owned.parented.artwork','title':'Owned Parented Artwork','status':'ok','created':count}]},'PositiveRepeatExactSourceResult')
 art=r['cases'][2];require([t['Original_get_value']for t in art['tasks']]==['ok','ok'],'BothActualNativeArtworkOriginalGetValues')
 require([task['native_responses'][1]['typed_result']['changed']for task in art['tasks']]==[True,False],'ActualConfirmedUpdateThenNoopServiceOutcomes')
 require([len(task['after']['Channels_decoded'])-len(task['before']['Channels_decoded'])for task in art['tasks']]==[1,0],'PerTaskActualEventDelta')
 require(art['tasks'][0]['after']['queues'][QUEUES['artwork']]==art['before']['queues'][QUEUES['artwork']][:-1]and art['tasks'][1]['before']['queues'][QUEUES['artwork']]==art['tasks'][0]['after']['queues'][QUEUES['artwork']]and art['tasks'][1]['after']['queues'][QUEUES['artwork']]==[],'ActualSerialRawFIFOConsumption')
 for task in art['tasks']:
  raw_snapshot_consistency(task['before']);raw_snapshot_consistency(task['after'])
 require(equal(art['tasks'][0]['after']['recordings'],art['tasks'][1]['before']['recordings'])and equal(art['tasks'][1]['before']['recordings'],art['tasks'][1]['after']['recordings']),'DuplicateArtworkActualRowsUnchanged')
 require(art['before']['Channels_decoded']+[{'type':'update','data':{'success':True,'type':'recording_updated','recording_id':r['final']['recordings'][0]['id']},'__asgi_channel__':['specific.rust_owned_parented!receiver']}]==art['after']['Channels_decoded'],'ActualSingleRecordingUpdatedEvent')
 default=[frame(raw)for raw in reversed(r['final']['queues']['celery'])];require([f['headers']['task']for f in default]==[SETUP,INDEX,INDEX],'ThreeDefaultDescendantsUntouchedFIFO')
 require(not any(v['task']=='apps.channels.tasks.run_recording'for v in r['all_prerun']),'NoFutureRecordingConsumed')
 require([v['task']for v in r['all_prerun']]==[REFRESH,SERIES,REFRESH,SERIES],'OnlyFourActualOriginalTaskPreruns')
 require(all(equal(r['final'][table],r['cases'][-1]['after'][table])for table in TABLES),'NoDiscardedLateSQLRowEffect');verify_files(r);verify_files(reference)
 for snapshot in [r['setup']]+[v for case in r['cases']for v in[case['before'],case['after']]]+[r['final']]:raw_snapshot_consistency(snapshot)
 cache_events=cache_and_event_effects(reference,r);rows=sql_rows_and_times(reference,r,wrapper['Source_lifecycle_began_utc'],r['lifecycle_began_utc'])
 return {'passed':True,'cases':5,'tasks':6,'Native_tasks':2,'Original_tasks':4,'capture_checks':24,'Native_verified':True,'Source_reference_sha256':wrapper['raw_Source_sha256'],'all_raw_snapshots_retained':True,'all_business_SQL_fields_compared':True,'Native_SQL_wire_claim':False,'comparison':rows,'cache_and_events':cache_events,'task_identity_bindings':ids,'Native_command_windows':native_windows,'unsupported':['Provider/other title domains','Generic Redis serialization byte parity','Native defaultFIFO/Main/fullgraph','Future run_recording'],'scope':'Original parented Refresh/Series producer to Native bounded local-VODLogo artwork effect/result interoperability; owned routes only'}
