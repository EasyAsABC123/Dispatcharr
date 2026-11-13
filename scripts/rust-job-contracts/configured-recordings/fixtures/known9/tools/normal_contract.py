"""UNEXECUTED normal-parent validator. No app/network on import.
Raw Source/Native snapshots stay intact. Explicit input-date correspondence is
separate from execution clocks; no blanket stripping or dictionary get/null
projection. No full default-FIFO/child-consumption or SQL-query-count parity.
"""
import base64,copy,io,json,math,pickle,pickletools
from datetime import datetime,timedelta,timezone
from uuid import UUID
TABLES=('channels','sources','epg_data','programmes','system_settings','recordings','recurring_rules','periodic','clocked','sentinel','dvr_settings')
EXECUTION={'created_at','updated_at','date_changed','last_update'}
SERIES='apps.channels.tasks.evaluate_series_rules';RECURRING='apps.channels.tasks.maintain_recurring_recordings';ARTWORK='apps.channels.tasks.prefetch_recording_artwork'
SOURCE_INDICES=(1,2,3,5,6,7,8,9,10)
def require(value,label):
 if not value:raise ValueError(label)
def equal(a,b):
 if type(a)is not type(b):return False
 if isinstance(a,dict):return set(a)==set(b)and all(equal(a[k],b[k])for k in a)
 if isinstance(a,list):return len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))
 return a==b
def instant(value):
 require(type(value)is str,'AwareTimeString');value=datetime.fromisoformat(value);require(value.tzinfo is not None,'ExplicitTimeOffset');return value.astimezone(timezone.utc)
def uuid(value):
 parsed=UUID(value);require(str(parsed)==value and parsed.version==4,'CanonicalUUID4');return value
def raw_frame(row):
 value=json.loads(base64.b64decode(row['raw_base64'],validate=True));require(equal(value,row['decoded']),'RawFrameProjection');return value
def raw_metadata(row):
 require(row['raw_base64']is not None,'ActualBackendPresent');value=json.loads(base64.b64decode(row['raw_base64'],validate=True));require(equal(value,row['metadata']),'RawBackendProjection');return value
class Restricted(pickle.Unpickler):
 def find_class(self,*unused):raise ValueError('NoPickleGlobals')
 def persistent_load(self,*unused):raise ValueError('NoPersistentPickle')
def cache_value(raw):
 if raw is None:return None
 data=base64.b64decode(raw,validate=True);require(len(data)<=1048576,'CacheByteCap')
 forbidden={'GLOBAL','STACK_GLOBAL','REDUCE','BUILD','NEWOBJ','NEWOBJ_EX','OBJ','INST','EXT1','EXT2','EXT4','PERSID','BINPERSID'}
 require(all(op.name not in forbidden for op,_,_ in pickletools.genops(data)),'DataOnlyPickleOpcodes')
 value=Restricted(io.BytesIO(data)).load();nodes=0
 def check(v,depth):
  nonlocal nodes
  nodes+=1;require(nodes<=16384 and depth<=32,'CacheLogicalCap')
  require(type(v)in(dict,list,str,int,float,bool,type(None)),'CachePrimitiveOnly')
  if type(v)is float:require(math.isfinite(v),'FiniteCacheFloat')
  if type(v)is dict:
   require(all(type(k)is str for k in v),'CacheStringKeys')
   for item in v.values():check(item,depth+1)
  elif type(v)is list:
   for item in v:check(item,depth+1)
 check(value,0);return value
class Comparison:
 def __init__(self,report):
  self.report=report;self.bindings=report.setdefault('row_id_bindings',{});self.clocks=report.setdefault('execution_clock_bindings',{});self.raw=report.setdefault('temporal_observations',[]);self.day_delta=report['fixture_date_delta_days']
 def mapped_id(self,table,value):
  if value is None:return None
  require(type(value)is int,'IntegerID:'+table)
  require(str(value)in self.bindings[table],'BoundID:'+table+':'+str(value));return self.bindings[table][str(value)]
 def bind_ids(self,snapshot,native):
  for table in TABLES:
   source=snapshot[table];actual=native[table];require(len(source)==len(actual),'FullRowCount:'+table)
   binding=self.bindings.setdefault(table,{})
   for s,n in zip(source,actual):
    key='ident'if table=='sentinel'else'id'
    require(key in s and key in n,'RowIDPresence:'+table)
    old=binding.setdefault(str(s[key]),n[key]);require(old==n[key],'StableRowIdentity:'+table)
   require(len(set(binding.values()))==len(binding),'OneToOneRowIdentity:'+table)
 def input_time(self,value):return(instant(value)+timedelta(days=self.day_delta))
 def clock(self,s,n,path,window,changed=False):
  require(type(s)is type(n),'ClockType:'+path)
  if s is None:return
  source=instant(s);native=instant(n);begin,end=map(instant,window)
  require(begin<=native<=end,'ActualNativeClockWindow:'+path)
  old=self.clocks.get(path)
  if old:
   require((old['Source']==s)==(old['Native']==n),'ChangedUnchangedClockRelation:'+path)
  self.clocks[path]={'Source':s,'Native':n};self.raw.append({'path':path,'Source_raw':s,'Native_raw':n,'Native_window':window,'changed_in_task':changed})
 def transformed(self,table,row):
  value=copy.deepcopy(row);key='ident'if table=='sentinel'else'id';value[key]=self.mapped_id(table,value[key])
  fk={'channel_id':'channels','epg_data_id':'epg_data','epg_id':'epg_data','epg_source_id':'sources','clocked_id':'clocked'}
  for field,target in fk.items():
   if field in value and value[field]is not None:value[field]=self.mapped_id(target,value[field])
  if table in('recordings','programmes'):
   for field in('start_time','end_time'):
    require(field in value,'FixtureTimePresence');value[field]=self.input_time(value[field])
  if table=='clocked':value['clocked_time']=self.input_time(value['clocked_time'])
  if table=='recurring_rules':
   for field in('start_date','end_date'):
    if value[field]is not None:value[field]=(datetime.fromisoformat(value[field])+timedelta(days=self.day_delta)).date().isoformat()
   value['days_of_week']=[(d+self.day_delta)%7 for d in value['days_of_week']]
  if table=='recordings':
   if value['task_id']is not None:
    prefix='dvr-recording-';require(value['task_id'].startswith(prefix),'RecordingTaskName');value['task_id']=prefix+str(value['id'])
   cp=value['custom_properties'];program=cp.get('program')if isinstance(cp,dict)else None
   if isinstance(program,dict):
    if 'id'in program:program['id']=self.mapped_id('programmes',program['id'])
    if 'epg_source_id'in program:program['epg_source_id']=self.mapped_id('sources',program['epg_source_id'])
    for field in('start_time','end_time'):
     if field in program:program[field]=self.input_time(program[field])
   if isinstance(cp,dict)and'rule'in cp:
    cp['rule']['id']=self.mapped_id('recurring_rules',cp['rule']['id']);cp['rule']['days_of_week']=[(d+self.day_delta)%7 for d in cp['rule']['days_of_week']]
  if table=='periodic'and value['task']=='apps.channels.tasks.run_recording':
   args=json.loads(value['args']);require(len(args)==4,'RecordingArgsShape');args[0]=self.mapped_id('recordings',args[0]);args[1]=self.mapped_id('channels',args[1]);args[2]=self.input_time(args[2]);args[3]=self.input_time(args[3]);value['args']=args
   value['name']='dvr-recording-'+str(args[0])
  return value
 def compare_value(self,s,n,path):
  if isinstance(s,datetime):require(s==instant(n),'InputDatetime:'+path);return
  require(type(s)is type(n),'ExactType:'+path)
  if isinstance(s,dict):
   require(set(s)==set(n),'FieldPresence:'+path)
   for key in s:self.compare_value(s[key],n[key],path+'.'+key)
  elif isinstance(s,list):
   require(len(s)==len(n),'ListCount:'+path)
   for i,(a,b)in enumerate(zip(s,n)):self.compare_value(a,b,path+str(i))
  else:require(s==n,'ExactValue:'+path)
 def rows(self,snapshot,native,phase,case):
  self.bind_ids(snapshot,native)
  for table in TABLES:
   for s,n in zip(snapshot[table],native[table]):
    require(set(s)==set(n),'FullRowFieldPresence:'+table)
    expected=self.transformed(table,s);actual=copy.deepcopy(n)
    if table=='periodic'and actual['task']=='apps.channels.tasks.run_recording':
     actual['args']=json.loads(actual['args'])
    for field in EXECUTION:
     if field not in s:continue
     key='ident'if table=='sentinel'else'id';path=table+':'+str(s[key])+'.'+field
     is_new=phase=='after'and not any(r[key]==s[key]for r in case['source_before'][table])
     was=next((r for r in case['source_before'][table]if r[key]==s[key]),None)
     changed=phase=='after'and was is not None and was[field]!=s[field]
     window=case['native_run_window']if is_new or changed else[self.report['lifecycle_began_utc'],case['finished_utc']]
     self.clock(s[field],n[field],path,window,changed);expected[field]=s[field];actual[field]=s[field]
    self.compare_value(expected,actual,phase+'.'+table)

def validate_case(reference,case,report):
 require(reference['name']==case['name'],'ReferenceCaseIdentity')
 require(case.get('case_cleanup_errors',[])==[],'NoDiscardedCaseCleanupError')
 before,after=case['before'],case['after'];source_before,source_after=reference['before'],reference['after']
 require(set(before)==set(source_before)and set(after)==set(source_after),'CompleteSnapshotFieldPresence')
 require(case['native_process']['exit']==0 and case['native_process']['settled']is True and case['native_process']['pipes_settled']is True and case['native_process']['writer_settled']is True and case['native_process']['invalid']is None,'ActualNativeProcess0AndPipes')
 replies=case['native_responses'];require(len(replies)==3 and replies[0].get('prepared')is True and replies[2]=={'closed':True},'ActualFinitePeerTranscript')
 require(replies[1]['ok']is True and replies[1]['result']=='Ok(Completed('+case['id']+'))'and replies[1]['caller_released']is True and replies[1]['available_slots']==1,'ActualCompletedOwnedTask')
 require(case['native_pg']['physical_close_verified']is True and not case['native_pg']['errors'],'ActualPGRelayClosed')
 case['native_run_window']=[replies[1]['began_utc'],replies[1]['finished_utc']]
 require(instant(case['began_utc'])<=instant(case['native_run_window'][0])<=instant(case['native_run_window'][1])<=instant(case['finished_utc']),'ActualCommandWindowOrder')
 case['source_before']=source_before
 parent_frames=case['published_parent_raw_base64'];require(len(parent_frames)==1,'OneActualOwnedParentFrame')
 parent=json.loads(base64.b64decode(parent_frames[0],validate=True));headers=parent['headers']
 expected_parent=next(v for v in reference['publications']if v['headers']['id']==reference['id'])
 require(set(headers)==set(expected_parent['headers']),'FullSourceParentHeaderFields')
 for key,value in expected_parent['headers'].items():
  if key!='origin':require(equal(value,headers[key]),'ExactSourceProducerHeader:'+key)
 require(parent['properties']['correlation_id']==case['id']and parent['properties']['delivery_info']=={'exchange':'','routing_key':'rust-owned-configured-recordings'},'ActualSourceProducerOwnedRoute')
 require(equal(json.loads(base64.b64decode(parent['body'],validate=True)),expected_parent['body']),'ActualSourceProducerTypedBody')
 comparison=Comparison(report);comparison.rows(source_before,before,'before',case);comparison.rows(source_after,after,'after',case)
 # All input rows and every previous native recording remain literally intact.
 for table in('channels','sources','epg_data','programmes','system_settings','recurring_rules','dvr_settings'):
  require(equal(before[table],after[table]),'NativeInputRowsUnchanged:'+table)
 for table in('recordings','clocked','periodic'):
  known={r['id']:r for r in after[table]};require(all(r['id']in known and equal(r,known[r['id']])for r in before[table]),'PriorNativeRowsLiteralUnchanged:'+table)
 # Snapshot observation clocks are never fixture-date shifted or discarded.
 require(instant(before['utc'])<=instant(case['began_utc'])<=instant(case['finished_utc'])<=instant(after['utc']),'ObservationTimeOrder')
 require(type(after['unacked'])is dict and not after['unacked'],'NoOutstandingReservation')
 oldframes=[raw_frame(f)for f in before['frames']];newframes=[raw_frame(f)for f in after['frames']]
 oldids=[f['headers']['id']for f in oldframes];ids=[f['headers']['id']for f in newframes]
 require(len(ids)==len(set(ids)),'UniqueRetainedDefaultFrameIDs')
 require(after['frames'][-len(oldframes):]==before['frames']if oldframes else True,'DefaultDescendantsLiteralSuffixPreserved')
 children=[f for f in newframes if f['headers']['id']not in oldids]
 metadata=raw_metadata(case['result']);require(set(metadata)==set(reference['result']['metadata']),'ExactMetadataFields')
 require(case['result']['key_base64']==reference['result']['key_base64'],'ActualConfiguredBackendPrefix')
 require(metadata['status']=='SUCCESS'and metadata['traceback']is None and metadata['task_id']==case['id'],'ActualSuccessIdentity')
 require(equal(metadata['result'],reference['actual_get_value']),'ExactSourceReturn')
 require(equal(case['actual_get_value'],metadata['result'])and case['actual_get_type']==reference['actual_get_type'],'ActualOriginalAsyncResultTypedValue')
 require(instant(case['native_run_window'][0])<=instant(metadata['date_done'])<=instant(case['native_run_window'][1]),'ActualCompletionUTCWindow')
 require(type(case['result']['ttl'])is int and 3580<=case['result']['ttl']<=3600,'Actual3600TTL')
 expected_children=len(reference['result']['metadata']['children']);require(len(children)==expected_children,'ExactArtworkMultiplicity')
 tuples=metadata['children'];require(all(type(v)is list and len(v)==2 and v[1]is None and type(v[0])is list and len(v[0])==2 and v[0][1]is None for v in tuples),'ExactCeleryChildTupleTypes')
 childids=[uuid(v[0][0])for v in tuples];require(len(childids)==len(set(childids))and childids==list(reversed([f['headers']['id']for f in children])),'ActualPublicationOrderAndChildren')
 source_oldids={raw_frame(v)['headers']['id']for v in source_before['frames']}
 source_children=[raw_frame(v)for v in source_after['frames']if raw_frame(v)['headers']['id']not in source_oldids]
 for frame in children:
  headers=frame['headers'];properties=frame['properties'];uuid(headers['id'])
  schema=source_children[0]
  require(set(frame)==set(schema)and set(headers)==set(schema['headers'])and set(properties)==set(schema['properties']),'AllChildEnvelopeFieldsPresent')
  require(frame['content-type']=='application/json'and frame['content-encoding']=='utf-8','ExactChildJSONEncoding')
  volatile={'id','root_id','parent_id','origin','argsrepr','eta'}
  for field in headers:
   if field not in volatile:require(equal(headers[field],schema['headers'][field]),'ExactChildHeader:'+field)
  for field in ('delivery_mode','priority','body_encoding'):require(equal(properties[field],schema['properties'][field]),'ExactChildProperty:'+field)
  reply=UUID(properties['reply_to']);require(str(reply)==properties['reply_to']and reply.int!=0,'CanonicalOpaqueReplyUUID')
  uuid(properties['delivery_tag'])
  require(headers['task']==ARTWORK and headers['root_id']==case['id']and headers['parent_id']==case['id'],'ActualChildLineage')
  require(headers['ignore_result']is False and headers['origin']=='rust@configured-recordings'and headers['expires']is None,'TypedObservedChildOptions')
  require(instant(case['native_run_window'][0])+timedelta(seconds=1)<=instant(headers['eta'])<=instant(case['native_run_window'][1])+timedelta(seconds=1),'ActualCountdownOneETAWindow')
  require(properties['correlation_id']==headers['id']and properties['delivery_info']=={'exchange':'','routing_key':'celery'},'ActualCorrelationAndRoute')
  body=json.loads(base64.b64decode(frame['body'],validate=True));require(type(body)is list and len(body)==3 and body[1]=={} and body[2]=={'callbacks':None,'errbacks':None,'chain':None,'chord':None},'ActualChildBodyStructure')
  require(type(body[0])is list and len(body[0])==1 and type(body[0][0])is int and body[0][0]>0,'ActualArtworkRecordingID')
  require(headers['argsrepr']==repr(body[0]),'ActualPythonListRepr')
  require(any(r['id']==body[0][0]for r in after['recordings']),'ChildReferencesActualRecording')
 for row in after['recordings']:
  if row['id']in {r['id']for r in before['recordings']}:continue
  tasks=[p for p in after['periodic']if p['name']==row['task_id']];require(len(tasks)==1,'OneActualClockedTaskPerRecording')
  task=tasks[0];require(task['args']==json.dumps([row['id'],row['channel_id'],row['start_time'],row['end_time']]),'ExactPythonSpacedRecordingArgs')
  clocks=[v for v in after['clocked']if v['id']==task['clocked_id']];require(len(clocks)==1 and instant(clocks[0]['clocked_time'])==instant(row['start_time']),'ActualClockedTaskInstant')
 # Exact per-case event delta; Source unsupported-prefix events are retained in
 # raw reference, not replayed or invented in Native prior history.
 for phase in(before,after):require(len(phase['Channels_raw_base64'])==len(phase['Channels_decoded']),'AllRawEventFramesPresent')
 require(after['Channels_raw_base64'][:len(before['Channels_raw_base64'])]==before['Channels_raw_base64'],'NativePriorRawEventsRetained')
 require(equal(after['Channels_decoded'][len(before['Channels_decoded']):],source_after['Channels_decoded'][len(source_before['Channels_decoded']):]),'ExactSourceEventDelta')
 require(set(before['output_cache'])==set(after['output_cache'])==set(source_before['output_cache']),'AllOutputCacheFields')
 for key,value in before['output_cache'].items():
  actual=after['output_cache'][key]
  if key.endswith('_ttl'):require(type(value)is int and type(actual)is int and 0<actual<=value<=600,'RetainedOutputTTL:'+key)
  else:require(equal(value,actual)and equal(source_before['output_cache'][key],source_after['output_cache'][key]),'OutputCachePreserved:'+key)
 for phase in('before','after'):
  s=reference[phase]['Django_cache'];n=case[phase]['Django_cache'];require(set(s)==set(n),'GroupedCacheFields')
  require(s['key']==n['key'],'ActualGroupedCacheKey');require(equal(cache_value(s['raw_base64']),cache_value(n['raw_base64'])),'DataOnlyGroupedCacheValue')
  require(type(s['ttl'])is int and type(n['ttl'])is int,'GroupedCacheTTLType')
  if n['raw_base64']is None:require(n['ttl']==s['ttl']==-2,'AbsentGroupedCacheTTL')
  else:require(1<=n['ttl']<=300 and 1<=s['ttl']<=300,'ActualGroupedCacheTTL300')
 require(set(before['lock'])==set(after['lock'])==set(source_before['lock']),'AllLockFields')
 if reference['name']=='series-busy':
  require(before['lock']['raw_base64']==after['lock']['raw_base64']==source_before['lock']['raw_base64'],'ForeignLeaseUnchanged')
  require(type(after['lock']['ttl'])is int and 0<after['lock']['ttl']<=before['lock']['ttl']<=300,'ForeignLeaseTTLNotRenewed')
 else:require(after['lock']['raw_base64']is None and after['lock']['ttl']==-2,'NoResidualGlobalLease')
 for ident,row in before['backend'].items():
  require(ident in after['backend']and row['raw_base64']==after['backend'][ident]['raw_base64'],'PriorActualBackendBytesUnchanged')
 queries=[]
 for frame in case['native_pg']['frames']:
  if frame['direction']!='frontend':continue
  payload=base64.b64decode(frame['payload_base64'],validate=True);kind=bytes.fromhex(frame['kind_hex'])
  if kind==b'P':queries.append(payload.split(b'\0',2)[1].decode('utf-8'))
  elif kind==b'Q':queries.append(payload[:-1].decode('utf-8'))
 case['native_actual_queries']=queries
 expected_created=len([v for v in source_after['recordings']if v['id']not in {r['id']for r in source_before['recordings']}])
 known_fault=reference['name']in ('series-SQL-P0001','recurring-SQL-P0001')
 require(sum(q.startswith('INSERT INTO dispatcharr_channels_recording ')for q in queries)==expected_created+int(known_fault),'ActualRecordingINSERTExecutionCount')
 if known_fault:
  require(reference['name'] in ('series-SQL-P0001','recurring-SQL-P0001')and expected_created==0,'BoundedKnownSourceFaultOnly')
  observed_source=[v for v in reference['sql']if 'exception_type'in v]
  require(len(observed_source)==1 and observed_source[0]['SQLSTATE']is None and observed_source[0]['exception_type']=='django.db.utils.ProgrammingError'and observed_source[0]['sql'].startswith('INSERT INTO "dispatcharr_channels_recording" '),'SourceRawNullCodeNotInvented')
  from pg_reply_proxy import error_fields
  actual_errors=[error_fields(base64.b64decode(v['payload_base64'],validate=True))for v in case['native_pg']['frames']if v['direction']=='backend'and v['kind_hex']=='45']
  case['actual_server_errors']=actual_errors
  require(len(actual_errors)==1 and actual_errors[0].get('S')=='ERROR'and actual_errors[0].get('C')=='P0001'and actual_errors[0].get('M')=='owned configured recording failure'and actual_errors[0].get('W')=='PL/pgSQL function rust_owned_recording_fail() line 1 at RAISE','ActualKnownNativeServerOriginNotSourceMeasuredCode')
  require(not any(k in actual_errors[0]for k in ('D','H','P','p','q','s','t','c','d','n')),'NoUnsupportedExtraDisplayedDiagnostics')
  injection=case['fault_injection'];require(injection['after_actual_target_Parse']is True and injection['actual_admin_DDL_completed']is True and injection['thread_settled']is True and injection['errors']==[] and injection['before_admission_trigger_installed']is False and injection['reply_hold_control']is False,'ActualAfterAdmissionFaultInjectorClosed')
  targets=[t for t in case['native_pg']['timeline']if t['stage']=='actual-tagged-INSERT-before-forward'];require(len(targets)==1,'OneActualTargetParseCausalReceipt')
 require(sum(q.startswith('INSERT INTO django_celery_beat_periodictask ')for q in queries)==expected_created,'ActualPeriodicINSERTExecutionCount')
 expected_clocked=len(source_after['clocked'])-len(source_before['clocked'])
 require(sum(q.startswith('INSERT INTO django_celery_beat_clockedschedule ')for q in queries)==expected_clocked,'ActualClockedCreationOrReuse')
 return {'passed':True,'scope':'full tracked row fields/input date correspondence/own execution clocks/new effects/actual Original decoder; no default FIFO or SQL-query-count parity','Source_reference_case':reference['name'],'source_fallback':False,'children_consumed':False,'raw_timestamps_retained':True}

def validate_report(code,report):
 require(type(code)is int and code==0 and report['passed']is True and report['Native_verified']is True,'ActualGuest0AndSemanticPass')
 names=['series-positive','series-repeat','series-second-episode','series-busy','series-SQL-P0001','recurring-positive','recurring-repeat','recurring-disabled','recurring-SQL-P0001']
 require(report['predeclared_cases']==names and [c['name']for c in report['cases']]==names,'ExactNineKnownOrder')
 require(report['source_reference_host_exit']==1 and report['captured_SQLSTATE_verified']is False,'HistoricalSourceHostFailureNotRelabelled')
 expected={'setup-Channel-signal-frame-preserved','foreign-series-lock-unchanged'}|{name+'-full-normal-contract'for name in names}
 require(len(report['checks'])==len(expected)and{c['name']for c in report['checks']}==expected and all(c['passed']is True for c in report['checks']),'ExactElevenSemanticChecks')
 require(report['worker_exit_receipts']==[],'NoOriginalParentOrDescendantWorkers')
 previous=None
 for c in report['cases']:
  if previous is not None:
   require(equal(previous['after']['frames'],c['before']['frames']),'NoBetweenTaskDescendantRemovalOrReordering')
   for table in ('recordings','clocked','periodic','sentinel'):require(equal(previous['after'][table],c['before'][table]),'NoBetweenTaskMutation:'+table)
   for ident,row in previous['after']['backend'].items():require(ident in c['before']['backend']and row['raw_base64']==c['before']['backend'][ident]['raw_base64'],'NoBetweenTaskBackendRewrite')
  previous=c
  require(c['source_fallback']is False and c['source_postrun']is None and c['comparison']['passed']is True,'ActualNativeNotSourceFallback')
  require(c['native_process']['exit']==0 and c['native_process']['settled']is True and c['native_process']['pipes_settled']is True and c['native_process']['writer_settled']is True,'NineActualNativeProcesses0')
 require(len(report['cases'][-1]['after']['recordings'])==3 and len(report['cases'][-1]['after']['clocked'])==2,'ActualThreeRecordingsTwoClocks')
 require(len(report['cases'][-1]['after']['frames'])==6,'AllFiveArtworkAndSetupFramesRetained')
 return {'passed':True,'cases':9,'checks':11,'actual_Native_parents':9,'created_recordings':3,'clocked_rows':2,'artwork_frames':5,'setup_parse_frames':1,'children_consumed':False,'full_workflow':False,'performance_verified':False}
