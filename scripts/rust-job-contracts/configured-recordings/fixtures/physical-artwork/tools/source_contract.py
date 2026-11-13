"""Socket-free finite corpus/retention validator. Synthetic tests are not Source authority."""
import base64,json
from datetime import datetime
CASES=['series-invalid','series-positive','series-repeat','series-second-episode','series-filter-miss','series-busy','series-SQL-P0001','recurring-positive','recurring-repeat','recurring-disabled','recurring-SQL-P0001']
CREATED=[0,1,0,1,0,0,0,1,0,0,0]
CHILDREN=[0,2,0,2,0,0,0,1,0,0,0]
CHECKS=['setup-Channel-signal-frame-preserved']+[name+'-'+suffix for name in CASES for suffix in ('normal-metadata','actual-created','actual-child-count','earlier-descendants-retained','actual-artwork-only','clocked-not-immediate-recording','queue-unreserved','result-ttl','typed-scheduled')]+['foreign-series-lock-unchanged','only-known-owned-tasks-executed','exact-eleven-workers-settled']
SERIES='apps.channels.tasks.evaluate_series_rules';RECURRING='apps.channels.tasks.maintain_recurring_recordings';ARTWORK='apps.channels.tasks.prefetch_recording_artwork';SETUP='apps.epg.tasks.parse_programs_for_tvg_id'
def require(value,label):
 if not value:raise ValueError(label)
def equal(a,b):
 if type(a)is not type(b):return False
 if isinstance(a,dict):return set(a)==set(b)and all(equal(a[k],b[k])for k in a)
 if isinstance(a,list):return len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))
 return a==b
def instant(value):
 require(type(value)is str,'ActualTimeString');value=datetime.fromisoformat(value);require(value.tzinfo is not None,'AwareActualTime');return value
def frame_id(value):
 raw=json.loads(base64.b64decode(value['raw_base64'],validate=True));require(equal(raw,value['decoded']),'ActualRawFrameProjection');return raw['headers']['id']
def validate_source_capture(returncode,report):
 require(type(returncode)is int and returncode==0 and report.get('passed')is True,'ActualProcess0AndSemanticPass')
 require(report.get('Native_verified')is False and report.get('performance_verified')is False,'SourceOnlyNotNativeResource')
 require([v['name']for v in report['cases']]==CASES,'ExactElevenCaseOrder')
 checks=report['checks'];require(len(checks)==len(CHECKS)and{v['name']for v in checks}==set(CHECKS)and all(v['passed']is True for v in checks),'Exact103AllPassedChecks')
 exits=report['worker_exit_receipts'];require(len(exits)==11 and all(type(v['exit'])is int and v['exit']==0 and v['settled']is True and v['forced']is False for v in exits),'ActualElevenWaits0')
 require(len(report['setup']['frames'])==1 and report['setup']['frames'][0]['decoded']['headers']['task']==SETUP,'ActualSetupSignalNotRemoved');setup_id=frame_id(report['setup']['frames'][0]);seen=[]
 for index,case in enumerate(report['cases']):
  ident=case['id'];require(ident not in seen,'UniqueActualTaskID');seen.append(ident)
  before,after=case['before'],case['after']
  for phase in (before,after):
   require(all(key in phase and type(phase[key])is list for key in ('channels','sources','epg_data','programmes','system_settings')),'FullInputSnapshotsPresent')
   require(len(phase['channels'])==len(phase['sources'])==len(phase['epg_data'])==1,'ExactOwnedChannelSourceEPG')
   require(phase['channels'][0]['epg_data_id']==phase['epg_data'][0]['id']and phase['epg_data'][0]['epg_source_id']==phase['sources'][0]['id'],'ActualInputForeignKeys')
   require(len(phase['programmes'])==([0,1,1,2,2,2,3,3,3,3,3][index]),'PredeclaredProgrammeInputs')
   require(all(type(v['custom_properties'])is dict and v['epg_id']==phase['epg_data'][0]['id']and instant(v['start_time'])<instant(v['end_time'])for v in phase['programmes']),'FullProgrammeTypesAndTimes')
  require(all(equal(before[key],after[key])for key in ('channels','sources','epg_data','programmes','system_settings')),'InputRowsUnchangedWithinTask')
  old={v['id']:v for v in before['recordings']};new=[v for v in after['recordings']if v['id']not in old]
  require(len(new)==CREATED[index],'PredeclaredCreatedCount:'+case['name'])
  present={v['id']:v for v in after['recordings']}
  require(all(ident in present and equal(row,present[ident])for ident,row in old.items()),'ExistingRecordingsLiteralUnchanged')
  beforeids=[frame_id(v)for v in before['frames']];afterids=[frame_id(v)for v in after['frames']]
  require(len(set(beforeids))==len(beforeids)and len(set(afterids))==len(afterids),'UniqueQueuedIDs')
  require(setup_id in beforeids and all(v in after['frames']for v in before['frames']),'AllPriorRawFramesRetained')
  children=[v for v in after['frames']if frame_id(v)not in beforeids]
  require(len(children)==CHILDREN[index],'ExactNewChildMultiplicity')
  require(all(v['decoded']['headers']['task']==ARTWORK and v['decoded']['headers']['parent_id']==ident and v['decoded']['headers']['root_id']==ident for v in children),'ActualArtworkLineage')
  for row in new:
   require(row['task_id']=='dvr-recording-'+str(row['id']),'ActualFutureBeatName')
   periodic=[v for v in after['periodic']if v['name']==row['task_id']];require(len(periodic)==1,'ExactlyOneOwnedPeriodic')
   pt=periodic[0];require(pt['task']=='apps.channels.tasks.run_recording'and pt['enabled']is True and pt['one_off']is True and pt['clocked_id']is not None,'ActualClockedOneOff')
   clocks=[v for v in after['clocked']if v['id']==pt['clocked_id']];require(len(clocks)==1 and instant(clocks[0]['clocked_time'])==instant(row['start_time']),'ActualClockedInstant')
   require(json.loads(pt['args'])==[row['id'],row['channel_id'],row['start_time'],row['end_time']],'ActualRecordingArguments')
  result=case['result'];metadata=result['metadata'];require(equal(json.loads(base64.b64decode(result['raw_base64'],validate=True)),metadata),'ActualRawBackendProjection')
  require(metadata['task_id']==ident and metadata['status']=='SUCCESS'and metadata['traceback']is None,'NormalActualMetadata')
  value=case['actual_get_value'];require(equal(metadata['result'],value),'ActualOriginalDecoderValue')
  if index<7:require(type(value)is dict and type(value['scheduled'])is int and value['scheduled']==CREATED[index]and case['actual_get_type']=='builtins.dict','TypedSeriesResult')
  else:require(type(value)is int and value==CREATED[index]and case['actual_get_type']=='builtins.int','TypedRecurringIntegerNotBool')
  require(type(result['ttl'])is int and 3580<=result['ttl']<=3600,'ActualResultTTL')
  require(instant(case['began_utc'])<=instant(metadata['date_done'])<=instant(case['finished_utc']),'ActualSourceCompletionWindow')
  require(case['postrun']['id']==ident and case['postrun']['task']==(SERIES if index<7 else RECURRING)and case['postrun']['state']=='SUCCESS','ActualOwnedPostrun')
  if index in (6,10):require(any(v.get('SQLSTATE')=='P0001'and v['sql'].lstrip().startswith('INSERT INTO "dispatcharr_channels_recording"')for v in case['sql']),'ActualRecordingInsertP0001')
  ids=[v[0][0]for v in metadata['children']];pubs=[v['headers']['id']for v in case['publications']if v['task']==ARTWORK]
  require(ids==pubs and len(ids)==CHILDREN[index]and set(ids)=={frame_id(v)for v in children},'ActualMetadataAndPublicationChildOrder')
 return {'passed':True,'cases':11,'created_recordings':sum(CREATED),'retained_artwork_frames':sum(CHILDREN),'setup_helper_frames':1,'Source_only':True,'full_workflow':False}
