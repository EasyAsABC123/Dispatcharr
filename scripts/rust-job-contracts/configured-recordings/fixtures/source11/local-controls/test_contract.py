"""Synthetic model corruption controls; never substituted for actual Source capture."""
import base64,copy,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from contract import CASES,CREATED,CHILDREN,CHECKS,SERIES,RECURRING,SETUP,ARTWORK,validate_source_capture

def frame(ident,task,parent=None):
 decoded={'headers':{'id':ident,'task':task,'root_id':parent,'parent_id':parent},'body':'synthetic-only-not-executed'}
 return {'decoded':decoded,'raw_base64':base64.b64encode(json.dumps(decoded).encode()).decode()}
def capture():
 setup=frame('setup-only',SETUP);state={'channels':[{'id':1,'epg_data_id':2}],'sources':[{'id':3}],'epg_data':[{'id':2,'epg_source_id':3}],'programmes':[],'system_settings':[],'recordings':[],'frames':[setup],'periodic':[],'clocked':[]};cases=[]
 for index,name in enumerate(CASES):
  if index in (1,3,6):state['programmes'].append({'id':len(state['programmes'])+1,'epg_id':2,'start_time':'2026-11-14T12:00:00+00:00','end_time':'2026-11-14T13:00:00+00:00','custom_properties':{}})
  ident='owned-'+str(index);before=copy.deepcopy(state);children=[];publications=[]
  if CREATED[index]:
   rid=len(state['recordings'])+1;start='2026-11-14 12:00:00+00:00';end='2026-11-14 13:00:00+00:00';taskid='dvr-recording-'+str(rid)
   state['recordings'].append({'id':rid,'channel_id':1,'task_id':taskid,'start_time':start,'end_time':end,'custom_properties':{'program':{'title':'Owned Synthetic'}}});state['clocked'].append({'id':rid,'clocked_time':start});state['periodic'].append({'id':rid,'name':taskid,'task':'apps.channels.tasks.run_recording','clocked_id':rid,'enabled':True,'one_off':True,'args':json.dumps([rid,1,start,end])})
  for seq in range(CHILDREN[index]):
   child=ident+'-child-'+str(seq);children.append([[child,None],None]);publications.append({'task':ARTWORK,'headers':{'id':child}});state['frames'].insert(0,frame(child,ARTWORK,ident))
  value={'scheduled':CREATED[index],'details':[]}if index<7 else CREATED[index]
  meta={'task_id':ident,'status':'SUCCESS','traceback':None,'result':value,'date_done':'2026-11-12T12:00:01+00:00','children':children};result={'metadata':meta,'raw_base64':base64.b64encode(json.dumps(meta).encode()).decode(),'ttl':3599}
  cases.append({'name':name,'id':ident,'before':before,'after':copy.deepcopy(state),'result':result,'actual_get_value':copy.deepcopy(value),'actual_get_type':'builtins.dict'if index<7 else'builtins.int','began_utc':'2026-11-12T12:00:00+00:00','finished_utc':'2026-11-12T12:00:02+00:00','postrun':{'id':ident,'task':SERIES if index<7 else RECURRING,'state':'SUCCESS'},'publications':publications,'sql':[{'SQLSTATE':'P0001','sql':'INSERT INTO "dispatcharr_channels_recording" synthetic_control'}]if index in(6,10)else[]})
 return {'passed':True,'Native_verified':False,'performance_verified':False,'cases':cases,'setup':{'frames':[setup]},'checks':[{'name':name,'passed':True}for name in CHECKS],'worker_exit_receipts':[{'exit':0,'settled':True,'forced':False}for _ in CASES]}
def encode_meta(report,index):
 result=report['cases'][index]['result'];result['raw_base64']=base64.b64encode(json.dumps(result['metadata']).encode()).decode()
class Controls(unittest.TestCase):
 def test_positive_synthetic_model_only(self):self.assertEqual(validate_source_capture(0,capture())['cases'],11)
 def test_process_nonzero_not_masked_by_passed_true(self):
  with self.assertRaisesRegex(ValueError,'ActualProcess0'):validate_source_capture(1,capture())
 def test_missing_check_not_masked_by_passed_true(self):
  report=capture();report['checks'].pop()
  with self.assertRaisesRegex(ValueError,'Exact103'):validate_source_capture(0,report)
 def test_wrong_method_order_not_sorted(self):
  report=capture();report['cases'][1],report['cases'][2]=report['cases'][2],report['cases'][1]
  with self.assertRaisesRegex(ValueError,'ExactElevenCaseOrder'):validate_source_capture(0,report)
 def test_removed_setup_child_refused(self):
  report=capture();report['setup']['frames']=[]
  with self.assertRaisesRegex(ValueError,'ActualSetupSignal'):validate_source_capture(0,report)
 def test_one_duplicate_artwork_dropped_refused(self):
  report=capture();report['cases'][1]['after']['frames'].pop(0)
  with self.assertRaisesRegex(ValueError,'ExactNewChildMultiplicity'):validate_source_capture(0,report)
 def test_prior_frame_bytes_changed_refused(self):
  report=capture();report['cases'][2]['after']['frames'][0]['decoded']['body']='changed'
  with self.assertRaisesRegex(ValueError,'ActualRawFrameProjection'):validate_source_capture(0,report)
 def test_raw_backend_projection_corruption_refused(self):
  report=capture();report['cases'][1]['result']['metadata']['result']['scheduled']=99
  with self.assertRaisesRegex(ValueError,'ActualRawBackendProjection'):validate_source_capture(0,report)
 def test_recurring_bool_not_integer_zero(self):
  report=capture();report['cases'][9]['actual_get_value']=False;report['cases'][9]['result']['metadata']['result']=False;encode_meta(report,9)
  with self.assertRaisesRegex(ValueError,'TypedRecurringIntegerNotBool'):validate_source_capture(0,report)
 def test_actual_clock_window_cannot_be_omitted_or_rebound(self):
  report=capture();report['cases'][1]['result']['metadata']['date_done']='2026-11-12T13:00:00+00:00';encode_meta(report,1)
  with self.assertRaisesRegex(ValueError,'ActualSourceCompletionWindow'):validate_source_capture(0,report)
 def test_unsettled_or_forced_worker_cannot_pass(self):
  for field,value in [('settled',False),('forced',True),('exit',1)]:
   report=capture();report['worker_exit_receipts'][0][field]=value
   with self.assertRaisesRegex(ValueError,'ActualElevenWaits0'):validate_source_capture(0,report)
 def test_recording_times_not_silently_changed(self):
  report=capture();report['cases'][1]['after']['clocked'][0]['clocked_time']='2026-11-14 12:01:00+00:00'
  with self.assertRaisesRegex(ValueError,'ActualClockedInstant'):validate_source_capture(0,report)
 def test_postrun_from_wrong_family_refused(self):
  report=capture();report['cases'][1]['postrun']['task']='apps.channels.tasks.run_recording'
  with self.assertRaisesRegex(ValueError,'ActualOwnedPostrun'):validate_source_capture(0,report)
 def test_fake_sql_fault_without_actual_tagged_statement_refused(self):
  report=capture();report['cases'][6]['sql'][0]['sql']='SELECT 1'
  with self.assertRaisesRegex(ValueError,'ActualRecordingInsertP0001'):validate_source_capture(0,report)
 def test_child_metadata_reordered_refused(self):
  report=capture();report['cases'][1]['result']['metadata']['children'].reverse();encode_meta(report,1)
  with self.assertRaisesRegex(ValueError,'ActualMetadataAndPublicationChildOrder'):validate_source_capture(0,report)
 def test_missing_full_input_snapshot_refused(self):
  report=capture();del report['cases'][0]['before']['programmes']
  with self.assertRaisesRegex(ValueError,'FullInputSnapshotsPresent'):validate_source_capture(0,report)
 def test_changed_input_rows_not_hidden_by_recording_count(self):
  report=capture();report['cases'][1]['after']['programmes'][0]['custom_properties']={'changed':True}
  with self.assertRaisesRegex(ValueError,'InputRowsUnchangedWithinTask'):validate_source_capture(0,report)
 def test_programme_foreign_key_not_silently_remapped(self):
  report=capture();report['cases'][1]['before']['programmes'][0]['epg_id']=999
  with self.assertRaisesRegex(ValueError,'FullProgrammeTypesAndTimes'):validate_source_capture(0,report)
 def test_source_component_cannot_be_relabelled_native_or_fullresource(self):
  for field in ('Native_verified','performance_verified'):
   report=capture();report[field]=True
   with self.assertRaisesRegex(ValueError,'SourceOnlyNotNativeResource'):validate_source_capture(0,report)
if __name__=='__main__':unittest.main()
