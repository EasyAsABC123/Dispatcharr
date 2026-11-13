"""Pure offline corruption controls, never an actual Native/Source fixture."""
import base64,copy,json,pickle,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from normal_contract import validate_case,cache_value
ROOT=Path(__file__).resolve().parents[1]
REFERENCE=json.loads((ROOT/'tools/reference.json').read_text())
LIFECYCLE=json.loads((ROOT/'local-controls/source-lifecycle.json').read_text())
def normal(index=1):
 source=copy.deepcopy(REFERENCE['cases'][index]);case=copy.deepcopy(source)
 case['native_process']={'exit':0,'settled':True,'pipes_settled':True,'writer_settled':True,'invalid':None}
 case['native_responses']=[{'prepared':True},{'began_utc':source['began_utc'],'finished_utc':source['finished_utc'],'ok':True,'result':'Ok(Completed('+source['id']+'))','caller_released':True,'available_slots':1},{'closed':True}]
 frames=[]
 created=len(source['after']['recordings'])-len(source['before']['recordings']);clocks=len(source['after']['clocked'])-len(source['before']['clocked'])
 queries=['INSERT INTO dispatcharr_channels_recording (...)']*created+['INSERT INTO django_celery_beat_periodictask (...)']*created+['INSERT INTO django_celery_beat_clockedschedule (...)']*clocks
 for q in queries:
  frames.append({'direction':'frontend','kind_hex':b'P'.hex(),'payload_base64':base64.b64encode(b'\0'+q.encode()+b'\0\0\0').decode()})
 if index in(6,10):
  frames.append({'direction':'frontend','kind_hex':'50','payload_base64':base64.b64encode(b'\0INSERT INTO dispatcharr_channels_recording (...)\0\0\0').decode()})
  error=b'SERROR\0VERROR\0CP0001\0Mowned configured recording failure\0WPL/pgSQL function rust_owned_recording_fail() line 1 at RAISE\0\0'
  frames.append({'direction':'backend','kind_hex':'45','payload_base64':base64.b64encode(error).decode()})
  case['fault_injection']={'after_actual_target_Parse':True,'actual_admin_DDL_completed':True,'thread_settled':True,'errors':[],'before_admission_trigger_installed':False,'reply_hold_control':False}
 case['native_pg']={'physical_close_verified':True,'errors':[],'frames':frames,'timeline':[{'stage':'actual-tagged-INSERT-before-forward'}]if index in(6,10)else[]}
 for phase in ['before','after']:
  for frame in case[phase]['frames']:
   if frame['decoded']['headers']['task']=='apps.channels.tasks.prefetch_recording_artwork':
    frame['decoded']['headers']['origin']='rust@configured-recordings';frame['raw_base64']=base64.b64encode(json.dumps(frame['decoded']).encode()).decode()
 parent=copy.deepcopy(REFERENCE['setup']['frames'][0]['decoded']);pub=next(v for v in source['publications']if v['headers']['id']==source['id'])
 parent['headers']=copy.deepcopy(pub['headers']);parent['properties'].update(copy.deepcopy(pub['properties']));parent['properties']['correlation_id']=source['id'];parent['properties']['delivery_info']={'exchange':'','routing_key':'rust-owned-configured-recordings'};parent['body']=base64.b64encode(json.dumps(pub['body']).encode()).decode()
 case['published_parent_raw_base64']=[base64.b64encode(json.dumps(parent).encode()).decode()]
 report={'fixture_date_delta_days':0,'lifecycle_began_utc':LIFECYCLE['began_utc']}
 return source,case,report
class Contract(unittest.TestCase):
 def test_real_source_shape_offline_control(self):
  s,n,r=normal();self.assertTrue(validate_case(s,n,r)['passed'])
 def test_all_seven_source_shapes_share_stable_clock_and_id_bindings(self):
  report=None
  for index in [1,2,3,5,7,8,9]:
   source,native,fresh=normal(index)
   if report is None:report=fresh
   self.assertTrue(validate_case(source,native,report)['passed'])
 def test_observed_failed_insert_id_gap_is_mapped_without_erasing_fields(self):
  source,native,report=normal(7)
  row=native['after']['recordings'][-1];self.assertEqual(row['id'],4);row['id']=3;row['task_id']='dvr-recording-3'
  periodic=native['after']['periodic'][-1];periodic['name']='dvr-recording-3';args=json.loads(periodic['args']);args[0]=3;periodic['args']=json.dumps(args)
  frame=native['after']['frames'][0];body=json.loads(base64.b64decode(frame['decoded']['body']));body[0]=[3];frame['decoded']['body']=base64.b64encode(json.dumps(body).encode()).decode();frame['decoded']['headers']['argsrepr']='[3]';frame['raw_base64']=base64.b64encode(json.dumps(frame['decoded']).encode()).decode()
  self.assertTrue(validate_case(source,native,report)['passed'])
  self.assertEqual(report['row_id_bindings']['recordings']['4'],3)
 def test_existing_clock_cannot_be_rebound_on_next_observation(self):
  source,native,report=normal();validate_case(source,native,report)
  source2,native2,_=normal(2)
  native2['before']['periodic'][-1]['date_changed']='2026-11-12T23:34:49.538518+00:00'
  with self.assertRaises(ValueError):validate_case(source2,native2,report)
 def corrupt(self,fn):
  s,n,r=normal();fn(n)
  with self.assertRaises((ValueError,KeyError,TypeError)):validate_case(s,n,r)
 def test_missing_explicit_null_row_field(self):self.corrupt(lambda n:n['after']['channels'][0].pop('logo_id'))
 def test_unknown_extra_row_field(self):self.corrupt(lambda n:n['after']['recordings'][0].update(unknown='x'))
 def test_false_writer_settlement(self):self.corrupt(lambda n:n['native_process'].update(writer_settled=False))
 def test_nonzero_native_exit(self):self.corrupt(lambda n:n['native_process'].update(exit=1))
 def test_outside_own_completion_clock(self):
  def bad(n):
   n['result']['metadata']['date_done']='2000-01-01T00:00:00+00:00';n['result']['raw_base64']=base64.b64encode(json.dumps(n['result']['metadata']).encode()).decode()
  self.corrupt(bad)
 def test_backend_raw_projection_tamper(self):self.corrupt(lambda n:n['result']['metadata'].update(result=None))
 def test_unmatched_child_metadata(self):
  def bad(n):
   n['result']['metadata']['children']=[];n['result']['raw_base64']=base64.b64encode(json.dumps(n['result']['metadata']).encode()).decode()
  self.corrupt(bad)
 def test_retained_descendant_removed(self):self.corrupt(lambda n:n['after']['frames'].pop())
 def test_missing_child_explicit_null_header(self):
  def bad(n):
   f=n['after']['frames'][0];f['decoded']['headers'].pop('shadow');f['raw_base64']=base64.b64encode(json.dumps(f['decoded']).encode()).decode()
  self.corrupt(bad)
 def test_missing_child_explicit_null_property(self):
  def bad(n):
   f=n['after']['frames'][0];f['decoded']['properties'].pop('priority');f['raw_base64']=base64.b64encode(json.dumps(f['decoded']).encode()).decode()
  self.corrupt(bad)
 def test_child_false_numeric_delivery_mode(self):
  def bad(n):
   f=n['after']['frames'][0];f['decoded']['properties']['delivery_mode']=2.0;f['raw_base64']=base64.b64encode(json.dumps(f['decoded']).encode()).decode()
  self.corrupt(bad)
 def test_wrong_backend_namespace(self):self.corrupt(lambda n:n['result'].update(key_base64=base64.b64encode(b'celery-task-meta-wrong').decode()))
 def test_parent_body_changed(self):
  def bad(n):
   frame=json.loads(base64.b64decode(n['published_parent_raw_base64'][0]));frame['body']=base64.b64encode(json.dumps([[999],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}]).encode()).decode();n['published_parent_raw_base64']=[base64.b64encode(json.dumps(frame).encode()).decode()]
  self.corrupt(bad)
 def test_parent_wrong_queue(self):
  def bad(n):
   frame=json.loads(base64.b64decode(n['published_parent_raw_base64'][0]));frame['properties']['delivery_info']['routing_key']='celery';n['published_parent_raw_base64']=[base64.b64encode(json.dumps(frame).encode()).decode()]
  self.corrupt(bad)
 def test_actual_recording_query_missing(self):self.corrupt(lambda n:n['native_pg']['frames'].pop(0))
 def test_data_only_pickle(self):self.assertEqual(cache_value(base64.b64encode(pickle.dumps({'x':[1,None,True]})).decode()),{'x':[1,None,True]})
 def test_pickle_global_refused(self):
  with self.assertRaises(ValueError):cache_value(base64.b64encode(b'cos\nsystem\n.').decode())
if __name__=='__main__':unittest.main()
