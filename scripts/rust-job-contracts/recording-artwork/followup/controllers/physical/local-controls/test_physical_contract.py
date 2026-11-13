"""Synthetic acceptor models from captured Source schema; NOT physical evidence."""
import copy,json,base64,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import physical_contract as c
REF=json.loads((ROOT/'tools/reference.json').read_text())
def model(mode):
 report=copy.deepcopy(REF['Source']);before=copy.deepcopy(report['cases'][2]['before']);after=copy.deepcopy(report['cases'][2]['after']);first_id,second_id=report['cases'][2]['ids'];after['queues'][c.QUEUES['artwork']]=before['queues'][c.QUEUES['artwork']][:-1];after['backend'][second_id]=copy.deepcopy(before['backend'][second_id]);report['cases']=report['cases'][:2];report['all_prerun']=report['all_prerun'][:2];report['worker_exit_receipts']=report['worker_exit_receipts'][:2]
 if mode=='update-error-Ready':after['recordings']=copy.deepcopy(before['recordings']);after['Channels_decoded']=copy.deepcopy(before['Channels_decoded']);after['Channels_raw_base64']=copy.deepcopy(before['Channels_raw_base64']);result={'metadata':None,'raw_base64':None,'ttl':-2}
 elif mode=='Channels-reply':result={'metadata':None,'raw_base64':None,'ttl':-2}
 else:result=copy.deepcopy(after['backend'][first_id])
 if mode=='fresh-status-merge':after['recordings'][0]['custom_properties'].update(status='recording',owned_current_row_marker='preserved')
 case={'name':mode,'id':first_id,'passed':True,'finally_errors':[],'case_elapsed_seconds':8.0,'native_process':{'spawn_state':'actual_child_started','exit':0,'settled':True,'pipes_settled':True,'writer_settled':True,'collector_invalid':None},'checks':[{'passed':True}for i in range(4)],'proxy_close_receipts':{},'before_native':before,'after_native':after,'result_after':result,'Original_get_value':'ok','Original_get_type':'builtins.str','actual_gate_release_request':{'monotonic':106.2,'gate_reached_monotonic':100,'utc':'2026-11-13T07:06:46.200000+00:00'},'observations':[{'label':'held-before-fixed-release','monotonic':103,'snapshot':copy.deepcopy(before)}],'responses':[{'label':'poll','value':{'caller_released':False,'available_slots':0}},{'label':'drain-completed','utc':'2026-11-13T07:06:46.300000+00:00','value':{'drained':True,'caller_released':True}}]}
 if mode=='oldSUCCESS-preserved':case['old_metadata_before']={'raw_base64':'b3duZWQtb2xkLW1ldGFkYXRh','ttl':600};case['result_after']={'raw_base64':case['old_metadata_before']['raw_base64'],'ttl':599,'metadata':{'status':'SUCCESS'}}
 if mode in('recording-read-Ready','update-Ready','update-error-Ready','fresh-status-merge'):case['proxy_close_receipts']['PG']={'physical_close_verified':True,'errors':[],'authentication_bytes_retained':False,'CancelKey_secret_retained':False,'handler_or_upstream_threads_alive':0,'timeline':[{'stage':'actual-matching-Ready-held','backend_pid':123,'monotonic':100,'subject_complete_occurrence':2 if mode=='fresh-status-merge'else 1}]}
 if 'PG'in case['proxy_close_receipts']:
  pg=case['proxy_close_receipts']['PG'];pg['frames']=[];subject=pg['timeline'][0];key={'recording-read-Ready':'recording_read','update-Ready':'update','update-error-Ready':'update','fresh-status-merge':'movie_read'}[mode]
  subject.update(identity=c.SQL[key],executions=1,frontend_Sync_or_simpleQuery_observed=True,connection=1,actual_error=mode=='update-error-Ready',matching_rows=1 if mode in('recording-read-Ready','fresh-status-merge')else 0,matching_commands_hex=[]if mode=='update-error-Ready'else[(b'SELECT 1\0'if mode in('recording-read-Ready','fresh-status-merge')else b'UPDATE 1\0').hex()])
  if mode=='update-error-Ready':pg['frames']=[{'connection':1,'direction':'backend','kind_hex':b'E'.hex(),'payload_base64':base64.b64encode(b'SERROR\0CP0001\0Mowned artwork post-admission fault\0\0').decode()}]
 if mode=='update-error-Ready':
  case['began_utc']='2026-11-13T07:06:40+00:00';case['native_process'].update(pid=456,starttime='123456');case['proxy_close_receipts']['PG']['timeline'][0]['monotonic']=100.0
  values={'caller_pending':True,'caller_released':False,'available_slots':0,'event_connections':512,'event_publish_bytes':1048576};case['post_error_held_samples']=[]
  case['responses'][0].update(monotonic=102.2,utc='2026-11-13T07:06:42.200000+00:00',value=copy.deepcopy(values))
  case['responses'].insert(1,{'label':'poll','monotonic':102.5,'utc':'2026-11-13T07:06:42.500000+00:00','value':copy.deepcopy(values)})
  for index,receipt in enumerate(case['responses'][:2]):
   case['post_error_held_samples'].append({'sampled_monotonic':receipt['monotonic'],'sampled_utc':receipt['utc'],'response':copy.deepcopy(values),'peer_pid':456,'peer_starttime':'123456','gate_backend_pid':123,'gate_connection':1,'gate_monotonic':100.0,'proxy_release_not_requested':True,'backend_raw_base64':None})
   case['observations'].append({'label':'post-error-held-ownership-'+str(index+1),'monotonic':receipt['monotonic']+.01,'backend':{'raw_base64':None}})
 if mode in('Channels-reply','result-reply'):case['proxy_close_receipts']['RESP']={'physical_close_verified':True,'errors':[],'receipts':[{'stage':'actual-target-reply-held','monotonic':100,'actual_server_reply_observed':True,'actual_selected_DB':0 if mode=='Channels-reply'else 9,'script_sha1':c.RESP['CHANNELS_PUBLISH_sha1'if mode=='Channels-reply'else'result_write_lua_sha1']}]}
 report['cases'].append(case);report.update(passed=True,Native_verified=True,Source_verified=False,performance_verified=False,predeclared_cases=[report['cases'][0]['name'],report['cases'][1]['name'],mode],finally_errors=[],final=copy.deepcopy(after));return report
class Contract(unittest.TestCase):
 def test_all_seven_positive_models_are_only_validator_controls(self):
  for mode in c.MODES:
   with self.subTest(mode=mode):self.assertTrue(c.validate(0,model(mode),REF)['passed'])
 def reject(self,change,mode='recording-read-Ready'):
  r=model(mode);change(r)
  with self.assertRaises((ValueError,KeyError,TypeError)):c.validate(0,r,REF)
 def test_native_process_failure(self):self.reject(lambda r:r['cases'][2]['native_process'].update(exit=1))
 def test_unsettled_native_pipe(self):self.reject(lambda r:r['cases'][2]['native_process'].update(pipes_settled=False))
 def test_false_caller_release_is_not_true(self):self.reject(lambda r:r['cases'][2]['responses'][0]['value'].update(caller_released=True))
 def test_unheld_slot(self):self.reject(lambda r:r['cases'][2]['responses'][0]['value'].update(available_slots=1))
 def test_shortened_hold(self):self.reject(lambda r:r['cases'][2]['actual_gate_release_request'].update(monotonic=102))
 def test_timeout_budget_failure(self):self.reject(lambda r:r['cases'][2].update(case_elapsed_seconds=90.1))
 def test_borrowed_other_parent(self):self.reject(lambda r:r['cases'][1]['tasks'][0]['result']['metadata'].update(parent_id='wrong'))
 def test_unknown_extra_child_consumption(self):self.reject(lambda r:r['cases'][2]['after_native']['queues'][c.QUEUES['artwork']].clear())
 def test_other_child_metadata_cannot_appear(self):self.reject(lambda r:r['cases'][2]['after_native']['backend'][r['cases'][1]['tasks'][0]['result']['metadata']['children'][1][0][0]].update(raw_base64='not-relevant'))
 def test_unrelated_copiedSQL_row_mutation(self):self.reject(lambda r:r['cases'][2]['after_native']['channels'][0].update(name='wrong'))
 def test_extra_recording_field_mutation(self):self.reject(lambda r:r['cases'][2]['after_native']['recordings'][0].update(task_id='wrong'))
 def test_error_cannot_be_fabricated_terminal_success(self):self.reject(lambda r:r['cases'][2]['result_after'].update(metadata={'status':'SUCCESS'}),mode='update-error-Ready')
 def test_fresh_status_not_overwritten(self):self.reject(lambda r:r['cases'][2]['after_native']['recordings'][0]['custom_properties'].pop('status'),mode='fresh-status-merge')
 def test_preflight_movie_not_execution_hold(self):self.reject(lambda r:r['cases'][2]['proxy_close_receipts']['PG']['timeline'][0].update(subject_complete_occurrence=1),mode='fresh-status-merge')
 def test_borrowed_SQL_identity_not_subject(self):self.reject(lambda r:r['cases'][2]['proxy_close_receipts']['PG']['timeline'][0].update(identity='SELECT unrelated'))
 def test_unknown_server_error_not_known_trigger(self):
  def change(r):r['cases'][2]['proxy_close_receipts']['PG']['frames'][0]['payload_base64']=base64.b64encode(b'SERROR\0C42501\0Mpermission denied\0\0').decode()
  self.reject(change,mode='update-error-Ready')
 def test_proxy_did_not_see_real_lua_reply(self):self.reject(lambda r:r['cases'][2]['proxy_close_receipts']['RESP']['receipts'][0].update(actual_server_reply_observed=False),mode='result-reply')
 def test_wrong_actual_Redis_DB(self):self.reject(lambda r:r['cases'][2]['proxy_close_receipts']['RESP']['receipts'][0].update(actual_selected_DB=8),mode='result-reply')
 def test_authentication_bytes_not_public(self):self.reject(lambda r:r['cases'][2]['proxy_close_receipts']['PG'].update(authentication_bytes_retained=True))
 def test_proxy_live_thread_not_closed(self):self.reject(lambda r:r['cases'][2]['proxy_close_receipts']['PG'].update(handler_or_upstream_threads_alive=1))
if __name__=='__main__':unittest.main()
