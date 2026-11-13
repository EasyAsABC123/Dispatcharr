"""Synthetic physical/effect negatives only; no PG, Docker, app or Cargo."""
import ast,copy,json,struct,sys,unittest,types
from pathlib import Path
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent;sys.path.insert(0,str(HERE))
from validate_dependency import validate_case,TABLES
from gate_validation import post_close_terminal
GATE=json.loads((ROOT/'inputs/gate-contracts.json').read_text())['dependency-backend-id-ready']
def fixture(number=3,passing=True):
 sockets=[{'fd':'7','inode':'12345','link':'socket:[12345]'}];sql=GATE['sql'];held={'stage':'dependency-backend-id-ready','ordinal':0,'connection':0,'backend_pid':123,'sql':sql,'rows':1,'commands':['SELECT 1'],'errors':[],'tx':'I'};events=[{'kind':'execute','connection':0,'sql':sql}]
 for kind,body in[(b'D',b'\0\0'),(b'C',b'SELECT 1\0'),(b'Z',b'I')]:events.append({'kind':'actual-target-backend-frame','connection':0,'sql':sql,'kind_hex':kind.hex(),'header_hex':struct.pack('!I',len(body)+4).hex(),'body_hex':body.hex()})
 events.append({'kind':'ready-held',**held});connections=[{'id':0,'backend_pid':123,'closed':passing,'up_joined':passing}];sample={'ordinal':1,'metadata_after':{'size':0 if passing else 1,'num_idle':1,'is_closed':True},'old_SQLx_socket_identity':sockets,'actual_self_FD_links':[]if passing else['socket:[12345]'],'current_SQLx_sockets':[]if passing else sockets,'backends':[{'pid':123,'state':[]if passing else[{'pid':123}]}],'relay':{'connections':connections},'elapsed_millis':1 if passing else 10000,'accepted':passing};before={'tables':{n:[]for n in TABLES},'sequences':{n:{'last_value':1,'is_called':False}for n in TABLES}};row={'id':number,'passed':passing,'Source_Main_or_HTTP_runs':0,'max_connections':1,'subject_close_calls':1,'application_statement_retries':0,'held_actual_select_ready':{'proxy':{'held':held,'events':events},'actual_SQLx_socket':sockets,'actual_backend':[{'pid':123}]},'actual_query_task_joined':True,'before':before,'after':copy.deepcopy(before),'all11_tables_and_sequences_unchanged':True,'after_release_calls':0 if number==2 else 1,'first_close_observation':{'window_seconds':10,'additional_close_calls':0,'application_statement_retries':0,'observations':1,'terminal':sample,'initial_metadata_before_observer_await':{'is_closed':True},'SQL_after_first_close_return':copy.deepcopy(before),'passed':passing},'first_close_contract_passed':passing,'cleanup_after_last_pool_handle_drop':{'original_socket_inodes_absent':True,'actual_backend_absent':[]},'pool_drop_cleanup_cannot_waive_first_close_negative':True,'cleanup_relay_backend_absence':{'backend_absence':[{'pid':123,'state':[]}],'proxy':{'connections':[{'closed':True,'up_joined':True}]}}}
 if number==1:row['before_first_close']={'is_closed':False,'size':1,'num_idle':1,'after_release_calls':1,'healthy_idle_proven':True}
 elif number==2:row['before_connection_drop']={'closed':True,'first_close_polled_pending':True,'after_release_calls':0}
 else:row['actual_after_release_barrier']={'hook_entered_after_initial_closed_check':True,'first_close_marks_closed_before_hook_release':True,'same_first_close_future_polled_pending':True,'after_release_calls':1,'no_sleep_as_ordering_authority':True}
 peer={'forced':False,'exit':0,'wait':0,'signal':None,'proc_PID_absent_after_wait':True,'stdout_reader_joined':True};return row,[sample],peer
class Controls(unittest.TestCase):
 def test_three_passing_shapes(self):
  for n in(1,2,3):self.assertTrue(validate_case(*fixture(n))['passed'])
 def test_real_negative_not_waived_by_cleanup(self):self.assertFalse(validate_case(*fixture(passing=False))['passed'])
 def reject(self,fn):
  data=fixture();fn(*data)
  with self.assertRaises((ValueError,KeyError,StopIteration)):validate_case(*data)
 def test_size0_alone_not_enough(self):self.assertFalse(post_close_terminal(fixture(passing=False)[1][0]))
 def test_oldFD_still_open(self):self.reject(lambda r,l,p:l[0]['actual_self_FD_links'].append('socket:[12345]'))
 def test_backend_still_alive(self):self.reject(lambda r,l,p:l[0]['backends'][0].update(state=[{'pid':123}]))
 def test_relay_not_joined(self):self.reject(lambda r,l,p:l[0]['relay']['connections'][0].update(up_joined=False))
 def test_size1(self):self.reject(lambda r,l,p:l[0]['metadata_after'].update(size=1))
 def test_second_close_not_allowed(self):self.reject(lambda r,l,p:r.update(subject_close_calls=2))
 def test_application_retry_not_allowed(self):self.reject(lambda r,l,p:r.update(application_statement_retries=1))
 def test_no_injected_replacement(self):self.reject(lambda r,l,p:l[0]['relay']['connections'].append({'id':1,'backend_pid':124,'closed':True,'up_joined':True}))
 def test_alias_backend_mismatch(self):self.reject(lambda r,l,p:r['held_actual_select_ready']['actual_backend'][0].update(pid=124))
 def test_raw_Ready_corruption(self):self.reject(lambda r,l,p:r['held_actual_select_ready']['proxy']['events'][3].update(body_hex='54'))
 def test_raw_command_corruption(self):self.reject(lambda r,l,p:r['held_actual_select_ready']['proxy']['events'][2].update(body_hex=b'SELECT 0\0'.hex()))
 def test_target_row_changed(self):self.reject(lambda r,l,p:r['after']['tables']['vod_movie'].append({'id':1}))
 def test_sequence_changed(self):self.reject(lambda r,l,p:r['after']['sequences']['accounts_user'].update(last_value=2))
 def test_missing_targettable(self):self.reject(lambda r,l,p:r['before']['tables'].pop('vod_movie'))
 def test_negative_cannot_be_relabelled(self):
  r,l,p=fixture(passing=False);r['passed']=True
  with self.assertRaises(ValueError):validate_case(r,l,p)
 def test_negative_window_not_truncated(self):
  r,l,p=fixture(passing=False);l[0]['elapsed_millis']=9999
  with self.assertRaises(ValueError):validate_case(r,l,p)
 def test_forced_peer_cleanup_not_success(self):self.reject(lambda r,l,p:p.update(forced=True,wait=None,signal=9))
 def test_peer_pipe_unjoined(self):self.reject(lambda r,l,p:p.update(stdout_reader_joined=False))
 def test_lastdrop_cleanup_not_actualabsence(self):self.reject(lambda r,l,p:r['cleanup_after_last_pool_handle_drop'].update(original_socket_inodes_absent=False))
 def test_callback_barrier_ordering(self):self.reject(lambda r,l,p:r['actual_after_release_barrier'].update(same_first_close_future_polled_pending=False))
 def test_python_syntax(self):
  for p in HERE.glob('*.py'):ast.parse(p.read_bytes(),filename=str(p))
 def test_default_cli_readonly(self):
  tree=ast.parse((HERE/'run.py').read_text());main=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='main');self.assertIn('if args.execute_reviewed_seal is None',ast.unparse(main))
 def test_wrong_review_hash_stops_before_output_or_process(self):
  import run
  with self.assertRaisesRegex(ValueError,'ExactReviewedSeal'):run.execute(types.SimpleNamespace(execute_reviewed_seal='0'*64),{})
 def test_original_ENOSPC_negative_and_two_receipt_recovery(self):
  old=json.loads((ROOT/'build/candidate-original/build.json').read_text());new=json.loads((ROOT/'build/candidate/recovery.json').read_text());self.assertFalse(old['passed']);self.assertEqual(old['commands'][3]['actual_wait'],101);self.assertEqual(len(new['commands']),2);self.assertTrue(all(r['actual_wait']==r['actual_poll']==0 for r in new['commands']))
if __name__=='__main__':unittest.main()
