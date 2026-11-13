"""Adversarial retained-evidence controls only, no application/socket calls."""
import base64,copy,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import physical_contract as c
class Contract(unittest.TestCase):
 def report(self):
  name='foreign-family-refused';ident='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';raw=base64.b64encode(json.dumps({'headers':{'id':ident}}).encode()).decode()
  needed=c.required_checks(name);case={'name':name,'id':ident,'transport_preflight':{'broker_result_same_address':True,'separate_broker_result_databases':[8,9]},'before_native_child':{'native_process_created':False,'action_commands_sent':0},'native_process':{'pid':9,'starttime':'123','exit':0,'settled':True,'pipes_settled':True,'writer_settled':True,'invalid':None},'began_utc':'2026-11-12T00:00:00+00:00','finished_utc':'2026-11-12T00:00:01+00:00','published_parent':[raw],'before_assertions':[{'name':n,'raw':{'backend':{},'parent_queue':[raw]}}for n in needed],'result':{'metadata':None},'after_run':{'parent_queue':[raw]}}
  return {'passed':True,'Native_verified':True,'performance_verified':False,'mode':name,'predeclared_modes':[name],'finally_errors':[],'cases':[case],'checks':[{'name':n,'passed':True}for n in needed]}
 def bad(self,mutate):
  r=self.report();mutate(r)
  with self.assertRaises(ValueError):c.validate_report(0,r,'foreign-family-refused')
 def test_complete_refusal_evidence(self):self.assertEqual(c.validate_report(0,self.report(),'foreign-family-refused')['cases'],1)
 def test_false_top_level_fails(self):self.bad(lambda r:r.update(passed=False))
 def test_unknown_settlement_fails(self):self.bad(lambda r:r['cases'][0]['native_process'].update(settled=None))
 def test_live_pipe_fails(self):self.bad(lambda r:r['cases'][0]['native_process'].update(pipes_settled=False))
 def test_nonzero_guest_not_relabelled(self):
  with self.assertRaises(ValueError):c.validate_report(1,self.report(),'foreign-family-refused')
 def test_missing_case_fails(self):self.bad(lambda r:r.update(cases=[]))
 def test_removed_raw_assertion_receipt_fails(self):self.bad(lambda r:r['cases'][0].update(before_assertions=[]))
 def test_missing_required_assertion_fails(self):self.bad(lambda r:r['checks'].pop())
 def test_consumed_refusal_head_fails(self):self.bad(lambda r:r['cases'][0]['after_run'].update(parent_queue=[]))
 def test_uncertain_proxy_close_fails(self):self.bad(lambda r:r['cases'][0].update(PG={'physical_close_verified':False,'errors':[]}))
 def test_other_parent_identity_fails(self):self.bad(lambda r:r['cases'][0].update(id='bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'))
 def test_metadata_on_refused_head_fails(self):self.bad(lambda r:r['cases'][0]['result'].update(metadata={'status':'SUCCESS'}))
 def test_cleanup_error_cannot_pass(self):self.bad(lambda r:r.update(finally_errors=['unsettled']))
 def test_duplicate_checks_cannot_inflate_count(self):self.bad(lambda r:r['checks'].append(copy.deepcopy(r['checks'][0])))
 def test_all_three_modes_are_finite(self):
  self.assertEqual(len(c.names('physical-artwork-onward')),8)
  for name in c.REFUSALS:self.assertEqual(c.names(name),[name])
  with self.assertRaises(ValueError):c.names('all')
if __name__=='__main__':unittest.main()
