"""Projected retained-evidence corruptions; never executes an app/request/SQL."""
import copy,json,unittest,tempfile
from pathlib import Path
import verify
class Controls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.data=verify.members()
 def corrupt(self,change,case=5):
  value=verify.load_projection(case);change(value)
  with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):verify.qualify_projected(case,value,self.data)
 def test_actual_both_offline(self):
  for case in(5,6):self.assertTrue(verify.qualify_projected(case,verify.load_projection(case),self.data)['passed'])
 def test_same_compiled_pool_max(self):self.assertEqual(verify.source_proof()['same_compiled_pool_max'],4)
 def test_raw_actual_passed_true(self):self.corrupt(lambda v:v.__setitem__('passed',False))
 def test_size_live(self):self.corrupt(lambda v:v.__setitem__('closed_pool_size',1))
 def test_size_nonint(self):self.corrupt(lambda v:v.__setitem__('closed_pool_size',0.0))
 def test_size_bool(self):self.corrupt(lambda v:v.__setitem__('closed_pool_size',False))
 def test_idle_negative(self):self.corrupt(lambda v:v.__setitem__('closed_pool_idle',-1))
 def test_idle_nonint(self):self.corrupt(lambda v:v.__setitem__('closed_pool_idle','2'))
 def test_idle_bool(self):self.corrupt(lambda v:v.__setitem__('closed_pool_idle',True))
 def test_idle_overbound(self):self.corrupt(lambda v:v.__setitem__('closed_pool_idle',5))
 def test_literal2_not_normalized(self):self.corrupt(lambda v:v.__setitem__('closed_pool_idle',3))
 def test_original_idle0_not_pass(self):self.corrupt(lambda v:v.__setitem__('closed_pool_idle',0))
 def test_Weak(self):self.corrupt(lambda v:v['while_held_after_logical_completion'].__setitem__('Weak_Some',False))
 def test_pending(self):self.corrupt(lambda v:v['logical_completion'].__setitem__('logical_done',False))
 def test_fake_owner(self):self.corrupt(lambda v:v['logical_completion'].__setitem__('weak_observer_stores_no_Arc',False))
 def test_slots(self):self.corrupt(lambda v:v['logical_completion'].__setitem__('real_jobs_slots_available',4))
 def test_SQL(self):self.corrupt(lambda v:v['after']['SQL']['tables']['accounts_user'].append({'id':99}))
 def test_broker(self):self.corrupt(lambda v:v['after']['all_broker']['rows'].append({'key_hex':'61'}))
 def test_FDloss_not_abort(self):self.corrupt(lambda v:v.__setitem__('handler_abort_claimed',True))
 def test_HTTP6(self):self.corrupt(lambda v:v['HTTP_response_while_reply_held'].__setitem__('status',200),6)
 def test_proxy(self):self.corrupt(lambda v:v['proxy_connections_absent_before_stop'].__setitem__('connections',1))
 def test_host1_not_relabel(self):
  data=dict(self.data);v=json.loads(data['case5/attempt2/execution-report.json']);v['actual_controller_exit']=0;data['case5/attempt2/execution-report.json']=json.dumps(v).encode()
  with self.assertRaises(ValueError):verify.qualify_projected(5,verify.load_projection(5),data)
 def test_defaultbytes(self):
  data=dict(self.data);data['case6/attempt2/default-after.json']+=b' '
  with self.assertRaises(ValueError):verify.qualify_projected(6,verify.load_projection(6),data)
 def test_afterELF_backend(self):
  data=dict(self.data);data['case6/attempt2/backend-after-ELF-exit.json']=json.dumps([{'actual_backends':[{'pid':1}]}]).encode()
  with self.assertRaises(ValueError):verify.qualify_projected(6,verify.load_projection(6),data)
 def test_strict_projection_SHA(self):
  with tempfile.TemporaryDirectory(prefix='backup-projection-control-')as d:
   root=Path(d)
   for name in('projection-policy.json','case5-projection.json'):(root/name).write_bytes((verify.ROOT/name).read_bytes())
   (root/'case5-projection.json').write_bytes((root/'case5-projection.json').read_bytes()+b' ')
   before=verify.ROOT
   try:
    verify.ROOT=root
    with self.assertRaises(ValueError):verify.load_projection(5)
   finally:verify.ROOT=before
if __name__=='__main__':unittest.main()
