"""Pure schedule guards only; no Source/HTTP/process activity."""
import unittest
from schedule import Controller,physical_close
PREFIX='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'
def ready():
 c=Controller()
 for role in['shared_initial','shared_follower','other']:c.prelude_open(role,200,PREFIX)
 c.admin_completed(10,200);c.shared_closed({'role':'shared_initial','fd_after':-1,'original_inode_absent':True,'server_cancellation_inferred':False,'close_begin':10.01,'close_complete':10.02},{'role':'shared_follower','fd_after':-1,'original_inode_absent':True,'server_cancellation_inferred':False,'close_begin':10.03,'close_complete':10.04});return c
class Controls(unittest.TestCase):
 def test_counter0_1_2_none_do_not_change_dispatch(self):
  receipts=[]
  for value in['0','1','2',None]:
   c=ready();c.evidence_counter('fixed',value);c.dispatch_probe1(13.04);c.probe1_observed(15,200);c.dispatch_probe2(15.25);receipts.append(c.snapshot()['dispatch_receipts'])
  self.assertTrue(all(row==receipts[0]for row in receipts))
 def test_every_probe1_status_still_dispatches_second(self):
  for status in[200,401,404,429,500,503]:
   c=ready();c.dispatch_probe1(13.04);c.probe1_observed(15,status);c.dispatch_probe2(15.25);self.assertEqual(len(c.receipts),2)
 def test_transport_error_is_recorded_without_skipping_second(self):
  c=ready();c.dispatch_probe1(13.04);c.probe1_observed(20,None,'ReadTimeout');c.dispatch_probe2(20.25);self.assertEqual(len(c.receipts),2)
 def test_no_duplicate_or_skipped_probe(self):
  c=ready()
  with self.assertRaises(ValueError):c.dispatch_probe2(13.04)
  c.dispatch_probe1(13.04)
  with self.assertRaises(ValueError):c.dispatch_probe1(13.04)
  c.probe1_observed(15,429);c.dispatch_probe2(15.25)
  with self.assertRaises(ValueError):c.dispatch_probe2(15.25)
 def test_no_early_or_expired_clock_dispatch(self):
  for at in[13,13.40,float('nan'),True]:
   with self.assertRaises(ValueError):ready().dispatch_probe1(at)
 def test_boolean_or_missing_fd_is_not_closed(self):
  for value in[False,None,0,'-1']:
   with self.assertRaises(ValueError):physical_close({'role':'shared_follower','fd_after':value,'original_inode_absent':True,'server_cancellation_inferred':False,'close_begin':1,'close_complete':2},'shared_follower')
 def test_counter_evidence_and_input_bounds(self):
  c=ready()
  for _ in range(16):c.evidence_counter('fixed','0')
  with self.assertRaises(ValueError):c.evidence_counter('fixed','0')
 def test_input_order_and_prefix_not_silently_qualified(self):
  for role,status,prefix in[('other',200,PREFIX),('shared_initial',429,PREFIX),('shared_initial',200,'bad')]:
   with self.assertRaises(ValueError):Controller().prelude_open(role,status,prefix)
if __name__=='__main__':unittest.main(verbosity=2)
