import unittest
import verify_physical as V
class Physical(unittest.TestCase):
 def setUp(self):self.p=V.load('physical-positives.json');self.a=V.load('physical-attempts.json');self.c=V.load('physical-agent-review.json')
 def bad(self,f):
  f()
  with self.assertRaises((ValueError,KeyError,TypeError)):V.validate(self.p,self.a,self.c)
 def test_actual(self):self.assertEqual(V.validate(self.p,self.a,self.c)['finite_physical_positives'],list(range(1,8)))
 def test_missing_positive(self):self.bad(lambda:self.p.pop())
 def test_owner_released(self):self.bad(lambda:self.p[2]['value']['cases'][0]['state_held'].update(active=0))
 def test_early_drain(self):self.bad(lambda:self.p[1]['value']['cases'][0].update(drain_held={'shutdown_drained':True}))
 def test_returned_before_logical_deadline(self):self.bad(lambda:self.p[2]['value']['cases'][0]['result'].update(elapsed_seconds=8))
 def test_close_before_reader_join(self):self.bad(lambda:self.p[3]['value']['cases'][0]['caller_close_details'].update(reader_joined=False))
 def test_wrong_acl_restoration_rules(self):self.bad(lambda:self.p[5]['value']['cases'][0]['ACL_restore'].update(command_tokens=['+@all']))
 def test_extra_fifth_grant(self):self.bad(lambda:self.p[6]['value']['cases'][0]['state_after_fifth'].update(available_writer_permits=1))
 def test_old_host_relabel(self):self.bad(lambda:self.a[0].update(actual_exit=0,actual_passed=True))
 def test_original_default_after_invented(self):self.bad(lambda:self.a[0].update(default_after=self.a[0]['default_before']))
 def test_missing_cleanup(self):self.bad(lambda:self.c['queries'].pop())
 def test_missing_identity_absence(self):self.bad(lambda:self.c['host_identity_absence'].pop())
if __name__=='__main__':unittest.main()
