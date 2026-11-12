"""Pure virtual /proc/Redis/packet controls. No actual proc or Redis reads."""
import unittest
from unittest import mock
from metrics import *
def status():return 'Uid:\t1\t1\t1\t1\nGid:\t1\t1\t1\t1\n'
def statline(start=10,state='S'):return '7 (name with spaces) '+state+' '+' '.join(str(x)for x in [1]+[0]*17+[start,0,2])
class Controls(unittest.TestCase):
 def setUp(self):
  for name in["geteuid","getegid"]:
   patch=mock.patch("metrics.os."+name,return_value=1);patch.start();self.addCleanup(patch.stop)
 def reader(self,deny=False,reuse=False):
  calls=[0]
  def read(path,limit=65536):
   if path.name=='status':return status()
   if path.name=='stat':calls[0]+=1;return statline(11 if reuse and calls[0]>1 else 10)
   if deny:raise PermissionError('captured-denial control')
   return 'Rss: 12 kB\nPss: 7 kB\n'
  return read
 def test_complete_memory_and_actual_executable_identity(self):
  r=collect_pid(7,[1,1],read=self.reader(),readlink=lambda p:'/usr/bin/ffmpeg');self.assertTrue(r['coverage_complete']);self.assertEqual(r['pss_bytes'],7168);self.assertTrue(r['identified_media'])
 def test_denial_partial_and_all_denied_never_zero(self):
  r=collect_pid(7,[1,1],read=self.reader(deny=True),readlink=lambda p:'/usr/bin/python');self.assertFalse(r['coverage_complete']);self.assertIsNone(r['pss_bytes']);self.assertEqual(r['denied_fields'],['memory'])
 def test_PID_reuse_invalidates(self):self.assertFalse(collect_pid(7,[1,1],read=self.reader(reuse=True),readlink=lambda p:'/usr/bin/python')['coverage_complete'])
 def test_wrong_UID_and_malformed_memory_are_fatal(self):
  with self.assertRaises(ValueError):collect_pid(7,[2,2],read=self.reader())
  with self.assertRaises(ValueError):memory('Rss: bad kB\nPss: 1 kB')
 def test_redis_restart_reset_or_absent_fields_invalid(self):
  a={'run_id':'a'*40,'total_commands_processed':3,'total_net_input_bytes':10,'total_net_output_bytes':20}
  self.assertFalse(redis_delta(a,{**a,'total_commands_processed':4})['application_only_attribution'])
  for b in[{**a,'run_id':'b'*40},{**a,'total_net_input_bytes':1}]:
   with self.assertRaises(ValueError):redis_delta(a,b)
  with self.assertRaises(KeyError):redis_info('run_id:'+'a'*40)
 def test_packet_CC_DI_and_null_PID_are_distinct(self):
  def packet(cc,di=False):return bytes([0x47,0,10,0x30|cc,1,128 if di else 0])+b'x'*182
  c=Continuity();c.feed(packet(1));c.feed(packet(3));c.feed(packet(9,True));self.assertEqual(c.snapshot()['CC_mismatches'],1);self.assertEqual(c.snapshot()['discontinuity_indicators'],1);self.assertIsNone(c.snapshot()['dropped_packets_inferred'])

class MoreCoverage(unittest.TestCase):
 def setUp(self):
  for name in["geteuid","getegid"]:
   patch=mock.patch("metrics.os."+name,return_value=1);patch.start();self.addCleanup(patch.stop)
 def test_all_denied_retains_null_memory_and_incomplete(self):
  def deny(*args,**kwargs):raise PermissionError('all denied')
  rows=[collect_pid(pid,[1,1],read=deny)for pid in[7,8]];r=aggregate(rows,[7,8],[7,8]);self.assertFalse(r['coverage_complete']);self.assertFalse(r['memory_sums_complete']);self.assertEqual(r['denied_processes'],[7,8])
 def test_duplicate_or_changed_inventory_never_complete(self):
  row={'pid':7,'coverage_complete':True,'stable':True,'state':'S','identified_media':False,'rss_bytes':1,'pss_bytes':1,'denied_fields':[]}
  self.assertFalse(aggregate([row,row],[7],[7])['coverage_complete']);self.assertFalse(aggregate([row],[7],[8])['coverage_complete'])

if __name__=='__main__':unittest.main()
