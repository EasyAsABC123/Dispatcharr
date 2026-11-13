"""Socket-free actual protocol-stage predicate negatives, not runtime proof."""
import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from pg_reply_proxy import target_query,ready_for_hold
class Gates(unittest.TestCase):
 def state(self,**kw):return dict({'target':True,'rows':1,'commands':[b'SELECT 1\0'],'error':False},**kw)
 def test_success_requires_rows_and_command(self):
  self.assertTrue(ready_for_hold('programme-success-Ready',self.state()))
  for state in [{'target':True,'rows':0,'commands':[],'error':False},{'target':True,'rows':1,'commands':[],'error':False},{'target':False,'rows':1,'commands':[b'SELECT 1\0'],'error':False}]:self.assertFalse(ready_for_hold('programme-success-Ready',state))
 def test_two_retained_programmes_need_exact_two_row_command(self):
  good={'target':True,'expected_rows':2,'rows':2,'commands':[b'SELECT 2\0'],'error':False}
  self.assertTrue(ready_for_hold('programme-success-Ready',good))
  for change in [{'rows':1},{'commands':[b'SELECT 1\0']},{'rows':3},{'commands':[]}]:self.assertFalse(ready_for_hold('programme-success-Ready',{**good,**change}))
 def test_unrelated_select_cannot_match(self):
  self.assertFalse(target_query('programme-success-Ready',b'SELECT id FROM epg_programdata',set()))
  self.assertTrue(target_query('programme-success-Ready',b'SELECT id FROM epg_programdata WHERE epg_id=$1 ORDER BY start_time LIMIT 33',set()))
 def test_commit_bound_to_its_transaction(self):
  self.assertFalse(target_query('Periodic-COMMIT-Ready',b'COMMIT',{'Clocked'}));self.assertTrue(target_query('Periodic-COMMIT-Ready',b'COMMIT',{'Periodic'}))
 def test_error_needs_actual_error_frame(self):
  self.assertFalse(ready_for_hold('settings-error-Ready',{'target':True,'rows':0,'commands':[],'error':False}));self.assertTrue(ready_for_hold('settings-error-Ready',{'target':True,'rows':0,'commands':[],'error':True}))
if __name__=='__main__':unittest.main()
