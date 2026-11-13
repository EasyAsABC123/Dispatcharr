"""No proc scan/process/sockets: corrupt metadata cannot release preflight."""
import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from roles import unexpected,validate
class Controls(unittest.TestCase):
 def test_known_infra_and_own_preflight_admitted_as_metadata_only(self):
  self.assertTrue(validate({'passed':True,'rows':[{'pid':1,'argv':['postgres']},{'pid':2,'argv':['redis-server']},{'pid':3,'argv':['python','/job-tools/verify_roles.py']}],'unexpected_web_worker_beat':[],'own_pid':3}))
 def test_direct_and_python_module_celery_roles_rejected(self):
  for argv in (['celery','-A','observe:app','worker'],['python','-m','celery','-A','app','beat']):self.assertTrue(unexpected(argv))
 def test_uwsgi_rejected(self):self.assertTrue(unexpected(['/dispatcharrpy/bin/uwsgi','--ini','/verification/uwsgi.ini']))
 def test_empty_failed_unknown_metadata_not_invented_as_empty_success(self):
  for report in ({},{'passed':False},{'passed':True,'rows':[]},{'passed':True,'rows':None}):
   with self.assertRaises(ValueError):validate(report)
 def test_passed_true_cannot_hide_actual_worker(self):
  with self.assertRaisesRegex(ValueError,'NoUnowned'):validate({'passed':True,'rows':[{'pid':1,'argv':['celery','worker']}],'unexpected_web_worker_beat':[],'own_pid':1})
 def test_missing_own_pid_not_settlement(self):
  with self.assertRaisesRegex(ValueError,'ActualPreflightPIDPresent'):validate({'passed':True,'rows':[{'pid':1,'argv':['postgres']}],'unexpected_web_worker_beat':[],'own_pid':9})
 def test_bool_pid_not_integer_identity(self):
  with self.assertRaisesRegex(ValueError,'ActualUniquePIDs'):validate({'passed':True,'rows':[{'pid':True,'argv':['postgres']}],'unexpected_web_worker_beat':[],'own_pid':True})
 def test_duplicate_pid_refused(self):
  with self.assertRaisesRegex(ValueError,'ActualUniquePIDs'):validate({'passed':True,'rows':[{'pid':1,'argv':['postgres']},{'pid':1,'argv':['python']}],'unexpected_web_worker_beat':[],'own_pid':1})
if __name__=='__main__':unittest.main()
