"""Pure host receipt/cleanup controls; no Docker/process/app dispatch."""
import os,sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import run_pair
class Host(unittest.TestCase):
 def test_literal_lowercase_absence(self):
  cid='a'*64;self.assertTrue(run_pair.exact_absent_cid(1,b'[]\n',('Error: No such object: '+cid+'\n').encode(),cid))
 def test_other_cid_never_counts_absent(self):self.assertFalse(run_pair.exact_absent_cid(1,b'[]',('error: no such object: '+'b'*64).encode(),'a'*64))
 def test_daemon_error_never_counts_absent(self):self.assertFalse(run_pair.exact_absent_cid(1,b'[]',b'permission denied','a'*64))
 def test_success_empty_output_is_not_missing_cid(self):self.assertFalse(run_pair.exact_absent_cid(0,b'',b'','a'*64))
 def test_unknown_cleanup_is_not_budget_pass(self):self.assertFalse(run_pair.cleanup_budget_status(0,91)['within_budget'])
 def test_no_widened_cleanup_budget(self):self.assertFalse(run_pair.cleanup_budget_status(0,91,120)['within_budget'])
 def test_inherited_job_or_git_environment_removed(self):
  with patch.dict(os.environ,{'GIT_WORK_TREE':'bad','CELERY_BROKER_URL':'bad','POSTGRES_DB':'real','FIXTURE_SECONDS':'1','BENCHMARK_CODEC':'bad','DOCKER_HOST':'owned','PATH':'bound'},clear=True):
   self.assertEqual(run_pair.environment(),{'DOCKER_HOST':'owned','PATH':'bound'})
 def test_executor_is_finite_twelve_tool_native_route(self):
  self.assertEqual(len(run_pair.REQUIRED),12);self.assertIn('normal_contract.py',run_pair.REQUIRED);self.assertIn('source_normal.py',run_pair.REQUIRED);self.assertNotIn('source_configured.py',run_pair.REQUIRED)
if __name__=='__main__':unittest.main()
