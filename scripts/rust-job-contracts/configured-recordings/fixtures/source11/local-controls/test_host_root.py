"""Socket-free host admission/cleanup parsing controls; no application imports."""
import importlib.util,os,pathlib,sys,unittest
TOOLS=pathlib.Path(__file__).resolve().parents[1]/'tools'
sys.path.insert(0,str(TOOLS))
import run_source as R
class HostControls(unittest.TestCase):
 def test_actual_lowercase_missing(self):self.assertTrue(R.exact_absent_cid(1,b'[]\n',('error: no such object: '+'a'*64).encode(),'a'*64))
 def test_original_uppercase_missing(self):self.assertTrue(R.exact_absent_cid(1,b'',('Error: No such object: '+'a'*64).encode(),'a'*64))
 def test_wrong_CID(self):self.assertFalse(R.exact_absent_cid(1,b'[]',('error: no such object: '+'b'*64).encode(),'a'*64))
 def test_wrong_exit(self):self.assertFalse(R.exact_absent_cid(0,b'[]',('error: no such object: '+'a'*64).encode(),'a'*64))
 def test_wrong_diagnostic(self):self.assertFalse(R.exact_absent_cid(1,b'[]',b'permission denied','a'*64))
 def test_present_output(self):self.assertFalse(R.exact_absent_cid(1,b'[{"Id":"present"}]',('error: no such object: '+'a'*64).encode(),'a'*64))
 def test_invalid_UTF8(self):self.assertFalse(R.exact_absent_cid(1,b'[]',b'\xff','a'*64))
 def test_invalid_CID(self):self.assertFalse(R.exact_absent_cid(1,b'[]',b'error: no such object: x','x'))
 def test_explicit_tool_closure(self):self.assertEqual(len(R.REQUIRED),9);self.assertIn('bounded_process.py',R.REQUIRED)
 def test_cleanup_acceptance_deadline_cannot_expand_for_join_or_stop(self):
  self.assertTrue(R.cleanup_budget_status(100,190)['within_budget']);self.assertFalse(R.cleanup_budget_status(100,190.001)['within_budget'])
 def test_cleanup_negative_time_or_budget_change_not_qualified(self):
  self.assertFalse(R.cleanup_budget_status(100,99)['within_budget']);self.assertFalse(R.cleanup_budget_status(100,190,120)['within_budget'])
 def test_recorded_start_precedes_popen_constructor(self):
  # Check the actual host closure, not a duplicate command implementation.
  import ast
  tree=ast.parse((TOOLS/'run_source.py').read_text());main=next(v for v in tree.body if isinstance(v,ast.FunctionDef)and v.name=='main');command=next(v for v in ast.walk(main)if isinstance(v,ast.FunctionDef)and v.name=='command');text=(TOOLS/'run_source.py').read_text().splitlines()
  body='\n'.join(text[command.lineno-1:command.end_lineno]);self.assertLess(body.index("save(out/(tag+'.started.json'),record)"),body.index('job=Collected('));self.assertIn('UnknownConstructorSettlement',body)
 def test_no_application_import(self):self.assertFalse(any(k=='django'or k.startswith('django.')for k in sys.modules))
 def test_environment_sanitized(self):
  old=dict(os.environ)
  try:
   os.environ.update(GIT_DIR='bad',COMPOSE_PROJECT_NAME='bad',BENCHMARK_RETENTION_SECONDS='bad',ENABLE_VOD_FIXTURE='bad',DOCKER_HOST='owned');e=R.environment();self.assertNotIn('GIT_DIR',e);self.assertNotIn('COMPOSE_PROJECT_NAME',e);self.assertNotIn('BENCHMARK_RETENTION_SECONDS',e);self.assertNotIn('ENABLE_VOD_FIXTURE',e);self.assertEqual(e['DOCKER_HOST'],'owned')
  finally:os.environ.clear();os.environ.update(old)
if __name__=='__main__':unittest.main(verbosity=2)
