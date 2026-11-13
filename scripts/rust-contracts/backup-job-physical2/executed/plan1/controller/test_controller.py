"""Socket-free budget/ownership/driver controls. Never dispatches a command."""
import ast,json,unittest
from pathlib import Path
from run import command_budget,cleanup_window,ROOT
class Controller(unittest.TestCase):
 def test_normal_command_limit(self):
  self.assertEqual(command_budget(223,0,False),1048576)
  with self.assertRaises(ValueError):command_budget(224,0,False)
 def test_cleanup_command_reserve(self):
  self.assertEqual(command_budget(224,0,True),1048576)
  with self.assertRaises(ValueError):command_budget(256,0,True)
 def test_normal_output_reserve(self):
  self.assertEqual(command_budget(0,56*1048576-1,False),1)
  with self.assertRaises(ValueError):command_budget(0,56*1048576,False)
 def test_total_output_reserved_stop(self):
  maximum=64*1048576-65536
  self.assertEqual(command_budget(0,maximum-1,True),1)
  with self.assertRaises(ValueError):command_budget(0,maximum,True)
 def test_typed_budget(self):
  for row in[(True,0,False),(0,True,False),(0,0,1)]:
   with self.assertRaises(ValueError):command_budget(*row)
 def test_cleanup90(self):
  self.assertTrue(cleanup_window(0,90)['within_declared_90_seconds']);self.assertFalse(cleanup_window(0,90.01)['within_declared_90_seconds']);self.assertFalse(cleanup_window(0,float('nan'))['within_declared_90_seconds'])
 def test_compile_source830(self):
  src=(ROOT/'controller/run.py').read_text();self.assertIn('Exact830Source',src);self.assertIn('FiveActualCompileWaitPoll0',src);self.assertIn('ActualCompletedOutsideCopy',src)
 def test_exact_future_namespace(self):
  src=(ROOT/'controller/run.py').read_text();self.assertIn('FixedFreshCaseOutputNamespace',src);self.assertIn('ExactlyPhysicalCase5Or6',src)
 def test_start_identity_before_acceptance(self):
  src=(ROOT/'controller/run.py').read_text();self.assertLess(src.index('ActualCommandPIDStartObservedBeforeAcceptance'),src.index("return(out/(tag+'.stdout')).read_text()"));self.assertIn("len(report['processes'])==serial",src)
 def test_default_beforeafter(self):
  src=(ROOT/'controller/run.py').read_text();self.assertIn("before=audit('default','default-before','dispatcharr')",src);self.assertIn("before==after",src);self.assertIn('default11_full62tables59seq_exact',src)
 def test_cleanup_begins_before_postprocess(self):
  src=(ROOT/'controller/run.py').read_text();self.assertLess(src.index('  begin_cleanup()\n  actual='),src.index('qualified=validate(actual,args.case,out)'))
 def test_one_existing_runtime_allowance(self):
  src=(ROOT/'controller/run.py').read_text();self.assertIn('1200-(time.monotonic()-began)',src);self.assertIn("],100,'actual-backup-physical-case-'",src);self.assertNotIn('phase_deadline=time.monotonic()+1200',src)
 def test_stop_nonrecursive_bounded_identity(self):
  src=(ROOT/'controller/run.py').read_text();stop=(ROOT/'controller/bounded_stop.py').read_text();self.assertIn('limit=65536',src);self.assertIn('lambda:None',stop);self.assertIn('ActualOwnedStopPIDIdentityRequired',stop)
 def test_driver_not_Django(self):
  src=(ROOT/'controller/audit.py').read_text();self.assertIn('import psycopg',src);self.assertNotIn('import django',src);self.assertNotIn('import psycopg2',src);self.assertIn('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY',src)
 def test_real_bootstrap_noapp(self):
  src=(ROOT/'controller/start.sh').read_text();self.assertNotIn('django.setup',src);self.assertNotIn('manage.py',src);self.assertIn('original-1241b3-schema.sql',src)
 def test_no_sources_on_import(self):
  for file in('run.py','validate_physical.py','audit.py'):
   tree=ast.parse((ROOT/'controller'/file).read_text())
   self.assertFalse(any(isinstance(n,ast.Expr)and isinstance(n.value,ast.Call)and isinstance(n.value.func,ast.Name)and n.value.func.id in('main','execute','Popen')for n in tree.body))
if __name__=='__main__':unittest.main()
