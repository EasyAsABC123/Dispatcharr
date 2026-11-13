"""Pure raw-auditor driver compatibility controls; simulated driver, no SQL/app."""
import ast,hashlib,importlib.util,json,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('forms_raw_auditor_control',ROOT/'controller/audit.py');audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
class Driver(unittest.TestCase):
 def invoke(self,readonly='on',isolation='repeatable read',database='dispatcharr'):
  events=[];closed=[];invoked=[]
  class Cursor:
   def __enter__(self):return self
   def __exit__(self,*args):pass
   def execute(self,sql):events.append(sql);self.sql=sql
   def fetchone(self):return (readonly,)if self.sql=='SHOW transaction_read_only'else(isolation,)if self.sql=='SHOW transaction_isolation'else(database,)
  class Connection:
   def cursor(self):return Cursor()
   def close(self):closed.append(True)
  def connect(**kwargs):
   self.assertEqual(kwargs,{'host':'/var/run/postgresql','user':'dispatch','dbname':'dispatcharr','connect_timeout':3,'options':'-c default_transaction_read_only=on -c statement_timeout=5000','autocommit':False});return Connection()
  def manifest(adapter,expected):
   self.assertEqual(events,['SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY','SHOW transaction_read_only','SHOW transaction_isolation','SELECT current_database()']);self.assertEqual(len(expected['tables']),62);self.assertEqual(len(expected['sequences']),59);self.assertEqual(adapter.ops.quote_name('a"b'),'"a""b"');invoked.append(True);return {'synthetic_boundary_only':True}
  raw_driver=types.ModuleType('psycopg');raw_driver.connect=connect;algorithm=types.ModuleType('public_manifest');algorithm.manifest=manifest
  expected=ROOT/'inputs/source-authority/executed-executor5/inputs/schema62-authority.json'
  def path(value):self.assertEqual(value,'/native-root/inputs/source-authority/executed-executor5/inputs/schema62-authority.json');return expected
  with patch.dict(sys.modules,{'psycopg':raw_driver,'public_manifest':algorithm}),patch.object(audit,'Path',path):
   try:value=audit.full_public_manifest('dispatcharr')
   finally:self.assertEqual(closed,[True])
  return value,events,invoked
 def test_actual_function_driver3_transaction_boundary(self):
  value,events,invoked=self.invoke();self.assertTrue(value['synthetic_boundary_only']);self.assertEqual(invoked,[True])
 def test_server_readonly_assertion_before_manifest(self):
  with self.assertRaisesRegex(ValueError,'ActualFullDefaultReadOnlyOn'):self.invoke(readonly='off')
 def test_isolation_assertion_before_manifest(self):
  with self.assertRaisesRegex(ValueError,'ActualFullDefaultRepeatableRead'):self.invoke(isolation='read committed')
 def test_exact_default_database_binding(self):
  with self.assertRaisesRegex(ValueError,'ExactFullDefaultDatabase'):self.invoke(database='other')
 def test_no_psycopg2_or_Django_boundary(self):
  tree=ast.parse((ROOT/'controller/audit.py').read_text());imports=[n.name for node in ast.walk(tree)if isinstance(node,ast.Import)for n in node.names]+[node.module for node in ast.walk(tree)if isinstance(node,ast.ImportFrom)];self.assertIn('psycopg',imports);self.assertFalse(any(n and(n.startswith('django')or n=='psycopg2')for n in imports));self.assertNotIn('set_session',(ROOT/'controller/audit.py').read_text())
 def test_manifest_algorithm_exact_executed_Source_copy(self):
  expected=Path('artifacts/rust/m5-notification-forms-source-executor-5/controller/public_manifest.py');self.assertEqual((ROOT/'controller/public_manifest.py').read_bytes(),expected.read_bytes())
if __name__=='__main__':unittest.main(verbosity=2)
