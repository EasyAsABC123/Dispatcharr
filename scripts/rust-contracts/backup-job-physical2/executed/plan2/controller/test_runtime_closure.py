"""Synthetic mounted-filesystem corruption controls, no image/app/socket execution."""
import ast,json,shutil,tempfile,unittest
from pathlib import Path
from runtime_closure import validate,ROOT,resolve_mount
SOURCE=Path(json.loads((ROOT/'inputs/runtime-file-declarations.json').read_text())['native_source_root'])
class RuntimeClosure(unittest.TestCase):
 def fake(self,change=None):
  with tempfile.TemporaryDirectory(prefix='backup-mounted-inputs-')as d:
   root=Path(d);(root/'controller').mkdir();(root/'inputs/pg-init').mkdir(parents=True)
   for p in(ROOT/'controller').glob('*.py'):shutil.copyfile(p,root/'controller'/p.name)
   shutil.copyfile(ROOT/'controller/start.sh',root/'controller/start.sh')
   for name in('runtime-file-declarations.json','schema62-authority.json','resp-contracts.json'):shutil.copyfile(ROOT/'inputs'/name,root/'inputs'/name)
   for p in(ROOT/'inputs/pg-init').glob('*'):shutil.copyfile(p,root/'inputs/pg-init'/p.name)
   # Existence-only fixture; actual binary SHA/size are independently mandatory in run.check.
   (root/'inputs/native-job-tests').write_bytes(b'not-an-executed-ELF')
   if change:change(root)
   return validate(root,SOURCE)
 def corrupt(self,change):
  with self.assertRaises((ValueError,FileNotFoundError)):self.fake(change)
 def test_actual_recursive_closure(self):self.assertTrue(validate(ROOT,SOURCE)['passed'])
 def test_mounted_synthetic_fixture(self):self.assertTrue(self.fake()['passed'])
 def test_missing_schema(self):self.corrupt(lambda r:(r/'inputs/schema62-authority.json').unlink())
 def test_original_failed_dynamic_path(self):
  def change(r):
   p=r/'controller/audit.py';p.write_text(p.read_text().replace('/native-inputs/schema62-authority.json','/native-root/inputs/source-authority/executed-executor5/inputs/schema62-authority.json'))
  self.corrupt(change)
 def test_wrong_schema_bytes(self):self.corrupt(lambda r:(r/'inputs/schema62-authority.json').write_text('{}'))
 def test_recursive_local_import_missing(self):self.corrupt(lambda r:(r/'controller/public_manifest.py').unlink())
 def test_bootstrap_sourced_file_missing(self):self.corrupt(lambda r:(r/'inputs/pg-init/02-postgres.sh').unlink())
 def test_peer_joined_file_missing(self):self.corrupt(lambda r:(r/'controller/owned_resp_proxy.py').unlink())
 def test_contract_file_missing(self):self.corrupt(lambda r:(r/'inputs/resp-contracts.json').unlink())
 def test_new_dynamic_path_missing(self):
  def change(r):
   p=r/'controller/audit.py';p.write_text(p.read_text()+"\ndef unexecuted_new_dependency(): return Path('/native-inputs/new-missing.json').read_text()\n")
  self.corrupt(change)
 def test_undeclared_mount(self):
  with self.assertRaises(ValueError):resolve_mount('/native-rooted/inputs/file',ROOT,SOURCE)
 def test_path_escape(self):
  with self.assertRaises(ValueError):resolve_mount('/native-root/../other',ROOT,SOURCE)
if __name__=='__main__':unittest.main()
