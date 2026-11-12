"""Fresh receipt corruption controls; no Docker, Cargo or workload execution."""
import argparse,importlib.util,json,tempfile,unittest,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--build',type=Path,required=True);a=p.parse_args();BUILD=a.build.resolve();HERE=Path(__file__).parent;s=importlib.util.spec_from_file_location('fresh_binding',HERE/'bind_build.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Controls(unittest.TestCase):
 def fixture(self,root):
  for name in ['build.json','compiler.jsonl','legacy-compiler.jsonl']:shutil.copy2(BUILD/name,root/name)
  for name in ['dispatcharr_store','dispatcharr_job_transport','import_sql']:(root/name).symlink_to(BUILD/name)
 def reject(self,mutate):
  with tempfile.TemporaryDirectory()as t:
   root=Path(t);self.fixture(root);record=json.loads((root/'build.json').read_text());mutate(root,record);(root/'build.json').write_text(json.dumps(record))
   with self.assertRaises((ValueError,FileNotFoundError)):m.verify(root)
 def test_positive(self):self.assertEqual(m.verify(BUILD)['source_file_count'],788)
 def test_false_build(self):self.reject(lambda p,r:r.update(passed=False))
 def test_wrong_source(self):self.reject(lambda p,r:r.update(input_manifest_sha256='0'*64))
 def test_wrong_count(self):self.reject(lambda p,r:r.update(source_file_count=787))
 def test_missing_peer(self):self.reject(lambda p,r:r['elf_sha256'].pop('import_sql'))
 def test_wrong_elf_digest(self):self.reject(lambda p,r:r['elf_sha256'].update(dispatcharr_job_transport='0'*64))
 def test_wrong_compiler(self):self.reject(lambda p,r:r.update(compiler_json_sha256='0'*64))
 def test_wrong_legacy_compiler(self):self.reject(lambda p,r:r.update(legacy_compiler_json_sha256='0'*64))
 def test_existing_output_preserved(self):
  with tempfile.TemporaryDirectory()as t:
   root=Path(t);sentinel=root/'sentinel';sentinel.write_text('keep')
   with self.assertRaises(FileExistsError):m.bind(BUILD,root)
   self.assertEqual(sentinel.read_text(),'keep')
if __name__=='__main__':unittest.main(argv=['test_bind_build'])
