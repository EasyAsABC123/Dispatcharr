"""Pure exact host/container namespace binding controls, no Docker/app import."""
import ast,importlib.util,json,unittest
from pathlib import Path
from definition import ROOT,definition_root
class MountRoot(unittest.TestCase):
 def test_exact_container_tools_bind_existing_root_mount(self):
  self.assertEqual(definition_root('/notification-tools/definition.py'),Path('/form-root'))
  controller=(ROOT/'controller/run.py').read_text();self.assertIn("str(HERE.parent)+':/form-root:ro'",controller);self.assertIn("str(HERE)+':/notification-tools:ro'",controller)
 def test_host_root_preserved(self):self.assertEqual(definition_root(ROOT/'controller/definition.py'),ROOT)
 def test_unrelated_container_path_has_no_guessing_fallback(self):self.assertEqual(definition_root('/other-controller/definition.py'),Path('/'))
 def test_literal_original_root_bug_reproduces(self):
  path=ROOT/'inputs/prior-static-negatives/executor4-Source2-pre-request/executor4-definition.py';namespace={'__file__':'/notification-tools/definition.py','__name__':'readonly_original_definition_control'};exec(compile(path.read_text(),str(path),'exec'),namespace);self.assertEqual(namespace['ROOT'],Path('/'));self.assertNotEqual(namespace['ROOT'],definition_root('/notification-tools/definition.py'))
 def test_actual_corrected_module_exec_root_and_schema_closure(self):
  namespace={'__file__':'/notification-tools/definition.py','__name__':'readonly_corrected_definition_control'};text=(ROOT/'controller/definition.py').read_text();exec(compile(text,'/notification-tools/definition.py','exec'),namespace);self.assertEqual(namespace['ROOT'],Path('/form-root'));self.assertTrue((ROOT/'inputs/schema62-authority.json').is_file());self.assertEqual(len(json.loads((ROOT/'inputs/schema62-authority.json').read_text())['tables']),62)
if __name__=='__main__':unittest.main()
