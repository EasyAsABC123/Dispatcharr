"""Image57 exact-runtime dictionary corruption controls, no app/Docker action."""
import copy,importlib.util,json,unittest
from pathlib import Path
P=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('notification55_guard',P/'tools/candidate_inputs.py');C=importlib.util.module_from_spec(spec);spec.loader.exec_module(C)
class Binding(unittest.TestCase):
 def setUp(self):
  self.expected=json.loads((P/'tools/expected-runtime.json').read_text());self.record={'source_sha256':copy.deepcopy(self.expected)}
 def test_exact(self):self.assertTrue(C.require_runtime_set(self.record,self.expected))
 def test_missing_runtime(self):
  self.record['source_sha256'].pop(next(iter(self.expected)))
  with self.assertRaises(ValueError):C.require_runtime_set(self.record,self.expected)
 def test_extra_runtime(self):
  self.record['source_sha256']['rust/unreviewed.rs']='0'*64
  with self.assertRaises(ValueError):C.require_runtime_set(self.record,self.expected)
 def test_changed_blob(self):
  self.record['source_sha256'][next(iter(self.expected))]='0'*64
  with self.assertRaises(ValueError):C.require_runtime_set(self.record,self.expected)
 def test_wrong_dictionary_count(self):
  self.expected.pop(next(iter(self.expected)))
  with self.assertRaises(ValueError):C.require_runtime_set(self.record,self.expected)
 def test_nonruntime_not_promoted(self):
  self.record['source_sha256']['frontend-unreviewed']='0'*64
  self.assertTrue(C.require_runtime_set(self.record,self.expected))
 def test_old_candidate_not_bound(self):
  old={'image_id':'sha256:8e3c133c0639d60057d8d05a5be2fce66e0f0968c35e29f674c348495a1a9ca0','manifest_sha256':'cac7bcdcc60dff6f26e233e78c2737b7f5d9450cf54042719b8d5cd61cf5ff8b','runtime_map_sha256':'7acb3cf3c8777286cbfab1a8e338920ede7151d9ef173db5b259f77e3b6e802f'}
  self.assertNotEqual(C.IDENTITY['image_id'],old['image_id']);self.assertNotEqual(C.MANIFEST_SHA,old['manifest_sha256']);self.assertNotEqual(C.EXPECTED_SHA,old['runtime_map_sha256'])
 def test_no_activation_or_old_namespace(self):
  binding=json.loads((P/'candidate-binding.json').read_text());plan=json.loads((P/'execution-plan.json').read_text());cue=json.loads((P/'cue-template.json').read_text())
  self.assertEqual(binding['runtime_inputs'],839);self.assertEqual(binding['app_inputs'],1336);self.assertFalse(cue['authorized']);self.assertFalse(cue['candidate_application_requests_authorized']);self.assertIn('image57-',plan['new_output']);self.assertIn('image57-',plan['cue_path']);self.assertIn('image57-',plan['Root_quiet_preflight'])
if __name__=='__main__':unittest.main()
