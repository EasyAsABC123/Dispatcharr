"""Pure image59 binding negatives; no archive extraction, Docker or app dispatch."""
import copy,importlib.util,json,tempfile,unittest
from pathlib import Path
HERE=Path(__file__).resolve().parent
REPO=Path('/home/justin/github/Dispatcharr')
spec=importlib.util.spec_from_file_location('candidate55_guard',HERE/'tools/candidate_inputs.py')
G=importlib.util.module_from_spec(spec);spec.loader.exec_module(G)
class Binding55(unittest.TestCase):
 def setUp(self):
  self.expected=json.loads((HERE/'tools/expected-runtime.json').read_text())
  self.binding=json.loads((HERE/'candidate-binding.json').read_text())
  self.manifest=self.binding.get('build_manifest',self.binding.get('manifest'))
  self.archive=self.binding.get('source_archive',self.binding.get('archive'))
  self.record=json.loads((REPO/self.manifest).read_text())
 def test_current854_set_exact(self):
  self.assertEqual(len(self.expected),854);self.assertTrue(G.require_runtime_set(self.record,self.expected))
 def test_older_or_extra_input_rejected(self):
  wrong=copy.deepcopy(self.expected)
  for _ in range(2):wrong.pop(next(iter(wrong)))
  for expected in [wrong,{**self.expected,'rust/unreviewed.rs':'0'*64}]:
   with self.assertRaises(ValueError):G.require_runtime_set(self.record,expected)
 def test_extra_or_changed_runtime_record_rejected(self):
  for extra in [True,False]:
   record=copy.deepcopy(self.record)
   key='rust/unreviewed.rs'if extra else next(iter(self.expected))
   record['source_sha256'][key]='0'*64
   with self.assertRaises(ValueError):G.require_runtime_set(record,self.expected)
 def test_changed_manifest_or_map_fails_before_archive(self):
  with tempfile.TemporaryDirectory()as name:
   root=Path(name);manifest=root/'build.json';manifest.write_text('{}');expected=root/'expected.json';expected.write_text('{}')
   with self.assertRaisesRegex(ValueError,'ManifestRequired'):G.bound_candidate(manifest,'none',HERE/'tools/expected-runtime.json',root/'NO_ARCHIVE')
   with self.assertRaisesRegex(ValueError,'Union854Required'):G.bound_candidate(REPO/self.manifest,'none',expected,root/'NO_ARCHIVE')
if __name__=='__main__':unittest.main()
