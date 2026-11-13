import copy,hashlib,importlib.util,json,tempfile,unittest
from pathlib import Path
HERE=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('configured_public_reconstruct',HERE/'reconstruct.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class ExactTree(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);(self.root/'a').write_bytes(b'actual captured bytes');self.expected={'a':hashlib.sha256(b'actual captured bytes').hexdigest()}
 def test_exact_bytes(self):m.validate_tree(self.root,self.expected)
 def test_missing_field_file_refused(self):
  (self.root/'a').unlink()
  with self.assertRaisesRegex(ValueError,'ExactHistoricalSourceRequired'):m.validate_tree(self.root,self.expected)
 def test_added_file_refused(self):
  (self.root/'unselected').write_text('must not be silently omitted')
  with self.assertRaisesRegex(ValueError,'ExactHistoricalSourceRequired'):m.validate_tree(self.root,self.expected)
 def test_corrupt_bytes_refused(self):
  (self.root/'a').write_bytes(b'other')
  with self.assertRaisesRegex(ValueError,'ExactHistoricalSourceRequired'):m.validate_tree(self.root,self.expected)
 def test_fresh_output_required_before_git(self):
  with self.assertRaisesRegex(ValueError,'FreshOutputRequired'):m.reconstruct(Path('/does-not-exist'),self.root,'source10')
 def test_unknown_variant_not_defaulted(self):
  with self.assertRaises(KeyError):m.reconstruct(Path('/does-not-exist'),self.root/'out','unknown')
 def test_corrupt_overlay_refused_before_git_and_output(self):
  package=self.root/'package';package.mkdir();manifest=json.loads((HERE/'historical-sources.json').read_text());entry=copy.deepcopy(manifest['variants']['source10']);row=entry['overlays'][0];(package/'historical-blobs').mkdir();(package/'historical-blobs'/row['sha256']).write_bytes(b'corrupt');manifest['variants']['source10']['overlays']=[row];(package/'historical-sources.json').write_text(json.dumps(manifest));(package/entry['source_map']).write_bytes((HERE/entry['source_map']).read_bytes());old=m.HERE;m.HERE=package
  try:
   with self.assertRaisesRegex(ValueError,'ChangedHistoricalOverlay'):m.reconstruct(Path('/does-not-exist'),self.root/'out','source10')
   self.assertFalse((self.root/'out').exists())
  finally:m.HERE=old
 def test_modified_map_requires_bound_digest(self):
  package=self.root/'map-package';package.mkdir();manifest=json.loads((HERE/'historical-sources.json').read_text());name=manifest['variants']['qa9']['source_map'];(package/'historical-sources.json').write_text(json.dumps(manifest));(package/name).write_text('{}');old=m.HERE;m.HERE=package
  try:
   with self.assertRaisesRegex(ValueError,'BoundSourceMap'):m.reconstruct(Path('/does-not-exist'),self.root/'out','qa9')
  finally:m.HERE=old
if __name__=='__main__':unittest.main()
