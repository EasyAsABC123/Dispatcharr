"""Offline publication/reconstruction corruption controls; no build/fixture."""
import copy,hashlib,json,shutil,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import reconstruct as R
import verify as V
ROOT=Path(__file__).resolve().parents[1]
class Controls(unittest.TestCase):
 def test_actual_public_raw_gzip_and_scope_bindings(self):self.assertTrue(V.verify()['passed'])
 def test_tree_content_corruption_detected(self):
  with tempfile.TemporaryDirectory()as name:
   root=Path(name);(root/'a').write_bytes(b'wrong')
   with self.assertRaisesRegex(ValueError,'ExactHistorical798'):R.validate_tree(root,{'a':hashlib.sha256(b'expected').hexdigest()})
 def test_missing_or_extra_path_not_silent(self):
  with tempfile.TemporaryDirectory()as name:
   root=Path(name);(root/'extra').write_text('x')
   with self.assertRaisesRegex(ValueError,'ExactHistorical798'):R.validate_tree(root,{})
 def test_existing_output_refused_before_git(self):
  with tempfile.TemporaryDirectory()as name,patch.object(R.subprocess,'run',side_effect=AssertionError('GitMustNotRun')):
   with self.assertRaisesRegex(ValueError,'FreshOutputRequired'):R.reconstruct(Path('/unused'),Path(name))
 def test_corrupt_historical_overlay_refused_before_git(self):
  with tempfile.TemporaryDirectory()as name:
   root=Path(name);shutil.copy2(ROOT/'source-inputs.json',root/'source-inputs.json');shutil.copy2(ROOT/'runtime-selection.json',root/'runtime-selection.json');rows=json.loads((root/'runtime-selection.json').read_text())['files'];target=root/'historical'/rows[0]['path'];target.parent.mkdir(parents=True);target.write_text('corrupt')
   with patch.object(R,'HERE',root),patch.object(R.subprocess,'run',side_effect=AssertionError('GitMustNotRun')):
    with self.assertRaisesRegex(ValueError,'ChangedHistoricalOverlay'):R.reconstruct(Path('/unused'),root/'new')
 def test_corrupt_compressed_bytes_rejected_before_unpack(self):
  with tempfile.TemporaryDirectory()as name:
   root=Path(name);(root/'x.gz').write_bytes(b'not-gzip');row={'public_gzip':'x.gz','public_gzip_sha256':'0'*64}
   with self.assertRaisesRegex(ValueError,'CompressedEvidenceChanged'):V.raw(root,row)
 def test_mutated_default_or_host_scope_rejected(self):
  for corrupt in ('default','host'):
   with tempfile.TemporaryDirectory()as name:
    root=Path(name)
    for p in ROOT.iterdir():
     if p.name not in ('evidence-bindings.json','summary.json'):continue
     shutil.copy2(p,root/p.name)
    binding=json.loads((root/'evidence-bindings.json').read_text())
    if corrupt=='default':next(iter(binding.values()))['default_manifest_binding_only']['actual_bytes_equal']=False
    else:
     summary=json.loads((root/'summary.json').read_text());summary['paired']['actual_host_exit']=0;(root/'summary.json').write_text(json.dumps(summary))
    (root/'evidence-bindings.json').write_text(json.dumps(binding))
    # Keep retained compressed paths exact; patch only the root-local pointer.
    for name2 in ('evidence','historical'):(root/name2).symlink_to(ROOT/name2,target_is_directory=True)
    shutil.copy2(ROOT/'runtime-selection.json',root/'runtime-selection.json')
    with self.assertRaisesRegex(ValueError,'DefaultBytesNotEqual|DoNotRelabelPairedHost1'):V.verify(root)
if __name__=='__main__':unittest.main()
