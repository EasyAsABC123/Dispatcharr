"""Portable corruption/freshness controls, no Docker/apps/compilers/network."""
import copy,gzip,hashlib,json,shutil,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import materialize,reconstruct,build_peer
ROOT=Path(__file__).resolve().parent
class Portable(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.base=Path(self.tmp.name)
 def tearDown(self):self.tmp.cleanup()
 def packet(self):
  p=self.base/'packet';shutil.copytree(ROOT,p,ignore=shutil.ignore_patterns('__pycache__'));return p
 def test_every_retained_file_restores_literally(self):
  out=self.base/'evidence';r=materialize.restore(out);self.assertTrue(r['all_original_bytes_restored']);m=json.loads((ROOT/'evidence-map.json').read_text());self.assertEqual(set(m),{p.name for p in out.iterdir()})
 def test_existing_materialization_never_overwrites(self):
  out=self.base/'evidence';out.mkdir();(out/'sentinel').write_text('owned')
  with self.assertRaises(FileExistsError):materialize.restore(out)
  self.assertEqual((out/'sentinel').read_text(),'owned')
 def test_compressed_corruption_rejected(self):
  p=self.packet();f=next((p/'evidence').glob('*.gz'));f.write_bytes(f.read_bytes()+b'corrupt')
  with self.assertRaises(ValueError):materialize.restore(self.base/'out',p)
  self.assertFalse((self.base/'out').exists())
 def test_gzip_reencoding_cannot_hide_changed_raw_evidence(self):
  p=self.packet();m=json.loads((p/'evidence-map.json').read_text());n=next(iter(m));f=p/'evidence'/(n+'.gz');f.write_bytes(gzip.compress(b'{}'));m[n]['gzip_sha256']=hashlib.sha256(f.read_bytes()).hexdigest();(p/'evidence-map.json').write_text(json.dumps(m))
  with self.assertRaises(ValueError):materialize.restore(self.base/'out',p)
 def test_traversal_evidence_name_rejected(self):
  p=self.packet();m=json.loads((p/'evidence-map.json').read_text());m['../escape']=m.pop(next(iter(m)));(p/'evidence-map.json').write_text(json.dumps(m))
  with self.assertRaises(ValueError):materialize.restore(self.base/'out',p)
 def test_existing_source_output_never_touches_git(self):
  out=self.base/'source';out.mkdir()
  with patch.object(reconstruct.subprocess,'run',side_effect=AssertionError('Git must not run')):
   with self.assertRaises(FileExistsError):reconstruct.reconstruct(self.base/'repo',out)
 def test_wrong_source_map_rejected_before_git(self):
  p=self.packet();(p/'source-inputs.json').write_text('{}')
  with patch.object(reconstruct.subprocess,'run',side_effect=AssertionError('Git must not run')):
   with self.assertRaises(ValueError):reconstruct.reconstruct(self.base/'repo',self.base/'source',p)
 def test_corrupt_overlay_rejected_before_git(self):
  p=self.packet();m=json.loads((p/'source-overlays.json').read_text());f=p/next(iter(m['overlays'].values()))['historical_file'];f.write_bytes(f.read_bytes()+b'altered')
  with patch.object(reconstruct.subprocess,'run',side_effect=AssertionError('Git must not run')):
   with self.assertRaises(ValueError):reconstruct.reconstruct(self.base/'repo',self.base/'source',p)
 def test_overlay_git_blob_provenance_cannot_change(self):
  p=self.packet();m=json.loads((p/'source-overlays.json').read_text());next(iter(m['overlays'].values()))['git_blob']='0'*40;(p/'source-overlays.json').write_text(json.dumps(m))
  with patch.object(reconstruct.subprocess,'run',side_effect=AssertionError('Git must not run')):
   with self.assertRaises(ValueError):reconstruct.reconstruct(self.base/'repo',self.base/'source',p)
 def test_build_rejects_wrong_source_before_compiler(self):
  source=self.base/'source';source.mkdir()
  with patch.object(build_peer.subprocess,'run',side_effect=AssertionError('Compiler must not run')):
   with self.assertRaises(AssertionError):build_peer.build(source,self.base/'target',self.base/'build')
if __name__=='__main__':unittest.main()
