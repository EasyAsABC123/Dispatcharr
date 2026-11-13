import copy,gzip,hashlib,json,shutil,tempfile,unittest
from pathlib import Path
import materialize,review
class Public(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.base=Path(self.tmp.name)
 def tearDown(self):self.tmp.cleanup()
 def tree(self):
  p=self.base/'packet';shutil.copytree(materialize.ROOT,p);return p
 def test_actual_contract_and_literal_restore(self):
  out=self.base/'evidence';r=materialize.restore(out);self.assertTrue(r['all_original_bytes_restored']);self.assertEqual(review.review(out)['tasks'],6)
 def test_existing_output_is_never_overwritten(self):
  out=self.base/'evidence';out.mkdir();(out/'sentinel').write_text('owned')
  with self.assertRaises(FileExistsError):materialize.restore(out)
  self.assertEqual((out/'sentinel').read_text(),'owned')
 def test_corrupt_compressed_bytes_rejected_before_creation(self):
  p=self.tree();n=next((p/'evidence').glob('*.gz'));n.write_bytes(n.read_bytes()+b'corrupt')
  with self.assertRaises(ValueError):materialize.restore(self.base/'out',p)
  self.assertFalse((self.base/'out').exists())
 def test_mutated_raw_even_with_new_gzip_hash_is_rejected(self):
  p=self.tree();mapping=json.loads((p/'evidence-map.json').read_text());name=next(iter(mapping));f=p/'evidence'/(name+'.gz');f.write_bytes(gzip.compress(b'{}',mtime=0));mapping[name]['gzip_sha256']=hashlib.sha256(f.read_bytes()).hexdigest();(p/'evidence-map.json').write_text(json.dumps(mapping))
  with self.assertRaises(ValueError):materialize.restore(self.base/'out',p)
 def test_path_traversal_is_rejected(self):
  p=self.tree();mapping=json.loads((p/'evidence-map.json').read_text());mapping['../escape']=mapping.pop(next(iter(mapping)));(p/'evidence-map.json').write_text(json.dumps(mapping))
  with self.assertRaises(ValueError):materialize.restore(self.base/'out',p)
 def test_source_missing_explicit_metadata_parent_cannot_pass(self):
  out=self.base/'evidence';materialize.restore(out);f=out/'source-parented-recordings-private.json';x=json.loads(f.read_text());x['cases'][1]['tasks'][0]['result']['metadata'].pop('parent_id');f.write_text(json.dumps(x))
  with self.assertRaises(ValueError):review.review(out)
 def test_source_artwork_root_cannot_be_series_parent(self):
  out=self.base/'evidence';materialize.restore(out);f=out/'source-parented-recordings-private.json';x=json.loads(f.read_text());t=x['cases'][2]['tasks'][0];t['prerun'][0]['root_id']=t['prerun'][0]['parent_id'];f.write_text(json.dumps(x))
  with self.assertRaises(ValueError):review.review(out)
 def test_changed_default_manifest_cannot_pass(self):
  out=self.base/'evidence';materialize.restore(out);(out/'default-after-private.json').write_text('{}')
  with self.assertRaises(ValueError):review.review(out)
if __name__=='__main__':unittest.main()
