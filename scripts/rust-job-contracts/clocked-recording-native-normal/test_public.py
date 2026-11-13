"""Portable copy corruption controls; no sockets/apps/builds/media decoders."""
import copy,gzip,hashlib,importlib.util,json,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;PUBLIC=ROOT;spec=importlib.util.spec_from_file_location('materializer',PUBLIC/'materialize.py');M=importlib.util.module_from_spec(spec);spec.loader.exec_module(M);sha=lambda x:hashlib.sha256(x).hexdigest()
class Controls(unittest.TestCase):
 def row(self,data):return {'gzip_sha256':sha(gzip.compress(data,mtime=0)),'projected_sha256':sha(data),'projected_bytes':len(data),'raw_sha256':sha(b'private-raw')}
 def test_only_projection_exact_bytes_order_roundtrip(self):
  data=b'{ "null": null, "field_order": [1,true,1.0], "nonce":"'+b'x'*32+b'"}\n';self.assertEqual(M.decode(gzip.compress(data,mtime=0),self.row(data)),data)
 def test_raw_fallback_forbidden(self):
  data=b'private-raw';row=self.row(b'projected-only');row['gzip_sha256']=sha(gzip.compress(data,mtime=0))
  with self.assertRaises(ValueError):M.decode(gzip.compress(data,mtime=0),row)
 def test_corrupt_payload(self):
  data=b'projected';blob=bytearray(gzip.compress(data,mtime=0));blob[-1]^=1
  with self.assertRaises(ValueError):M.decode(bytes(blob),self.row(data))
 def test_wrong_projected_size(self):
  data=b'projected';row=self.row(data);row['projected_bytes']+=1
  with self.assertRaises(ValueError):M.decode(gzip.compress(data,mtime=0),row)
 def test_wrong_projected_hash(self):
  data=b'projected';row=self.row(data);row['projected_sha256']='0'*64
  with self.assertRaises(ValueError):M.decode(gzip.compress(data,mtime=0),row)
 def test_existing_output_refused(self):
  with tempfile.TemporaryDirectory()as tmp:
   with self.assertRaises(ValueError):M.restore(Path(tmp),PUBLIC)
 def test_traversal_refused_before_output(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp);(root/'evidence-manifest.json').write_text(json.dumps({'../escape':{'payload':'payload'}}));out=root/'fresh'
   with self.assertRaises(ValueError):M.restore(out,root)
   self.assertFalse(out.exists())
 def test_missing_bound_payload_no_partial_success(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp);(root/'evidence-manifest.json').write_text(json.dumps({'owned/file':{'payload':'missing'}}));out=root/'fresh'
   with self.assertRaises(FileNotFoundError):M.restore(out,root)
   self.assertFalse(out.exists())
if __name__=='__main__':unittest.main()
