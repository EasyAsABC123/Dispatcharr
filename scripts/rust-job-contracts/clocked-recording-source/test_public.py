"""Authored offline controls. Run only after Root resource quiet release."""
import gzip,hashlib,importlib.util,json,shutil,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;HERE=ROOT
def load(name):
 spec=importlib.util.spec_from_file_location('clocked_public_'+name,HERE/(name+'.py'));v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v);return v
M=load('materialize')
class Controls(unittest.TestCase):
 def row(self,data):return {'gzip_sha256':hashlib.sha256(gzip.compress(data,mtime=0)).hexdigest(),'projected_sha256':hashlib.sha256(data).hexdigest(),'projected_bytes':len(data),'raw_sha256':hashlib.sha256(b'PRIVATE_RAW').hexdigest(),'raw_bytes':11}
 def test_projection_exact_only(self):
  data=b'{"unchanged":null,"nonce":"'+b'x'*32+b'"}';row=self.row(data);self.assertEqual(M.decode(gzip.compress(data,mtime=0),row),data)
 def test_raw_hash_is_not_fallback(self):
  projected=b'{"nonce":"'+b'x'*32+b'"}';raw=b'{"nonce":"'+b'7'*32+b'"}';row=self.row(projected);row['raw_sha256']=hashlib.sha256(raw).hexdigest();blob=gzip.compress(raw,mtime=0);row['gzip_sha256']=hashlib.sha256(blob).hexdigest()
  with self.assertRaises(ValueError):M.decode(blob,row)
 def test_corrupt_gzip_hash(self):
  data=b'owned';row=self.row(data)
  with self.assertRaises(ValueError):M.decode(gzip.compress(b'changed',mtime=0),row)
 def test_corrupt_projected_bytes(self):
  data=b'owned';row=self.row(data);row['projected_bytes']+=1
  with self.assertRaises(ValueError):M.decode(gzip.compress(data,mtime=0),row)
 def test_corrupt_projected_hash(self):
  data=b'owned';row=self.row(data);row['projected_sha256']='0'*64
  with self.assertRaises(ValueError):M.decode(gzip.compress(data,mtime=0),row)
 def test_file_cap_before_acceptance(self):
  data=b'x'*100;row=self.row(data);old=M.MAX_FILE;M.MAX_FILE=99
  try:
   with self.assertRaises(ValueError):M.decode(gzip.compress(data,mtime=0),row)
  finally:M.MAX_FILE=old
 def test_relative_path_no_traversal(self):
  for value in ('/tmp/unsafe','../unsafe'):
   with self.assertRaises(ValueError):M.relative(value)
 def test_existing_output_never_overwritten(self):
  with tempfile.TemporaryDirectory(dir=ROOT)as temp:
   out=Path(temp)/'evidence';out.mkdir();marker=out/'marker';marker.write_bytes(b'original')
   with self.assertRaises(ValueError):M.materialize(out)
   self.assertEqual(marker.read_bytes(),b'original')
 def test_false_scope_flags_preserved_literal(self):
  scope=json.loads((HERE/'scope.json').read_text());self.assertIs(scope['Native_verified'],False);self.assertIs(scope['performance_verified'],False);self.assertIs(scope['Main_verified'],False);self.assertIs(scope['scanner_sibling_execution_verified'],False)
 def test_exact_order_and_nonsecret_bytes(self):
  nonce=b'7'*32;raw=b'{"b":0,"credential":"'+nonce+b'","a":null}\n';projection=raw.replace(nonce,b'x'*32);self.assertEqual(len(raw),len(projection));self.assertEqual(projection.replace(b'x'*32,nonce),raw)
if __name__=='__main__':unittest.main()
