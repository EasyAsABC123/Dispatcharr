import importlib.util,copy,gzip,hashlib,json,tempfile,unittest
from pathlib import Path
R=Path(__file__).resolve().parent;P=R;spec=importlib.util.spec_from_file_location('materialize',P/'materialize.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Controls(unittest.TestCase):
 def test_actual_all_payloads(self):
  for row in json.loads((P/'evidence-manifest.json').read_text()).values():self.assertEqual(len(m.payload(P,row)),row['projected_bytes'])
 def test_wrong_gzip_hash(self):
  row=copy.deepcopy(next(iter(json.loads((P/'evidence-manifest.json').read_text()).values())));row['gzip_sha256']='0'*64
  with self.assertRaises(ValueError):m.payload(P,row)
 def test_raw_hash_not_projected_fallback(self):
  row=copy.deepcopy(next(r for r in json.loads((P/'evidence-manifest.json').read_text()).values()if r['raw_sha256']!=r['projected_sha256']));row['projected_sha256']=row['raw_sha256']
  with self.assertRaises(ValueError):m.payload(P,row)
 def test_bad_size(self):
  row=copy.deepcopy(next(iter(json.loads((P/'evidence-manifest.json').read_text()).values())));row['projected_bytes']-=1
  with self.assertRaises(ValueError):m.payload(P,row)
 def test_reuse_output_refused(self):
  with tempfile.TemporaryDirectory()as d:
   with self.assertRaises(ValueError):m.fresh(Path(d))
 def test_symlink_refused(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'link';p.symlink_to(Path(d)/'missing')
   with self.assertRaises(ValueError):m.fresh(p)
 def test_parent_escape_refused(self):
  with self.assertRaises(ValueError):m.destination(Path('/tmp'),'../outside')
 def test_absolute_refused(self):
  with self.assertRaises(ValueError):m.destination(Path('/tmp'),'/outside')
if __name__=='__main__':unittest.main()
