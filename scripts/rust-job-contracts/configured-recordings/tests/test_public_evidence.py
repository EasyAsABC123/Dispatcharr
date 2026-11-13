import gzip,hashlib,importlib.util,json,tempfile,unittest,sys
from pathlib import Path
HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
def module(name,file):
 spec=importlib.util.spec_from_file_location(name,HERE/file);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
m=module('configured_materialize','materialize_evidence.py');r=module('configured_review','review.py')
class Materialization(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.package=self.root/'package';(self.package/'proofs').mkdir(parents=True);self.old=m.HERE;m.HERE=self.package;self.addCleanup(setattr,m,'HERE',self.old);self.raw=b'exact captured bytes\0\xff';data=gzip.compress(self.raw,mtime=0);(self.package/'proofs/raw.json.gz').write_bytes(data);self.manifest={'raw.json':{'projected_bytes':len(self.raw),'projected_sha256':hashlib.sha256(self.raw).hexdigest(),'compressed_bytes':len(data),'compressed_sha256':hashlib.sha256(data).hexdigest(),'compressed_path':'raw.json.gz'}};self.save()
 def save(self):
  (self.package/'proof-compression.json').write_text(json.dumps(self.manifest));(self.package/'projected-proof-map.json').write_text(json.dumps({'raw.json':hashlib.sha256(self.raw).hexdigest()}))
 def test_actual_binary_roundtrip(self):m.materialize(self.root/'out');self.assertEqual((self.root/'out/raw.json').read_bytes(),self.raw)
 def test_corrupt_compressed_input_refused(self):
  (self.package/'proofs/raw.json.gz').write_bytes(b'not the recorded bytes')
  with self.assertRaisesRegex(ValueError,'CompressedProofChanged'):m.materialize(self.root/'out')
 def test_raw_hash_mismatch_refused(self):
  self.manifest['raw.json']['projected_sha256']='0'*64;self.save()
  with self.assertRaisesRegex(ValueError,'ProjectedProofChanged'):m.materialize(self.root/'out')
 def test_expansion_bound_refused(self):
  self.manifest['raw.json']['projected_bytes']-=1;self.save()
  with self.assertRaisesRegex(ValueError,'ExpandedProjectedProofTooLarge'):m.materialize(self.root/'out')
 def test_fresh_output_required(self):
  out=self.root/'out';out.mkdir()
  with self.assertRaisesRegex(ValueError,'FreshOutputRequired'):m.materialize(out)
class ActualFlags(unittest.TestCase):
 def test_false_or_nonboolean_actual_flags_never_ignored(self):
  for domain,key,value in [('host','passed',False),('host','Native_verified',False),('guest','passed',False),('guest','Native_verified',False),('host','passed',1)]:
   with self.subTest(domain=domain,key=key,value=value),tempfile.TemporaryDirectory()as tmp:
    root=Path(tmp);folder=root/'normal2';folder.mkdir();host={'passed':True,'Native_verified':True,'actual_host_exit':0,'actual_controller_exit':0,'actual_controller_settled':True};guest={'passed':True,'Native_verified':True,'cases':[{}]*7,'checks':[{'passed':True}]*9};(host if domain=='host'else guest)[key]=value;(folder/'integration.json').write_text(json.dumps(host));(folder/'source-configured-recordings-private.json').write_text(json.dumps(guest))
    import materialize_evidence as bound
    package=root.parent/('binding-'+root.name);package.mkdir();(package/'projected-proof-map.json').write_text(json.dumps({str(q.relative_to(root)):hashlib.sha256(q.read_bytes()).hexdigest()for q in root.rglob('*')if q.is_file()}));old=bound.HERE;bound.HERE=package
    try:
     with self.assertRaisesRegex(ValueError,'ActualSuccessfulScopeRequired'):r.review(root)
    finally:bound.HERE=old;import shutil;shutil.rmtree(package)
if __name__=='__main__':unittest.main()
