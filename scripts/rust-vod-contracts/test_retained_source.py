"""Offline authority/candidate/deadline controls. No Docker/apps/media/Cargo."""
import hashlib,importlib.util,json,socket,tempfile,time,types,unittest
from unittest.mock import patch
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
HELPER=Path(__file__).with_name('retained_source.py')
spec=importlib.util.spec_from_file_location('retained_vod_authority',HELPER);helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
def sha(data):return hashlib.sha256(data).hexdigest()
class AuthorityTests(unittest.TestCase):
 def fixture(self,folder,mutate=None):
  observation=(ROOT/'scripts/rust-vod-contracts/python-goldens.json').read_bytes();qualification=(ROOT/'scripts/rust-vod-contracts/verified-compose.json').read_bytes()
  if mutate:
   value=json.loads(observation);mutate(value);observation=json.dumps(value).encode()
  (folder/'observation.json').write_bytes(observation);(folder/'qualification.json').write_bytes(qualification)
  binding={'schema':1,'Source_replay_permitted':False,'observation':{'path':'observation.json','sha256':sha(observation)},'qualification':{'path':'qualification.json','sha256':sha(qualification)}}
  path=folder/'binding.json';path.write_text(json.dumps(binding));return path
 def test_real_retained_identity_and_fixture_match(self):
  with tempfile.TemporaryDirectory()as tmp:
   authority=helper.load_authority(self.fixture(Path(tmp)));helper.validate_fixture(authority,authority['observation']['fixture']);self.assertFalse(authority['proof']['Source_replayed'])
 def test_wrong_source_and_reordered_cases_refused(self):
  for mutate in (lambda d:d.update(source_commit='other'),lambda d:d['wire']['events'].reverse()):
   with tempfile.TemporaryDirectory()as tmp:
    with self.assertRaises(ValueError):helper.load_authority(self.fixture(Path(tmp),mutate))
 def test_fixture_drift_refused_before_candidate_requests(self):
  with tempfile.TemporaryDirectory()as tmp:
   authority=helper.load_authority(self.fixture(Path(tmp)));fixture=dict(authority['observation']['fixture']);fixture['fixture_bytes']+=1
   with self.assertRaises(ValueError):helper.validate_fixture(authority,fixture)
 def test_hashed_authority_mutation_refused(self):
  with tempfile.TemporaryDirectory()as tmp:
   folder=Path(tmp);binding=self.fixture(folder);(folder/'observation.json').write_text('{}')
   with self.assertRaises(ValueError):helper.load_authority(binding)
 def test_expired_absolute_budget_does_not_open_transport(self):
  class NoRedirect:pass
  module=types.SimpleNamespace(NoRedirect=NoRedirect)
  helper.install_absolute_probe(module,0)
  with self.assertRaises(TimeoutError):module.fetch('http://unused.invalid')
 def test_buffered_response_after_socket_close_preserves_body_and_deadline(self):
  transport=socket.socket();transport.close()
  class Response:
   status=200;headers={'Content-Length':'3'};fp=transport
   def __init__(self):self.chunks=iter((b'abc',b''))
   def __enter__(self):return self
   def __exit__(self,*args):pass
   def read1(self,size):return next(self.chunks)
  module=types.SimpleNamespace(NoRedirect=object)
  helper.install_absolute_probe(module,time.monotonic()+5)
  opener=types.SimpleNamespace(open=lambda *args,**kw:Response())
  with patch.object(helper.urllib.request,'build_opener',return_value=opener):
   self.assertEqual(module.fetch('http://unused.invalid'),(200,{'Content-Length':'3'},b'abc'))
  helper.install_absolute_probe(module,0)
  with self.assertRaises(TimeoutError):module.fetch('http://unused.invalid')
 def test_candidate_wrong_physical_image_and_incomplete_map_refused(self):
  with tempfile.TemporaryDirectory()as tmp:
   folder=Path(tmp);dig='a'*64
   build={'passed':True,'exit_code':0,'image_id':'sha256:'+dig,'git_revision':'b'*40,'source_tree_sha256':'c'*64,'source_sha256':{'rust/a.rs':dig,'ATTRIBUTION.md':dig}}
   proof={**{k:build[k]for k in('image_id','git_revision','source_tree_sha256')},'passed':True,'actual_build_wait_exit':0,'all_archive_files_byteexact_Git':True}
   def write(name,value):p=folder/name;p.write_text(json.dumps(value));return p
   b=write('build.json',build);p=write('proof.json',proof);i=write('inputs.json',build['source_sha256']);self.assertEqual(helper.validate_candidate_bundle(b,p,i)['Rust_inputs'],1)
   proof['image_id']='sha256:'+'d'*64;write('proof.json',proof)
   with self.assertRaises(ValueError):helper.validate_candidate_bundle(b,p,i)
   proof['image_id']=build['image_id'];write('proof.json',proof);write('inputs.json',{'ATTRIBUTION.md':dig})
   with self.assertRaises(ValueError):helper.validate_candidate_bundle(b,p,i)
 def test_five_real_nonarchive_coupled_authorities_verified_without_runtime_adoption(self):
  with tempfile.TemporaryDirectory()as tmp:
   folder=Path(tmp);dig='a'*64
   build={'passed':True,'exit_code':0,'image_id':'sha256:'+dig,'git_revision':'b'*40,'source_tree_sha256':'c'*64,'source_sha256':{'rust/a.rs':dig,'ATTRIBUTION.md':dig}}
   proof={**{k:build[k]for k in('image_id','git_revision','source_tree_sha256')},'passed':True,'actual_build_wait_exit':0,'all_archive_files_byteexact_Git':True}
   inputs=dict(build['source_sha256']);self.assertEqual(len(helper.REPO_COUPLED_AUTHORITIES),5)
   for key in helper.REPO_COUPLED_AUTHORITIES:
    raw=(ROOT/key).read_bytes();target=folder/key;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw);inputs[key]=sha(raw)
   def write(name,value):p=folder/name;p.write_text(json.dumps(value));return p
   b=write('build.json',build);p=write('proof.json',proof);i=write('inputs.json',inputs)
   actual=helper.validate_candidate_bundle(b,p,i,repo_root=folder)
   self.assertEqual(actual['Rust_inputs'],1);self.assertFalse(actual['working_tree_runtime_adopted'])
   self.assertEqual(set(actual['repo_only_coupled_authorities']),helper.REPO_COUPLED_AUTHORITIES)
   self.assertEqual(actual['archived_non_Rust_coupled_inputs'],{'ATTRIBUTION.md':dig})
   for key,receipt in actual['repo_only_coupled_authorities'].items():
    self.assertEqual(receipt['sha256'],inputs[key]);self.assertNotIn(key,build['source_sha256']);self.assertIn('NOT an archived runtime/image input',receipt['scope'])
   key=sorted(helper.REPO_COUPLED_AUTHORITIES)[0];target=folder/key;raw=target.read_bytes();target.write_bytes(raw+b' ')
   with self.assertRaises(ValueError):helper.validate_candidate_bundle(b,p,i,repo_root=folder)
   target.unlink()
   with self.assertRaises(ValueError):helper.validate_candidate_bundle(b,p,i,repo_root=folder)
   outside=folder/'outside.json';outside.write_bytes(raw);target.symlink_to(outside)
   with self.assertRaises(ValueError):helper.validate_candidate_bundle(b,p,i,repo_root=folder)
   target.unlink();target.write_bytes(raw)
   bad=dict(inputs);bad['scripts/unreviewed.json']=dig;write('inputs.json',bad)
   with self.assertRaises(ValueError):helper.validate_candidate_bundle(b,p,i,repo_root=folder)
   bad=dict(inputs);bad['ATTRIBUTION.md']='d'*64;write('inputs.json',bad)
   with self.assertRaises(ValueError):helper.validate_candidate_bundle(b,p,i,repo_root=folder)
   bad=dict(inputs);bad['rust/a.rs']='d'*64;write('inputs.json',bad)
   with self.assertRaises(ValueError):helper.validate_candidate_bundle(b,p,i,repo_root=folder)
if __name__=='__main__':unittest.main()
