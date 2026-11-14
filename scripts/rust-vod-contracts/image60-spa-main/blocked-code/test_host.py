"""Finite actual subprocess and synthetic guard tests; no Docker or app."""
import copy,json,os,sys,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
import once_guard as G
import run as R
from bounded_process import Collected,CollectionFailure
class Cue(unittest.TestCase):
 def setUp(self):self.binding={'manifest_sha256':'a'*64,'image_id':'image','archive_sha256':'b'*64,'qualified_full_input_map_sha256':'c'*64};self.v={'authorized':True,'action':G.ACTION,'Native_business_requests':1,'Source_requests':0,'resource_sampling':False,'full_M4_acceptance':False,'default_worker_activation':False,'Source_replay':False,'seal_sha256':'d'*64,'execution_run_sha256':'e'*64,'new_output':str(G.OUTPUT),'candidate_manifest_sha256':'a'*64,'candidate_image':'image','source_archive_sha256':'b'*64,'qualified_map_sha256':'c'*64,'boot_id':'boot','nonce':'a'*32,'issued_epoch':10.,'expires_epoch':20.}
 def bad(self,f):
  v=copy.deepcopy(self.v);f(v)
  with self.assertRaises(ValueError):G.validate(v,'d'*64,'e'*64,'boot',15.,self.binding)
 def test_positive_synthetic(self):G.validate(self.v,'d'*64,'e'*64,'boot',15.,self.binding)
 def test_no_authority(self):self.bad(lambda v:v.update(authorized=False))
 def test_empty_nonce(self):self.bad(lambda v:v.update(nonce=''))
 def test_wrong_boot(self):self.bad(lambda v:v.update(boot_id='other'))
 def test_expired(self):self.bad(lambda v:v.update(expires_epoch=14))
 def test_nan(self):self.bad(lambda v:v.update(issued_epoch=float('nan')))
 def test_scope(self):self.bad(lambda v:v.update(Native_business_requests=2))
 def test_Source_replay(self):self.bad(lambda v:v.update(Source_replay=True))
 def test_wrong_image(self):self.bad(lambda v:v.update(candidate_image='other'))
 def test_wrong_seal(self):self.bad(lambda v:v.update(seal_sha256='0'*64))
 def test_wrong_run(self):self.bad(lambda v:v.update(execution_run_sha256='0'*64))
 def test_exclusive_claim_before_output(self):
  with tempfile.TemporaryDirectory()as folder:
   root=Path(folder);output=root/'fresh';cue=root/'cue';seal=root/'seal';run=root/'run';seal.write_text('seal');run.write_text('run');v=copy.deepcopy(self.v);now=time.time();v.update(new_output=str(output),seal_sha256=G.sha(seal),execution_run_sha256=G.sha(run),boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),issued_epoch=now-1,expires_epoch=now+30);cue.write_text(json.dumps(v))
   with patch.object(G,'CUE',cue),patch.object(G,'OUTPUT',output):
    G.admit(cue,seal,run,output,self.binding);self.assertFalse(output.exists())
    with self.assertRaises(FileExistsError):G.admit(cue,seal,run,output,self.binding)
class Host(unittest.TestCase):
 def test_guest_closure_exact_readable_modes(self):
  with tempfile.TemporaryDirectory()as folder:
   guest=R.export_guest(Path(folder));self.assertEqual(guest.stat().st_mode&0o777,0o555);self.assertEqual(len(list(guest.iterdir())),len(R.GUEST)+1)
   for name in R.GUEST:self.assertEqual((guest/name).read_bytes(),(R.HERE/name).read_bytes());self.assertEqual((guest/name).stat().st_mode&0o777,0o444)
 def test_file_backed_exact_stdout_after_wait(self):
  with tempfile.TemporaryDirectory()as folder:
   root=Path(folder);job=Collected([sys.executable,'-c',"print('exact retained result')"],dict(os.environ),1024,lambda:None,stdout_path=root/'stdout',stderr_path=root/'stderr');raw,kind=R.settled_output(job,3,(0,),root/'stdout');self.assertEqual(raw,b'exact retained result\n');self.assertIsNone(kind);self.assertTrue(job.physically_settled)
 def test_real_readiness_exit1_stays_typed_allowed(self):
  with tempfile.TemporaryDirectory()as folder:
   root=Path(folder);job=Collected([sys.executable,'-c',"import sys;print('not ready');sys.exit(1)"],dict(os.environ),1024,lambda:None,stdout_path=root/'stdout',stderr_path=root/'stderr');raw,kind=R.settled_output(job,3,(0,1),root/'stdout');self.assertEqual(raw,b'not ready\n');self.assertIn('CommandExit1',kind);self.assertTrue(job.physically_settled)
 def test_business_exit1_not_swallowed(self):
  with tempfile.TemporaryDirectory()as folder:
   root=Path(folder);job=Collected([sys.executable,'-c','raise SystemExit(1)'],dict(os.environ),1024,lambda:None,stdout_path=root/'stdout',stderr_path=root/'stderr')
   with self.assertRaises(CollectionFailure):R.settled_output(job,3,(0,),root/'stdout')
   self.assertTrue(job.physically_settled)
 def test_actual_overflow_invalid_stops_and_drains(self):
  called=[]
  with tempfile.TemporaryDirectory()as folder:
   root=Path(folder);job=Collected([sys.executable,'-c',"import sys;sys.stdout.buffer.write(b'x'*1048576)"],dict(os.environ),1024,lambda:called.append(True),stdout_path=root/'stdout',stderr_path=root/'stderr')
   with self.assertRaises(CollectionFailure):R.settled_output(job,3,(0,1),root/'stdout')
   self.assertTrue(called);self.assertLessEqual((root/'stdout').stat().st_size,1024);self.assertIsNotNone(job.process.poll());self.assertTrue(all(not t.is_alive()for t in job.threads))
 def test_ambient_fixture_environment_removed(self):
  meta=json.loads((R.HERE/'tooling-inputs.json').read_text())
  with patch.dict(os.environ,{'COMPOSE_FILE':'wrong','POSTGRES_DB':'wrong','REDIS_URL':'wrong','GIT_DIR':'wrong','RUST_BIND':'wrong','SPA_FAKE':'true'}):
   env=R.controlled_environment(meta)
   for key in ['COMPOSE_FILE','POSTGRES_DB','REDIS_URL','GIT_DIR','RUST_BIND','SPA_FAKE']:self.assertNotIn(key,env)
   for k,v in meta['selected_environment'].items():self.assertEqual(env[k],v)
if __name__=='__main__':unittest.main()
