"""Pure source/target corruption controls; no Cargo should start in these cases."""
import argparse,os,shutil,subprocess,sys,tempfile,unittest
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);a=p.parse_args();SOURCE=a.source.resolve();TOOL=Path(__file__).with_name('build_peer.py')
class Controls(unittest.TestCase):
 def setUp(self):
  self.temporary=tempfile.TemporaryDirectory(prefix='rust-owned-scanner-builder-');self.root=Path(self.temporary.name);self.source=self.root/'source';shutil.copytree(SOURCE/'rust',self.source/'rust');self.target=self.root/'target';self.output=self.root/'out';self.bin=self.root/'bin';self.bin.mkdir();self.marker=self.root/'cargo-was-called';cargo=self.bin/'cargo';cargo.write_text('#!/bin/sh\ntouch "'+str(self.marker)+'"\nexit 99\n');cargo.chmod(0o755)
 def tearDown(self):self.temporary.cleanup()
 def reject(self):
  result=subprocess.run([sys.executable,str(TOOL),'--source',str(self.source),'--target',str(self.target),'--output',str(self.output)],env=dict(os.environ,PATH=str(self.bin)+':'+os.environ['PATH']),capture_output=True,text=True);self.assertNotEqual(result.returncode,0);self.assertFalse(self.marker.exists());self.assertFalse(self.target.exists());return result
 def test_changed_runtime_rejected_before_compiler(self):
  path=self.source/'rust/crates/job-transport/src/logo_scanner.rs';path.write_text(path.read_text()+'\n// owned corruption\n');self.assertIn('qualified788',self.reject().stderr);self.assertFalse(self.output.exists())
 def test_extra_unmapped_file_rejected(self):
  (self.source/'rust/owned-unmapped.rs').write_text('// unexpected');self.assertIn('qualified788',self.reject().stderr);self.assertFalse(self.output.exists())
 def test_target_inside_frozen_source_rejected(self):
  self.target=self.source/'rust/owned-target';self.assertIn('outside frozen source',self.reject().stderr);self.assertFalse(self.output.exists())
 def test_output_inside_frozen_source_rejected(self):
  self.output=self.source/'rust/owned-output';self.assertIn('outside frozen source',self.reject().stderr);self.assertFalse(self.output.exists())
 def test_existing_output_is_preserved(self):
  self.output.mkdir();sentinel=self.output/'owned-sentinel';sentinel.write_text('preserve');self.reject();self.assertEqual(sentinel.read_text(),'preserve')
if __name__=='__main__':unittest.main(argv=[sys.argv[0]])
