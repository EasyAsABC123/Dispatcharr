"""No Docker may start when support/provenance inputs are corrupt."""
import json,os,subprocess,sys,tempfile,unittest
from pathlib import Path
HERE=Path(__file__).parent
class Controls(unittest.TestCase):
 def test_changed_support_rejected_before_fixture_or_output(self):
  with tempfile.TemporaryDirectory(prefix='rust-owned-recovery-runner-')as tmp:
   root=Path(tmp);support=root/'support';support.mkdir();expected=json.loads((HERE/'support-inputs.json').read_text());first=next(iter(expected));path=support/first;path.parent.mkdir(parents=True);path.write_text('owned corruption')
   out=root/'out';result=subprocess.run([sys.executable,str(HERE/'run_pair.py'),'--repo',str(root),'--output',str(out),'--support-root',str(support),'--native-binary',str(root/'absent-peer'),'--reference',str(root/'absent-ref')],capture_output=True,text=True)
   self.assertNotEqual(result.returncode,0);self.assertIn('Changed public support',result.stderr);self.assertFalse(out.exists())
 def test_missing_explicit_peer_never_uses_private_fallback(self):
  # argparse itself requires an explicit peer; no environment/private default.
  result=subprocess.run([sys.executable,str(HERE/'run_pair.py'),'--repo','.','--output','/unused','--support-root','/unused','--reference','/unused'],capture_output=True,text=True)
  self.assertEqual(result.returncode,2);self.assertIn('--native-binary',result.stderr)
if __name__=='__main__':unittest.main()
