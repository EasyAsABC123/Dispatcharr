import json,shutil,tempfile,unittest
from pathlib import Path
import verify
class Public(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)/'public';shutil.copytree(Path(__file__).parent,self.root,ignore=shutil.ignore_patterns('__pycache__'))
 def bad(self,name,fn):
  p=self.root/name;v=json.loads(p.read_text());fn(v);p.write_text(json.dumps(v))
  with self.assertRaises(AssertionError):verify.verify(self.root)
 def test_public_only(self):self.assertEqual(verify.verify(self.root)['Source2'],'NOT_RUN')
 def test_Source1_no_PSS(self):self.bad('verified-setup-negative-1.json',lambda v:v.update(supplemental_points=1))
 def test_unknown_Source1_stdout_not_fabricated(self):self.bad('verified-setup-negative-1.json',lambda v:v['endpoint_diagnosis'].update(actual_stdout=':0\n'))
 def test_realdaemon_not_wrong_UID(self):self.bad('verified-redis-info-1.json',lambda v:v['actual_daemon_credentials'].update(uids=[0]*4))
 def test_reset_must_reject(self):self.bad('verified-redis-info-1.json',lambda v:v['reset_witness'].update(adapter_rejected_exactly='none'))
 def test_Source2_fields_not_predeclared_complete(self):self.bad('blocked-recipe5.json',lambda v:v.update(Source2_actual_observations={'complete':True}))
 def test_runtime_must_stay_blocked(self):
  p=self.root/'controller/run.py';p.write_bytes(p.read_bytes().replace(b'SUPPLEMENTAL_EXECUTION_BLOCKED=True',b'SUPPLEMENTAL_EXECUTION_BLOCKED=False'))
  with self.assertRaises(AssertionError):verify.verify(self.root)
 def test_deep_absent_no_fallback(self):
  with self.assertRaisesRegex(ValueError,'DeepRaw'):verify.verify(self.root,Path(self.tmp.name)/'missing')
if __name__=='__main__':unittest.main()
