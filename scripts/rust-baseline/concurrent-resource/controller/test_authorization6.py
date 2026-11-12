"""Pure cue negatives; no Source, Docker or timing sampler."""
import copy,json,unittest
from pathlib import Path
from authorization import bindings,validate
POLICY=json.loads((Path(__file__).parent/'policy.json').read_text())
class Authority(unittest.TestCase):
 def records(self):
  common=dict(scope='m0-concurrent-import-playback-source-resource',policy_sha256='policy',controller_sha256={'run.py':'source'},immutable_bindings=bindings(POLICY))
  return dict(common,approved=True),dict(common,heavy_idle_verified=True,boot_id='boot',issued_unix=990,expires_unix=1100,authorized_repetitions=[1,2,3])
 def check(self,r,c):return validate(r,c,POLICY,'policy',{'run.py':'source'},1,'boot',1000)
 def test_exact_positive(self):self.assertTrue(self.check(*self.records())['validated'])
 def test_future_expired_stale_and_excessive_lifetime(self):
  for times in [(1001,1100),(800,999),(699,1100),(990,2791)]:
   r,c=self.records();c.update(issued_unix=times[0],expires_unix=times[1])
   with self.assertRaisesRegex(ValueError,'QuietTimeBinding'):self.check(r,c)
 def test_missing_or_nonfinite_time_rejected(self):
  for time in [None,True,float('nan'),float('inf'),'990']:
   r,c=self.records();c['issued_unix']=time
   with self.assertRaisesRegex(ValueError,'QuietTimeFormat'):self.check(r,c)
 def test_wrong_boot_input_policy_and_support(self):
  for key,value in [('boot_id','other'),('controller_sha256',{}),('policy_sha256','other'),('immutable_bindings',{})]:
   r,c=self.records();c[key]=value
   with self.assertRaises(ValueError):self.check(r,c)
 def test_original_archive_image_and_caps_are_exact(self):
  for key in bindings(POLICY):
   r,c=self.records();c=copy.deepcopy(c);c['immutable_bindings'][key]='wrong'
   with self.assertRaisesRegex(ValueError,'ReviewedInputBinding'):self.check(r,c)
 def test_repetition_membership_and_type(self):
  for reps in [[],[2],[1,1],[True],[1,4],None,'1']:
   r,c=self.records();c['authorized_repetitions']=reps
   with self.assertRaisesRegex(ValueError,'QuietRepetitionBinding'):self.check(r,c)
 def test_approval_and_heavy_idle_are_explicit_true(self):
  for name in ['approved','heavy_idle_verified']:
   r,c=self.records();(r if name=='approved'else c)[name]=1
   with self.assertRaisesRegex(ValueError,'MissingFreshQuietAuthorization'):self.check(r,c)
if __name__=='__main__':unittest.main()
