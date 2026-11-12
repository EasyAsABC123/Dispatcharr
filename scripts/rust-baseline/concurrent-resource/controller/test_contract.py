"""Pure plan/approval controls; never starts Source, Docker or a sampler."""
import copy,json,unittest
from unittest import mock
from pathlib import Path
import contract
class ContractControls(unittest.TestCase):
 def records(self):
  p={'scope':'m0-resource-observer-integration-contract','controller_sha256':{'a':'hash'},'full_resource_window_enabled':False,'Native_enabled':False,'contract_policy':{'observation_schedule_seconds':10}}
  a={'scope':p['scope'],'approved':True,'plan_sha256':'plan','boot_id':'boot','issued_unix':990,'expires_unix':1100};return p,a
 def check(self,p,a):return contract.validate(p,a,'boot',1000,'plan',{'a':'hash'})
 def test_exact_approval_passes_only_pure_validation(self):self.check(*self.records())
 def test_missing_wrong_plan_boot_scope_and_explicit_approval(self):
  for key,value in [('approved',1),('plan_sha256','other'),('boot_id','other'),('scope','full-resource')]:
   p,a=self.records();a[key]=value
   with self.assertRaisesRegex(ValueError,'RootApproval'):self.check(p,a)
 def test_future_stale_expired_or_long_approval_rejected(self):
  for issued,expires in [(1001,1100),(699,1100),(990,999),(990,2791)]:
   p,a=self.records();a.update(issued_unix=issued,expires_unix=expires)
   with self.assertRaisesRegex(ValueError,'ApprovalTime'):self.check(p,a)
 def test_wrong_input_hash_and_full_window_or_native_prohibited(self):
  for key,value in [('controller_sha256',{}),('full_resource_window_enabled',True),('Native_enabled',True)]:
   p,a=self.records();p[key]=value
   with self.assertRaises(ValueError):self.check(p,a)
 def test_observation_cannot_be_extended_by_plan(self):
  p,a=self.records();p['contract_policy']['observation_schedule_seconds']=120
  with self.assertRaisesRegex(ValueError,'ContractScope'):self.check(p,a)
 def test_copy_prepares_world_readable_readonly_script_without_byte_changes(self):
  import tempfile,hashlib
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);p=root/'proc_snapshot.py';p.write_bytes(b'captured helper');p.chmod(0o600);before=p.read_bytes();contract.prepare_mount(root)
   self.assertEqual(p.stat().st_mode&0o777,0o444);self.assertEqual(p.read_bytes(),before)
 def test_original_Resource6_entries_remain_unexecutable(self):
  from run import main,execute
  with mock.patch('subprocess.Popen',side_effect=AssertionError('no subprocess')):
   for call in [lambda:main(),lambda:execute(None,None,None,1)]:
    with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):call()
if __name__=='__main__':unittest.main()
