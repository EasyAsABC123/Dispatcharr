"""Public-only static negative controls; no fixtures, samplers or private fallback."""
import hashlib,json,shutil,tempfile,unittest
from pathlib import Path
import validate
HERE=Path(__file__).resolve().parent
class Portable(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name);self.directory=self.root/'scripts/rust-baseline/crosswalk';self.directory.mkdir(parents=True)
  for name in ['crosswalk.json','public-authorities.json']:shutil.copyfile(HERE/name,self.directory/name)
  for r in json.loads((HERE/'public-authorities.json').read_text())['authorities']:
   p=self.root/r['path'];p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(HERE.parents[2]/r['path'],p)
 def run_validate(self,deep=False):return validate.validate(self.root,self.directory,deep)
 def mutate_authority(self,name,mutate):
  p=self.root/name;v=json.loads(p.read_text());mutate(v);p.write_text(json.dumps(v));m=json.loads((self.directory/'public-authorities.json').read_text())
  for r in m['authorities']:
   if r['path']==name:r['sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
  (self.directory/'public-authorities.json').write_text(json.dumps(m))
 def test_fresh_public_only_root_without_artifacts(self):
  self.assertFalse((self.root/'artifacts').exists());r=self.run_validate();self.assertTrue(r['passed']);self.assertEqual(r['private_evidence_files_checked'],0);self.assertEqual(r['registry_counts']['fully_verified'],0)
 def test_historical_crosswalk_bytes_cannot_be_rewritten(self):
  p=self.directory/'crosswalk.json';v=json.loads(p.read_text());v['Native_acceptance']=True;p.write_text(json.dumps(v))
  with self.assertRaisesRegex(ValueError,'HistoricalBytes'):self.run_validate()
 def test_missing_public_file_has_no_private_fallback(self):
  (self.root/'docs/rust-contract-register.json').unlink()
  with self.assertRaises(FileNotFoundError):self.run_validate()
 def test_public_hash_mutation_fails(self):
  p=self.root/'scripts/rust-baseline/idle-churn/policy.json';p.write_bytes(p.read_bytes()+b'\n')
  with self.assertRaisesRegex(ValueError,'PublicAuthorityHash'):self.run_validate()
 def test_registry_counts_or_full_verification_cannot_drift(self):
  self.mutate_authority('docs/rust-contract-register.json',lambda v:v['counts'].update(fully_verified_route_operations=1))
  with self.assertRaisesRegex(ValueError,'LiteralRegistryCountsChanged'):self.run_validate()
 def test_literal_growth_negative_not_silently_green(self):
  self.mutate_authority('scripts/rust-baseline/idle-churn/verified-original-three-repetitions-corrected-2.json',lambda v:v.update(Source_budget_acceptance=True))
  with self.assertRaisesRegex(ValueError,'SourceGrowthNegativeLost'):self.run_validate()
 def test_public_resource_acceptance_cannot_be_invented(self):
  self.mutate_authority('scripts/rust-playback-baseline/verified-source-accounting-1.json',lambda v:v.update(resource_acceptance=True))
  with self.assertRaisesRegex(ValueError,'ResourceAcceptanceOverclaim'):self.run_validate()
 def test_real_worker_scope_not_fabricated(self):
  self.mutate_authority('scripts/rust-baseline/concurrent-import/verified-source-bounded-concurrent-1.json',lambda v:v.update(real_worker_task_executions=2))
  with self.assertRaisesRegex(ValueError,'WorkerReferenceScopeChanged'):self.run_validate()
 def test_duplicate_or_private_authority_path_rejected(self):
  p=self.directory/'public-authorities.json';v=json.loads(p.read_text());v['authorities'][0]['path']='artifacts/private.json';p.write_text(json.dumps(v))
  with self.assertRaisesRegex(ValueError,'PublicAuthoritySet'):self.run_validate()
 def test_deep_mode_requires_original_private_evidence(self):
  with self.assertRaises(FileNotFoundError):self.run_validate(True)
 def test_validator_does_not_mutate_register(self):
  p=self.root/'docs/rust-contract-register.json';before=p.read_bytes();self.run_validate();self.assertEqual(p.read_bytes(),before)
if __name__=='__main__':unittest.main()
