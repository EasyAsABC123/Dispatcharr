"""Pure Source-definition controls; never imports Django or starts a fixture."""
import copy,json,shutil,tempfile,unittest
from pathlib import Path
import logo_source_contracts
ROOT=Path(__file__).resolve().parents[2]
class LogoSourceControls(unittest.TestCase):
 def test_exact_source_fixture_assertions_and_six_false_claim_rejections(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory)
   for relative in logo_source_contracts.dependencies()+['docs/rust-contract-register.json']:
    target=root/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/relative,target)
   path=root/'scripts/rust-contracts/logo-source/bindings.json';value=json.loads(path.read_bytes());result=logo_source_contracts.validate(root,value);self.assertEqual((result['operations'],result['historical_workflows'],result['Source_executions']),(3,5,0))
   changes=[lambda v:v['contracts'][0].__setitem__('method','POST'),lambda v:v['contracts'][0].__setitem__('contract_id','route-2810ce064a414006350d:GET'),lambda v:v['contracts'][0].__setitem__('fully_verified',True),lambda v:v['contracts'][0]['result'].__setitem__('wire_equality_claimed',True),lambda v:next(iter(v['fixtures'].values()))['setup'].__setitem__('source','generic template'),lambda v:v['contracts'][0]['cases'][0]['result'].__setitem__('historical_result','not observed')]
   for number,change in enumerate(changes):
    with self.subTest(control=number):
     altered=copy.deepcopy(value);change(altered)
     with self.assertRaises(AssertionError):logo_source_contracts.validate(root,altered)
