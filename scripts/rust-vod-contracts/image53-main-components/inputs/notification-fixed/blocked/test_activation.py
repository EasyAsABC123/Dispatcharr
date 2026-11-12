"""Pure launcher tests; candidate verifier mocked only inside temporary synthetic fixtures."""
import copy
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('worker_launcher',ROOT/'launch_once.py');L=importlib.util.module_from_spec(spec);spec.loader.exec_module(L)

def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(v,indent=2)+'\n')

class Launcher(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.repo=Path(self.temp.name);self.here=self.repo/'controller';shutil.copytree(ROOT,self.here,ignore=shutil.ignore_patterns('__pycache__','selection.json'))
        self.binding=json.loads((self.here/'candidate-binding.json').read_text())
        mf=self.repo/self.binding['manifest'];mf.parent.mkdir(parents=True);mf.write_text('{}');self.binding['manifest_sha256']=digest(mf);save(self.here/'candidate-binding.json',self.binding)
        self.plan=json.loads((self.here/'execution-plan.json').read_text());self.now=time.time()
        pre=self.repo/self.plan['Root_quiet_preflight'];save(pre,{'passed':True,'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'checked_epoch':self.now-1})
        self.seal()
        self.cue={'authorized':True,'candidate_application_requests_authorized':True,'Source_notification_requests_authorized':False,'action':L.ACTION,'cases':self.plan['case_names'],'resource_sampling':False,'performance_acceptance':False,'full_M4_acceptance':False,'nonce':'a'*32,'Original_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','Original_archive_sha256':'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','issued_epoch':self.now,'expires_epoch':self.now+600,'issued_utc':datetime.datetime.fromtimestamp(self.now,datetime.timezone.utc).isoformat().replace('+00:00','Z'),'expires_utc':datetime.datetime.fromtimestamp(self.now+600,datetime.timezone.utc).isoformat().replace('+00:00','Z'),'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'new_output':self.plan['new_output'],'Root_quiet_preflight_sha256':digest(pre)}
        for k,f in [('selection_sha256','selection.json'),('plan_sha256','execution-plan.json'),('policy_sha256','policy.json'),('binding_sha256','candidate-binding.json')]:self.cue[k]=digest(self.here/f)
        for k,b in [('build_manifest_sha256','manifest_sha256'),('candidate_image','image_id'),('candidate_tag','tag'),('git_revision','revision'),('source_tree_sha256','tree'),('source_archive_sha256','archive_sha256'),('Root_reviewed_Rust_input_count','runtime_inputs'),('Root_reviewed_input_map_sha256','runtime_map_sha256')]:self.cue[k]=self.binding[b]
    def seal(self):
        rows=[{'path':str(p.relative_to(self.here)),'sha256':digest(p)}for p in sorted(self.here.rglob('*'))if p.is_file()and p.name!='selection.json']
        self.plan['sealed_file_count']=len(rows);save(self.here/'execution-plan.json',self.plan)
        for row in rows:
            if row['path']=='execution-plan.json':row['sha256']=digest(self.here/'execution-plan.json')
        save(self.here/'selection.json',{'files':rows})
    def tearDown(self):self.temp.cleanup()
    def guard(self,*args):return {'image_id':self.binding['image_id'],'git_revision':self.binding['revision'],'source_tree_sha256':self.binding['tree']}
    def validate(self,cue=None):
        with patch.object(L,'guard_candidate',side_effect=self.guard):return L.validate(self.repo,self.here,self.cue if cue is None else cue,self.now+.01)
    def reject(self,mut):
        c=copy.deepcopy(self.cue);mut(c)
        with self.assertRaises((ValueError,KeyError,FileNotFoundError)):self.validate(c)
    def test_exact_positive_synthetic(self):self.validate()
    def test_missing_nonce(self):self.reject(lambda c:c.pop('nonce'))
    def test_empty_nonce(self):self.reject(lambda c:c.update(nonce=''))
    def test_malformed_nonce(self):self.reject(lambda c:c.update(nonce='../bad'))
    def test_wrong_boot(self):self.reject(lambda c:c.update(boot_id='wrong'))
    def test_expired(self):self.reject(lambda c:c.update(expires_epoch=self.now-1))
    def test_future_issue(self):self.reject(lambda c:c.update(issued_epoch=self.now+10))
    def test_nonfinite(self):self.reject(lambda c:c.update(issued_epoch=float('nan')))
    def test_long_lifetime(self):self.reject(lambda c:c.update(expires_epoch=self.now+1801))
    def test_utc_mismatch(self):self.reject(lambda c:c.update(issued_utc='2000-01-01T00:00:00Z'))
    def test_no_authorization(self):self.reject(lambda c:c.update(authorized=False))
    def test_wrong_action(self):self.reject(lambda c:c.update(action='other'))
    def test_case_omission(self):self.reject(lambda c:c['cases'].pop())
    def test_resource_not_permitted(self):self.reject(lambda c:c.update(resource_sampling=True))
    def test_wrong_source(self):self.reject(lambda c:c.update(Original_revision='wrong'))
    def test_wrong_image(self):self.reject(lambda c:c.update(candidate_image='wrong'))
    def test_wrong_runtime_count(self):self.reject(lambda c:c.update(Root_reviewed_Rust_input_count=788))
    def test_wrong_runtime_map(self):self.reject(lambda c:c.update(Root_reviewed_input_map_sha256='0'*64))
    def test_wrong_output(self):self.reject(lambda c:c.update(new_output='other'))
    def test_wrong_selection(self):self.reject(lambda c:c.update(selection_sha256='0'*64))
    def test_corrupt_payload(self):
        (self.here/'tools/phase.py').write_text('changed')
        with self.assertRaises(ValueError):self.validate()
    def test_existing_output(self):
        (self.repo/self.plan['new_output']).mkdir(parents=True)
        with self.assertRaises(ValueError):self.validate()
    def test_symlink_output(self):
        p=self.repo/self.plan['new_output'];p.parent.mkdir(parents=True,exist_ok=True);p.symlink_to(self.repo/'absent')
        with self.assertRaises(ValueError):self.validate()
    def test_stale_preflight(self):
        p=self.repo/self.plan['Root_quiet_preflight'];x=json.loads(p.read_text());x['checked_epoch']=self.now-301;save(p,x);self.cue['Root_quiet_preflight_sha256']=digest(p)
        with self.assertRaises(ValueError):self.validate()
    def test_unreviewed_runtime_proof(self):
        with patch.object(L,'guard_candidate',return_value={'image_id':'wrong','git_revision':self.binding['revision'],'source_tree_sha256':self.binding['tree']}):
            with self.assertRaises(ValueError):L.validate(self.repo,self.here,self.cue,self.now+.01)
    def test_hardblocked_before_cue_read(self):
        with patch.object(L,'HERE',self.here),patch.object(sys,'argv',['launch_once','--repo',str(self.repo)]):
            with self.assertRaisesRegex(RuntimeError,'ROOT_ACTIVATION'):L.main()
    def test_flag_only_execution_copy_qualifies(self):
        p=self.here/'launch_once.py';p.write_bytes(p.read_bytes().replace(b'\nEXECUTION_BLOCKED=True\n',b'\nEXECUTION_BLOCKED=False\n'));self.validate()
    def test_claim_once_and_no_retry_after_failure(self):
        cuepath=self.repo/self.plan['cue_path'];save(cuepath,self.cue)
        invoked=[]
        def dispatch(*args):invoked.append(True);raise RuntimeError('SyntheticDispatchFailure')
        with patch.object(L,'HERE',self.here),patch.object(L,'EXECUTION_BLOCKED',False),patch.object(L,'guard_candidate',side_effect=self.guard),patch.object(L,'dispatch',side_effect=dispatch),patch.object(sys,'argv',['launch_once','--repo',str(self.repo)]):
            with self.assertRaisesRegex(RuntimeError,'SyntheticDispatchFailure'):L.main()
            with self.assertRaisesRegex(ValueError,'AlreadyClaimed'):L.main()
        claim=json.loads(cuepath.with_name(cuepath.stem+'.claimed.json').read_text());self.assertEqual(claim['nonce'],self.cue['nonce']);self.assertTrue(claim['process_starttime'].isdigit());self.assertEqual(len(invoked),1)

if __name__=='__main__':unittest.main()
