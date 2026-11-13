"""Local finite-state/clearance controls only. No Docker, app or subprocess."""
import copy,datetime,hashlib,json,tempfile,unittest
from pathlib import Path
from policy import *
PLAN=json.loads((Path(__file__).parent/'inputs/plan.json').read_text());NOW=datetime.datetime(2026,11,12,17,10,tzinfo=datetime.timezone.utc).timestamp()
class Controls(unittest.TestCase):
 def started(self):
  s=fresh_state(PLAN);s['started']=True;s['nodes'][GUIDE]['status']='passed-clean';return s
 def ready_native(self):
  s=self.started();s['nodes'][SOURCE]={'status':'passed-clean','lane':1,'reference_clock_epoch':NOW-60};s['lanes']['1']['reserved']=NATIVE;return s
 def heavy_ready(self):
  s=self.started()
  for n in s['nodes']:
   if n not in HEAVY:s['nodes'][n]['status']='passed-clean'
  return s
 def denied(self,*args):
  with self.assertRaises(Refusal):allowed(PLAN,*args)
 def test_first_guide(self):self.assertTrue(allowed(PLAN,fresh_state(PLAN),GUIDE,0,NOW))
 def test_non_guide_first(self):self.denied(fresh_state(PLAN),'baseline-all',0,NOW)
 def test_clock_35_allowed(self):self.assertTrue(allowed(PLAN,fresh_state(PLAN),GUIDE,0,NOW+25*60))
 def test_clock_36_refused(self):self.denied(fresh_state(PLAN),GUIDE,0,NOW+26*60)
 def test_fourth_lane(self):self.denied(self.started(),'baseline-all',3,NOW)
 def test_awaiting_review_holds_slot(self):
  s=self.started();s['lanes']['0']['node']='proxy-auth';s['nodes']['proxy-auth']['status']='awaiting-review';self.denied(s,'baseline-all',0,NOW)
 def test_no_retries(self):
  s=self.started();s['nodes']['baseline-all']['status']='failed-clean';self.denied(s,'baseline-all',0,NOW)
 def test_dependency_pass_required(self):self.denied(self.started(),'shipped-safe17',0,NOW)
 def test_failed_dependency_blocks(self):
  s=self.started();s['nodes']['fresh-Original17capture']['status']='failed-clean';self.assertTrue(derived_blocked(PLAN,s,'shipped-safe17'));self.denied(s,'shipped-safe17',0,NOW)
 def test_cleanup_halt_blocks(self):
  s=self.started();s['halted']=True;self.denied(s,'baseline-all',0,NOW)
 def test_heavy_ordinary_pending(self):self.denied(self.started(),HEAVY[0],0,NOW)
 def test_heavy_positive(self):self.assertTrue(allowed(PLAN,self.heavy_ready(),HEAVY[0],0,NOW))
 def test_heavy_awaiting_review(self):
  s=self.heavy_ready();s['lanes']['1']['node']='baseline-all';self.denied(s,HEAVY[0],0,NOW)
 def test_heavy_fixed_order(self):self.denied(self.heavy_ready(),HEAVY[1],0,NOW)
 def test_ordinary_during_heavy(self):
  s=self.started();s['lanes']['1']['node']=HEAVY[0];self.denied(s,'baseline-all',0,NOW)
 def test_refresh_native_same_lane(self):self.assertTrue(allowed(PLAN,self.ready_native(),NATIVE,1,NOW))
 def test_refresh_wrong_lane(self):self.denied(self.ready_native(),NATIVE,0,NOW)
 def test_refresh_no_intervening(self):self.denied(self.ready_native(),'baseline-all',1,NOW)
 def test_reference_900_boundary(self):
  s=self.ready_native();s['nodes'][SOURCE]['reference_clock_epoch']=NOW-900;self.assertTrue(allowed(PLAN,s,NATIVE,1,NOW))
 def test_reference_old(self):
  s=self.ready_native();s['nodes'][SOURCE]['reference_clock_epoch']=NOW-900.001;self.denied(s,NATIVE,1,NOW)
 def test_reference_future(self):
  s=self.ready_native();s['nodes'][SOURCE]['reference_clock_epoch']=NOW+1;self.denied(s,NATIVE,1,NOW)
 def test_exact_blocked_extra(self):self.denied(self.started(),'Main4s54',0,NOW)
 def test_next_reserved_chain_only(self):self.assertEqual(next_names(PLAN,self.ready_native(),1,NOW),[NATIVE])
 def clearance(self,root):
  p=root/'settlement.json';p.write_text('synthetic settled exit1, not business acceptance');q=root/'cleanup.json';q.write_text('synthetic independent empty cleanup')
  role={'settled':True,'independent_cleanup_verified':True,'actual_controller_exit':1,'business_passed':False,'settled_epoch':NOW-4,'cleanup_verified_epoch':NOW-3,'settlement_receipt':{'path':str(p),'sha256':sha(p)},'cleanup_review':{'path':str(q),'sha256':sha(q)}}
  return {'authorized':True,'scheduler_seal_sha256':'seal','plan_sha256':PLAN_SHA,'initial_host_tooling_review':{'path':str(Path(__file__).resolve().parents[1]/'image54-packaged-matrix-1/initial-host-tooling-negative-review.json'),'sha256':'de1cf855b36982ecf37ccdd6a23b3796ff6123c9ebf9a50870d874581593e7bd'},'action':'image57-package28-once','candidate_manifest_sha256':'c1e096bdf7789c5fc2d0d9004ca6e3a68f663fe9100158b26fc621092082df0a','candidate_image':'sha256:fc5ea2aba28f86a3ebf3672f6bf8d874a6f79b2d9dd4c8a1efb8993de680375a','support_revision':'f0bfc2f200dcfd1f64ae32a9855ea250731ce67f','issued_epoch':NOW-1,'valid_until_epoch':NOW+30,'settled_prerequisites':{name:copy.deepcopy(role)for name in ['SDKd77f','image57_build']},'heavy_idle':{'all_Docker_empty':True,'all_other_heavy_idle':True,'checked_epoch':NOW-2}}
 def test_settled_failed_business_is_not_condition(self):
  with tempfile.TemporaryDirectory()as t:self.assertTrue(validate_clearance(self.clearance(Path(t)),'seal',NOW))
 def test_clearance_guards(self):
  changes=[lambda v:v['initial_host_tooling_review'].update(sha256='wrong'),lambda v:v.update(action='image53-package28-finite-once'),lambda v:v.update(candidate_image='wrong'),lambda v:v.update(support_revision='wrong'),lambda v:v['settled_prerequisites'].pop('image57_build'),lambda v:v.update(Source48={},settled_prerequisites={}),lambda v:v.update(authorized=False),lambda v:v.update(scheduler_seal_sha256='other'),lambda v:v.update(plan_sha256='other'),lambda v:v.update(valid_until_epoch=NOW-2),lambda v:v['settled_prerequisites']['SDKd77f'].update(settled=False),lambda v:v['settled_prerequisites']['SDKd77f'].update(independent_cleanup_verified=False),lambda v:v['settled_prerequisites']['SDKd77f'].update(actual_controller_exit=None),lambda v:v['heavy_idle'].update(all_Docker_empty=False),lambda v:v['heavy_idle'].update(all_other_heavy_idle=False),lambda v:v['settled_prerequisites']['SDKd77f']['cleanup_review'].update(sha256='0'*64)]
  with tempfile.TemporaryDirectory()as t:
   for i,change in enumerate(changes):
    with self.subTest(i=i):
     c=self.clearance(Path(t));change(c)
     with self.assertRaises(Refusal):validate_clearance(c,'seal',NOW)
 def test_each_prerequisite_settlement_guard(self):
  changes=[lambda r:r.update(settled=False),lambda r:r.update(independent_cleanup_verified=False),lambda r:r.update(actual_controller_exit=None),lambda r:r.update(settled_epoch=NOW+1),lambda r:r['cleanup_review'].update(sha256='0'*64)]
  with tempfile.TemporaryDirectory()as t:
   for role in ['SDKd77f','image57_build']:
    for change in changes:
     with self.subTest(role=role,change=change):
      c=self.clearance(Path(t));change(c['settled_prerequisites'][role])
      with self.assertRaises(Refusal):validate_clearance(c,'seal',NOW)
if __name__=='__main__':unittest.main()
