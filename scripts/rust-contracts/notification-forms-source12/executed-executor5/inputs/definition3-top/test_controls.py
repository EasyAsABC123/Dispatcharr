import copy,json,unittest
from definition import ROOT,target,validate,validate_row
class Controls(unittest.TestCase):
 def setUp(self):self.plan=json.loads((ROOT/'inputs/plan.json').read_text())
 def bad(self,mutate):
  p=copy.deepcopy(self.plan);mutate(p)
  with self.assertRaises(ValueError):validate(p)
 def test_positive(self):self.assertTrue(validate(self.plan))
 def test_role_level(self):self.bad(lambda p:p['roles'][0].update(user_level=1))
 def test_role_bool(self):self.bad(lambda p:p['roles'][0].update(is_active=1))
 def test_seed_bool(self):self.bad(lambda p:p['seed_notifications'][0].update(admin_only=0))
 def test_seed_message(self):self.bad(lambda p:p['seed_notifications'][0].update(message='different'))
 def test_alias_swap(self):self.bad(lambda p:p['seed_aliases'].update(aliasDelete=11,foreign=10))
 def test_dismissal_foreign(self):self.bad(lambda p:p['seed_dismissals'][0].update(notification_id=11))
 def test_dismissal_action(self):self.bad(lambda p:p['seed_dismissals'][0].update(action_taken=0))
 def test_bool_true_action_seed(self):self.bad(lambda p:p['seed_dismissals'][1].update(action_taken=True))
 def test_boolean_false_action_seed(self):self.bad(lambda p:p['seed_dismissals'][0].update(action_taken=False))
 def test_body_hash(self):self.bad(lambda p:p['cases'][0].update(body_sha256='0'*64))
 def test_body_path_swap(self):self.bad(lambda p:p['cases'][0].update(body_path='bodies/02.bin'))
 def test_case_order(self):self.bad(lambda p:p['cases'].reverse())
 def test_target_alias(self):self.bad(lambda p:p['cases'][7].update(target_alias='FormA'))
 def test_dependency_hash(self):self.bad(lambda p:p['actual_dependencies'][0].update(sha256='0'*64))
 def test_natural_authority(self):self.bad(lambda p:p['natural_aliases']['FormB'].update(from_actual_response_ordinal=1))
 def test_unexecuted(self):self.bad(lambda p:p.update(requests_executed=12))
 def test_target_binding(self):
  c=self.plan['cases'][7];aliases={'FormA':12,'FormB':13};row=dict(c,path=target(c,aliases));self.assertTrue(validate_row(row,c,aliases));row['path']='/api/core/notifications/12/'
  with self.assertRaises(ValueError):validate_row(row,c,aliases)
 def test_missing_alias(self):
  with self.assertRaises(ValueError):target(self.plan['cases'][7],{})
if __name__=='__main__':unittest.main(verbosity=2)
