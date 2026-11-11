import unittest
from finite_capacity import finite_state_matches,refusal_matches
class FiniteControls(unittest.TestCase):
    def state(self):return {'profile':{'max_streams':1,'is_active':True},'provider_slots':1,'clients':2,'origin':{'active':1,'connections':1}}
    def test_configured_shared_finite_slot(self):self.assertTrue(finite_state_matches(self.state(),2,1))
    def test_unlimited_not_finite(self):
        d=self.state();d['profile']['max_streams']=0;self.assertFalse(finite_state_matches(d,2,1))
    def test_unexpected_second_slot_refused(self):
        d=self.state();d['provider_slots']=2;self.assertFalse(finite_state_matches(d,2,1))
    def test_refusal_requires_no_new_origin(self):
        d=self.state();self.assertTrue(refusal_matches(503,{'error':'maximum connection limits'},d,d));after=self.state();after['origin']['connections']=2;self.assertFalse(refusal_matches(503,{'error':'full'},d,after))
    def test_wrong_status_or_shape_refused(self):
        d=self.state();self.assertFalse(refusal_matches(200,{'error':'full'},d,d));self.assertFalse(refusal_matches(503,{},d,d))
if __name__=='__main__':unittest.main()
