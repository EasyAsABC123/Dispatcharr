import unittest
from pathlib import Path
from finite_ten import finite_state_matches,refusal_matches
class FiniteTenControls(unittest.TestCase):
    def state(self):return {'profile':{'max_streams':10,'is_active':True},'provider_slots':10,'clients':11,'origin':{'active':10,'connections':10}}
    def test_ten_slots_eleven_clients_valid(self):self.assertTrue(finite_state_matches(self.state(),11,10))
    def test_unlimited_cannot_substitute(self):
        d=self.state();d['profile']['max_streams']=0;self.assertFalse(finite_state_matches(d,11,10))
    def test_extra_slot_rejected(self):
        d=self.state();d['provider_slots']=11;self.assertFalse(finite_state_matches(d,11,10))
    def test_rejected_channel_cannot_create_origin(self):
        d=self.state();self.assertTrue(refusal_matches(503,{'error':'full'},d,d));after=self.state();after['origin']['connections']=11;self.assertFalse(refusal_matches(503,{'error':'full'},d,after))
    def test_wrong_status_or_missing_error_rejected(self):
        d=self.state();self.assertFalse(refusal_matches(200,{'error':'full'},d,d));self.assertFalse(refusal_matches(503,{},d,d))
    def test_bounded_existingten_plus_one_owned_overflow(self):
        text=Path(__file__).with_name('finite_ten.py').read_text();self.assertIn("seed['channels'][:10]",text);self.assertIn('range(10)',text);self.assertIn('channels.append(overflow)',text);self.assertIn('m0-owned-finite-ten-overflow',text)
    def test_no_sampler_or_force_slot_cleanup(self):
        text=Path(__file__).with_name('finite_ten.py').read_text();self.assertNotIn('Window(',text);self.assertNotIn('resources(',text);self.assertNotIn("'DECR'",text);self.assertNotIn("'FLUSHDB'",text)
if __name__=='__main__':unittest.main()
