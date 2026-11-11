"""Offline strict-type/clock/entropy controls. No fixture/network on import."""
import copy,datetime,unittest,uuid
from provider_stale_modal_browser_runner import compare_rows,strict_equal

class Controls(unittest.TestCase):
 def check(self,a,b):
  source={'prepared_rows':{'vod_episode':[]},'rows':{'vod_episode':[a]}}
  native={'prepared_rows':{'vod_episode':[]},'rows':{'vod_episode':[b]}}
  return compare_rows(source,native,[(0,9999999999),(0,9999999999)])['passed']
 def test_same_types(self):self.assertTrue(strict_equal({'a':[True,1,'x']},{'a':[True,1,'x']}))
 def test_bool_is_not_integer(self):self.assertFalse(strict_equal({'a':[True]},{'a':[1]}))
 def test_integer_is_not_float(self):self.assertFalse(strict_equal(1,1.0))
 def test_extra_keys(self):self.assertFalse(self.check({'id':1},{'id':1,'extra':True}))
 def test_array_order(self):self.assertFalse(self.check({'id':1,'p':[1,2]},{'id':1,'p':[2,1]}))
 def test_nested_bool_corruption(self):self.assertFalse(self.check({'id':1,'p':{'flag':True}},{'id':1,'p':{'flag':1}}))
 def test_new_uuid_v4(self):self.assertTrue(self.check({'id':1,'uuid':str(uuid.uuid4())},{'id':1,'uuid':str(uuid.uuid4())}))
 def test_uuid_other_versions_rejected(self):self.assertFalse(self.check({'id':1,'uuid':str(uuid.uuid1())},{'id':1,'uuid':str(uuid.uuid4())}))
 def test_clock_outside_own_interval(self):
  source={'prepared_rows':{'vod_episode':[]},'rows':{'vod_episode':[{'id':1,'created_at':'2026-11-11T12:00:00+00:00'}]}}
  native=copy.deepcopy(source);native['rows']['vod_episode'][0]['created_at']='2026-11-11T12:00:01+00:00'
  self.assertFalse(compare_rows(source,native,[(0,1),(0,1)])['passed'])
if __name__=='__main__':unittest.main()
