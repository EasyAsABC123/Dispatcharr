import copy,importlib.util,json,unittest
from pathlib import Path
from compare_trace import compare,instant,TABLES,capture_snapshot
from datetime import datetime,timezone
ROOT=Path(__file__).parent
class Controls(unittest.TestCase):
 def data(self):
  before={k:[]for k in TABLES};after=copy.deepcopy(before);before['streams']=[{'id':1,'name':'Owned','updated_at':'2020-01-01T00:00:00+00:00','last_seen':'2020-01-01T00:00:00+00:00'}];after['streams']=[{'id':1,'name':'Owned','updated_at':'2020-01-01T00:00:00+00:00','last_seen':'2020-01-02T00:00:00+00:00'}];after.update(events=[],files=[]);return {'before':before,'after':after},before,after,{'parent':[instant('2020-01-01T00:00:00+00:00'),instant('2020-01-01T01:00:00+00:00')],'child':[instant('2020-01-02T00:00:00+00:00'),instant('2020-01-03T00:00:00+00:00')]}
 def test_datetime_capture_retains_all_fields_and_precision(self):
  value={'updated_at':datetime(2026,11,12,1,2,3,456789,tzinfo=timezone.utc),'null':None,'nested':{'keep':[1,True]}};got=capture_snapshot(value);self.assertEqual(got,{'updated_at':'2026-11-12 01:02:03.456789+00:00','null':None,'nested':{'keep':[1,True]}})
 def test_preserved_and_changed_fields_retained(self):
  r,b,a,w=self.data();result=compare(r,b,a,w,w);self.assertTrue(result['passed']);self.assertEqual(result['timestamp_fields_dropped'],0);self.assertEqual(len(result['temporal_fields_retained']),2)
 def test_unresolved_Source_origin_rejected(self):
  r,b,a,w=self.data();r=copy.deepcopy(r);r['after']['streams'][0]['last_seen']='2020-01-05T00:00:00+00:00';self.assertFalse(compare(r,b,a,w,w)['passed'])
 def test_missing_timestamp_rejected(self):
  r,b,a,w=self.data();a=copy.deepcopy(a);del a['streams'][0]['updated_at'];self.assertFalse(compare(r,b,a,w,w)['passed'])
 def test_changed_preserved_updated_at_rejected(self):
  r,b,a,w=self.data();a=copy.deepcopy(a);a['streams'][0]['updated_at']='2020-01-02T00:00:00+00:00';self.assertFalse(compare(r,b,a,w,w)['passed'])
 def test_outside_window_rejected(self):
  r,b,a,w=self.data();a=copy.deepcopy(a);a['streams'][0]['last_seen']='2020-01-04T00:00:00+00:00';self.assertFalse(compare(r,b,a,w,w)['passed'])
 def test_unchanged_touch_rejected(self):
  r,b,a,w=self.data();a=copy.deepcopy(a);a['streams'][0]['last_seen']=b['streams'][0]['last_seen'];self.assertFalse(compare(r,b,a,w,w)['passed'])
 def test_null_shape_rejected(self):
  r,b,a,w=self.data();a=copy.deepcopy(a);a['streams'][0]['last_seen']=None;self.assertFalse(compare(r,b,a,w,w)['passed'])
if __name__=='__main__':unittest.main()
