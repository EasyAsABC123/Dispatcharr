import copy,datetime,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'tools'));from result_stage import stage_wait,settled_media
T=datetime.datetime(2026,11,13,tzinfo=datetime.timezone.utc)
RAW=json.loads((ROOT.parents[1]/'clocked-recordings-native-physical-4-1/native-clocked-recording-private.json').read_text());ROWS=RAW['physical']['samples'][-1]['reply']['diagnostics']['processes']['rows']
class Controls(unittest.TestCase):
 def test_actual_retained_capture_remux_waits(self):self.assertTrue(settled_media(ROWS))
 def test_nominal30_stage_does_not_spend_probe(self):
  p=stage_wait(T+datetime.timedelta(seconds=30),T,100.,100.);self.assertEqual(p['end_monotonic'],130.);self.assertEqual(p['target_probe_seconds'],15)
 def test_future_end_over60_refuses(self):
  with self.assertRaises(ValueError):stage_wait(T+datetime.timedelta(seconds=61),T,100.,100.)
 def test_past_end_refuses(self):
  with self.assertRaises(ValueError):stage_wait(T-datetime.timedelta(seconds=1),T,100.,100.)
 def test_remaining180_refuses(self):
  with self.assertRaises(ValueError):stage_wait(T+datetime.timedelta(seconds=30),T,200.,100.)
 def test_naive_refuses(self):
  with self.assertRaises(ValueError):stage_wait(T.replace(tzinfo=None),T,100.,100.)
 def test_bool_clock_refuses(self):
  with self.assertRaises(ValueError):stage_wait(T,T,True,False)
 def test_nan_clock_refuses(self):
  with self.assertRaises(ValueError):stage_wait(T,T,float('nan'),0)
 def test_missing_remux_refuses(self):
  with self.assertRaises(ValueError):settled_media(ROWS[:1])
 def test_unsettled_remux_refuses(self):
  rows=copy.deepcopy(ROWS);rows[1]['settled_utc']=None
  with self.assertRaises(ValueError):settled_media(rows)
 def test_wrong_code_refuses(self):
  rows=copy.deepcopy(ROWS);rows[0]['code']=0
  with self.assertRaises(ValueError):settled_media(rows)
 def test_bool_code_refuses(self):
  rows=copy.deepcopy(ROWS);rows[1]['code']=False
  with self.assertRaises(ValueError):settled_media(rows)
if __name__=='__main__':unittest.main()
