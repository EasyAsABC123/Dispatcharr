import copy,json,unittest
from pathlib import Path
from recompute import HERE,recompute
class Controls(unittest.TestCase):
 def setUp(self):
  self.full=json.loads((HERE/'full-retained.json').read_text());self.projected=json.loads((HERE/'retained-input.json').read_text());self.cleanup=json.loads((HERE/'independent-cleanup.json').read_text())
 def run_check(self):return recompute(self.full,self.projected,self.cleanup)
 def test_exact_raw_recomputed_negative_and_separate_offline_positive(self):
  r=self.run_check();self.assertEqual(r['actual_exit_code'],1);self.assertFalse(r['actual_gate_recomputed']['normal_scenario_qualified']);self.assertTrue(r['separate_offline_gate_recomputed']['normal_scenario_qualified']);self.assertEqual(r['same_statuses'],{'source':[200,503],'native':[200,503]})
 def test_claimed_actual_pass_cannot_replace_negative(self):
  self.full['summary']['normal_scenario_qualified']=True
  with self.assertRaises(ValueError):self.run_check()
 def test_lost_other_or_required_checkpoint_cannot_pass(self):
  for mutate in ['reader','checkpoint']:
   self.setUp();n=self.full['summary']['backends']['native']
   if mutate=='reader':n['other_at_probe2_dispatch']['eof']=True
   else:n['checkpoints'][1]['observation_error']='lost'
   with self.assertRaises(ValueError):self.run_check()
 def test_decoder_and_caps_and_origin_corruption(self):
  for kind in ['decode','caps','origin']:
   self.setUp();s=self.full['summary']
   if kind=='decode':s['backends']['source']['decoders']['other']['frames']=49
   elif kind=='caps':s['backends']['native']['caps_after']['profile']=1
   else:s['native_origin_terminal']['connections'][0]['fd_after']=8
   with self.assertRaises(ValueError):self.run_check()
 def test_default_audit_or_live_resource_cannot_pass(self):
  self.full['summary']['default_after']['dataset_sha256']='corrupt'
  with self.assertRaises(ValueError):self.run_check()
  self.setUp();self.cleanup['remaining']['containers']='live'
  with self.assertRaises(ValueError):self.run_check()
 def test_raw_and_projection_disagreement_rejected(self):
  self.projected['native_other_after']['ledger']['reserved']='0'
  with self.assertRaises(ValueError):self.run_check()
 def test_origin_counts_are_not_viewer_count(self):
  r=self.run_check()
  for mode in ['source','native']:
   origin=r['owned_origin_request_groups'][mode];self.assertEqual(origin['accepted_TCP_total'],11);self.assertEqual(origin['control_total'],7);self.assertEqual(origin['groups']['GET:shared_movie:stream_GET']['requests'],2);self.assertFalse(origin['one_TCP_per_viewer_inferred'])
if __name__=='__main__':unittest.main(verbosity=2)
