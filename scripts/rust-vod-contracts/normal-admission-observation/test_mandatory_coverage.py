"""Mandatory coverage corruption tests; no application execution."""
import copy,json,unittest
from recompute import HERE,recompute
class Controls(unittest.TestCase):
 def setUp(self):
  self.full=json.loads((HERE/'full-retained.json').read_text());self.projected=json.loads((HERE/'retained-input.json').read_text());self.cleanup=json.loads((HERE/'independent-cleanup.json').read_text())
 def check(self):return recompute(self.full,self.projected,self.cleanup)
 def test_actual_coverage_still_recomputes_exact_negative(self):
  r=self.check();self.assertFalse(r['actual_gate_recomputed']['normal_scenario_qualified']);self.assertTrue(r['separate_offline_gate_recomputed']['normal_scenario_qualified'])
 def test_all_decoders_or_only_other_or_probe1_missing_each_backend(self):
  for mode in ['source','native']:
   for removed in ['all','other','probe1']:
    with self.subTest(mode=mode,removed=removed):
     self.setUp();phase=self.full['summary']['backends'][mode]
     if removed=='all':phase['decoders']={}
     else:phase['decoders'].pop(removed)
     with self.assertRaisesRegex(ValueError,'MandatoryDecoderReaderCoverage'):self.check()
 def test_matching_readers_and_extra_decoder_cannot_waive_required_role(self):
  for kind in ['both_removed','extra','reader_removed']:
   self.setUp()
   for p in self.full['summary']['backends'].values():
    if kind=='both_removed':p['decoders'].pop('other');p['readers_before_teardown'].pop('other')
    elif kind=='extra':p['decoders']['unrequested']=copy.deepcopy(p['decoders']['other'])
    else:p['readers_before_teardown'].pop('other')
   with self.assertRaises(ValueError):self.check()
 def test_empty_origin_requests_or_connections_each_backend(self):
  for mode in ['source','native']:
   for field in ['requests','connections']:
    with self.subTest(mode=mode,field=field):
     self.setUp();o=self.full['summary'][mode+'_origin_terminal'];o[field]=[]
     with self.assertRaisesRegex(ValueError,'MandatoryOriginRecords'):self.check()
 def test_missing_positive_probe_origin_role(self):
  for mode in ['source','native']:
   self.setUp();o=self.full['summary'][mode+'_origin_terminal']
   for r in o['requests']:
    if r['path_role']=='probe1_movie':r['path_role']='unowned'
   with self.assertRaisesRegex(ValueError,'MandatoryMediaRoleCoverage'):self.check()
 def test_missing_connection_or_changed_past_request_not_ignored(self):
  for kind in ['connection','past_request','aggregate']:
   self.setUp();o=self.full['summary']['native_origin_terminal']
   if kind=='connection':o['connections'].pop(2)
   elif kind=='past_request':o['requests'][1]['began']+=1
   else:o['accepted_TCP_total']+=1
   with self.assertRaises(ValueError):self.check()
if __name__=='__main__':unittest.main(verbosity=2)
