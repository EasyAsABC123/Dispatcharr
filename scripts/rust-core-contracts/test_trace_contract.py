import copy,json,unittest
from pathlib import Path
from trace_contract import compare
HERE=Path(__file__).resolve().parent
class ActualTraceControls(unittest.TestCase):
 def setUp(self):
  self.source=json.loads((HERE/'original-buffer-source-extended-1.json').read_text());self.native=json.loads((HERE/'native-buffer-extended-1.json').read_text())
 def test_actual_source_native23(self):self.assertEqual(compare(self.source,self.native),[])
 def test_missing_record(self):
  self.native['records'].pop()
  with self.assertRaises(ValueError):compare(self.source,self.native)
 def test_reordered_records(self):
  self.native['records'][0],self.native['records'][1]=self.native['records'][1],self.native['records'][0]
  with self.assertRaises(ValueError):compare(self.source,self.native)
 def test_fabricated_prospective_cursor(self):
  self.native['records'][3]['native_execution_observed']=False
  with self.assertRaises(ValueError):compare(self.source,self.native)
 def test_wrong_cursor(self):
  self.native['records'][3]['native_next_after']=99
  with self.assertRaises(ValueError):compare(self.source,self.native)
 def test_full_byte_difference(self):
  self.native['records'][2]['chunks'][0]['hex']='00'+self.native['records'][2]['chunks'][0]['hex'][2:];self.assertTrue(compare(self.source,self.native))
 def test_cached_metadata_expiry_regression(self):
  self.native['records'][-1]['reader_discontinuity']=True;self.assertTrue(compare(self.source,self.native))
 def test_index_bool_not_integer(self):
  self.native['records'][2]['index']=True;self.assertTrue(compare(self.source,self.native))
 def test_missing_explicit_null(self):
  del self.native['records'][0]['oldest_last_consumed'];self.assertTrue(compare(self.source,self.native))
 def test_missing_empty_chunks(self):
  del self.native['records'][0]['chunks'];self.assertTrue(compare(self.source,self.native))
 def test_source_byte_verification_failed(self):
  self.source['records'][3]['full_bytes_verified']=False
  with self.assertRaises(ValueError):compare(self.source,self.native)
 def test_peer_failure(self):
  self.native['passed']=False
  with self.assertRaises(ValueError):compare(self.source,self.native)

class PendingBoundaryControls(unittest.TestCase):
 def setUp(self):
  from trace_contract import compare_pending
  self.compare=compare_pending;self.source=json.loads((HERE/'original-buffer-pending-source-1.json').read_text());self.native=json.loads((HERE/'native-buffer-pending-1.json').read_text())
 def test_actual_within_budget_pending(self):self.assertEqual(self.compare(self.source,self.native),[])
 def test_pending_member_dropped(self):
  self.native['records'][-1]['discontinuities']=[1,2];self.assertTrue(self.compare(self.source,self.native))
 def test_resource_cap_misclassified_as_parity(self):
  self.native['records'][-1].pop('mark_result');self.native['records'][-1]['mark_error']='Capacity';self.assertTrue(self.compare(self.source,self.native))
 def test_missing_nullable_state(self):
  del self.native['records'][0]['oldest_last_consumed'];self.assertTrue(self.compare(self.source,self.native))
 def test_third_payload_not_hidden_as_common_work(self):
  self.source['source_only_over_budget_third_publication']['chunks'].pop()
  with self.assertRaises(ValueError):self.compare(self.source,self.native)

if __name__=='__main__':unittest.main()
