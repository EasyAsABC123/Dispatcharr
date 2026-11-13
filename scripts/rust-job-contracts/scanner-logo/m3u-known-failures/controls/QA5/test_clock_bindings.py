"""No Native67 equal-clock bypass: retain raw values, bindings, own windows."""
import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from compare_full import Clocks
S='2026-11-01T00:00:00+00:00';N='2026-11-12T00:00:00+00:00'
WINDOW={'began_utc':'2026-11-11T23:59:00+00:00','finished_utc':'2026-11-12T00:01:00+00:00'}
class Controls(unittest.TestCase):
 def test_generated_coincidence_still_records_binding(self):
  clocks=Clocks();clocks.bind(N,N,'generated');self.assertEqual(len(clocks.bindings),1);self.assertEqual(len(clocks.raw),1)
 def test_existing_binding_wins_over_new_equal_source_native(self):
  clocks=Clocks();clocks.bind(S,N,'first')
  with self.assertRaisesRegex(AssertionError,'ExistingClockBinding'):clocks.bind(S,S,'same-but-wrong')
 def test_equal_source_native_generated_value_still_requires_native_window(self):
  clocks=Clocks()
  with self.assertRaisesRegex(AssertionError,'ActualNativeCommandWindow'):clocks.rows([{'id':1,'updated_at':S}],[{'id':1,'updated_at':S}],'generated',WINDOW)
 def test_changed_and_unchanged_relations_remain_literal(self):
  clocks=Clocks();clocks.rows([{'id':1,'updated_at':S}],[{'id':1,'updated_at':N}],'unchanged',WINDOW,[{'id':1,'updated_at':S}],[{'id':1,'updated_at':N}])
  with self.assertRaisesRegex(AssertionError,'ExistingClockBinding|ChangedUnchanged'):clocks.rows([{'id':1,'updated_at':S}],[{'id':1,'updated_at':'2026-11-12T00:00:01+00:00'}],'changed',WINDOW,[{'id':1,'updated_at':S}],[{'id':1,'updated_at':N}])
if __name__=='__main__':unittest.main()
