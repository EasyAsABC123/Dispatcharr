"""Known-error raw/cause/Source-null controls, never an actual Native fixture."""
import base64,copy,sys,unittest
from pathlib import Path
sys.path[:0]=[str(Path(__file__).resolve().parent),str(Path(__file__).resolve().parents[1]/'tools')]
from test_normal import normal,validate_case
class Known(unittest.TestCase):
 def test_actual_Source_two_error_shapes_validate_without_inventing_code(self):
  for index in(6,10):
   source,native,report=normal(index);self.assertTrue(validate_case(source,native,report)['passed']);self.assertIsNone([s for s in source['sql']if 'exception_type'in s][0]['SQLSTATE']);self.assertEqual(native['actual_server_errors'][0]['C'],'P0001')
 def reject(self,mutate,index=6):
  s,n,r=normal(index);mutate(s,n)
  with self.assertRaises((ValueError,KeyError,TypeError)):validate_case(s,n,r)
 def test_all_nine_Source_shapes_preserve_stable_bindings(self):
  report=None
  for index in(1,2,3,5,6,7,8,9,10):
   s,n,r=normal(index)
   if report is None:report=r
   self.assertTrue(validate_case(s,n,report)['passed'])
 def test_discarded_cleanup_error_cannot_qualify(self):self.reject(lambda s,n:n.update(case_cleanup_errors=['unsettled']))
 def test_source_null_cannot_be_filled_from_declared_trigger(self):
  def change(s,n):next(q for q in s['sql']if 'exception_type'in q)['SQLSTATE']='P0001'
  self.reject(change)
 def test_unknown_actual_native_code_cannot_borrow_normal_result(self):
  def change(s,n):
   row=n['native_pg']['frames'][-1];row['payload_base64']=base64.b64encode(base64.b64decode(row['payload_base64']).replace(b'CP0001',b'C23505')).decode()
  self.reject(change)
 def test_extra_displayed_diagnostic_cannot_disappear(self):
  def change(s,n):
   row=n['native_pg']['frames'][-1];raw=base64.b64decode(row['payload_base64']);row['payload_base64']=base64.b64encode(raw[:-1]+b'Dextra diagnostic\0\0').decode()
  self.reject(change)
 def test_missing_actual_error_not_replaced_by_expected_text(self):self.reject(lambda s,n:n['native_pg']['frames'].pop())
 def test_no_insert_attempt_cannot_qualify_error(self):
  def change(s,n):n['native_pg']['frames']=[r for r in n['native_pg']['frames']if r['direction']!='frontend']
  self.reject(change)
 def test_preinstalled_trigger_not_after_admission(self):self.reject(lambda s,n:n['fault_injection'].update(before_admission_trigger_installed=True))
 def test_live_injector_is_not_settlement(self):self.reject(lambda s,n:n['fault_injection'].update(thread_settled=False))
 def test_recurring_zero_with_unobserved_event_rejected(self):self.reject(lambda s,n:n['after']['Channels_decoded'].append({'type':'update','data':{'scheduled':0}}),10)
if __name__=='__main__':unittest.main()
