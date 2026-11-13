"""Finite offline retained-data corruption controls, no application execution."""
from pathlib import Path
import copy,json,tempfile,unittest
import verify as V
from validate_current23 import validate as normal
from validate_physical import validate as physical

class Portable(unittest.TestCase):
 def physical(self,case,mutate):
  name='Metadata'+str(case);raw=copy.deepcopy(V.load_projection(name));mutate(raw)
  with tempfile.TemporaryDirectory(prefix='metadata-corruption-')as d:
   for filename in V.read(V.ROOT/'lifecycle.json')[name]:
    if filename.endswith('.json'):(Path(d)/filename).write_text(json.dumps(V.lifecycle(name,filename)))
   with self.assertRaises((ValueError,TypeError,KeyError)):physical(raw,case,d)
 def test_complete_retained_qualifications(self):self.assertTrue(V.run()['passed'])
 def test_no_fake24(self):
  v=V.load_projection('Current23');v['actual_request_count']=24
  with self.assertRaises(ValueError):normal(V.load_source(),v)
 def test_no_source_case24_request(self):
  v=V.load_projection('Current23');v['Source_case24_requested']=True
  with self.assertRaises(ValueError):normal(V.load_source(),v)
 def test_actual_body_sha_required(self):
  v=V.load_projection('Current23');v['requests'][0]['response_body_sha256']='0'*64
  with self.assertRaises(ValueError):normal(V.load_source(),v)
 def test_no_foreign_user_field_change(self):
  v=V.load_projection('Current23');v['requests'][0]['after']['users'][0]['is_active']=False
  with self.assertRaises(ValueError):normal(V.load_source(),v)
 def test_original_source_reference_count24(self):self.assertEqual(V.load_source()['actual_request_count'],24)
 def test_raw_recorderfalse_staysfalse(self):self.assertIs(V.load_projection('Current23')['passed'],False)
 def test_case1_host1_stays1(self):self.assertEqual(V.lifecycle('Metadata1','execution-report.json')['actual_controller_exit'],1)
 def test_case1_full_post_audit_missing_not_synthesized(self):self.assertNotIn('owned-state-after.json',V.read(V.ROOT/'lifecycle.json')['Metadata1'])
 def test_wrong_imported_instant(self):self.physical(2,lambda v:v['before']['tables']['plugins_pluginconfig'][1].update(updated_at='2026-11-13T16:10:00.146411+00:00'))
 def test_naive_imported_timestamp(self):self.physical(2,lambda v:v['before']['tables']['plugins_pluginconfig'][1].update(updated_at='2026-11-13T16:10:00.14641'))
 def test_missing_second_owned_child(self):self.physical(2,lambda v:v['before']['tables']['plugins_pluginconfig'].pop())
 def test_wrong_child_wire_tag1(self):self.physical(2,lambda v:v['first_held']['held'].update(commands=['UPDATE 1']))
 def test_early_response_during_commit(self):self.physical(2,lambda v:v['while_commit_ready_held_HTTP_fate'].update(response_statuses=[200]))
 def test_failed_original_weak(self):self.physical(3,lambda v:v['error_ready_samples'][0].update(original_weak_alive=[False]))
 def test_wrong_backend(self):self.physical(3,lambda v:v['parent_57014_held']['held'].update(backend_pid=1))
 def test_wrong_error(self):self.physical(3,lambda v:v['parent_57014_held']['held'].update(errors=['P0001']))
 def test_error_ready_zero_clocks(self):
  def bad(v):
   for e in v['actual_relay_terminal']['events']:
    if e.get('stage')=='parent_error_ready'and e['kind']in('ready-held','ready-forwarded'):e['monotonic']=0
  self.physical(3,bad)
 def test_bool_writer_slots(self):self.physical(4,lambda v:v['actual_first_four'][0].update(available_slots=True))
 def test_autherror_frame_released(self):self.physical(4,lambda v:v['actual_first_four'][3].update(original_weak=[True,True,True,False]))
 def test_fifth_auth_event(self):self.physical(4,lambda v:v['fifth_credentials_after'].update(events=[{}]))
 def test_sixth_no_new_owner(self):self.physical(4,lambda v:v.update(sixth_original_weak=[False,True,True,True,False]))
 def test_pool_close_size1(self):self.physical(4,lambda v:v['pool'].update(closed_pool_size=1))
 def test_no_unknown_projection_epoch(self):
  with self.assertRaises(ValueError):V.load_projection('Native24')
 def test_no_binary_in_packet(self):self.assertFalse(any(p.name=='native-store-tests'for p in V.ROOT.rglob('*')))
if __name__=='__main__':unittest.main()
