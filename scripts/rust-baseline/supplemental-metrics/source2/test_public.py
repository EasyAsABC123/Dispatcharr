"""Retained-only numerical counterfactuals; no Source/kernel/network/sampler."""
import hashlib,json,shutil,tempfile,unittest
from pathlib import Path
import verify
class Public(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)/'public';shutil.copytree(Path(__file__).parent,self.root,ignore=shutil.ignore_patterns('__pycache__'))
 def mutate(self,file,fn,rebind=True):
  p=self.root/file;v=json.loads(p.read_text());fn(v);p.write_text(json.dumps(v))
  if rebind:
   key={'observations.json':'observations_sha256','ts-header-projection.json':'TS_projection_sha256'}.get(file)
   if key:
    p=self.root/'verified-source2.json';s=json.loads(p.read_text());s[key]=hashlib.sha256((self.root/file).read_bytes()).hexdigest();p.write_text(json.dumps(s))
 def bad(self,file,fn,rebind=True):
  self.mutate(file,fn,rebind)
  with self.assertRaises((AssertionError,ValueError,KeyError)):verify.verify(self.root)
 def role(self,v):return v['supplemental_points'][0]['roles']['backend']
 def test_public_only_positive(self):self.assertEqual(verify.verify(self.root)['per_PID_rows'],75)
 def test_hash_corruption(self):self.bad('observations.json',lambda v:v.update(process_exit=0),False)
 def test_missing_point(self):self.bad('observations.json',lambda v:v['supplemental_points'].pop())
 def test_missing_role(self):self.bad('observations.json',lambda v:v['supplemental_points'][0]['roles'].pop('worker'))
 def test_duplicate_PID(self):self.bad('observations.json',lambda v:self.role(v)['processes'].append(self.role(v)['processes'][0]))
 def test_inventory_start_tick_reuse(self):self.bad('observations.json',lambda v:self.role(v)['inventory_end']['processes'][0].update(start_ticks=1))
 def test_UID_identity_changed(self):self.bad('observations.json',lambda v:self.role(v)['inventory_end']['processes'][0].update(identity=[9,9]))
 def test_row_start_tick_disagrees(self):self.bad('observations.json',lambda v:self.role(v)['processes'][0].update(start_ticks=1))
 def test_denied_row_not_zero_or_complete(self):self.bad('observations.json',lambda v:self.role(v)['processes'][0].update(denied_fields=['memory'],rss_bytes=None,pss_bytes=None))
 def test_incomplete_role(self):self.bad('observations.json',lambda v:self.role(v).update(coverage_complete=False))
 def test_RSS_sum_arithmetic(self):self.bad('observations.json',lambda v:self.role(v).update(rss_bytes_sum_observed=1))
 def test_PSS_sum_arithmetic(self):self.bad('observations.json',lambda v:self.role(v).update(pss_bytes_sum_observed=1))
 def test_bool_not_memory_integer(self):self.bad('observations.json',lambda v:self.role(v)['processes'][0].update(rss_bytes=True))
 def test_media_identity_not_invented(self):self.bad('observations.json',lambda v:self.role(v)['processes'][0].update(identified_media=True))
 def test_Redis_generation(self):self.bad('observations.json',lambda v:v['supplemental_points'][0]['Redis']['server'].update(run_id='b'*40))
 def test_Redis_counter_reset(self):self.bad('observations.json',lambda v:v['supplemental_points'][1]['Redis']['server'].update(total_net_input_bytes=0))
 def test_Redis_recorded_delta(self):self.bad('observations.json',lambda v:v['supplemental_points'][0]['Redis']['delta_from_previous']['server_protocol_counters_including_observer'].update(total_commands_processed=1))
 def test_TS_header_TEI_recomputed(self):
  def change(v):
   h=v['0.ts'];raw=bytearray.fromhex(h['first_six_bytes_per_complete_packet_hex']);raw[1]^=128;h['first_six_bytes_per_complete_packet_hex']=raw.hex()
  self.bad('ts-header-projection.json',change)
 def test_TS_tail_length(self):self.bad('ts-header-projection.json',lambda v:v['0.ts'].update(tail_hex=''))
 def test_TS_CC_count_not_attested_only(self):self.bad('observations.json',lambda v:v['sampled_TS']['0.ts']['snapshot'].update(CC_mismatches=1))
 def test_overhead_math(self):self.bad('observations.json',lambda v:v['supplemental_points'][0].update(overhead_seconds=0))
 def test_physical_close(self):self.bad('observations.json',lambda v:v['playback']['viewers'][0]['physical_close'].update(socket_closed=False))
 def test_retention_before70(self):self.bad('observations.json',lambda v:v['supplemental_points'][3].update(begin_monotonic=v['playback']['viewers'][0]['physical_close']['completed_monotonic']+69))
 def test_default_DB_unchanged(self):self.bad('observations.json',lambda v:v.update(default_database_unchanged=False))
 def test_cleanup_empty(self):self.bad('observations.json',lambda v:v['independent_cleanup'].update(all_empty=False))
 def test_unsettled_command(self):self.bad('observations.json',lambda v:v['host_command_settlements'][0].update(pipes_alive=1))
 def test_task_outside_window(self):self.bad('observations.json',lambda v:v['tasks']['workflow-postrun.jsonl'][0].update(monotonic=v['playback']['resources']['window_end_monotonic']+1))
 def test_task_progress(self):
  def change(v):
   for r in v['playback']['viewers'][0]['byte_trace']:r['bytes']=0
  self.bad('observations.json',change)
 def test_AV_actual_count_and_floor(self):self.bad('observations.json',lambda v:v['decodes'][0].update(frames=24))
 def test_no_full_M0_promotion(self):self.bad('verified-source2.json',lambda v:v.update(M0_complete=True))
 def test_deep_missing_no_fallback(self):
  with self.assertRaisesRegex(ValueError,'DeepRaw'):verify.verify(self.root,Path(self.tmp.name)/'missing')
if __name__=='__main__':unittest.main()
