"""Portable positive/corruption controls; no private artifacts or Source runs."""
import hashlib,json,shutil,tempfile,unittest
from pathlib import Path
import verify
class Public(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);root=Path(self.temp.name)/'public';shutil.copytree(Path(__file__).parent.parent,root,ignore=shutil.ignore_patterns('__pycache__'));self.ref=root/'reference'
 def rewrite(self,change):
  p=self.ref/'observations.json';v=json.loads(p.read_text());change(v);p.write_text(json.dumps(v));p=self.ref/'verified-source-once-1.json';summary=json.loads(p.read_text());summary['observations_sha256']=hashlib.sha256((self.ref/'observations.json').read_bytes()).hexdigest();p.write_text(json.dumps(summary))
 def test_public_only_positive_no_private_paths(self):self.assertTrue(verify.verify(self.ref)['valid_retained_observation'])
 def bad(self,change):
  self.rewrite(change)
  with self.assertRaises((AssertionError,ValueError)):verify.verify(self.ref)
 def test_shorter_outage_rejected(self):
  def change(v):
   start=v['events'][0]['monotonic'];v['events'][1]['monotonic']=start+101;v['outage_elapsed_seconds']=101
  self.bad(change)
 def test_one_candidate_healthy_rejected(self):self.bad(lambda v:v['events'][0]['probes'][0].update(status=200))
 def test_no_warmed_connection_receipt_rejected(self):self.bad(lambda v:v['events'][0]['acks'][0]['physical_close'].update(closed_ids=[]))
 def test_foreign_origin_peer_rejected(self):self.bad(lambda v:v['before']['origin_socket_witness']['active_input_connections'][0].update(peer=['foreign',1]))
 def test_fd_not_closed_rejected(self):self.bad(lambda v:v['before_restore']['origin_socket_witness']['physical_closed_inputs'][0].update(socket_fileno=4))
 def test_restore_less_than88_rejected(self):
  def change(v):v['restore_observation_end_monotonic']=v['restored_monotonic']+87;v['restore_observation_elapsed_seconds']=87
  self.bad(change)
 def test_fresh_decode_cannot_replace_original(self):self.bad(lambda v:v['decodes'][0].update(frames=24))
 def test_timeout_not_erased_as_recovery(self):self.bad(lambda v:v['original_body_outcome']['before_controlled_close'].update(errors=[]))
 def test_failed_cleanup_rejected(self):self.bad(lambda v:v['terminal_actions']['down'].update(ok=False))
 def test_exit1_not_relabelled_green(self):self.bad(lambda v:v.update(controller_process_exit=0,process_success=True))
 def test_wrong_failure_location_rejected(self):self.bad(lambda v:v['reporting_failure'].update(scope='final console print'))
 def test_Source_budget_not_invented(self):
  p=self.ref/'verified-source-once-1.json';v=json.loads(p.read_text());v['Source_budget_acceptance']=True;p.write_text(json.dumps(v))
  with self.assertRaises(AssertionError):verify.verify(self.ref)
 def test_deep_absent_is_error_not_fallback(self):
  with self.assertRaisesRegex(ValueError,'DeepRaw'):verify.verify(self.ref,deep_repo=Path(self.temp.name)/'missing')
if __name__=='__main__':unittest.main()
