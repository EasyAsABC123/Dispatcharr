"""Finite offline corruption controls; no Cargo, apps, or Docker."""
import copy,json,tempfile,unittest
from pathlib import Path
import verify
from validate_dependency import validate_case
class PortableControls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.tmp=tempfile.TemporaryDirectory(prefix="sqlx-controls-",dir=verify.ROOT.parent);cls.root=Path(cls.tmp.name)
  for v in("baseline","candidate"):
   p=cls.root/v;p.mkdir();verify.unpack(v,p)
 @classmethod
 def tearDownClass(cls):cls.tmp.cleanup()
 def sample(self,variant="candidate",case=3):
  p=self.root/variant/"rust"/("case-"+str(case));return json.loads((p/"case.json").read_text()),[json.loads(x)for x in(p/"post-first-close-observations.jsonl").read_text().splitlines()],json.loads((p/"proxy-process.json").read_text())
 def reject(self,mutate,variant="candidate"):
  row,ledger,peer=self.sample(variant);mutate(row,ledger,peer)
  with self.assertRaises((ValueError,KeyError,AssertionError)):validate_case(row,ledger,peer)
 def test_actual_baseline_negative(self):self.assertFalse(verify.validate_run(self.root/"baseline","baseline")["passed"])
 def test_actual_candidate_positive(self):self.assertTrue(verify.validate_run(self.root/"candidate","candidate")["passed"])
 def test_definition_and_negatives(self):self.assertEqual(verify.check_executed_definition()["executed_input_inventory"],66)
 def test_second_close(self):self.reject(lambda r,l,p:r.update(subject_close_calls=2))
 def test_query_retry(self):self.reject(lambda r,l,p:r.update(application_statement_retries=1))
 def test_fake_main_scope(self):self.reject(lambda r,l,p:r.update(Source_Main_or_HTTP_runs=1))
 def test_fake_max_pool(self):self.reject(lambda r,l,p:r.update(max_connections=2))
 def test_missing_barrier(self):self.reject(lambda r,l,p:r["actual_after_release_barrier"].update(hook_entered_after_initial_closed_check=False))
 def test_sleep_ordering(self):self.reject(lambda r,l,p:r["actual_after_release_barrier"].update(no_sleep_as_ordering_authority=False))
 def test_cross_row_change(self):self.reject(lambda r,l,p:r["after"]["tables"]["core_systemnotification"].append({"id":777}))
 def test_sequence_change(self):self.reject(lambda r,l,p:r["after"]["sequences"]["core_systemnotification"].update(last_value=999))
 def test_missing_ledger(self):self.reject(lambda r,l,p:l.clear())
 def test_lost_sqlx_socket_identity(self):self.reject(lambda r,l,p:l[-1].update(old_SQLx_socket_identity=[]))
 def test_claim_baseline_green(self):self.reject(lambda r,l,p:r.update(first_close_contract_passed=True,passed=True),"baseline")
 def test_peer_forced_kill(self):self.reject(lambda r,l,p:p.update(forced=True,signal=9))
 def test_peer_unjoined_pipe(self):self.reject(lambda r,l,p:p.update(stdout_reader_joined=False))
 def test_last_handle_cannot_waive(self):self.reject(lambda r,l,p:r.update(pool_drop_cleanup_cannot_waive_first_close_negative=False))
 def test_cleanup_backend_present(self):self.reject(lambda r,l,p:r["cleanup_after_last_pool_handle_drop"].update(actual_backend_absent=[123]))
if __name__=="__main__":unittest.main()
