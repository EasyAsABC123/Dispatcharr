"""Finite metadata/file corruption controls, no sockets/apps/workloads."""
import ast,hashlib,json,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'tools'));import run_source
class Controls(unittest.TestCase):
 def test_all_required_imports_mounted_and_bound(self):
  inputs=json.loads(Path(json.loads((ROOT/'once-plan.json').read_text())['commands'][0]['proposed_inputs']).read_text());self.assertEqual(set(inputs['tools_sha256']),run_source.REQUIRED);self.assertEqual(len(run_source.REQUIRED),24)
  for name,h in inputs['tools_sha256'].items():self.assertEqual(hashlib.sha256((ROOT/'tools'/name).read_bytes()).hexdigest(),h)
 def test_case_mode_before_fixture(self):
  text=(ROOT/'tools/run_source.py').read_text();self.assertLess(text.index('ExactFixedInputModeBeforeApp'),text.index("out.mkdir(parents=True"));self.assertIn("'DVR_PHYSICAL_MODE='+a.mode",text)
 def test_each_fresh_output_unique_and_not_consumed(self):
  rows=json.loads((ROOT/'once-plan.json').read_text())['commands'];self.assertEqual(len(rows),2);self.assertEqual(len({r['output']for r in rows}),2);self.assertTrue(all(not Path(r['output']).exists()for r in rows))
 def test_all_once_modes_match_exact_actual_parser_choices(self):
  spec=ast.parse((ROOT/'tools/run_source.py').read_text());choices=next(ast.literal_eval(k.value)for n in ast.walk(spec)if isinstance(n,ast.Call)and any(isinstance(x,ast.Constant)and x.value=='--mode'for x in n.args)for k in n.keywords if k.arg=='choices');self.assertEqual(list(choices)[5:],[r['mode']for r in json.loads((ROOT/'once-plan.json').read_text())['commands']])
 def test_missing_pid_identity_refuses(self):
  for value in [None,'',0,True,'0','abc']:
   with self.assertRaises(ValueError):run_source.observed_pid_record('a'*64,123,value)
 def test_wrong_cid_error_not_absence(self):self.assertFalse(run_source.exact_absent_cid(1,b'',('Error: No such object: '+'b'*64).encode(),'a'*64))
 def test_failed_query_not_empty(self):self.assertFalse(run_source.exact_absent_cid(1,b'',b'permission denied','a'*64))
 def test_old_uppercase_actual_absence_case_safe(self):self.assertTrue(run_source.exact_absent_cid(1,b'[]',('Error: No such object: '+'a'*64).encode(),'a'*64))
 def test_cleanup_budget_cannot_reset_elapsed(self):self.assertFalse(run_source.cleanup_budget_status(1,92)['within_budget']);self.assertTrue(run_source.cleanup_budget_status(1,91)['within_budget'])
 def test_current_QA2_changes_only_test_peer_from_full_defaults_preflight(self):
  current=json.loads((ROOT/'current-union-authority/QA2-source-inputs.json').read_text());old=json.loads((ROOT/'current-union-authority/preflight-source-inputs.json').read_text());self.assertEqual(set(current),set(old));self.assertEqual({n for n in current if current[n]!=old[n]},{'rust/crates/job-transport/src/recording_dvr_physical_peer.rs'})
 def test_node_controls_real_kernel_records_not_latency_authority(self):
  data=json.loads((ROOT/'socket-controls.json').read_text());self.assertEqual(data['actual_exit'],0);self.assertEqual(data['tests_run'],6);self.assertEqual(len(data['actual_owned_socket_readbacks']),4);self.assertTrue(all(type(r['actual_TCP_NODELAY_after'])is int and r['actual_TCP_NODELAY_after']==1 for r in data['actual_owned_socket_readbacks']))
 def test_current_exact_ELF_file_bind_has_legacy_guest_name(self):
  text=(ROOT/'tools/run_source.py').read_text();self.assertIn("str(Path(native['elf_path']))+':/native-peer/native-job-tests:ro'",text);self.assertNotIn("str(Path(native['elf_path']).parent)+':/native-peer:ro'",text)
 def test_fixed_compiled_writer3600(self):
  data=json.loads((ROOT/'tools/redis-policy.json').read_text());self.assertEqual(data['actual_SETEX_seconds'],3600);self.assertIn("redis.call('SETEX',KEYS[1],3600,ARGV[1])",data['result_write']['script'])
if __name__=='__main__':unittest.main()
