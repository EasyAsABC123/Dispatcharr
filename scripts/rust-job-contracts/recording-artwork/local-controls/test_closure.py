"""Socket/process-free host admission and cross-file closure controls."""
import ast,copy,hashlib,importlib.util,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'));import run_source as host
class Closure(unittest.TestCase):
 def test_exact_guest_native_and_host_required_closure(self):
  self.assertEqual(host.REQUIRED,{p.name for p in(ROOT/'tools').iterdir()if p.is_file()and p.suffix in('.py','.json')})
  for p in(ROOT/'tools').glob('*.py'):ast.parse(p.read_text())
 def test_unauthorized_never_reaches_docker_or_output(self):
  with tempfile.TemporaryDirectory()as td:
   p=Path(td);inputs=p/'inputs.json';inputs.write_text(json.dumps({'authorized':False}));output=p/'output'
   with patch.object(sys,'argv',['run_source.py','--repo',str(p),'--support-root',str(p/'support'),'--output',str(output),'--execution-inputs',str(inputs)]),patch.object(host.shutil,'which',side_effect=AssertionError('Docker preflight must not be reached')):
    with self.assertRaisesRegex(ValueError,'ExplicitRootParentedPairAuthority'):host.main()
   self.assertFalse(output.exists())
 def test_six_safe_exports_are_exhaustive(self):self.assertEqual(set(host.SAFE),{'PATH','DOCKER_HOST','DOCKER_CONFIG','CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN'})
 def test_hidden_inputs_stripped(self):
  with patch.dict(host.os.environ,{'GIT_INDEX_FILE':'wrong','DATABASE_URL':'wrong','BENCHMARK_FAKE':'wrong','FIXTURE_FAKE':'wrong','CELERY_BROKER_URL':'wrong'}):
   actual=host.environment();self.assertFalse(any(k in actual for k in ['GIT_INDEX_FILE','DATABASE_URL','BENCHMARK_FAKE','FIXTURE_FAKE','CELERY_BROKER_URL']))
 def test_cleanup_clock_has_no_reset_or_extension(self):
  self.assertTrue(host.cleanup_budget_status(0,90)['within_budget']);self.assertFalse(host.cleanup_budget_status(0,90.0001)['within_budget']);self.assertFalse(host.cleanup_budget_status(0,89,91)['within_budget'])
 def test_exact_cid_absence_is_case_insensitive_but_not_generic(self):
  cid='a'*64;self.assertTrue(host.exact_absent_cid(1,b'[]\n',('Error: No such object: '+cid).encode(),cid));self.assertFalse(host.exact_absent_cid(1,b'[]',('error: no such object: '+'b'*64).encode(),cid));self.assertFalse(host.exact_absent_cid(1,b'',b'permission denied',cid));self.assertFalse(host.exact_absent_cid(0,b'',b'',cid))
 def test_host_and_guest_cannot_revert_to_Source_only_acceptor(self):
  tree=ast.parse((ROOT/'tools/source_configured.py').read_text());last=(ROOT/'tools/source_configured.py').read_text().split("if not report['passed']")[-1];self.assertIn('from pair_contract import validate',last);self.assertNotIn('validate_source_capture(0,report)',last);self.assertIn("report['Source_verified']=False",last)
  self.assertIn("report.update(passed=True,Source_verified=False,Source_reference_verified=True,Native_verified=True",(ROOT/'tools/run_source.py').read_text())
 def test_repeat_producer_is_unchanged_captured_positional_list(self):
  text=(ROOT/'tools/source_configured.py').read_text();self.assertIn('epg_tasks.refresh_epg_data.apply_async(args=[source.id],task_id=root_b)',text);self.assertNotIn('refresh_epg_data.apply_async(kwargs=',text)
 def test_observed_pid_identity_must_exist_before_capture(self):
  self.assertEqual(host.observed_pid_record('a'*64,123,'456'),{'CID':'a'*64,'pid':123,'starttime':'456'})
 def test_missing_original_identity_can_never_be_marked_absent(self):
  for start in [None,'','not-observed','0']:
   with self.assertRaisesRegex(ValueError,'ObservedOriginalHostPIDStartRequired'):host.observed_pid_record('a'*64,123,start)
 def test_pid_and_container_identity_are_typed(self):
  for pid in [True,None,'123',0,-1]:
   with self.assertRaises(ValueError):host.observed_pid_record('a'*64,pid,'456')
  with self.assertRaises(ValueError):host.observed_pid_record('unknown',123,'456')
 def test_no_native_Source_prerun_or_signal_fabrication(self):
  text=(ROOT/'tools/source_configured.py').read_text();self.assertIn("'Source_task_prerun_fabricated':False",text);self.assertIn("native_artwork_phase",text);self.assertNotIn('before_task_publish.send',text);self.assertNotIn('task_prerun.send',text)
if __name__=='__main__':unittest.main()
