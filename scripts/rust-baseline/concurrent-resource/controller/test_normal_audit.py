import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import normal_audit
class NormalAuditControls(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  for name in normal_audit.KINDS:(self.root/('workflow-'+name+'.jsonl')).write_bytes(b'{}\n')
  (self.root/'worker-observer-budget.json').write_text(json.dumps({'records':3,'bytes':9}))
  (self.root/'workflow-controller-private.stdout').write_bytes(b'ab');(self.root/'workflow-controller-private.stderr').write_bytes(b'cd')
  self.collector=SimpleNamespace(physically_settled=True,process=SimpleNamespace(poll=lambda:0),threads=[SimpleNamespace(is_alive=lambda:False)],writer=None,used=4,invalid=None)
 def test_normal_bounded_terminal_positive(self):self.assertTrue(normal_audit.inspect(self.root,self.collector)['physical_process_and_pipes_settled'])
 def test_swallowed_signal_persistent_marker_invalidates(self):
  (self.root/'worker-observer-capacity.json').write_text('{}')
  with self.assertRaisesRegex(ValueError,'Persistent'):normal_audit.inspect(self.root,self.collector)
 def test_journal_accounting_mismatch_fails(self):
  (self.root/'worker-observer-budget.json').write_text(json.dumps({'records':3,'bytes':8}))
  with self.assertRaisesRegex(ValueError,'Bytes'):normal_audit.inspect(self.root,self.collector)
 def test_live_pipe_never_reports_drained(self):
  self.collector.threads=[SimpleNamespace(is_alive=lambda:True)]
  with self.assertRaisesRegex(ValueError,'Physical'):normal_audit.inspect(self.root,self.collector)
 def test_stdout_stderr_share_one_budget(self):
  self.collector.used=2
  with self.assertRaisesRegex(ValueError,'Output'):normal_audit.inspect(self.root,self.collector)
 def test_no_resource_mode_or_unblock(self):
  text=Path(__file__).with_name('run.py').read_text();self.assertIn('RESOURCE_EXECUTION_BLOCKED=True',text);self.assertIn("choices=[1,2,3]",text);self.assertIn("raise RuntimeError('UnvalidatedBoundedCollectorExecutionBlocked')",text)
if __name__=='__main__':unittest.main()
