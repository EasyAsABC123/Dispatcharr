"""Offline public closure/corruption controls; never execute Source or Docker."""
import copy,json,shutil,tempfile,unittest
from pathlib import Path
import validate,restore_execution
HERE=Path(__file__).resolve().parent
class PublicControls(unittest.TestCase):
 def setUp(self):
  root=HERE/'evidence';self.trace=[json.loads(x)for x in(root/'source-causal.jsonl').read_text().splitlines()];self.wire=[json.loads(x)for x in(root/'source-wire.jsonl').read_text().splitlines()];self.terminal=json.loads((root/'terminal.json').read_text());self.proxy=json.loads((root/'proxy.json').read_text());self.signals=[json.loads(x)for x in(root/'request-signals.jsonl').read_text().splitlines()];self.phase=json.loads((root/'phase.json').read_text());self.metadata=json.loads((root/'metadata.json').read_text())
 def analyze(self):
  self.terminal.update(rows=len(self.trace),bytes=sum(len((json.dumps(row,separators=(',',':'))+'\n').encode())for row in self.trace));self.proxy['journal'].update(rows=len(self.wire),bytes=sum(len((json.dumps(row,separators=(',',':'))+'\n').encode())for row in self.wire));self.metadata.update(trace_rows=len(self.trace),wire_rows=len(self.wire));return validate.analyze(self.trace,self.wire,self.terminal,self.proxy,self.signals,self.phase,self.metadata)
 def test_public_historical_recomputation(self):
  result=validate.validate(HERE/'evidence');self.assertTrue(result['valid']);self.assertEqual((result['before_client_shutdown'],result['after_client_shutdown_before_generator_finally']),(56,1))
 def test_raw_byte_corruption_rejected_by_evidence_seal(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory)/'evidence';shutil.copytree(HERE/'evidence',root);(root/'source-wire.jsonl').write_bytes((root/'source-wire.jsonl').read_bytes()+b' ')
   with self.assertRaisesRegex(ValueError,'EvidenceHash'):validate.validate(root)
 def test_empty_writes_never_vacuously_qualify(self):
  self.wire=[]
  with self.assertRaisesRegex(ValueError,'MissingActualBusyINCRReference'):self.analyze()
 def test_sticky_invalid_or_incomplete_proxy_rejected(self):
  self.terminal['invalid']=['overflow']
  with self.assertRaisesRegex(ValueError,'IncompleteTraceOwnershipOrStatus'):self.analyze()
 def test_missing_admin_action_invalidates_causal_coverage(self):
  self.trace=[row for row in self.trace if row['kind']!='admin_action_finalized']
  with self.assertRaisesRegex(ValueError,'DistinctAdminActionCoverage'):self.analyze()
 def test_ambiguous_actual_zero_caller_rejected(self):
  frame=validate.validate(HERE/'evidence')['zero_writer']['wire_call_frame'];copied=[row for row in self.trace if row.get('frame')==frame and row['kind']in['call','normal_exit','wire_link']]
  for row in copied:
   row=copy.deepcopy(row);row['frame']=99999;self.trace.append(row)
  with self.assertRaisesRegex(ValueError,'IncompleteWireCallerAttribution'):self.analyze()
 def test_unknown_writer_cannot_hide_owned_actual_zero(self):
  for row in self.trace:
   if row['kind']=='call'and row.get('frame')==149:row['rule_kind']='function'
  with self.assertRaisesRegex(ValueError,'IncompleteWireCallerAttribution'):self.analyze()
 def test_historical_zero_creator_corruption_rejected(self):
  for row in self.trace:
   if row['kind']=='call'and row.get('frame')==149:row['generator']=999
  with self.assertRaisesRegex(ValueError,'HistoricalZeroWriterIdentity'):self.analyze()
 def test_explicit_client_shutdown_boundary_not_generator_completion(self):
  for row in self.phase['client_closes']:
   if row['role']=='other':row['close_begin']=0
  with self.assertRaisesRegex(ValueError,'HistoricalGeneratorProgressBoundaries'):self.analyze()
 def test_boolean_fd_is_not_integer_close_ack(self):
  self.phase['client_closes'][0]['fd_after']=False
  with self.assertRaisesRegex(ValueError,'PhysicalClientShutdown'):self.analyze()
 def test_admin200_alone_does_not_substitute_stop_key(self):
  self.phase['early_stop_witness']['stop']=None
  with self.assertRaisesRegex(ValueError,'LiteralEarlyStopOrMetadata'):self.analyze()
 def test_missing_both_generation_markers_is_not_preservation(self):
  self.phase['early_stop_witness'].update(idle_created_at=None,follower_created_at=None)
  with self.assertRaisesRegex(ValueError,'PresentGenerationMarker'):self.analyze()
 def test_HTTP_layer_not_substituted_by_causal_success(self):
  self.phase['HTTP_cohort_valid']=False
  with self.assertRaisesRegex(ValueError,'HTTPOrAuditLayer'):self.analyze()
 def test_exact_execution_restoration_and_existing_output_rejection(self):
  with tempfile.TemporaryDirectory()as directory:
   out=Path(directory)/'restored';result=restore_execution.restore(out);self.assertEqual(result['files'],86);self.assertFalse(result['source_or_Docker_executed']);self.assertEqual(validate.sha(out/'selection.json'),validate.EXECUTION);self.assertIn('EXECUTION_BLOCKED=False',(out/'controller/run.py').read_text())
   with self.assertRaisesRegex(ValueError,'FreshRestorationOutputRequired'):restore_execution.restore(out)
if __name__=='__main__':unittest.main(verbosity=2)
