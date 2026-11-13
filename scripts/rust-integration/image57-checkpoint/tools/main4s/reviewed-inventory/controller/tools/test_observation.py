"""Offline4s/signal controls. No Django/Source app, socket server or Docker executes."""
import copy,json,tempfile,threading,unittest
from pathlib import Path
from unittest import mock
import observation
from request_observer import Journal,Observer,validate
class Tests(unittest.TestCase):
 def test_valid_capture503_is_not_parity_pass(self):
  s={'observation_valid':True,'target_outcome':{'status':200}};n={'observation_valid':True,'target_outcome':{'status':503,'error_body_hex':'00'}}
  self.assertEqual(observation.compare(s,n),{'observation_valid':True,'parity_passed':False,'status_equal':False,'Source_status':200,'native_status':503,'full_M4_claim':False})
 def test_matching_outcome_and_invalid_capture_separate(self):
  s={'observation_valid':True,'target_outcome':{'status':429,'error_body_hex':'dead'}};self.assertTrue(observation.compare(s,copy.deepcopy(s))['parity_passed'])
  n=copy.deepcopy(s);n['target_outcome']['error_body_hex']='beef';self.assertFalse(observation.compare(s,n)['parity_passed']);n['observation_valid']=False;self.assertFalse(observation.compare(s,n)['observation_valid'])
 def test_four_seconds_does_not_reuse_or_expand_old_policy(self):
  self.assertEqual(observation.hold(10,12,14.01)['target_seconds'],4)
  for values in[(10,12,12.4),(10,12,14.4),(10,13.2,14.01),(10,float('nan'),14)]:
   with self.assertRaises(ValueError):observation.hold(*values)
 def test_checkpoint3_1_offset_and_overshoot(self):
  observation.at_checkpoint(13.15,10,3.1)
  for actual in[13,13.5]:
   with self.assertRaises(ValueError):observation.at_checkpoint(actual,10,3.1)
 def fixture(self,d,**kwargs):
  j=Journal(Path(d)/'rows',Path(d)/'invalid',**kwargs);return j,Observer(j,threading.local(),{'/owned/sid':{'owned-agent':'target'}})
 def test_signal_correlation_and_sensitive_fields_never_persist(self):
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d);o.started(environ={'PATH_INFO':'/owned/sid','HTTP_USER_AGENT':'owned-agent','HTTP_AUTHORIZATION':'secret','HTTP_COOKIE':'secret','QUERY_STRING':'secret'});o.finished();raw=j.path.read_text();self.assertNotIn('secret',raw);rows=[json.loads(x)for x in raw.splitlines()];self.assertTrue(validate(rows,['target'])['valid']);self.assertFalse(j.invalid)
 def test_unowned_requests_ignored_and_missing_finish_not_zero(self):
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d);o.started(environ={'PATH_INFO':'/other','HTTP_USER_AGENT':'owned-agent'});o.finished();self.assertEqual(j.rows,0)
   o.started(environ={'PATH_INFO':'/owned/sid','HTTP_USER_AGENT':'owned-agent'})
   with self.assertRaisesRegex(ValueError,'MissingSignalCompletion'):validate([json.loads(j.path.read_text())],['target'])
 def test_duplicate_finished_and_overlap_sticky_invalid(self):
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d);o.finished();self.assertIn('UncorrelatedRequestFinished',j.snapshot()['invalid']);self.assertTrue(j.marker.exists())
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d);env={'PATH_INFO':'/owned/sid','HTTP_USER_AGENT':'owned-agent'};o.started(environ=env);o.started(environ=env);self.assertIn('OverlappingSignalContext',j.invalid)
 def test_signal_capacity_stops_retention_even_when_dispatch_ignores_receiver(self):
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d,max_rows=1);env={'PATH_INFO':'/owned/sid','HTTP_USER_AGENT':'owned-agent'}
   # Receiver never propagates failure; framework-level execution is a future gate.
   o.started(environ=env);o.finished();self.assertEqual(j.rows,1);self.assertIn('SignalJournalCapacity',j.snapshot()['invalid']);self.assertTrue(j.marker.exists())
 def test_byte_cap_and_write_denial_visible_via_memory_status(self):
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d,max_bytes=1);o.started(environ={'PATH_INFO':'/owned/sid','HTTP_USER_AGENT':'owned-agent'});self.assertEqual(j.used,0);self.assertTrue(j.invalid)
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d)
   with mock.patch.object(Path,'open',side_effect=PermissionError):o.started(environ={'PATH_INFO':'/owned/sid','HTTP_USER_AGENT':'owned-agent'})
   self.assertIn('SignalJournalWrite',j.snapshot()['invalid']);self.assertEqual(j.rows,0)
 def test_duplicate_role_and_wrong_context_cannot_validate(self):
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d);env={'PATH_INFO':'/owned/sid','HTTP_USER_AGENT':'owned-agent'};o.started(environ=env);o.finished();rows=[json.loads(x)for x in j.path.read_text().splitlines()];bad=copy.deepcopy(rows);bad[-1]['sid_sha256']='x'*64
   with self.assertRaises(ValueError):validate(bad,['target'])
   duplicate=[{**x,'request':2}for x in rows]
   with self.assertRaisesRegex(ValueError,'SignalRoleCoverage'):validate(rows+duplicate,['target'])
 def test_concurrent_thread_contexts_have_distinct_identity(self):
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d);barrier=threading.Barrier(2)
   def task():o.started(environ={'PATH_INFO':'/owned/sid','HTTP_USER_AGENT':'owned-agent'});barrier.wait();o.finished()
   jobs=[threading.Thread(target=task)for _ in range(2)]
   for t in jobs:t.start()
   for t in jobs:t.join(2);self.assertFalse(t.is_alive())
   rows=[json.loads(x)for x in j.path.read_text().splitlines()];self.assertEqual(len({x['request']for x in rows}),2);self.assertFalse(j.invalid)
 def test_cleanup_or_default_audit_failure_cannot_be_overwritten_as_pass(self):
  value={'observation_valid':True,'parity_passed':True,'Source_reference_expectation_matched':True,'remaining':{'containers':'','networks':'','volumes':''},'default_named_tables_sequences_unchanged':True,'Source_signal_terminal_valid':True,'proxy_terminal':{'invalid':[],'connections':0,'seen':1},'Source_proxy_terminal':{'invalid':[],'connections':0,'seen':1}}
  self.assertTrue(observation.finalize(value)['passed'])
  for field in['default_named_tables_sequences_unchanged','proxy_terminal','Source_proxy_terminal','remaining']:
   bad=copy.deepcopy(value)
   if field=='remaining':bad[field]['containers']='left'
   elif field=='default_named_tables_sequences_unchanged':bad[field]=False
   else:bad[field]['seen']=2
   self.assertFalse(observation.finalize(bad)['observation_valid'])
  value['parity_passed']=False;self.assertTrue(observation.finalize(value)['observation_valid']);self.assertFalse(observation.finalize(value)['passed'])
 def test_sticky_invalid_retains_no_further_journal_rows(self):
  with tempfile.TemporaryDirectory()as d:
   j,o=self.fixture(d);j.fail('InjectedWriterFailure');before=j.snapshot();self.assertFalse(j.append({'anything':'ignored'}));self.assertEqual(j.rows,before['rows']);self.assertEqual(j.used,before['bytes']);self.assertFalse(j.path.exists())
 def test_late_failure_or_truncated_journal_rejects_prior_complete_coverage(self):
  rows=[{}];validator=mock.Mock(return_value={'valid':True})
  self.assertTrue(observation.signal_evidence({'invalid':[],'rows':1},rows,['target'],validator)['valid'])
  for status in[{'invalid':['LateFailure'],'rows':1},{'invalid':[],'rows':2},{'invalid':[],'rows':True}]:
   with self.assertRaisesRegex(ValueError,'SignalStatusJournalMismatch'):observation.signal_evidence(status,rows,['target'],validator)
  report={'observation_valid':True,'parity_passed':True,'Source_reference_expectation_matched':True,'remaining':{},'default_named_tables_sequences_unchanged':True,'proxy_terminal':{'invalid':[],'connections':0,'seen':1},'Source_proxy_terminal':{'invalid':[],'connections':0,'seen':1}}
  self.assertFalse(observation.finalize(report)['observation_valid']);report['Source_signal_terminal_valid']=False;self.assertFalse(observation.finalize(report)['observation_valid'])
 def test_missing_inventory_or_boolean_proxy_count_never_zero(self):
  value={'observation_valid':True,'parity_passed':True,'Source_reference_expectation_matched':True,'remaining':{'containers':'','networks':'','volumes':''},'Source_signal_terminal_valid':True,'default_named_tables_sequences_unchanged':True,'proxy_terminal':{'invalid':[],'connections':0,'seen':1},'Source_proxy_terminal':{'invalid':[],'connections':0,'seen':1}}
  self.assertTrue(observation.finalize(value)['passed'])
  for remaining in[{},None,{'containers':'','networks':''}]:
   bad=copy.deepcopy(value);bad['remaining']=remaining;self.assertFalse(observation.finalize(bad)['observation_valid'])
  for field,wrong in[('seen',True),('connections',False),('invalid',None)]:
   bad=copy.deepcopy(value);bad['proxy_terminal'][field]=wrong;self.assertFalse(observation.finalize(bad)['observation_valid'])
 def test_controller_still_hardblocked_and_no_production_view_wrapping(self):
  root=Path(__file__).parent;text=(root/'run.py').read_text();self.assertIn("raise RuntimeError('IMAGE57_MAIN4S_EXECUTION_BLOCKED",text)
  wsgi=(root/'source_wsgi.py').read_text();self.assertIn("WSGIServer(('127.0.0.1',5657),application",wsgi);self.assertNotIn('application=',wsgi);self.assertNotIn('client.incr=',wsgi)
if __name__=='__main__':unittest.main(verbosity=2)
