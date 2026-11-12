"""All eight peer findings and trace-specificity; no application execution."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import validate as module
from validate import HERE,origin,validate

def omit_media(data,mode):
 snapshots=[r['origin']for r in data['summary']['backends'][mode]['checkpoints']if'origin'in r]+[data['summary'][mode+'_origin_terminal']]
 for o in snapshots:
  remove=[r for r in o['requests']if r['kind']=='stream_GET'][-1];rid,cid=remove['id'],remove['connection'];o['requests'].remove(remove);connection=next(c for c in o['connections']if c['id']==cid);o['connections'].remove(connection)
  for r in o['requests']:
   if r['id']>rid:r['id']-=1
   if r['connection']>cid:r['connection']-=1
  for c in o['connections']:
   if c['id']>cid:c['id']-=1
  o['accepted_TCP_total']-=1;o['actual_open_TCP']=sum(not c['socket_closed']for c in o['connections']);o['actual_stream_GET_inflight']=sum(r['kind']=='stream_GET'and r['in_flight']for r in o['requests'])
class Controls(unittest.TestCase):
 def setUp(self):self.data=json.loads((HERE/'retained.json').read_text());self.cleanup=json.loads((HERE/'independent-cleanup.json').read_text())
 def check(self):return validate(self.data,self.cleanup)
 def test_each_backend_observation_before_phase_rejected(self):
  for mode in ['source','native']:
   self.setUp();self.data['summary']['backends'][mode]['checkpoints'][0].update(origin_observation_began=-2,origin_observation_completed=-1)
   with self.assertRaisesRegex(ValueError,'PhaseRelativeObservationClock'):self.check()
 def test_each_backend_zero_or_short_nonfinite_elapsed_rejected(self):
  for mode in ['source','native']:
   for value in [0,-1,float('nan'),1]:
    self.setUp();self.data['summary']['backends'][mode]['elapsed_seconds']=value
    with self.assertRaisesRegex(ValueError,'ElapsedCoversObservedSpan'):self.check()
 def test_all_source_signals_before_requests_rejected(self):
  for i,row in enumerate(self.data['summary']['backends']['source']['Source_signal_rows']):row['monotonic']=-100+i
  with self.assertRaisesRegex(ValueError,'PhaseRelativeSignalClock'):self.check()
 def test_sequence_schema_empty_or_missing_even_when_both_audits_equal(self):
  for change in ['empty','missing','partial','bad_type']:
   self.setUp()
   for key in ['default_before','default_after']:
    d=self.data['summary'][key]
    if change=='empty':d['sequences']={}
    elif change=='missing':d.pop('sequences')
    elif change=='partial':d['sequences'].pop(next(iter(d['sequences'])))
    else:d['sequences'][next(iter(d['sequences']))]['is_called']='false'
   with self.assertRaises(ValueError):self.check()
 def test_consistent_media_removal_rejected_by_retained_authority_each_backend(self):
  for mode in ['source','native']:
   self.setUp();omit_media(self.data,mode)
   with self.assertRaisesRegex(ValueError,'RetainedTerminalCoverage'):self.check()
 def test_one_upstream_structurally_permitted_but_not_this_retained_trace(self):
  # A future independently reviewed authority may legitimately describe one
  # upstream with two clients. No media-count==client-count rule is imposed.
  terminal=copy.deepcopy(self.data['summary']['source_origin_terminal']);request=next(r for r in terminal['requests']if r['kind']=='stream_GET');connection=next(c for c in terminal['connections']if c['id']==request['connection']);request['id']=1;request['connection']=1;connection['id']=1;connection['requests']=1;terminal.update(requests=[request],connections=[connection],events=[],accepted_TCP_total=1,actual_open_TCP=0,actual_stream_GET_inflight=0,media_HEAD_total=0,control_total=0,transient_TCP_without_request=0)
  self.assertEqual(len(origin(terminal,settled=True)['media_GET']),1)
  self.data['summary']['source_origin_terminal']=terminal
  with self.assertRaisesRegex(ValueError,'RetainedTerminalCoverage'):self.check()
 def test_coverage_authority_file_itself_hash_bound(self):
  with tempfile.TemporaryDirectory()as tmp:
   p=Path(tmp);(p/'origin-coverage-authority.json').write_text('{}')
   with patch.object(module,'HERE',p):
    with self.assertRaisesRegex(ValueError,'ImmutableOriginCoverageAuthority'):self.check()
 def test_actual_original_still_qualifies_without_reexecution(self):
  result=self.check();self.assertTrue(result['characterization_qualified']);self.assertTrue(result['retained_origin_authority_checked']);self.assertFalse(result['runtime_or_application_rerun'])
if __name__=='__main__':unittest.main(verbosity=2)
