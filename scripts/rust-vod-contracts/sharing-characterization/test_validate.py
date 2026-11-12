"""Offline coverage/corruption controls only; no application/network execution."""
import copy,json,unittest
from validate import HERE,validate
class Controls(unittest.TestCase):
 def setUp(self):self.data=json.loads((HERE/'retained.json').read_text());self.cleanup=json.loads((HERE/'independent-cleanup.json').read_text())
 def check(self):return validate(self.data,self.cleanup)
 def fail(self,change):
  self.setUp();change(self.data,self.cleanup)
  with self.assertRaises((ValueError,KeyError,TypeError)):self.check()
 def test_actual_complete_characterization(self):
  r=self.check();self.assertTrue(r['characterization_qualified']);self.assertFalse(r['full_M4_acceptance'])
  for p in r['phase_results'].values():self.assertEqual(p['media_GET_count'],2);self.assertEqual(p['literal_nonrange_200'],[True,True]);self.assertEqual(p['full_delivery'],[False,False]);self.assertFalse(p['two_people_inferred'])
 def test_missing_or_duplicate_clients_each_backend(self):
  for mode in ['source','native']:
   for change in ['empty','missing','duplicate']:
    self.setUp();v=self.data['summary']['backends'][mode]
    if change=='empty':v['clients']=[]
    elif change=='missing':v['clients'].pop()
    else:v['clients'][1]['role']='first'
    with self.assertRaises(ValueError):self.check()
 def test_mandatory_decoders_and_files_each_backend(self):
  for mode in ['source','native']:
   for field in ['decoders','files']:
    for missing in ['all','first','second']:
     self.setUp();target=self.data['summary']['backends'][mode]['decoders']if field=='decoders'else self.data['retained_files'][mode]
     if missing=='all':target.clear()
     else:target.pop(missing)
     with self.assertRaises(ValueError):self.check()
 def test_reader_roles_progress_eof_and_retention(self):
  for mode in ['source','native']:
   for change in ['missing','flat','eof','closed','cap','no_snapshots']:
    self.setUp();phase=self.data['summary']['backends'][mode];state=phase['checkpoints'][1]['readers']['first']
    if change=='missing':phase['checkpoints'][1]['readers'].pop('second')
    elif change=='flat':state['received']=phase['checkpoints'][0]['readers']['first']['received'];state['retained']=state['received']
    elif change=='eof':state['eof']=True
    elif change=='closed':state['socket_fd']=-1
    elif change=='cap':state['cap']=2
    else:phase['checkpoints']=[]
    with self.assertRaises(ValueError):self.check()
 def test_missing_origin_records_each_backend(self):
  for mode in ['source','native']:
   for field in ['connections','requests']:
    self.setUp();self.data['summary'][mode+'_origin_terminal'][field]=[]
    with self.assertRaises(ValueError):self.check()
 def test_origin_ids_references_totals_and_epochs(self):
  for change in ['id','reference','total','bool','epoch']:
   self.setUp();o=self.data['summary']['native_origin_terminal']
   if change=='id':o['connections'][0]['id']=999
   elif change=='reference':o['requests'][0]['connection']=999
   elif change=='total':o['accepted_TCP_total']+=1
   elif change=='bool':o['media_HEAD_total']=False
   else:o['epoch']=self.data['summary']['source_origin_terminal']['epoch']
   with self.assertRaises(ValueError):self.check()
 def test_literal_header_status_bodyreturn_and_shutdown_coverage(self):
  for field in ['request_headers','response_headers','response_status','serve_file_return']:
   self.setUp();o=self.data['summary']['source_origin_terminal'];r=next(r for r in o['requests']if r['kind']=='stream_GET');r.pop(field)
   with self.assertRaises(ValueError):self.check()
  self.fail(lambda d,c:d['summary']['native_origin_terminal']['connections'][0].update(fd_after=8))
 def test_real_times_deadlines_and_both_close_starts(self):
  for change in ['start','snapshot','teardown','second_close','absolute']:
   self.setUp();p=self.data['summary']['backends']['native']
   if change=='start':p['second_dispatch_actual']+=1
   elif change=='snapshot':p['checkpoints'][0]['at']+=1
   elif change=='teardown':p['teardown_began']+=1
   elif change=='second_close':p['client_closes']['second']['close_begin']+=1
   else:p['clients'][0]['absolute_observation']['completed']=p['clients'][0]['absolute_observation']['deadline']+1
   with self.assertRaises(ValueError):self.check()
 def test_missing_decoder_frame_pipe_close_and_auth_witnesses(self):
  for change in ['frames','pipes','close','auth','caps']:
   self.setUp();p=self.data['summary']['backends']['source']
   if change=='frames':p['decoders']['first']['frames']=49
   elif change=='pipes':p['decoders']['second']['pipes_settled']=False
   elif change=='close':p['client_closes'].pop('first')
   elif change=='auth':p['actual_SQL_Bearer_verified']=False
   else:p['caps_after']['profile']=1
   with self.assertRaises(ValueError):self.check()
 def test_signal_identity_and_missing_finalization(self):
  for change in ['empty','duplicate','wrong_sid']:
   self.setUp();p=self.data['summary']['backends']['source']
   if change=='empty':p['Source_signal_rows']=[]
   elif change=='duplicate':p['Source_signal_rows'][1]['request']=p['Source_signal_rows'][0]['request']
   else:p['Source_signal_rows'][0]['sid_sha256']='0'*64
   with self.assertRaises(ValueError):self.check()
 def test_cleanup_audit_candidate_and_flag_tampering(self):
  self.fail(lambda d,c:c['remaining'].update(containers='live'))
  self.fail(lambda d,c:d['summary']['default_after'].update(dataset_sha256='changed'))
  self.fail(lambda d,c:d['summary'].update(actual_Main_image_id='wrong'))
  self.fail(lambda d,c:d['summary'].update(sharing_characterization_qualified=False))
 def test_counter_is_literal_not_expected_value_and_must_be_captured(self):
  p=self.data['summary']['backends']['source']['checkpoints'][0];p['literal_profile_counter']=None
  self.assertIsNone(self.check()['phase_results']['source']['literal_profile_counters'][0]['value'])
  for value in [True,'bad']:
   self.setUp();self.data['summary']['backends']['source']['checkpoints'][0]['literal_profile_counter']=value
   with self.assertRaises(ValueError):self.check()
  self.setUp();self.data['summary']['backends']['source']['checkpoints'][0].pop('literal_profile_counter')
  with self.assertRaises(ValueError):self.check()
 def test_distinct_errors_not_normalized_or_assigned_to_people(self):
  r=self.check()['phase_results'];self.assertEqual(r['source']['write_errors'],[['ConnectionResetError'],['ConnectionResetError']]);self.assertEqual(r['native']['write_errors'],[['BrokenPipeError'],['BrokenPipeError']]);self.assertEqual(r['source']['upstream_to_client_association'],'unresolved')
if __name__=='__main__':unittest.main(verbosity=2)
