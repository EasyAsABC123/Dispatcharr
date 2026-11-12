"""Focused adversarial controls, only local synthetic HTTP; no application pair."""
import copy,hashlib,json,os,socket,tempfile,threading,time,unittest
from pathlib import Path
from normal_guard import validate_claim,process_start
from absolute_http import Observation
from acceptance import phase,pair
class Claim(unittest.TestCase):
 def rows(self):
  now=time.time();cue={'boot_id':'boot','selection_sha256':'selection','issued_epoch':now-2,'expires_epoch':now+100,'new_output':'exact-owned'};claim={'phase':'exclusive_claim_before_dispatch','cue_sha256':'cue','boot_id':'boot','selection_sha256':'selection','pid':os.getpid(),'pid_starttime':process_start(os.getpid()),'claimed_epoch':now-1,'output':'/tmp/exact-owned','repo':'/tmp'};activation={**claim,'nonce':'a'*32,'claim_sha256':'claim'};return cue,claim,activation
 def test_positive_bound_claim(self):
  c,q,a=self.rows();self.assertTrue(validate_claim(c,q,a,'a'*32,'cue','claim'))
 def test_missing_nonce_cannot_equal_none(self):
  c,q,a=self.rows();a.pop('nonce')
  for nonce in[None,'','bad','A'*32]:
   with self.assertRaises(ValueError):validate_claim(c,q,a,nonce,'cue','claim')
 def test_every_claim_identity_required_and_exact(self):
  for key in['phase','cue_sha256','boot_id','selection_sha256','pid','pid_starttime','claimed_epoch','output','repo']:
   c,q,a=self.rows();q.pop(key)
   with self.assertRaises(ValueError):validate_claim(c,q,a,'a'*32,'cue','claim')
  for key,value in [('pid',True),('pid',0),('pid_starttime','0'),('claimed_epoch',float('nan')),('claimed_epoch',time.time()+1000),('claimed_epoch',0),('output','relative'),('output','/tmp/../escape')]:
   c,q,a=self.rows();q[key]=value;a[key]=value
   with self.assertRaises(ValueError):validate_claim(c,q,a,'a'*32,'cue','claim')
 def test_activation_identity_and_claim_hash_required(self):
  for key in['pid','pid_starttime','claimed_epoch','output','boot_id','selection_sha256','cue_sha256','claim_sha256']:
   c,q,a=self.rows();a.pop(key)
   with self.assertRaises(ValueError):validate_claim(c,q,a,'a'*32,'cue','claim')
 def test_once_claim_exclusive_and_symlink_failclosed(self):
  from normal_guard import write_once_claim
  c,q,a=self.rows()
  with tempfile.TemporaryDirectory()as directory:
   path=Path(directory)/'claim';write_once_claim(path,q)
   with self.assertRaises(FileExistsError):write_once_claim(path,q)
   link=Path(directory)/'link';link.symlink_to(Path(directory)/'target')
   with self.assertRaises(FileExistsError):write_once_claim(link,q)
   self.assertFalse((Path(directory)/'target').exists())
 def test_output_cannot_be_rebound(self):
  c,q,a=self.rows();q['output']='/different/exact-owned';a['output']=q['output']
  with self.assertRaises(ValueError):validate_claim(c,q,a,'a'*32,'cue','claim')
class Accept(unittest.TestCase):
 def row(self):return {'eof':False,'invalid':None,'reader_done':False,'socket_fd':9}
 def report(self):
  return {'mode':'native','probe_outcomes':[{'role':'probe1','status':200,'prefix_matches_fixture':True},{'role':'probe2','status':429}],'checkpoints':[{'label':s}for s in ['before_stop','fixed_probe1_boundary','after_probe1','after_probe2','after_decode']],'readers_before_teardown':{'other':self.row(),'probe1':self.row()},'other_at_probe1_dispatch':self.row(),'other_at_probe2_dispatch':self.row(),'other_before_teardown':self.row(),'probe1_at_probe2_dispatch':self.row(),'native_other_grant_preserved':{'preserved':True},'admin_stop':{'status':200},'early_stop_literal':'true'}
 def test_positive_no_status_equality_requirement(self):self.assertTrue(phase(self.report())['scenario_ready'])
 def test_other_eof_error_and_done_fail_normal(self):
  for field,value in [('eof',True),('invalid','lateError'),('reader_done',True),('socket_fd',-1)]:
   r=self.report();r['other_at_probe2_dispatch'][field]=value;self.assertFalse(phase(r)['scenario_ready'])
 def test_admitted_probe1_closed_fails_even_with_good_prefix(self):
  r=self.report();r['probe1_at_probe2_dispatch']['socket_fd']=-1;self.assertFalse(phase(r)['scenario_ready'])
 def test_missing_private_grant_and_checkpoints_incomplete(self):
  for value in [None,False,{}, {'preserved':False}]:
   r=self.report();r['native_other_grant_preserved']=value
   self.assertFalse(phase(r)['scenario_ready'])
  r=self.report();r['checkpoints'][1]['observation_error']='bad';self.assertFalse(phase(r)['HTTP_capture_valid'])
 def test_late_reader_error_not_masked_by_earlier_prefix(self):
  r=self.report();r['readers_before_teardown']['probe1']['invalid']='late';self.assertFalse(phase(r)['HTTP_capture_valid'])
 def test_error_discovered_at_physical_reader_close_is_invalid(self):
  r=self.report();r['client_closes']=[{'snapshot':{'invalid':'wire overflow after last snapshot'}}];self.assertFalse(phase(r)['HTTP_capture_valid'])
 def test_pair_literal_complete_is_not_scenario_pass(self):
  r=self.report();r.update(phase(r));s=copy.deepcopy(r);s['shared_response_finalized_by_probe1']=True;r['scenario_ready']=False
  report={'backends':{'source':s,'native':r},'HTTP_pair_complete':True,'owned_cleanup_valid':True,'default_named_tables_sequences_unchanged':True};v=pair(report);self.assertTrue(v['observation_complete']);self.assertFalse(v['normal_scenario_qualified']);self.assertTrue(v['no_reproduction'])
class Wire(unittest.TestCase):
 def serve(self,payload,delay):
  listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen();listener.settimeout(2);stop=threading.Event();state={}
  def worker():
   peer=None
   try:
    peer,_=listener.accept();peer.settimeout(2);state['accepted']=True;peer.recv(8192)
    for chunk in payload:
     if stop.wait(delay):break
     peer.sendall(chunk)
   except (OSError,TimeoutError):pass
   finally:
    if peer:peer.close()
    listener.close();state['closed']=True
  t=threading.Thread(target=worker);t.start();return listener.getsockname()[1],stop,t,state
 def case(self,payload,delay,seconds,read):
  port,stop,t,state=self.serve(payload,delay);owner=Observation('http://127.0.0.1:'+str(port),seconds=seconds);began=time.monotonic()
  try:
   with self.assertRaises((TimeoutError,OSError,http_client_error())):
    r=owner.request('/',{});read(r);owner.complete()
  finally:
   receipt=owner.cancel();stop.set();t.join(3)
  self.assertLess(time.monotonic()-began,1.5);self.assertFalse(t.is_alive());self.assertTrue(state['closed']);self.assertFalse(owner.timer.is_alive());self.assertEqual(receipt['fd_after'],-1);self.assertTrue(receipt['original_inode_absent'])
 def test_header_drip_cannot_extend_absolute_deadline(self):self.case([bytes([c])for c in b'HTTP/1.1 429 Too Many Requests\r\nX-Long: abcdefghijklmnopqrstuvwxyz\r\n\r\n'],.02,.2,lambda r:None)
 def test_error_body_drip_cannot_extend_deadline(self):self.case([b'HTTP/1.1 429 Busy\r\nContent-Length: 50\r\n\r\n']+[b'x']*50,.02,.2,lambda r:r.raw.read(50))
 def test_cancel_before_connect_joins_and_closes(self):
  owner=Observation('http://127.0.0.1:1',seconds=.2);self.assertEqual(owner.cancel()['fd_after'],-1)
 def test_inflight_header_byte_limit(self):
  port,stop,t,state=self.serve([b'HTTP/1.1 429 Busy\r\n']+[b'X: '+b'x'*4000+b'\r\n']*20+[b'\r\n'],0);owner=Observation('http://127.0.0.1:'+str(port),seconds=1)
  try:
   with self.assertRaisesRegex(ValueError,'ResponseHeaderCapacity'):owner.request('/',{})
  finally:owner.cancel();stop.set();t.join(3)
  self.assertFalse(t.is_alive())
 def test_descriptor_reuse_is_not_old_socket_identity(self):
  from reader import descriptor_absent
  owner=Observation('http://127.0.0.1:1',seconds=.2);fd,inode=owner.fd,owner.inode;owner.cancel()
  with tempfile.TemporaryFile()as file:
   os.dup2(file.fileno(),fd)
   try:self.assertTrue(descriptor_absent(fd,inode))
   finally:
    if file.fileno()!=fd:os.close(fd)
 def test_completed_owner_does_not_timer_close_transferred_socket(self):
  port,stop,t,state=self.serve([b'HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx'],0);owner=Observation('http://127.0.0.1:'+str(port),seconds=.2)
  try:
   r=owner.request('/',{});self.assertEqual(r.raw.read(1),b'x');owner.complete();time.sleep(.25);self.assertFalse(owner.expired);self.assertGreaterEqual(owner.socket.fileno(),0)
  finally:owner.cancel();stop.set();t.join(3)
def http_client_error():
 import http.client
 return http.client.HTTPException
if __name__=='__main__':unittest.main(verbosity=2)
