"""Controller-only controls: no Source/Main/Docker or external network.
One local socketpair qualifies actual blocked HTTP read shutdown/join only.
"""
import copy,http.client,socket,threading,time,unittest,json,tempfile,types,sys
from pathlib import Path
from request_observer import install,validate
from unittest import mock
import observation
from target_client import Target

class Tests(unittest.TestCase):
 def report(self):
  return{'case':'clientloss','observation_valid':True,'held_boundary_monotonic':100,'target_client_loss':{'close_begin':102,'reader_joined_monotonic':102.02,'fd_after':-1,'local_shutdown':True,'reader_joined':True,'original_descriptor_identity_absent':True,'server_cancellation_inferred':False},'target_outcome':{'request_started':99.9,'completed':102.01,'transport_error':'RemoteDisconnected'}}
 def test_valid_loss_never_infers_server_cancel(self):
  r=self.report();self.assertTrue(observation.client_loss(r)['matched']);self.assertTrue(observation.compare(r,copy.deepcopy(r))['parity_passed'])
 def test_premature503_or_missing_actual_close_cannot_pass(self):
  r=self.report()
  for key,value in[('status',503),('headers_received',101),('transport_error','TimeoutError'),('completed',101.9)]:
   n=copy.deepcopy(r);n['target_outcome'][key]=value;self.assertFalse(observation.compare(r,n)['parity_passed'])
  for key,value in[('fd_after',3),('reader_joined',False),('original_descriptor_identity_absent',False),('server_cancellation_inferred',True),('reader_joined_monotonic',103.2),('close_begin',float('nan'))]:
   n=copy.deepcopy(r);n['target_client_loss'][key]=value;self.assertFalse(observation.client_loss(n)['matched'])
 def test_header_key_absence_and_strict_integer_descriptor_required(self):
  r=self.report();self.assertTrue(observation.client_loss(r)['matched'])
  for value in[0,None,False,'',0.0]:
   bad=copy.deepcopy(r);bad['target_outcome']['headers_received']=value
   self.assertFalse(observation.client_loss(bad)['matched'])
  for value in[-1.0,True,False,'-1',None]:
   bad=copy.deepcopy(r);bad['target_client_loss']['fd_after']=value
   self.assertFalse(observation.client_loss(bad)['matched'])
 def test_foreign_target_or_unregistered_abort_rejected(self):
  with self.assertRaises(ValueError):Target('https://example.org')
  with self.assertRaises(ValueError):Target('http://127.0.0.1:5656')
  target=Target('http://127.0.0.1:5657')
  with self.assertRaises(ValueError):target.abort()
  with self.assertRaises(ValueError):target.joined()
 def test_clientloss_path_registered_with_exact_source_roles(self):
  signals=types.ModuleType('django.core.signals');signals.request_started=mock.Mock();signals.request_finished=mock.Mock();local=types.ModuleType('gevent.local');local.local=threading.local
  with tempfile.TemporaryDirectory()as output,mock.patch.dict(sys.modules,{'django.core.signals':signals,'gevent.local':local}):
   observer=install({'movie_uuid':'owned-movie','other_movie_uuid':'other-movie'},output);path='/proxy/vod/movie/owned-movie/fixture-pending-main-clientloss'
   for role in['initial','target','follower']:
    observer.started(environ={'PATH_INFO':path,'HTTP_USER_AGENT':'owned-pending-'+role});observer.finished()
   rows=[json.loads(line)for line in observer.journal.path.read_text().splitlines()]
   self.assertTrue(validate(rows,['initial','target','follower'])['valid']);self.assertEqual(len(rows),6)
 def test_candidate_rejects_missing_or_extra_unreviewed_rust(self):
  import candidate_inputs
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);expected={'rust/file'+str(i):'a'*64 for i in range(792)};(root/'expected').write_text(json.dumps(expected))
   for actual in[{},dict(list(expected.items())[:-1]),{**expected,'rust/unreviewed':'b'*64}]:
    with self.assertRaisesRegex(ValueError,'ExactReviewedUnion792'):candidate_inputs.require_runtime_set({'source_sha256':actual},expected)
   self.assertTrue(candidate_inputs.require_runtime_set({'source_sha256':expected},expected))

 def test_socketpair_real_blocked_HTTP_read_abort_and_join(self):
  left,right=socket.socketpair();target=Target('http://127.0.0.1:5657');target.connection.sock=left;received=threading.Event();peer_eof=threading.Event();errors=[]
  def peer():
   try:
    data=b''
    while b'\r\n\r\n'not in data:
     block=right.recv(1024)
     if not block:raise ValueError('MissingRequest')
     data+=block
     if len(data)>8192:raise ValueError('RequestCapacity')
    received.set()
    if right.recv(1)==b'':peer_eof.set()
   finally:right.close()
  def read():
   try:target.request('/owned',{'User-Agent':'owned-pending-target'})
   except BaseException as error:errors.append(type(error).__name__)
  threads=[threading.Thread(target=peer),threading.Thread(target=read)]
  try:
   for thread in threads:thread.start()
   self.assertTrue(received.wait(2));end=time.monotonic()+2
   while target.sock is None and time.monotonic()<end:time.sleep(.001)
   target.abort()
   for thread in threads:thread.join(2);self.assertFalse(thread.is_alive())
   receipt=target.joined();self.assertTrue(receipt['original_descriptor_identity_absent']);self.assertEqual(receipt['fd_after'],-1);self.assertTrue(peer_eof.is_set());self.assertEqual(errors,['RemoteDisconnected'])
   with self.assertRaises(ValueError):target.abort()
  finally:
   left.close();right.close()
   for thread in threads:
    if thread.ident is not None:thread.join(2)
if __name__=='__main__':unittest.main(verbosity=2)
