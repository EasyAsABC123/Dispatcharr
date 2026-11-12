"""Pure and real local-socket controls only; never Source/Native playback."""
import copy,json,socket,tempfile,threading,time,unittest,urllib.request
from pathlib import Path
from ledger_origin import Ledger,Server,ALIASES
from reader import Reader
from test_adapters import Response
from sharing_acceptance import phase,literal_origin
class Controls(unittest.TestCase):
 def setup_ledger(self):
  ledger=Ledger('a'*32)
  class Sock:
   def fileno(self):return 7
  connection=ledger.opened(Sock());request=ledger.request(connection,'GET',next(iter(ALIASES)));return ledger,request
 def test_literal_typed_headers_status_and_no_secret_fields(self):
  ledger,n=self.setup_ledger();ledger.headers(n,'request_headers',{'Range':['bytes=0-31'],'If-Range':[]});ledger.response_status(n,206);ledger.headers(n,'response_headers',{'content-range':['bytes 0-31/64']});ledger.body_return(n,(206,32));r=ledger.snapshot()['requests'][0];self.assertEqual(r['response_status'],206);self.assertEqual(r['request_headers']['Range'],['bytes=0-31']);self.assertEqual(r['serve_file_return']['body_bytes'],32)
 def test_oversize_and_auth_header_capture_fail_sticky(self):
  for value in [{'Range':['x'*2049]},{'Range':['x']*9},{'Authorization':['secret']}]:
   ledger,n=self.setup_ledger()
   with self.assertRaises(ValueError):ledger.headers(n,'request_headers',value)
   self.assertIn('LiteralHeaderCapacity',ledger.snapshot()['invalid'])
 def test_method_status_bodyreturn_reject_boolean_or_badtypes(self):
  ledger,n=self.setup_ledger()
  for code in [True,99,600,'200']:
   with self.assertRaises(ValueError):ledger.response_status(n,code)
  for value in [(206,True),(206,-1),('206',1),[206,1]]:
   with self.assertRaises(ValueError):ledger.body_return(n,value)
  with self.assertRaises(ValueError):ledger.request(1,True,next(iter(ALIASES)))
 def test_real_literal_full_GET_and_Range206_and_HEAD(self):
  with tempfile.TemporaryDirectory()as tmp:
   media=Path(tmp)/'media';media.write_bytes(b'x'*65536);server=Server(('127.0.0.1',0),media,'a'*32,rate=0);thread=threading.Thread(target=server.serve_forever);thread.start();url='http://127.0.0.1:'+str(server.server_port)+next(iter(ALIASES))
   try:
    with urllib.request.urlopen(url,timeout=2)as r:self.assertEqual(r.status,200);self.assertEqual(len(r.read()),65536)
    with urllib.request.urlopen(urllib.request.Request(url,headers={'Range':'bytes=0-31'}),timeout=2)as r:self.assertEqual(r.status,206);self.assertEqual(r.headers['Content-Range'],'bytes 0-31/65536');self.assertEqual(len(r.read()),32)
    with urllib.request.urlopen(urllib.request.Request(url,method='HEAD'),timeout=2)as r:self.assertEqual(r.status,200);self.assertEqual(r.read(),b'')
   finally:server.stop_owned();thread.join(3)
   data=server.ledger.snapshot();self.assertFalse(data['invalid']);self.assertEqual(data['actual_open_TCP'],0);requests=data['requests'];self.assertEqual(requests[0]['request_headers']['Range'],[]);self.assertEqual(requests[0]['serve_file_return'],{'status':200,'body_bytes':65536});self.assertEqual(requests[1]['response_headers']['content-range'],['bytes 0-31/65536']);self.assertEqual(requests[2]['serve_file_return'],{'status':200,'body_bytes':0});self.assertTrue(all(c['fd_after']==-1 for c in data['connections']))
 def test_two_bounded_readers_keep_draining_after_retention_cap(self):
  with tempfile.TemporaryDirectory()as tmp:
   readers=[];producers=[]
   try:
    for role in ['first','second']:
     client,producer=socket.socketpair();client.settimeout(2);producers.append(producer);readers.append(Reader(Response(client),Path(tmp)/role,role,cap=16384,wire_cap=65536));producer.sendall(b'x'*32768)
    for r in readers:r.await_prefix()
    before=[r.snapshot()['received']for r in readers]
    for p in producers:p.sendall(b'y'*16384)
    end=time.monotonic()+2
    while any(r.snapshot()['received']<49152 for r in readers)and time.monotonic()<end:time.sleep(.01)
    for index,r in enumerate(readers):self.assertGreater(r.snapshot()['received'],before[index]);self.assertEqual(r.snapshot()['retained'],16384);self.assertFalse(r.snapshot()['reader_done'])
   finally:
    for r in readers:r.close()
    for p in producers:p.close()
 def origin_fixture(self):
  return {'invalid':[],'actual_open_TCP':0,'server_thread_settled':True,'connections':[{'id':1,'fd_after':-1,'socket_closed':True}],'requests':[{'id':1,'connection':1,'path_role':'shared_movie','method':'GET','bytes_written':65536,'in_flight':False,'response_status':206,'request_headers':{'Range':['bytes=0-65535'],'If-Range':[]},'response_headers':{'content-length':['65536'],'content-type':['video/mp4'],'accept-ranges':['bytes'],'content-range':['bytes 0-65535/65536']},'serve_file_return':{'status':206,'body_bytes':65536}}]}
 def test_literal_origin_coverage_not_vacuous(self):
  self.assertTrue(literal_origin(self.origin_fixture(),settled=True))
  for key in ['requests','connections']:
   o=self.origin_fixture();o[key]=[];self.assertFalse(literal_origin(o,settled=True))
  for key in ['request_headers','response_headers','response_status','serve_file_return']:
   o=self.origin_fixture();o['requests'][0].pop(key);self.assertFalse(literal_origin(o,settled=True))
 def good_report(self):
  readers=lambda size:{role:{'received':size,'eof':False,'invalid':None,'reader_done':False,'socket_fd':9}for role in ['first','second']}
  return {'clients':[{'role':r,'status':200,'prefix_sha256':'02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'}for r in ['first','second']],'checkpoints':[{'label':label,'readers':readers((i+1)*32768)}for i,label in enumerate(['drain_5','drain_15','drain_30','fixed_teardown'])],'decoders':{r:{'passed':True,'frames':75,'retained_sha256':'b4a9992aa94b69eaaebf85025ec5b55f3ebf6360b143196b0d056fcb04c6b097'}for r in ['first','second']},'second_dispatch_target':5,'second_dispatch_actual':5.01,'fixed_teardown_target':40,'teardown_began':40.01,'client_closes':{r:{'fd_after':-1,'reader_joined':True,'original_inode_absent':True,'close_begin':40.01}for r in ['first','second']},'elapsed_seconds':50,'terminal_origin_before_service_stop':self.origin_fixture()}
 def test_acceptance_needs_both_continuous_clients_not_just_socket_count(self):
  report=self.good_report();self.assertTrue(phase(report)['scenario_ready'])
  for kind in ['eof','missing','no_progress','late','late_second_close','missing_decode']:
   r=copy.deepcopy(report)
   if kind=='eof':r['checkpoints'][1]['readers']['first']['eof']=True
   elif kind=='missing':r['checkpoints'][1]['readers'].pop('second')
   elif kind=='no_progress':r['checkpoints'][2]['readers']['first']['received']=r['checkpoints'][1]['readers']['first']['received']
   elif kind=='late':r['teardown_began']=41
   elif kind=='late_second_close':r['client_closes']['second']['close_begin']=41
   else:r['decoders']={}
   self.assertFalse(phase(r)['scenario_ready'])
 def test_entry_still_blocks_before_inputs(self):
  import run,sys
  from unittest.mock import patch
  args=['run','--repo','/tmp','--candidate-image','absent','--build-manifest','absent','--source-archive','absent','--fixture-preflight','absent','--cue','absent','--output','absent']
  with patch.object(sys,'argv',args):
   with self.assertRaisesRegex(RuntimeError,'RootReviewFreshPairCueRequired'):run.main()
if __name__=='__main__':unittest.main(verbosity=2)
