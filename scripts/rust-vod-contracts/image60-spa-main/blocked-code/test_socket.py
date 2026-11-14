"""Real local helper/control sockets only; no Django, Main, Docker or media."""
import http.client,json,socket,tempfile,threading,unittest,time
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from origin import Trap
from native_capture import capture
class Sockets(unittest.TestCase):
 def test_actual_single_capture_and_no_media_trap_shutdown(self):
  nonce='a'*32
  with tempfile.TemporaryDirectory()as folder:
   root=Path(folder);trap=Trap(nonce,root,'127.0.0.1',0);trap.start();requests=[]
   class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_GET(self):
     requests.append({'path':self.path,'auth':self.headers.get('Authorization'),'agent':self.headers.get('User-Agent')});data=b'bounded local page';self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
   server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever);thread.start()
   try:
    plan={'Native_case':{'id':1,'name':'local_not_Main','method':'GET','path':'/proxy/vod/movie/not-a-uuid/owned-method-route','headers':{'User-Agent':'owned-source-method-1'}}};r=capture(root,plan,'127.0.0.1','127.0.0.1',nonce,server.server_port,trap.server.server_port)
    self.assertEqual(r['actual_business_request_invocations'],1);self.assertEqual(len(requests),1);self.assertIsNone(requests[0]['auth']);self.assertEqual(requests[0]['agent'],'owned-source-method-1');self.assertEqual(bytes.fromhex(r['cases'][0]['body_hex']),b'bounded local page');self.assertEqual(r['cases'][0]['physical_close']['fd_after'],-1);self.assertTrue(r['cases'][0]['physical_close']['original_inode_absent']);self.assertTrue(r['cases'][0]['absolute_observation']['timer_joined']);self.assertEqual(r['origin_before']['rows'],[]);self.assertEqual(r['origin_after']['rows'],[])
   finally:server.shutdown();thread.join(3);server.server_close();terminal=trap.close()
   self.assertTrue(terminal['shutdown_acknowledged']);self.assertTrue(terminal['socket_listener_closed']);self.assertTrue(terminal['handler_threads_joined']);self.assertTrue(all(r['socket_closed']for r in terminal['connections']));self.assertEqual(terminal['control_requests'],2)
 def test_actual_unexpected_provider_get_and_wrong_nonce_are_not_hidden(self):
  with tempfile.TemporaryDirectory()as folder:
   trap=Trap('b'*32,Path(folder),'127.0.0.1',0);trap.start()
   try:
    for path,headers in [('/movie/unexpected',{}),('/__capture_state__',{'X-Fixture-Nonce':'wrong'})]:
     client=http.client.HTTPConnection('127.0.0.1',trap.server.server_port,timeout=3);client.request('GET',path,headers=headers);response=client.getresponse();self.assertEqual(response.status,503);response.read();client.close()
   finally:terminal=trap.close()
   self.assertEqual(len(terminal['rows']),2);self.assertTrue(all(r['requests']!=r['controls']for r in terminal['connections']));self.assertEqual(terminal['unclassified_connections'],0)
 def test_actual_transient_TCP_is_not_a_control_or_viewer(self):
  with tempfile.TemporaryDirectory()as folder:
   trap=Trap('c'*32,Path(folder),'127.0.0.1',0);trap.start();client=socket.create_connection(('127.0.0.1',trap.server.server_port),timeout=3);deadline=time.monotonic()+2
   while trap.snapshot()['accepted_TCP_total']==0 and time.monotonic()<deadline:time.sleep(.01)
   self.assertEqual(trap.snapshot()['accepted_TCP_total'],1);client.close();terminal=trap.close();self.assertEqual(terminal['rows'],[]);self.assertGreaterEqual(terminal['unclassified_connections'],1);self.assertTrue(terminal['handler_threads_joined'])
if __name__=='__main__':unittest.main()
