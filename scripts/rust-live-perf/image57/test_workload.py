"""Real local HTTP ownership/barrier and no-retry controls; no resource claims."""
import contextlib,http.server,threading,time,tempfile,unittest
from pathlib import Path
import workload
@contextlib.contextmanager
def peer(status=200,early_eof=False):
 state={'requests':0,'closed':threading.Event(),'auth':False}
 class Handler(http.server.BaseHTTPRequestHandler):
  protocol_version='HTTP/1.1'
  def log_message(self,*args):pass
  def do_GET(self):
   state['requests']+=1;state['auth']=self.headers.get('Authorization')=='Bearer fixture-only'
   self.send_response(status);self.send_header('Content-Type','video/MP2T');self.send_header('Connection','close');self.end_headers()
   try:
    if early_eof:return
    for _ in range(200):self.wfile.write(b'G'*4096);self.wfile.flush();time.sleep(.005)
   except (BrokenPipeError,ConnectionResetError):pass
   finally:state['closed'].set()
 server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
 try:yield 'http://127.0.0.1:'+str(server.server_port),state
 finally:server.shutdown();server.server_close();thread.join(2)
class Workload(unittest.TestCase):
 def test_continuous_auth_prefix_and_close_barrier(self):
  with peer()as(base,state),tempfile.TemporaryDirectory()as d:
   def after(*args):self.assertFalse(state['closed'].is_set());return {'test_barrier_checked':True}
   result=workload.run(base,'/controlled','fixture-only',1,.05,.05,Path(d)/'prefix',lambda:None,after,lambda n:{'clients':n},prefix_limit=1024,timeout=1)
   row=result['viewers'][0];self.assertTrue(state['auth']);self.assertEqual(state['requests'],1);self.assertEqual(row['prefix_bytes'],1024);self.assertTrue(row['post_window_barrier']);self.assertEqual(row['errors'],[]);self.assertGreater(row['measured_bytes'],0)
 def test_auth_rejection_is_not_retried(self):
  with peer(status=401)as(base,state),tempfile.TemporaryDirectory()as d:
   with self.assertRaises(RuntimeError):workload.run(base,'/controlled','fixture-only',1,.05,.05,Path(d)/'prefix',lambda:None,lambda *a:None,lambda n:None,timeout=1)
   self.assertEqual(state['requests'],1)
 def test_early_eof_is_not_success_or_retried(self):
  with peer(early_eof=True)as(base,state),tempfile.TemporaryDirectory()as d:
   with self.assertRaises(RuntimeError):workload.run(base,'/controlled','fixture-only',1,.05,.05,Path(d)/'prefix',lambda:None,lambda *a:None,lambda n:None,timeout=1)
   self.assertEqual(state['requests'],1)
 def test_observer_failure_still_retires_connection(self):
  with peer()as(base,state),tempfile.TemporaryDirectory()as d:
   def fail(*args):raise ValueError('ControlledObserverFailure')
   with self.assertRaises(ValueError):workload.run(base,'/controlled','fixture-only',1,.05,.05,Path(d)/'prefix',lambda:None,fail,lambda n:None,timeout=1)
   self.assertTrue(state['closed'].wait(2));self.assertEqual(state['requests'],1)
if __name__=='__main__':unittest.main(verbosity=2)
