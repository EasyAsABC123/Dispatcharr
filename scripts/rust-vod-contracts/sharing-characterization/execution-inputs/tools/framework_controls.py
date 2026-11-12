"""Pinned framework/status controls; no django.setup or Source app import."""
from gevent import monkey;monkey.patch_all()
import json,os,sys,tempfile,threading,unittest,urllib.request,urllib.error
from pathlib import Path
from django.core.signals import request_started,request_finished
from normal_signals import Observer,validate
from status_app import app_for
class Controls(unittest.TestCase):
 def test_real_signal_nonce_status_and_independent_physical_stop(self):
  from gevent.pywsgi import WSGIServer
  from gevent.pool import Pool
  from gevent import spawn
  from gevent.local import local
  with tempfile.TemporaryDirectory()as directory:
   observer=Observer({'movie_uuid':'a','other_movie_uuid':'b','probe1_movie_uuid':'c','probe2_movie_uuid':'d'},directory,local());request_started.connect(observer.started,weak=False,dispatch_uid='local-normal-start');request_finished.connect(observer.finished,weak=False,dispatch_uid='local-normal-finish');pool=Pool(2);server=WSGIServer(('127.0.0.1',0),app_for(observer,'a'*32),spawn=pool,log=None,error_log=None);server.start();listener=server.socket
   try:
    with self.assertRaises(urllib.error.HTTPError):urllib.request.urlopen('http://127.0.0.1:'+str(server.server_port)+'/state',timeout=2)
    for role,movie,sid in[('first','a','fixture-sharing-same'),('second','a','fixture-sharing-same')]:
     request_started.send(sender=self.__class__,environ={'PATH_INFO':'/proxy/vod/movie/'+movie+'/'+sid,'HTTP_USER_AGENT':'owned-sharing-'+role});request_finished.send(sender=self.__class__)
    with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:'+str(server.server_port)+'/state',headers={'X-Fixture-Nonce':'a'*32}),timeout=2)as response:status=json.loads(response.read(4097))
    rows=[json.loads(line)for line in(Path(directory)/'normal-signals.jsonl').read_text().splitlines()];self.assertTrue(validate(status,rows,complete=True)['complete'])
    rows.pop()
    with self.assertRaises(ValueError):validate({**status,'rows':len(rows)},rows,complete=True)
   finally:
    server.stop(timeout=2);request_started.disconnect(dispatch_uid='local-normal-start');request_finished.disconnect(dispatch_uid='local-normal-finish')
   self.assertEqual(listener.fileno(),-1);self.assertEqual(pool.free_count(),2)
 def test_signal_overflow_cannot_fake_complete(self):
  from gevent.local import local
  with tempfile.TemporaryDirectory()as directory:
   observer=Observer({'movie_uuid':'a','other_movie_uuid':'b','probe1_movie_uuid':'c','probe2_movie_uuid':'d'},directory,local());observer.journal.max_rows=1;observer.started(environ={'PATH_INFO':'/proxy/vod/movie/a/fixture-sharing-same','HTTP_USER_AGENT':'owned-sharing-first'});observer.finished();self.assertTrue(observer.journal.invalid)
if __name__=='__main__':
 result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Controls));report={'passed':result.wasSuccessful(),'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'Source_application_imported':any(name=='apps'or name.startswith('apps.')for name in sys.modules),'Django_setup_called':False};Path('/evidence/framework-controls.json').write_text(json.dumps(report,indent=2)+'\n');raise SystemExit(0 if report['passed']and not report['Source_application_imported']else 1)
