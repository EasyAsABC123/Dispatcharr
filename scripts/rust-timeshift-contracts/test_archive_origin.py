"""Executable range/fault validation for the deterministic archive fixture."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'rust-playback-baseline'))
from archive_origin import ArchiveMixin

class Fixture(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.payload = bytes(range(256))*20
        path = Path(self.directory.name)/'fixture.ts'
        path.write_bytes(self.payload)
        class Handler(ArchiveMixin,BaseHTTPRequestHandler):
            archive_path=path
            archive_hash=hashlib.sha256(path.read_bytes()).hexdigest()
            archive_rate=10_000_000
            archive_mode='all'
            archive_requests=[]
            def log_message(self,*args):pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.worker=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.worker.start()
        self.base='http://127.0.0.1:'+str(self.server.server_port)
    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.worker.join();self.directory.cleanup()
    def fetch(self,path,**kwargs):
        try: response=urllib.request.urlopen(urllib.request.Request(self.base+path,**kwargs),timeout=2)
        except urllib.error.HTTPError as error:response=error
        with response:return response.status,response.headers,response.read()
    def control(self,mode):
        return self.fetch('/archive-control',data=json.dumps({'mode':mode}).encode(),method='POST',headers={'Content-Type':'application/json'})
    def test_exact_range_head_and_unsatisfiable(self):
        path='/timeshift/fixture/fixture/35/2026-11-09:11-00/fixture.ts'
        status,headers,payload=self.fetch(path,headers={'Range':'bytes=7-100'})
        self.assertEqual((status,payload),(206,self.payload[7:101]))
        self.assertEqual(headers['Content-Range'],'bytes 7-100/5120')
        self.assertEqual(self.fetch(path,method='HEAD')[2],b'')
        self.assertEqual(self.fetch(path,headers={'Range':'bytes=9000-'})[0],416)
    def test_modes_failures_and_normalized_request_log(self):
        path='/timeshift/fixture/fixture/35/2026-11-09:11-00/fixture.ts'
        for mode,expected in [('forbidden',403),('not-found',404),('server-error',500),('query-only',404)]:
            self.assertEqual(self.control(mode)[0],200)
            self.assertEqual(self.fetch(path)[0],expected)
        query='/streaming/timeshift.php?username=fixture&password=fixture&stream=fixture'
        self.assertEqual(self.fetch(query)[0],200)
        stats=json.loads(self.fetch('/archive-stats')[2])
        self.assertEqual([r['shape'] for r in stats['requests']],['colon-dash','query'])
        self.assertNotIn('password',json.dumps(stats))
        self.assertEqual(self.control('html-first')[0],200)
        self.assertIn(b'<html>',self.fetch(path)[2])
        self.assertEqual(self.control('invalid')[0],400)
if __name__=='__main__':unittest.main()
