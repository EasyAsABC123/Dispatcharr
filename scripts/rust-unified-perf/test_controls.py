"""Fail-closed protocol/reader controls; no Docker or resource workload.
Original Dispatcharr authors retain application credit.
"""
import copy,datetime,http.server,json,socketserver,tempfile,threading,unittest
from pathlib import Path
import protocol,acceptance,reader
class Control(unittest.TestCase):
 def manifest(self):return {'passed':True,'image_id':protocol.IMAGE,'source_tree_sha256':protocol.TREE,'source_archive_sha256':protocol.ARCHIVE}
 def rows(self):return [{'backend':'source','period_seconds':period,'errors':0,'correct_responses':True,'pacing_fraction':1,'cpu_percent_mean':20,'memory_fraction':.5,'throttled_period_fraction':0}for period in [10,5,2,1]]
 def test_manifest_and_only_original_can_choose_pacing(self):
  protocol.identity(self.manifest());work=protocol.POLICY['workloads'][0];self.assertEqual(protocol.select_period(work,self.rows()),1)
  bad=self.manifest();bad['image_id']='sha256:'+'0'*64
  with self.assertRaises(ValueError):protocol.identity(bad)
  bad=self.rows();bad[0]['candidate_cpu_percent_mean']=0
  with self.assertRaises(ValueError):protocol.select_period(work,bad)
  with self.assertRaises(ValueError):protocol.select_period(work,self.rows()[:-1])
 def test_measurements_require_separate_fresh_matching_cue_and_frozen_plan(self):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);manifest=self.manifest();qualification={work['name']:self.rows()for work in protocol.POLICY['workloads']};fixture={'fixture_sha256':'f'*64,'schema_sha256':'a'*64}
   plan=protocol.freeze(root/'plan.json',qualification,manifest,fixture);protocol.validate_plan(plan,qualification,manifest,fixture)
   with self.assertRaises(FileNotFoundError):protocol.authorize(root/'missing',manifest,'vod-resource-measurement',plan['plan_sha256'])
   cue={'scope':'vod-source-qualification','authorized':True,'candidate_image':protocol.IMAGE,'candidate_manifest_canonical_sha256':protocol.digest(manifest),'policy_sha256':protocol.digest(protocol.POLICY),'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'expires_utc':(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=1)).isoformat()};path=root/'cue.json';path.write_text(json.dumps(cue))
   with self.assertRaises(ValueError):protocol.authorize(path,manifest,'vod-resource-measurement',plan['plan_sha256'])
   cue.update(scope='vod-resource-measurement',plan_sha256=plan['plan_sha256']);path.write_text(json.dumps(cue));protocol.authorize(path,manifest,'vod-resource-measurement',plan['plan_sha256'])
   altered=copy.deepcopy(plan);altered['selected_periods'][protocol.POLICY['workloads'][0]['name']]=10
   with self.assertRaises(ValueError):protocol.validate_plan(altered,qualification,manifest,fixture)
   cue['expires_utc']='2000-01-01T00:00:00+00:00';path.write_text(json.dumps(cue))
   with self.assertRaises(ValueError):protocol.authorize(path,manifest,'vod-resource-measurement',plan['plan_sha256'])
 def test_measured_late_and_fewer_than_sixty_samples_fail(self):
  value=dict(cpu_percent_mean=1,peak_working_memory_bytes=1,ttfb_p95_ms=1,completion_p95_ms=1,throughput_per_second=1,pacing_fraction=1,samples=60,errors=0,sample_errors=0,late_completed=0,oom_kills=0,counter_resets=0,correct_responses=True,cache_state_verified=True,fixture_unchanged=True,limits_verified=True)
  self.assertEqual(acceptance.phase(value),[]);value['late_completed']=1;self.assertIn('late_completed',acceptance.phase(value));value['samples']=59;self.assertIn('insufficient_samples',acceptance.phase(value))
 def test_exact_reader_rejects_wrong_bytes_even_when_json_equal(self):
  payload={'count':9000,'next':'http://vod-perf.invalid:8088/page2','previous':None,'results':[{'id':i}for i in range(24)]};state={'body':json.dumps(payload,separators=(',',':')).encode()}
  class Handler(http.server.BaseHTTPRequestHandler):
   def do_GET(self):
    body=state['body'];self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
   def log_message(self,*args):pass
  server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
  try:
   base='http://127.0.0.1:'+str(server.server_port);expected,_,_=reader.request(base,protocol.PATHS[0],'owned-fixture-token');state['body']=json.dumps(payload,indent=4).encode()
   with self.assertRaises(ValueError):reader.request(base,protocol.PATHS[0],'owned-fixture-token',expected=expected)
   payload['count']=8999;state['body']=json.dumps(payload).encode()
   with self.assertRaises(ValueError):reader.request(base,protocol.PATHS[0],'owned-fixture-token')
  finally:server.shutdown();server.server_close();thread.join(2)
if __name__=='__main__':unittest.main()
