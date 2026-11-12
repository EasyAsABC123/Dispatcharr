"""Private integration invariants; no Docker/Source or workload invocation."""
import ast,importlib.util,pathlib,tempfile,threading,unittest
from unittest import mock
import workload
HERE=pathlib.Path(__file__).parent
def load_run():
 spec=importlib.util.spec_from_file_location('supplemental_private_run',HERE/'run.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
class Integration(unittest.TestCase):
 def test_blocked_before_output_archive_or_commands(self):
  module=load_run()
  with mock.patch.object(module.subprocess,'check_output',side_effect=AssertionError('no archive')):
   with self.assertRaisesRegex(RuntimeError,'SupplementalSourceExecutionBlocked'):module.main()
  self.assertTrue(module.RESOURCE_EXECUTION_BLOCKED)
 def test_callbacks_are_outside_workflow_marker_and_window(self):
  source=(HERE/'run.py').read_text();tree=ast.parse(source);main=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='main');start=next(n for n in ast.walk(main)if isinstance(n,ast.FunctionDef)and n.name=='start')
  text=ast.get_source_segment(source,start);self.assertLess(text.index("supplemental_point('after-warm-before-window')"),text.index("save(out/'concurrent-window.json'"))
  self.assertLess(source.index("supplemental_point('before-reader')"),source.index('phase=work.run('));self.assertLess(source.index('phase=work.run('),source.index("supplemental_point('after-physical-close')"));self.assertIn('physical_close_at+70-time.monotonic()',source)
 def test_response_and_connection_close_before_receipt_and_prefix_output(self):
  events=[];sock=mock.Mock();sock.fileno.return_value=5;response=mock.Mock();response.status=200;response.fp.raw._sock=sock;response.read1.return_value=b'abc';response.close.side_effect=lambda:events.append('response-close')
  conn=mock.Mock();conn.getresponse.return_value=response
  def close():events.append('connection-close');sock.fileno.return_value=-1
  conn.close.side_effect=close
  stop=mock.Mock();stop.is_set.side_effect=[False,True]
  shared={'stop':stop,'release':threading.Event(),'prefix_limit':2097152};ready=threading.Event()
  with tempfile.TemporaryDirectory()as folder,mock.patch.object(workload.http.client,'HTTPConnection',return_value=conn):
   row=workload.reader('http://127.0.0.1','/','fixture',shared,ready,pathlib.Path(folder)/'0.ts',20)
  self.assertEqual(events,['response-close','connection-close']);self.assertTrue(row['physical_close']['socket_closed']);self.assertLessEqual(row['physical_close']['completed_monotonic'],row['reader_end_monotonic'])
 def endpoint_fixture(self):
  allocated={'services':{'baseline':[{'host':64569,'container':5656},{'host':64570,'container':6379}]}};expected=[{'HostIp':'127.0.0.1','HostPort':'64570'}];inspection={'HostConfig':{'PortBindings':{'6379/tcp':expected}},'NetworkSettings':{'Ports':{'6379/tcp':expected}}};return allocated,inspection
 def test_endpoint_uses_allocated_host_and_actual_binding(self):
  a,i=self.endpoint_fixture();self.assertEqual(load_run().observer_redis_endpoint(a,i,b'127.0.0.1:64570\n'),('127.0.0.1',64570))
 def test_internal_only_null_effective_binding_never_relaxed(self):
  a,i=self.endpoint_fixture();i['NetworkSettings']['Ports']['6379/tcp']=None
  with self.assertRaisesRegex(ValueError,'BindingMismatch'):load_run().observer_redis_endpoint(a,i,b':0\n')
 def test_actual_binding_wrong_ip_or_port_rejected(self):
  for value in [{'HostIp':'0.0.0.0','HostPort':'64570'},{'HostIp':'127.0.0.1','HostPort':'64571'}]:
   a,i=self.endpoint_fixture();i['NetworkSettings']['Ports']['6379/tcp']=[value]
   with self.assertRaisesRegex(ValueError,'BindingMismatch'):load_run().observer_redis_endpoint(a,i,b'127.0.0.1:64570\n')
 def test_port_stdout_and_allocator_cannot_silently_diverge(self):
  for raw in [b':0\n',b'127.0.0.1:64571\n',b'0.0.0.0:64570\n',b'127.0.0.1:64570\nother:3']:
   a,i=self.endpoint_fixture()
   with self.assertRaisesRegex(ValueError,'PortWitnessMismatch'):load_run().observer_redis_endpoint(a,i,raw)
 def test_historical_timing_and_media_policy_unchanged(self):
  import json
  p=json.loads((HERE/'policy.json').read_text());self.assertEqual((p['viewers'],p['warmup_seconds'],p['measure_seconds'],p['imports_at_seconds'],p['prefix_bytes']),(1,15,120,[5,60],2097152));self.assertFalse(p['resource_acceptance'])
if __name__=='__main__':unittest.main()
