"""Actual original redis-py, passive wire and original caller frames; no Django."""
import copy,json,os,tempfile,unittest,uuid
from pathlib import Path
from causal_probe import Journal,Probe
from raw_redis import Client
from passive_relay import Relay,decode
from attribution import join
import redis_binding

def direct_writer(client):return client.decr(client.owned_key)
def background_writer(client,profile_id):return client.decr(client.owned_key)
def pipeline_writer(client):
 with client.pipeline()as pipeline:
  pipeline.watch(client.owned_key)
  value=int(pipeline.get(client.owned_key))
  pipeline.multi()
  if value>0:pipeline.decr(client.owned_key)
  return pipeline.execute()

class ActualWire(unittest.TestCase):
 def setUp(self):
  if os.environ.get('CAUSAL_REDIS_CONTROL')!='playback-fixture-only':self.skipTest('explicit disposable Redis guard required')
  import redis
  self.redis=redis;self.port=int(os.environ['CAUSAL_REDIS_PORT']);self.key='causal-wire:'+uuid.uuid4().hex;self.backend=Client(self.port);self.tmp=tempfile.TemporaryDirectory();self.j=Journal(Path(self.tmp.name)/'events');self.relay=Relay(self.j,self.key,self.port);self.relay.start();self.client=redis.Redis(host='127.0.0.1',port=self.relay.port,socket_timeout=3);self.client.owned_key=self.key
 def tearDown(self):
  if hasattr(self,'backend'):
   self.client.close();self.client.connection_pool.disconnect();self.relay.stop();self.backend.send('DEL',self.key);self.backend.close();self.tmp.cleanup()
 def events(self):return[json.loads(x)for x in self.j.path.read_text().splitlines()]
 def run_writer(self,writer,initial=1):
  self.backend.send('SET',self.key,initial);sid='owned-wire';probe=Probe(self.j,{sid:'follower'},[2],wire_ports=(self.relay.port,));probe.add(writer.__code__,'counter');metadata,codes=redis_binding.describe();before=list(codes);redis_binding.bind(probe,metadata['files']);probe.start()
  try:
   with probe.request('follower',sid):value=writer(self.client)
  finally:probe.stop()
  self.client.close();self.client.connection_pool.disconnect();self.relay.stop();self.assertEqual(redis_binding.describe()[1],before);self.assertFalse(self.j.invalid,self.j.invalid);events=self.events();proof=join(events)
  destination=Path('/evidence')
  if destination.is_dir():
   (destination/(self._testMethodName+'-events.json')).write_text(json.dumps(events,indent=2)+'\n');(destination/(self._testMethodName+'-attribution.json')).write_text(json.dumps(proof,indent=2)+'\n')
  return value,events,proof
 def test_actual_callback_wire_preserves_weak_generator_creator(self):
  def stream():
   yield b'owned'
  def callback():return direct_writer(self.client)
  self.backend.send('SET',self.key,1);sid='owned-wire';probe=Probe(self.j,{sid:'follower'},[2],wire_ports=(self.relay.port,));probe.add(stream.__code__,'generator');probe.add(callback.__code__,'callback');probe.add(direct_writer.__code__,'counter');metadata,_=redis_binding.describe();redis_binding.bind(probe,metadata['files']);probe.start()
  try:
   with probe.request('follower',sid):
    generator=stream();ordinal=probe.bind_generator(generator);next(generator);probe.bind_callback(callback,{'role':'follower','generator':ordinal});self.assertEqual(callback(),0);generator.close()
  finally:probe.stop()
  self.client.close();self.client.connection_pool.disconnect();self.relay.stop();self.assertFalse(self.j.invalid,self.j.invalid);events=self.events();proof=join(events);self.assertTrue(proof['all_actual_counter_writes_attributed'],proof);self.assertEqual(len(proof['matched']),1);self.assertEqual(proof['matched'][0]['creator_generator'],ordinal);self.assertIsInstance(proof['matched'][0]['callback'],int)
  if Path('/evidence').is_dir():
   Path('/evidence/callback-events.json').write_text(json.dumps(events,indent=2)+'\n');Path('/evidence/callback-attribution.json').write_text(json.dumps(proof,indent=2)+'\n')
 def test_actual_owned_profile_background_write_has_no_guessed_viewer(self):
  self.backend.send('SET',self.key,1);probe=Probe(self.j,{'owned-wire':'follower'},[2],wire_ports=(self.relay.port,));probe.add(background_writer.__code__,'counter');metadata,_=redis_binding.describe();redis_binding.bind(probe,metadata['files']);probe.start()
  try:self.assertEqual(background_writer(self.client,2),0)
  finally:probe.stop()
  self.client.close();self.client.connection_pool.disconnect();self.relay.stop();self.assertFalse(self.j.invalid,self.j.invalid);proof=join(self.events());self.assertTrue(proof['all_actual_counter_writes_attributed'],proof);self.assertEqual(len(proof['matched']),1);matched=proof['matched'][0];self.assertEqual(matched['role'],'owned_profile');self.assertEqual(matched['action'],'known_owned_profile');self.assertIsNone(matched['sid_sha256']);self.assertIsNone(matched['creator_generator'])
  if Path('/evidence').is_dir():
   Path('/evidence/background-events.json').write_text(json.dumps(self.events(),indent=2)+'\n');Path('/evidence/background-attribution.json').write_text(json.dumps(proof,indent=2)+'\n')
 def test_direct_actual_zero_unique_original_frame(self):
  value,events,proof=self.run_writer(direct_writer);self.assertEqual(value,0);self.assertTrue(proof['all_actual_counter_writes_attributed'],proof);self.assertEqual(len(proof['matched']),1);self.assertEqual(proof['matched'][0]['role'],'follower');self.assertEqual(proof['matched'][0]['via'],'direct');self.assertEqual(proof['matched'][0]['writer_firstline'],direct_writer.__code__.co_firstlineno)
 def test_pipeline_EXEC_unique_original_frame_not_QUEUED(self):
  value,events,proof=self.run_writer(pipeline_writer);self.assertEqual(value,[0]);self.assertTrue(proof['all_actual_counter_writes_attributed'],proof);self.assertEqual(len(proof['matched']),1);self.assertEqual(proof['matched'][0]['via'],'EXEC')
 def test_pipeline_noop_no_invented_counter_write(self):
  value,events,proof=self.run_writer(pipeline_writer,0);self.assertEqual(value,[]);self.assertEqual(proof['matched'],[]);self.assertFalse([e for e in events if e['kind']=='actual_counter_commit'])
 def test_missing_socket_caller_or_terminal_window_fails_closed(self):
  _,events,proof=self.run_writer(direct_writer);self.assertEqual(len(proof['matched']),1)
  for kind in['wire_link','wire_connection_close','normal_exit']:
   result=join([e for e in events if e['kind']!=kind]);self.assertFalse(result['all_actual_counter_writes_attributed'],kind)
 def test_overlapping_caller_windows_rejected(self):
  _,events,_=self.run_writer(direct_writer);link=next(e for e in events if e['kind']=='wire_link');call=next(e for e in events if e['kind']=='call'and e['frame']==link['frame']);end=next(e for e in events if e['kind']=='normal_exit'and e['frame']==link['frame']);extra=[]
  for original in[link,call,end]:
   duplicate=copy.deepcopy(original);duplicate['frame']=10000;extra.append(duplicate)
  result=join(events+extra);self.assertFalse(result['all_actual_counter_writes_attributed']);self.assertEqual(result['unmatched'][0]['candidate_count'],2)
 def test_owned_profile_delete_is_explicitly_unsupported_not_hidden_zero(self):
  self.backend.send('SET',self.key,1);self.assertEqual(self.client.delete('causal-unrelated-missing-key',self.key),1);self.assertIsNone(self.backend.send('GET',self.key));self.assertIn('UnsupportedOwnedCounterCommand',self.j.invalid)
 def test_telemetry_overflow_does_not_rewrite_or_replay(self):
  self.j.max_rows=1;self.backend.send('SET',self.key,1);self.assertEqual(self.client.decr(self.key),0);self.assertEqual(self.client.ping(),True);self.assertEqual(self.backend.send('GET',self.key),b'0');self.assertTrue(self.j.invalid)

class Decoder(unittest.TestCase):
 def test_wrong_library_binding_rejected_before_trace(self):
  metadata,_=redis_binding.describe();files=dict(metadata['files']);files[next(iter(files))]='0'*64
  with tempfile.TemporaryDirectory()as directory:
   probe=Probe(Journal(Path(directory)/'events'),{'owned':'other'},[])
   with self.assertRaises(ValueError):redis_binding.bind(probe,files)
   self.assertFalse(probe.started);self.assertEqual(probe.rules,{})
 def test_complete_typed_response_and_errors(self):
  self.assertEqual(decode(b'*2\r\n:0\r\n$-1\r\n'),([0,None],13))
  self.assertEqual(decode(b'+QUEUED\r\n'),(b'QUEUED',9))
 def test_array_depth_and_array_capacity_reject(self):
  for raw in[b'*1025\r\n',b'*1\r\n'*18+b':0\r\n']:
   with self.assertRaises(ValueError):decode(raw)
if __name__=='__main__':unittest.main(verbosity=2)
