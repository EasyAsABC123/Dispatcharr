"""Owned local pinned-container Redis controls, not Source application evidence."""
import json,os,tempfile,unittest,uuid
from pathlib import Path
from causal_probe import Journal,RedisEffects,Probe,Liveness
from raw_redis import Client
class ActualRedis(unittest.TestCase):
 def setUp(self):
  if os.environ.get('CAUSAL_REDIS_CONTROL')!='playback-fixture-only':self.skipTest('explicit disposable local Redis guard required')
  self.port=int(os.environ['CAUSAL_REDIS_PORT']);self.client=Client(self.port);self.other=Client(self.port);self.key='causal-owned:'+uuid.uuid4().hex;self.tmp=tempfile.TemporaryDirectory();self.j=Journal(Path(self.tmp.name)/'events');self.effects=RedisEffects(self.j)
 def tearDown(self):
  if hasattr(self,'client'):
   self.client.send('DEL',self.key);self.client.close();self.other.close();self.tmp.cleanup()
 def events(self):return[json.loads(x)for x in self.j.path.read_text().splitlines()]if self.j.path.exists()else[]
 def tracked(self,family,*args,role=None):
  self.effects.command(1,family,role);value=self.client.send(family,*args);self.effects.reply(1,value);return value
 def test_actual_EXEC_committed_zero_not_QUEUED(self):
  self.client.send('SET',self.key,1);self.tracked('MULTI');self.tracked('DECR',self.key,role='profile_counter');self.assertFalse(self.events());self.tracked('EXEC');events=self.events();self.assertEqual(len(events),1);self.assertEqual(events[0]['value'],0);self.assertEqual(events[0]['via'],'EXEC');self.assertEqual(self.client.send('GET',self.key),b'0');self.effects.close(1);self.assertFalse(self.j.invalid)
 def test_actual_WATCH_abort_has_no_committed_DECR(self):
  self.client.send('SET',self.key,1);self.tracked('WATCH',self.key);self.tracked('MULTI');self.tracked('DECR',self.key,role='profile_counter');self.other.send('INCR',self.key);self.assertIsNone(self.tracked('EXEC'));self.assertEqual([e['kind']for e in self.events()],['transaction_aborted']);self.assertEqual(self.client.send('GET',self.key),b'2');self.effects.close(1);self.assertFalse(self.j.invalid)
 def test_actual_Redis_zero_then_actual_other_generator_yield(self):
  def other_stream():
   yield b'before'
   yield b'after'
  sid='owned-local-other';probe=Probe(self.j,{sid:'other'},[2]);probe.add(other_stream.__code__,'generator');probe.start()
  try:
   with probe.request('other',sid):generator=other_stream();probe.bind_generator(generator)
   self.assertEqual(next(generator),b'before');self.client.send('SET',self.key,1);self.assertEqual(self.tracked('DECR',self.key,role='profile_counter'),0);self.assertEqual(next(generator),b'after');generator.close()
  finally:probe.stop()
  live=Liveness();events=self.events()
  for event in events:live.observe(event)
  self.assertEqual(len(live.qualified),1);self.assertFalse(live.invalid);self.assertFalse(self.j.invalid)
  zero=next(e['at']for e in events if e['kind']=='actual_counter_commit');buffered=Liveness()
  for event in events:
   if event['kind']!='yield'or event['at']<zero:buffered.observe(event)
  buffered.observe({'kind':'consumer_buffer_read','at':zero+1});buffered.observe({'kind':'origin_write','at':zero+2});self.assertFalse(buffered.qualified)
 def test_actual_zero_release_noop_and_wrongtype_never_invent_commit(self):
  self.client.send('SET',self.key,0);self.tracked('MULTI');self.tracked('DEL',self.key+'-release',role='metadata');self.tracked('EXEC');self.assertFalse(self.events());self.effects.close(1);self.client.send('SET',self.key,'not-an-integer');self.effects.command(2,'DECR','profile_counter');self.effects.reply(2,self.client.send('DECR',self.key));self.assertIn('CounterReplySchema',self.j.invalid);self.assertFalse(self.events())
if __name__=='__main__':unittest.main(verbosity=2)
