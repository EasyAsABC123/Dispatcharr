"""Local/pinned-interpreter controls only; no Django/Redis/WSGI app execution."""
import gc,inspect,json,os,sys,tempfile,threading,time,types,unittest,weakref
from pathlib import Path
from unittest import mock
from causal_probe import Journal,Identities,Probe,RedisEffects,Liveness
import pinned_source

SID='owned-sid'
class Connection:
 def __init__(self):self.session_id=SID

def stream(finalized):
 secret='NEVER_STORE_AUTH_SENTINEL'
 try:
  yield b'first'
  yield b'second'
 finally:
  finalized.append(True) # MARK_FINALLY

def broken_stream(finalized):
 try:yield b'first'
 finally:
  finalized.append(True) # MARK_FINALLY
  raise RuntimeError('NEVER_STORE_EXCEPTION_VALUE')

def markers(function):
 lines,start=inspect.getsourcelines(function)
 return{start+i:'finally_enter'for i,line in enumerate(lines)if '# MARK_FINALLY'in line}

def rows(journal):return[json.loads(line)for line in journal.path.read_text().splitlines()]if journal.path.exists()else[]
class Controls(unittest.TestCase):
 def fixture(self,directory,**kwargs):
  journal=Journal(Path(directory)/'events.jsonl',**kwargs);return journal,Probe(journal,{SID:'owned'},[2])
 def test_journal_caps_fail_before_append_and_stay_invalid(self):
  with tempfile.TemporaryDirectory()as d:
   j=Journal(Path(d)/'events',max_rows=1);self.assertTrue(j.append({'kind':'first'}));before=j.path.read_bytes();self.assertFalse(j.append({'kind':'overflow'}));self.assertFalse(j.append({'kind':'late'}));self.assertEqual(j.path.read_bytes(),before);self.assertEqual(j.rows,1)
  with tempfile.TemporaryDirectory()as d:
   j=Journal(Path(d)/'events',row_bytes=1);self.assertFalse(j.append({'kind':'too-large'}));self.assertEqual(j.used,0)
 def test_oversized_or_nonprimitive_telemetry_rejected_before_encoding(self):
  for value in['x'*100000,{'deep':{'deep':{'deep':{'deep':1}}}},object(),float('nan')]:
   with tempfile.TemporaryDirectory()as d:
    j=Journal(Path(d)/'events')
    original=json.dumps;violations=[]
    def encode(item,*args,**kwargs):
     if item is value or type(item)in[dict,list]or(type(item)is str and len(item)>128):violations.append(type(item).__name__)
     return original(item,*args,**kwargs)
    with mock.patch('causal_probe.json.dumps',side_effect=encode):self.assertFalse(j.append({'payload':value}))
    self.assertFalse(violations);self.assertTrue(j.invalid);self.assertEqual(j.rows,0);self.assertFalse(j.path.exists())
 def test_writer_denial_does_not_escape_or_report_zero_valid(self):
  with tempfile.TemporaryDirectory()as d:
   j=Journal(Path(d)/'events')
   with mock.patch.object(Path,'open',side_effect=PermissionError):self.assertFalse(j.append({'kind':'owned'}))
   self.assertIn('TelemetryWrite',j.status()['invalid']);self.assertEqual(j.rows,0)
 def test_identity_is_weak_and_never_forces_gc(self):
  with tempfile.TemporaryDirectory()as d,mock.patch.object(gc,'collect',side_effect=AssertionError('forcedGC forbidden')):
   j=Journal(Path(d)/'events');identities=Identities(j);obj=Connection();reference=weakref.ref(obj);self.assertIsNotNone(identities.bind(obj,'connection'));del obj;self.assertIsNone(reference());self.assertEqual(identities.count(),0)
 def test_identity_and_code_capacity_and_metadata_are_bounded(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d);ids=Identities(j,capacity=1);a,b=Connection(),Connection();self.assertIsNotNone(ids.bind(a,'connection'));self.assertIsNone(ids.bind(b,'connection'));self.assertIn('IdentityCapacity',j.invalid)
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d);self.assertIsNone(p.identities.bind(Connection(),'connection',{'frame':sys._getframe()}));self.assertIn('IdentityMetadataNotScalar',j.invalid)
  with tempfile.TemporaryDirectory()as d:
   j=Journal(Path(d)/'events');p=Probe(j,{SID:'owned'},[2],max_codes=1);p.add(stream.__code__);p.add(broken_stream.__code__);self.assertIn('CodeRegistryCapacity',j.invalid)
 def test_real_generator_yield_is_not_completion_finally_normal_return_is(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d);p.add(stream.__code__,'generator',markers(stream));finished=[];p.start()
   with p.request('other',SID):generator=stream(finished);p.bind_generator(generator)
   try:
    self.assertEqual(next(generator),b'first');events=rows(j);self.assertEqual(len([r for r in events if r['kind']=='yield']),1,{'events':events,'status':j.status()});self.assertFalse(any(r['kind']=='normal_exit'for r in events));self.assertEqual(next(generator),b'second')
    with self.assertRaises(StopIteration):next(generator)
   finally:p.stop()
   self.assertEqual(finished,[True]);events=rows(j);terminal=[r for r in events if r['kind']=='normal_exit'and r.get('is_generator')];self.assertEqual(len(terminal),1);self.assertTrue(terminal[0]['finally_entered']);self.assertEqual(p.frames,{});self.assertEqual(p.generators,{});self.assertNotIn('NEVER_STORE',j.path.read_text());self.assertFalse(j.invalid)
 def test_real_generator_finally_unwind_is_not_successful_return(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d);p.add(broken_stream.__code__,'generator',markers(broken_stream));finished=[];p.start()
   with p.request('target',SID):generator=broken_stream(finished);p.bind_generator(generator)
   try:
    next(generator)
    with self.assertRaises(RuntimeError):generator.close()
   finally:p.stop()
   events=rows(j);self.assertTrue(any(r['kind']=='unwind'and r['finally_entered']for r in events));self.assertFalse(any(r['kind']=='normal_exit'and r.get('is_generator')for r in events));self.assertNotIn('NEVER_STORE',j.path.read_text());self.assertFalse(j.invalid)
 def test_real_thread_callback_has_weak_creator_and_role(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d);conn=Connection();called=[]
   def delayed_cleanup():called.append(conn.session_id)
   p.add(delayed_cleanup.__code__,'callback');p.bind_callback(delayed_cleanup,{'role':'initial','sid_sha256':'a'*64,'generator':7},conn)
   function_reference=weakref.ref(delayed_cleanup);p.start();thread=threading.Thread(target=delayed_cleanup,daemon=True)
   try:thread.start();thread.join(2);self.assertFalse(thread.is_alive())
   finally:p.stop()
   self.assertEqual(called,[SID]);events=rows(j);calls=[r for r in events if r['kind']=='call'];self.assertEqual(len(calls),1);self.assertEqual(calls[0]['role'],'initial');self.assertEqual(calls[0]['creator_generator'],7);self.assertIs(type(calls[0]['logical_thread_id']),int);self.assertIs(type(calls[0]['native_thread_id']),int);self.assertFalse(j.invalid);del thread;del delayed_cleanup;self.assertIsNone(function_reference())
 def test_generator_binding_requires_trace_startup(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d);generator=stream([])
   try:
    with self.assertRaisesRegex(ValueError,'TraceStartupBefore'):p.bind_generator(generator)
   finally:generator.close()
 def test_existing_trace_is_never_replaced(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d)
   with mock.patch.object(sys,'gettrace',return_value=object()),mock.patch.object(sys,'settrace')as setter:
    with self.assertRaises(ValueError):p.start()
    setter.assert_not_called()
 def test_actual_TYPED_commit_distinct_from_QUEUED_and_watch_abort(self):
  with tempfile.TemporaryDirectory()as d:
   j=Journal(Path(d)/'events');wire=RedisEffects(j);wire.command(1,'MULTI');wire.reply(1,'OK');wire.command(1,'DEL','metadata');wire.reply(1,'QUEUED');wire.command(1,'DECR','profile_counter');wire.reply(1,'QUEUED');self.assertFalse(rows(j));wire.command(1,'EXEC');wire.reply(1,[1,0]);events=rows(j);self.assertEqual(len(events),1);self.assertEqual(events[0]['value'],0);self.assertEqual(events[0]['via'],'EXEC');wire.close(1);self.assertFalse(j.invalid)
  with tempfile.TemporaryDirectory()as d:
   j=Journal(Path(d)/'events');wire=RedisEffects(j);wire.command(1,'MULTI');wire.reply(1,'OK');wire.command(1,'DECR','profile_counter');wire.reply(1,'QUEUED');wire.command(1,'EXEC');wire.reply(1,None);self.assertEqual([r['kind']for r in rows(j)],['transaction_aborted']);wire.close(1);self.assertFalse(j.invalid)
 def test_profile_zero_noop_and_method_true_do_not_invent_write(self):
  with tempfile.TemporaryDirectory()as d:
   j=Journal(Path(d)/'events');wire=RedisEffects(j);wire.command(1,'MULTI');wire.reply(1,'OK');wire.command(1,'DEL','metadata');wire.reply(1,'QUEUED');wire.command(1,'EXEC');wire.reply(1,[1]);self.assertFalse(rows(j));wire.close(1)
 def test_wrong_wire_types_pending_or_reply_schema_invalidate(self):
  for value in[True,'ERR secret',None,[0]]:
   with tempfile.TemporaryDirectory()as d:
    j=Journal(Path(d)/'events');wire=RedisEffects(j);wire.command(1,'DECR','profile_counter');wire.reply(1,value);self.assertTrue(j.invalid);self.assertFalse(rows(j))
  with tempfile.TemporaryDirectory()as d:
   j=Journal(Path(d)/'events');wire=RedisEffects(j,max_pending=1);wire.command(1,'GET');wire.command(1,'GET');self.assertIn('WirePendingCapacity',j.invalid)
 def test_other_real_generator_progress_after_zero_not_buffered_reads(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d);p.add(stream.__code__,'generator',markers(stream));finished=[];p.start()
   with p.request('other',SID):generator=stream(finished);p.bind_generator(generator)
   try:
    next(generator);wire=RedisEffects(j);wire.command(1,'DECR','profile_counter');wire.reply(1,0);next(generator);generator.close()
   finally:p.stop()
   live=Liveness()
   for event in rows(j):live.observe(event)
   self.assertEqual(len(live.qualified),1);self.assertFalse(live.invalid)
   for kind in['consumer_buffer_read','origin_write']:
    no=Liveness();no.observe({'kind':'actual_counter_commit','value':0,'at':1});no.observe({'kind':kind,'at':2,'role':'other','generator':1,'yield_bytes':8192});self.assertFalse(no.qualified)
 def test_finalizing_closed_or_no_zero_generator_cannot_qualify(self):
  for close in['finally_enter','normal_exit','unwind']:
   live=Liveness();live.observe({'kind':'generator_bound','generator':1,'at':.5});live.observe({'kind':'actual_counter_commit','value':0,'at':1});live.observe({'kind':close,'generator':1,'is_generator':True,'at':1.1});live.observe({'kind':'yield','role':'other','generator':1,'yield_bytes':8192,'at':2});self.assertFalse(live.qualified)
 def test_generation_marker_retains_only_scalar_identity_not_state_or_secrets(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d)
   class State:
    def __init__(self):self.created_at=123.25;self.content_uuid='owned-content';self.m3u_profile_id=2;self.password='NEVER_STORE_STATE_SECRET'
   def read_state():return State()
   p.add(read_state.__code__,'state_read');p.start()
   try:
    with p.request('initial',SID):state=read_state()
   finally:p.stop()
   reference=weakref.ref(state);del state;self.assertIsNone(reference());events=rows(j);marker=next(e['state_marker']for e in events if 'state_marker'in e);self.assertEqual(marker['created_at'],123.25);self.assertEqual(marker['profile'],2);self.assertNotIn('NEVER_STORE',j.path.read_text());self.assertFalse(j.invalid)
 def test_atomic_active_decrement_result_is_bounded_bool_pair(self):
  with tempfile.TemporaryDirectory()as d:
   j,p=self.fixture(d)
   def decr():return True,False
   p.add(decr.__code__,'active_decr');p.start()
   try:
    with p.request('follower',SID):self.assertEqual(decr(),(True,False))
   finally:p.stop()
   self.assertEqual(next(e['result']for e in rows(j)if 'result'in e),[True,False]);self.assertFalse(j.invalid)
 def test_source_install_is_blocked_before_Django_trace_or_IO(self):
  import source_install
  with mock.patch.object(sys,'settrace')as setter:
   with self.assertRaisesRegex(RuntimeError,'RequiresRootReview'):source_install.install({},'busy','unused')
   setter.assert_not_called()
 def test_liveness_requires_registered_generator_and_integer_counter_zero(self):
  for identifier in[None,False,'1',1]:
   live=Liveness();live.observe({'kind':'actual_counter_commit','value':0,'at':1});live.observe({'kind':'yield','role':'other','generator':identifier,'yield_bytes':8192,'at':2});self.assertFalse(live.qualified)
  live=Liveness();live.observe({'kind':'generator_bound','generator':1,'at':.5});live.observe({'kind':'actual_counter_commit','value':False,'at':1});live.observe({'kind':'yield','role':'other','generator':1,'yield_bytes':8192,'at':2});self.assertTrue(live.invalid);self.assertFalse(live.qualified)
 def test_pinned_original_compile_rules_without_module_execution(self):
  source=Path(os.environ['CAUSAL_PINNED_SOURCE']);selected=pinned_source.rules(source);self.assertEqual(sum(kind=='callback'for _,kind,_ in selected),6);self.assertLessEqual(len(selected),48);self.assertTrue(all(code.co_filename.startswith('/app/')for code,_,_ in selected))
if __name__=='__main__':unittest.main(verbosity=2)
