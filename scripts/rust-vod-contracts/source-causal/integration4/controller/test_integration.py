"""Real Django signal dispatch, synthetic requests; never django.setup/Source apps."""
import copy,datetime,hashlib,json,os,tempfile,threading,time,unittest
from pathlib import Path
from django.core.signals import request_started,request_finished
import redis_binding
from source_install import Installation,StickyJournal
from causal_probe import Journal,RedisEffects
import activation,coverage
class Integration(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);metadata,_=redis_binding.describe();self.observer=Installation({'provider_profile_id':2,'movie_uuid':'ownedmovie','other_movie_uuid':'othermovie'},self.root,threading.local(),os.environ['CAUSAL_PINNED_SOURCE'],metadata['files']);self.observer.start(request_started,request_finished)
 def tearDown(self):self.observer.stop();self.tmp.cleanup()
 def test_real_signal_dispatch_request_finalization_is_not_generator_finally(self):
  request_started.send(sender=self.__class__,environ={'PATH_INFO':'/proxy/vod/movie/ownedmovie/fixture-pending-main-busy','HTTP_USER_AGENT':'owned-pending-target','HTTP_AUTHORIZATION':'not-collected'})
  request_finished.send(sender=self.__class__);rows=[json.loads(line)for line in self.observer.journal.path.read_text().splitlines()];self.assertEqual([row['kind']for row in rows],['request_started','request_finalized']);self.assertEqual(rows[0]['request'],rows[1]['request']);self.assertNotIn('not-collected',self.observer.journal.path.read_text());status=self.observer.snapshot();self.assertTrue(status['trace_owned']);self.assertTrue(status['future_thread_trace_owned']);self.assertFalse(status['invalid'])
 def test_automatic_generator_constructor_binds_under_selected_parent(self):
  class Closing:
   def __init__(self,generator):self.generator=generator
  def stream():yield b'owned'
  def outer():return Closing(stream())
  probe=self.observer.probe;probe.add(Closing.__init__.__code__,'generator_ctor');probe.add(outer.__code__,'function');probe.add(stream.__code__,'generator')
  request_started.send(sender=self.__class__,environ={'PATH_INFO':'/proxy/vod/movie/ownedmovie/fixture-pending-main-busy','HTTP_USER_AGENT':'owned-pending-initial'})
  wrapper=outer();self.assertEqual(next(wrapper.generator),b'owned');wrapper.generator.close();request_finished.send(sender=self.__class__);self.assertFalse(self.observer.journal.invalid,self.observer.journal.invalid)
  rows=[json.loads(line)for line in self.observer.journal.path.read_text().splitlines()];self.assertEqual(sum(row['kind']=='generator_bound'for row in rows),1)
 def test_distinct_admin_action_signal_never_reads_business_body_or_guesses_viewer(self):
  class ForbiddenBody:
   def read(self,*args):raise AssertionError('ObserverConsumedBusinessBody')
  request_started.send(sender=self.__class__,environ={'PATH_INFO':'/proxy/vod/stop_client/','HTTP_USER_AGENT':'owned-causal-admin-stop','wsgi.input':ForbiddenBody()});self.assertEqual(self.observer.context.current,{'role':'admin_action','action':'stop_client'});request_finished.send(sender=self.__class__)
  rows=[json.loads(line)for line in self.observer.journal.path.read_text().splitlines()];self.assertEqual([row['kind']for row in rows],['admin_action_started','admin_action_finalized']);self.assertTrue(all('sid_sha256'not in row for row in rows));self.assertFalse(self.observer.journal.invalid)
 def test_owned_profile_or_client_argument_background_scope_is_not_viewer(self):
  def profile_counter(profile_id):return True
  def client_counter(client_id):return True
  probe=self.observer.probe;probe.add(profile_counter.__code__,'counter');probe.add(client_counter.__code__,'counter');profile_counter(2);client_counter('fixture-pending-main-other');profile_counter('2');profile_counter('02');profile_counter(999)
  rows=[json.loads(line)for line in self.observer.journal.path.read_text().splitlines()];calls=[row for row in rows if row['kind']=='call'];self.assertEqual([row['role']for row in calls],['owned_profile','owned_object','owned_profile']);self.assertEqual(calls[0]['action'],'known_owned_profile');self.assertEqual(calls[1]['action'],'known_owned_argument');self.assertTrue(all('creator_generator'not in row for row in calls));self.assertFalse(self.observer.journal.invalid)
 def test_all_pinned_manager_counter_mutators_and_admin_view_registered(self):
  selected=self.observer.registry
  for suffix in['create_connection','remove_connection']:
   matches=[row for row in selected if row['qualname'].endswith('.'+suffix)];self.assertEqual(len(matches),1);self.assertEqual(matches[0]['kind'],'counter')
  self.assertEqual(sum(row['qualname']=='stop_vod_client'for row in selected),1)
 def test_ignored_request_not_owned_or_spurious_finalized(self):
  request_started.send(sender=self.__class__,environ={'PATH_INFO':'/api/core/version/','HTTP_USER_AGENT':'unowned'});request_finished.send(sender=self.__class__);self.assertEqual(self.observer.journal.rows,0);self.assertFalse(self.observer.journal.invalid)
 def test_real_signal_dispatch_overflow_sticky_marker_never_escapes(self):
  self.observer.journal.max_rows=1
  request_started.send(sender=self.__class__,environ={'PATH_INFO':'/proxy/vod/movie/ownedmovie/fixture-pending-main-busy','HTTP_USER_AGENT':'owned-pending-target'});request_finished.send(sender=self.__class__);self.assertTrue(self.observer.journal.invalid);self.assertTrue(self.observer.journal.marker.exists());self.assertEqual(self.observer.journal.rows,1)
 def test_uncorrelated_finalization_and_lost_thread_hook_invalidate(self):
  request_finished.send(sender=self.__class__);self.assertIn('UncorrelatedRequestFinalized',self.observer.journal.invalid)
 def test_real_gevent_status_pool_retains_tracer_ownership(self):
  from gevent.pywsgi import WSGIServer
  from gevent.pool import Pool
  import urllib.request
  def app(environ,start_response):
   raw=json.dumps(self.observer.snapshot()).encode();start_response('200 OK',[('Content-Length',str(len(raw)))]);return[raw]
  server=WSGIServer(('127.0.0.1',0),app,spawn=Pool(2),log=None,error_log=None);server.start()
  try:
   with urllib.request.urlopen('http://127.0.0.1:'+str(server.server_port)+'/state',timeout=2)as response:status=json.loads(response.read(4097))
   self.assertTrue(status['trace_owned']);self.assertTrue(status['future_thread_trace_owned']);self.assertFalse(status['invalid'])
  finally:server.stop(timeout=2)
 def test_thread_hook_loss_is_sticky_invalid(self):
  threading.settrace(None)
  try:self.assertIn('TraceOwnershipLost',self.observer.snapshot()['invalid'])
  finally:threading.settrace(self.observer.probe.trace)
 def test_nonASCII_unrelated_bulk_is_not_a_counter_or_status(self):
  journal=Journal(self.root/'wire');effects=RedisEffects(journal);effects.command(1,'OTHER');effects.reply(1,b'\x80\xffpickle');effects.close(1);self.assertFalse(journal.invalid);self.assertEqual(journal.rows,0)

class Coverage(unittest.TestCase):
 def complete(self):
  trace=[];wire=[{'kind':'wire_connection_open','connection':1,'at':0,'peer_port':41000,'local_port':6383}];registry=[]
  for ordinal,family,value,start in[(1,'INCR',2,1),(2,'DECR',1,6)]:
   writer=ordinal*2-1;frame=ordinal*2;code=str(ordinal)*64
   trace.extend([{'kind':'call','frame':writer,'at':start,'role':'target','code':code,'firstline':ordinal,'rule_kind':'counter'},{'kind':'call','frame':frame,'at':start+1,'code':'f'*64,'firstline':99,'rule_kind':'wire_call'},{'kind':'wire_link','frame':frame,'writer_frame':writer,'at':start+1.1,'local_port':41000,'remote_port':6383},{'kind':'normal_exit','frame':frame,'at':start+3},{'kind':'normal_exit','frame':writer,'at':start+4}]);wire.append({'kind':'actual_counter_commit','connection':1,'sequence':ordinal,'command_at':start+1.5,'at':start+2,'family':family,'value':value,'via':'direct'});registry.append({'qualname':'reserve_profile_slot'if ordinal==1 else'release_profile_slot','code_sha256':code,'firstline':ordinal})
  wire.append({'kind':'wire_connection_close','connection':1,'at':11})
  for identity,role in enumerate(['initial','follower','other','target'],10):
   trace.extend([{'kind':'request_started','request':identity,'role':role,'sid_sha256':'a'*64,'at':.1},{'kind':'request_finalized','request':identity,'role':role,'sid_sha256':'a'*64,'at':15}])
   if role!='target':trace.extend([{'kind':'generator_bound','generator':identity,'role':role,'at':.2},{'kind':'yield','generator':identity,'role':role,'at':.3,'yield_bytes':8192}])
  trace.extend([{'kind':'finally_enter','generator':10,'at':12},{'kind':'callback_bound','callback':1,'creator_generator':10,'logical_thread_id':1,'at':12},{'kind':'call','frame':40,'code':'d'*64,'firstline':100,'rule_kind':'callback','callback':1,'creator_generator':10,'logical_thread_id':2,'native_thread_id':1,'at':14}])
  trace.extend([{'kind':'admin_action_started','request':99,'role':'admin_action','action':'stop_client','at':1.1},{'kind':'admin_action_finalized','request':99,'role':'admin_action','action':'stop_client','at':2.1},{'kind':'call','frame':100,'code':'c'*64,'firstline':1413,'role':'admin_action','action':'stop_client','at':1.2}]);registry.append({'qualname':'stop_vod_client','code_sha256':'c'*64,'firstline':1413})
  status={'invalid':[],'marker_present':False,'trace_owned':True,'future_thread_trace_owned':True};proxy={'invalid':[],'connections':0,'seen':1,'threads_settled':True,'journal':{'invalid':[]}};return trace,wire,status,proxy,registry
 def check(self,trace,wire,status,proxy,registry):
  status.update(rows=len(trace),bytes=sum(len((json.dumps(row,separators=(',',':'))+'\n').encode())for row in trace));proxy['journal'].update(rows=len(wire),bytes=sum(len((json.dumps(row,separators=(',',':'))+'\n').encode())for row in wire));return coverage.validate(trace,wire,status,proxy,registry,{'valid':True})
 def test_complete_scalar_fixture_and_required_coverage_negatives(self):
  # Explicit synthetic evaluator fixture, NOT executed Source trace authority.
  args=self.complete();self.assertTrue(self.check(*args)['valid'])
  for missing in['finally_enter','callback_bound','request_finalized','generator_bound','admin_action_started']:
   args=list(self.complete());args[0]=[row for row in args[0]if row['kind']!=missing]
   with self.assertRaises(ValueError):self.check(*args)
  args=list(self.complete());args[4]=[]
  with self.assertRaisesRegex(ValueError,'RequiredCounterCodeCoverage'):self.check(*args)
 def test_capture_validity_is_distinct_from_reference_expectation(self):
  report={'source':{'observation_valid':True},'Source_signal_terminal_valid':True,'causal_coverage':{'valid':True},'default_named_tables_sequences_unchanged':True,'remaining':{'containers':'','networks':'','volumes':''},'Source_reference_expectation_matched':False};result=coverage.finalize_capture(report);self.assertTrue(result['observation_valid']);self.assertFalse(result['passed']);self.assertIsNone(result['parity_passed'])
  report['remaining']['containers']='owned-not-cleaned';self.assertFalse(coverage.finalize_capture(report)['observation_valid'])
 def test_HTTP_success_does_not_hide_incomplete_causal_coverage_or_invent_zero(self):
  report={'source':{'observation_valid':True},'Source_signal_terminal_valid':True,'causal_coverage':{'valid':False,'incomplete':True},'default_named_tables_sequences_unchanged':True,'remaining':{'containers':'','networks':'','volumes':''},'Source_reference_expectation_matched':True};result=coverage.finalize_capture(report);self.assertTrue(result['HTTP_cohort_valid']);self.assertFalse(result['causal_coverage_valid']);self.assertFalse(result['observation_valid']);self.assertFalse(coverage.scalar_observations([],[])['zero_observed'])
  zero=coverage.scalar_observations([],[{'kind':'actual_counter_commit','at':1,'value':0}]);self.assertTrue(zero['zero_observed']);self.assertEqual(zero['raw_other_generator_yields_after_zero'],[])
 def test_empty_writes_cannot_vacuously_qualify(self):
  status={'invalid':[],'marker_present':False,'trace_owned':True,'future_thread_trace_owned':True,'rows':0,'bytes':0};proxy={'invalid':[],'journal':{'invalid':[],'rows':0,'bytes':0},'connections':0,'seen':1,'threads_settled':True}
  with self.assertRaisesRegex(ValueError,'MissingActualBusyINCRReference'):coverage.validate([],[],status,proxy,[],{})
 def test_invalid_status_incomplete_proxy_or_counts_never_qualify(self):
  status={'invalid':[],'marker_present':False,'trace_owned':True,'future_thread_trace_owned':True,'rows':0,'bytes':0};proxy={'invalid':[],'journal':{'invalid':[],'rows':0,'bytes':0},'connections':0,'seen':1,'threads_settled':True}
  for key,value in[('invalid',['overflow']),('marker_present',True),('trace_owned',False),('future_thread_trace_owned',False),('rows',1)]:
   changed={**status,key:value}
   with self.assertRaises(ValueError):coverage.validate([],[],changed,proxy,[],{})
 def test_counter_without_complete_caller_socket_windows_rejected(self):
  status={'invalid':[],'marker_present':False,'trace_owned':True,'future_thread_trace_owned':True,'rows':0,'bytes':0};proxy={'invalid':[],'journal':{'invalid':[],'rows':0,'bytes':0},'connections':0,'seen':1,'threads_settled':True};effect={'kind':'actual_counter_commit','connection':1,'sequence':1,'command_at':1,'at':2,'family':'INCR','value':2,'via':'direct'}
  proxy['journal'].update(rows=1,bytes=len((json.dumps(effect,separators=(',',':'))+'\n').encode()))
  with self.assertRaisesRegex(ValueError,'IncompleteWireCallerAttribution'):coverage.validate([],[effect],status,proxy,[],{})

class Activation(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);(self.root/'policy.json').write_text('{}');self.rows=[{'path':'policy.json','sha256':activation.digest(self.root/'policy.json')}];(self.root/'selection.json').write_text(json.dumps({'files':self.rows}));self.now=time.time();self.cue={'authorized':True,'Source_application_requests_authorized':True,'action':activation.ACTION,'new_output':activation.OUTPUT,'Native_requests':False,'resource_sampling':False,'performance_acceptance':False,'full_M4_acceptance':False,'cases':['busy'],'repetitions':1,'Original_revision':activation.ORIGINAL,'Original_archive_sha256':activation.ARCHIVE,'boot_id':'owned','issued_epoch':self.now-1,'expires_epoch':self.now+100,'reviewed_selection_sha256':activation.digest(self.root/'selection.json'),'reviewed_policy_sha256':activation.digest(self.root/'policy.json')}
  for key in['issued','expires']:self.cue[key+'_utc']=datetime.datetime.fromtimestamp(self.cue[key+'_epoch'],datetime.timezone.utc).isoformat().replace('+00:00','Z')
 def tearDown(self):self.tmp.cleanup()
 def test_once_claim_child_binding_and_preexisting_output(self):
  cue={**self.cue,'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()};cuepath=self.root/'cue.json';cuepath.write_text(json.dumps(cue));output=self.root/activation.OUTPUT
  bound,claim,proof=activation.claim(cuepath,self.root,output,self.root);self.assertTrue(claim.exists())
  with self.assertRaises(FileExistsError):activation.claim(cuepath,self.root,output,self.root)
  child=self.root/'child';child.mkdir();(child/'execution-cue.json').write_text(json.dumps(cue));(child/'execution-claim.json').write_bytes(claim.read_bytes());self.assertEqual(activation.child_guard(child/'execution-cue.json',self.root,self.root)['file_count'],1)
  value=json.loads((child/'execution-claim.json').read_text());value['cue_sha256']='0'*64;(child/'execution-claim.json').write_text(json.dumps(value))
  with self.assertRaisesRegex(ValueError,'ExactParentClaimRequired'):activation.child_guard(child/'execution-cue.json',self.root,self.root)
 def test_entry_block_precedes_any_dispatch(self):
  import importlib.util,sys
  from unittest import mock
  spec=importlib.util.spec_from_file_location('blocked_causal_entry',Path(__file__).with_name('run.py'));module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  with mock.patch.object(sys,'argv',['run','--repo',str(self.root),'--cue',str(self.root/'absent-cue'),'--output',str(self.root/'out')]),mock.patch.object(module,'execute')as execute:
   with self.assertRaisesRegex(RuntimeError,'RootReviewAndFreshCueRequired'):module.main()
   execute.assert_not_called()
 def test_unsafe_or_duplicate_selected_paths_reject(self):
  for rows in[[{'path':'../outside','sha256':'0'*64}],self.rows+self.rows]:
   (self.root/'selection.json').write_text(json.dumps({'files':rows}));cue={**self.cue,'reviewed_selection_sha256':activation.digest(self.root/'selection.json')}
   with self.assertRaises(ValueError):activation.validate(cue,self.root,self.now,'owned')
 def test_exact_pure_guard_and_negative_boundaries(self):
  self.assertEqual(activation.validate(self.cue,self.root,self.now,'owned')['file_count'],1)
  for key,value in[('authorized',False),('Native_requests',True),('cases',['success']),('repetitions',True),('boot_id','wrong'),('expires_epoch',float('nan')),('reviewed_selection_sha256','0'*64),('Original_archive_sha256','0'*64)]:
   with self.assertRaises(ValueError):activation.validate({**self.cue,key:value},self.root,self.now,'owned')
  (self.root/'policy.json').write_text('modified')
  with self.assertRaises(ValueError):activation.validate(self.cue,self.root,self.now,'owned')
if __name__=='__main__':unittest.main(verbosity=2)
