"""Pre-WSGI causal observation. Application remains blocked pending Root cue."""
import hashlib,json,os,sys,threading,time
from pathlib import Path
from causal_probe import Journal,Probe
EXECUTION_BLOCKED=True
class StickyJournal(Journal):
 def __init__(self,path,**limits):
  super().__init__(path,**limits);self.marker=self.path.with_name('Source-causal-invalid.json');self.kinds={};self.code_hits={};self.threads=set()
 def fail(self,reason):
  super().fail(reason)
  try:
   with self.lock:
    if not self.marker.exists():
     with self.marker.open('x')as stream:stream.write('{"invalid":true}\n');stream.flush();os.fsync(stream.fileno())
  except Exception:pass
 def append(self,row):
  with self.lock:
   kind=row.get('kind');code=row.get('code');key=(code,row.get('firstline'));thread=row.get('logical_thread_id')
   if (kind not in self.kinds and len(self.kinds)>=64)or(code is not None and key not in self.code_hits and len(self.code_hits)>=48)or(thread is not None and thread not in self.threads and len(self.threads)>=128):self.fail('CoverageMapCapacity');return False
   if not super().append(row):return False
   kind=row['kind'];self.kinds[kind]=self.kinds.get(kind,0)+1
   code=row.get('code')
   if code is not None:
    key=(code,row.get('firstline'));self.code_hits[key]=self.code_hits.get(key,0)+1
   thread=row.get('logical_thread_id')
   if thread is not None:self.threads.add(thread)
   if len(self.kinds)>64 or len(self.code_hits)>48 or len(self.threads)>128:self.fail('CoverageMapCapacity')
   return True
 def snapshot(self):
  with self.lock:return{**self.status(),'kinds':dict(self.kinds),'hit_code_count':len(self.code_hits),'logical_thread_count':len(self.threads),'marker_present':self.marker.exists()}
class Installation:
 def __init__(self,seed,output,context,registry_root,expected_redis_files,wire_port=6383):
  import pinned_source,redis_binding
  if type(wire_port)is not int or not 1<=wire_port<=65535 or not expected_redis_files:raise ValueError('ExplicitReviewedWireBindingRequired')
  sid='fixture-pending-main-busy';other='fixture-pending-main-other';self.context=context;self.journal=StickyJournal(Path(output)/'Source-causal.jsonl',max_rows=3072,max_bytes=6291456);self.probe=Probe(self.journal,{sid:'target-family',other:'other-family'},[seed['provider_profile_id']],context=context,wire_ports=(wire_port,));self.registry=pinned_source.bind(self.probe,registry_root);self.redis_registry=redis_binding.bind(self.probe,expected_redis_files);self.sequence=0;self.lock=threading.RLock();self.connected=[]
  self.owned={'/proxy/vod/movie/'+seed['movie_uuid']+'/'+sid:{'owned-pending-initial':'initial','owned-pending-target':'target','owned-pending-follower':'follower'},'/proxy/vod/movie/'+seed['other_movie_uuid']+'/'+other:{'owned-pending-other':'other'}}
 def started(self,sender=None,environ=None,**kwargs):
  try:
   environ=environ or{};path=environ.get('PATH_INFO','');agent=environ.get('HTTP_USER_AGENT','');admin=path=='/proxy/vod/stop_client/'and agent=='owned-causal-admin-stop';role='admin_action'if admin else self.owned.get(path,{}).get(agent)
   if getattr(self.context,'current',None)is not None:self.journal.fail('OverlappingSourceRequestContext')
   self.context.current={'ignored':True};self.context.request_identity=None
   if role is None:return
   with self.lock:self.sequence+=1;identity=self.sequence
   if identity>256:self.journal.fail('RequestIdentityCapacity');return
   row={'role':role,'action':'stop_client'}if admin else{'role':role,'sid_sha256':hashlib.sha256(path.rsplit('/',1)[-1].encode()).hexdigest()};self.context.current=row;self.context.request_identity=identity;self.probe.emit('admin_action_started'if admin else'request_started',row,request=identity)
  except Exception:self.journal.fail('SourceContextStartFailure')
 def finished(self,sender=None,**kwargs):
  try:
   row=getattr(self.context,'current',None);identity=getattr(self.context,'request_identity',None);self.context.current=None;self.context.request_identity=None
   if row is None:self.journal.fail('UncorrelatedRequestFinalized');return
   if not row.get('ignored'):self.probe.emit('admin_action_finalized'if row.get('role')=='admin_action'else'request_finalized',row,request=identity)
  except Exception:self.journal.fail('SourceContextFinishFailure')
 def start(self,started_signal,finished_signal):
  try:
   self.probe.start()
   for signal,receiver,identity in[(started_signal,self.started,'owned-causal-start'),(finished_signal,self.finished,'owned-causal-finish')]:
    signal.connect(receiver,weak=False,dispatch_uid=identity);self.connected.append((signal,identity))
  except Exception:
   self.stop();raise
 def stop(self):
  for signal,identity in self.connected:signal.disconnect(dispatch_uid=identity)
  self.connected=[]
  if self.probe.started:self.probe.stop()
 def snapshot(self):
  value={**self.journal.snapshot(),'trace_owned':self.probe.started and sys.gettrace()==self.probe.trace,'future_thread_trace_owned':threading.gettrace()==self.probe.trace,'rule_count':len(self.probe.rules),'monotonic':time.monotonic(),'scope':'request_finalized means response-close signal, not successful generator finally'}
  if not value['trace_owned']or not value['future_thread_trace_owned']:self.journal.fail('TraceOwnershipLost');value.update(invalid=list(self.journal.invalid),marker_present=self.journal.marker.exists())
  if len(json.dumps(value).encode())>4096:self.journal.fail('StatusCapacity');return{'invalid':['StatusCapacity']}
  return value
 def terminal(self):
  value=self.snapshot();value['rule_hits']=[{'code':code,'firstline':line,'events':count}for(code,line),count in self.journal.code_hits.items()];value['registry']=self.registry;value['redis_registry']=self.redis_registry;return value

def install(seed,case,output,expected_redis_files=None,wire_port=6383):
 if EXECUTION_BLOCKED:raise RuntimeError('SourceCausalInstrumentationRequiresRootReview')
 if case!='busy':raise ValueError('ExactlyOnePredeclaredBusyCohort')
 from gevent.local import local
 from django.core.signals import request_started,request_finished
 observer=Installation(seed,output,local(),'/app',expected_redis_files,wire_port);observer.start(request_started,request_finished);return observer
