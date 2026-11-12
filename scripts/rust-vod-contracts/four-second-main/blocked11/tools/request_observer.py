"""Bounded signal observer: never replaces views/Redis or raises into dispatch."""
import hashlib,json,math,os,re,threading,time
from pathlib import Path
class Journal:
 def __init__(self,path,marker,max_rows=256,max_bytes=1048576):
  self.path,self.marker=Path(path),Path(marker);self.max_rows,self.max_bytes=max_rows,max_bytes;self.rows=0;self.used=0;self.invalid=[];self.lock=threading.RLock()
 def fail(self,reason):
  with self.lock:
   if reason not in self.invalid:self.invalid.append(reason)
   try:
    if not self.marker.exists():
     with self.marker.open('x')as stream:stream.write('{"invalid":true}\n');stream.flush();os.fsync(stream.fileno())
   except Exception:pass
 def snapshot(self):
  with self.lock:return{'invalid':list(self.invalid),'rows':self.rows,'bytes':self.used,'maximum_rows':self.max_rows,'maximum_bytes':self.max_bytes}
 def append(self,row):
  try:
   raw=(json.dumps(row,sort_keys=True)+'\n').encode()
   with self.lock:
    if self.invalid:return False
    if self.rows>=self.max_rows or len(raw)>4096 or self.used+len(raw)>self.max_bytes:self.fail('SignalJournalCapacity');return False
    with self.path.open('ab')as stream:
     if stream.write(raw)!=len(raw):raise OSError('ShortSignalWrite')
    self.rows+=1;self.used+=len(raw)
   return True
  except Exception:self.fail('SignalJournalWrite');return False
class Observer:
 def __init__(self,journal,context,owned):
  self.journal,self.context,self.owned=journal,context,owned;self.sequence=0;self.lock=threading.RLock()
 def started(self,sender=None,environ=None,**_):
  try:
   if self.journal.invalid:self.context.current={'ignored':True};return
   if getattr(self.context,'current',None)is not None:self.journal.fail('OverlappingSignalContext')
   environ={}if environ is None else environ
   path=environ.get('PATH_INFO','');role=environ.get('HTTP_USER_AGENT','')
   if path not in self.owned or role not in self.owned[path]:self.context.current={'ignored':True};return
   with self.lock:self.sequence+=1;identity=self.sequence
   value={'request':identity,'role':self.owned[path][role],'sid_sha256':hashlib.sha256(path.rsplit('/',1)[-1].encode()).hexdigest()}
   self.context.current=value;self.journal.append({**value,'kind':'request_started','monotonic':time.monotonic()})
  except Exception:self.journal.fail('SignalStartException')
 def finished(self,sender=None,**_):
  try:
   value=getattr(self.context,'current',None)
   if value is None:self.journal.fail('UncorrelatedRequestFinished');return
   self.context.current=None
   if not value.get('ignored'):self.journal.append({**value,'kind':'request_finished','monotonic':time.monotonic()})
  except Exception:self.journal.fail('SignalFinishException')
def validate(rows,required_roles):
 started={};finished={}
 for row in rows:
  if set(row)!={'request','role','sid_sha256','kind','monotonic'}:raise ValueError('SignalRowAllowlist')
  if type(row['request'])is not int or not 1<=row['request']<=256 or not isinstance(row['sid_sha256'],str)or not re.fullmatch('[0-9a-f]{64}',row['sid_sha256']):raise ValueError('SignalIdentityShape')
  if type(row['monotonic'])not in[int,float]or not math.isfinite(row['monotonic']):raise ValueError('SignalClockShape')
  if row['kind']=='request_started':target=started
  elif row['kind']=='request_finished':target=finished
  else:raise ValueError('SignalKind')
  if row['request']in target:raise ValueError('DuplicateSignalIdentity')
  target[row['request']]=row
 if not started or set(started)!=set(finished):raise ValueError('MissingSignalCompletion')
 for identity,begin in started.items():
  end=finished[identity]
  if (begin['role'],begin['sid_sha256'])!=(end['role'],end['sid_sha256'])or end['monotonic']<begin['monotonic']:raise ValueError('SignalCorrelation')
 if set(x['role']for x in started.values())!=set(required_roles)or any(sum(x['role']==role for x in started.values())!=1 for role in required_roles):raise ValueError('SignalRoleCoverage')
 return{'valid':True,'started':len(started),'finished':len(finished),'roles':sorted(required_roles),'scope':'Django request_finished after response-close resource-closer attempts; not successful generator-finally, provider body EOF/TCP close or daemon completion'}
def install(seed,output):
 from django.core.signals import request_started,request_finished
 from gevent.local import local
 output=Path(output);journal=Journal(output/'Source-request-signals.jsonl',output/'Source-request-signals-invalid.json');owned={}
 for case in['success','busy']:
  path='/proxy/vod/movie/'+seed['movie_uuid']+'/fixture-pending-main-'+case
  owned[path]={'owned-pending-initial':'initial','owned-pending-target':'target','owned-pending-follower':'follower'}
 owned['/proxy/vod/movie/'+seed['other_movie_uuid']+'/fixture-pending-main-other']={'owned-pending-other':'other'}
 observer=Observer(journal,local(),owned)
 request_started.connect(observer.started,weak=False,dispatch_uid='owned-4s-observation-start')
 request_finished.connect(observer.finished,weak=False,dispatch_uid='owned-4s-observation-finish')
 return observer
