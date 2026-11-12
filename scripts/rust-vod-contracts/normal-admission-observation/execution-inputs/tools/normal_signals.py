"""Bounded genuine Django request finalization; no body read/generator claim."""
import hashlib,json,threading,time
from pathlib import Path
from request_observer import Journal
class Observer:
 def __init__(self,seed,root,context):
  self.journal=Journal(Path(root)/'normal-signals.jsonl',Path(root)/'normal-signals-invalid.json',max_rows=32,max_bytes=1048576);self.context=context;self.sequence=0;self.lock=threading.Lock();self.owned={}
  for role,key,sid in[('shared_initial','movie_uuid','fixture-normal-shared'),('shared_follower','movie_uuid','fixture-normal-shared'),('other','other_movie_uuid','fixture-normal-other'),('probe1','probe1_movie_uuid','fixture-normal-new-1'),('probe2','probe2_movie_uuid','fixture-normal-new-2')]:self.owned.setdefault('/proxy/vod/movie/'+seed[key]+'/'+sid,{})['owned-normal-'+role]=role
 def started(self,sender=None,environ=None,**kwargs):
  try:
   environ=environ or{};path=environ.get('PATH_INFO','');agent=environ.get('HTTP_USER_AGENT','');role=self.owned.get(path,{}).get(agent)
   if path=='/proxy/vod/stop_client/'and agent=='owned-normal-admin-stop':role='admin_action'
   if getattr(self.context,'current',None)is not None:self.journal.fail('OverlappingNormalRequest')
   self.context.current={'ignored':True}
   if role is None:return
   with self.lock:self.sequence+=1;identity=self.sequence
   if identity>16:self.journal.fail('NormalRequestCount');return
   row={'request':identity,'role':role,'sid_sha256':None if role=='admin_action'else hashlib.sha256(path.rsplit('/',1)[-1].encode()).hexdigest()};self.context.current=row;self.journal.append({**row,'kind':'request_started','monotonic':time.monotonic()})
  except Exception:self.journal.fail('NormalSignalStart')
 def finished(self,sender=None,**kwargs):
  try:
   value=getattr(self.context,'current',None);self.context.current=None
   if value is None:self.journal.fail('UncorrelatedNormalFinalization');return
   if not value.get('ignored'):self.journal.append({**value,'kind':'request_finalized','monotonic':time.monotonic()})
  except Exception:self.journal.fail('NormalSignalFinish')
def install(seed,root):
 from django.core.signals import request_started,request_finished
 from gevent.local import local
 observer=Observer(seed,root,local());request_started.connect(observer.started,weak=False,dispatch_uid='owned-normal-start');request_finished.connect(observer.finished,weak=False,dispatch_uid='owned-normal-finish');return observer

def validate(status,rows,complete=False):
 import re,math
 if status.get('invalid')or status.get('rows')!=len(rows):raise ValueError('NormalSignalStatusCount')
 started={};ended={};roles={'shared_initial','shared_follower','other','probe1','probe2','admin_action'}
 for row in rows:
  if set(row)!={'request','role','sid_sha256','kind','monotonic'}or type(row['request'])is not int or not 1<=row['request']<=16 or row['role']not in roles or type(row['monotonic'])not in[int,float]or not math.isfinite(row['monotonic']):raise ValueError('NormalSignalShape')
  if row['role']=='admin_action':
   if row['sid_sha256']is not None:raise ValueError('NoAdminViewerGuess')
  elif type(row['sid_sha256'])is not str or not re.fullmatch('[0-9a-f]{64}',row['sid_sha256']):raise ValueError('OwnedSessionHash')
  target=started if row['kind']=='request_started'else ended if row['kind']=='request_finalized'else None
  if target is None or row['request']in target:raise ValueError('DuplicateNormalSignal')
  target[row['request']]=row
 for identity,end in ended.items():
  begin=started.get(identity)
  if begin is None or(begin['role'],begin['sid_sha256'])!=(end['role'],end['sid_sha256'])or end['monotonic']<begin['monotonic']:raise ValueError('NormalSignalIdentityOrder')
 if complete and(set(started)!=set(ended)or{row['role']for row in started.values()}!=roles or len(started)!=6):raise ValueError('NormalFinalizationIncomplete')
 return{'valid':True,'complete':set(started)==set(ended)and len(started)==6,'started':len(started),'finalized':len(ended),'not_generator_finally':True}
