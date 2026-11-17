"""Root-issued single functional deadline; cleanup is separately bounded once."""
import math,time
class LeaseExpired(TimeoutError):pass
class LeaseBudget:
 def __init__(self,deadline):
  if isinstance(deadline,bool)or not isinstance(deadline,(int,float))or not math.isfinite(deadline):raise ValueError('RootAbsoluteMonotonicDeadlineRequired')
  self.deadline=float(deadline);self.cleanup_deadline=None;self.cleaning=False
 def expired(self):return time.monotonic()>=self.deadline
 def begin_cleanup(self):
  if self.cleanup_deadline is None:self.cleanup_deadline=time.monotonic()+90
  self.cleaning=True
 def remaining(self,maximum):
  deadline=self.cleanup_deadline if self.cleaning else self.deadline;left=deadline-time.monotonic()
  if left<=0:raise LeaseExpired('Cleanup90sExpired'if self.cleaning else'WholeCohortAbsoluteDeadlineExpired')
  return min(float(maximum),left)

def bounded_read(response,limit,remaining):
 """Enforce the lease between actual socket reads, even for a slow trickle."""
 result=bytearray()
 while len(result)<=limit:
  seconds=remaining(5);pending=[response];seen=set();sock=None
  while pending:
   item=pending.pop()
   if id(item)in seen:continue
   seen.add(id(item))
   candidate=getattr(item,'_sock',None)
   if candidate is not None:sock=candidate;break
   for name in ('fp','raw'):
    child=getattr(item,name,None)
    if child is not None:pending.append(child)
  if sock is None:
   if getattr(response,'length',None)==0 or (callable(getattr(response,'isclosed',None))and response.isclosed()):return bytes(result)
   raise RuntimeError('OwnedHTTPDeadlineSocketUnavailable')
  sock.settimeout(seconds);block=response.read1(min(65536,limit+1-len(result)))
  if not block:return bytes(result)
  result.extend(block)
 return bytes(result)
