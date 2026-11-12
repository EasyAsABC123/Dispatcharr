"""Blocked ordinary-next-admission controller core. No I/O on import.
This reviewed schedule receives clock/physical-close acknowledgements only;
profile counters and HTTP outcomes are evidence, never scheduling inputs.
"""
import argparse,json,math
from pathlib import Path
EXECUTION_BLOCKED=True
ROLES=('shared_initial','shared_follower','other','probe1','probe2')
SESSIONS={'shared_initial':'fixture-normal-shared','shared_follower':'fixture-normal-shared','other':'fixture-normal-other','probe1':'fixture-normal-new-1','probe2':'fixture-normal-new-2'}
def finite(value):
 if type(value)not in[int,float]or not math.isfinite(value):raise ValueError('FiniteMonotonicClockRequired')
 return value

def physical_close(row,role):
 if row.get('role')!=role or type(row.get('fd_after'))is not int or row['fd_after']!=-1 or row.get('original_inode_absent')is not True or row.get('server_cancellation_inferred')is not False:raise ValueError('ExactOwnedSocketAcknowledgement')
 began=finite(row.get('close_begin'));ended=finite(row.get('close_complete'))
 if ended<began:raise ValueError('CloseClockOrder')
 return ended

class Controller:
 def __init__(self):self.state='fresh';self.prelude=[];self.receipts=[];self.counter_evidence=[];self.probe1_done=None
 def prelude_open(self,role,status,prefix_sha256):
  expected=('shared_initial','shared_follower','other')
  if self.state!='fresh'or len(self.prelude)>=3 or role!=expected[len(self.prelude)]:raise ValueError('FixedPreludeOrder')
  if type(status)is not int or status!=200 or prefix_sha256!='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f':raise ValueError('PreludeScenarioNotEstablished')
  self.prelude.append(role)
  if len(self.prelude)==3:self.state='prelude'
 def admin_completed(self,at,status):
  if self.state!='prelude':raise ValueError('AdminAfterPreludeOnly')
  self.admin_at=finite(at);self.admin_status=status;self.state='admin'
 def shared_closed(self,initial,follower):
  if self.state!='admin':raise ValueError('SharedCloseAfterAdminOnly')
  first=physical_close(initial,'shared_initial');second=physical_close(follower,'shared_follower')
  if initial['close_begin']<self.admin_at or follower['close_begin']<first:raise ValueError('FixedPhysicalCloseOrder')
  self.shared_close_at=second;self.probe1_at=second+3;self.state='closed_shared'
 def evidence_counter(self,label,value):
  # Literal bounded evidence only. No branch, wait condition or scheduling change.
  if type(label)is not str or len(label)>64 or len(self.counter_evidence)>=16:raise ValueError('CounterEvidenceCapacity')
  if value is not None and(type(value)is not str or len(value)>32):raise ValueError('LiteralCounterSchema')
  self.counter_evidence.append({'label':label,'literal':value})
 def dispatch_probe1(self,at):
  if self.state!='closed_shared':raise ValueError('ExactlyOneProbe1')
  actual=finite(at)
  if not self.probe1_at<=actual<=self.probe1_at+.35:raise ValueError('FixedProbe1ClockBoundary')
  self.receipts.append({'role':'probe1','session':SESSIONS['probe1'],'dispatched':actual});self.state='probe1'
 def probe1_observed(self,at,status,transport_error=None):
  if self.state!='probe1':raise ValueError('ActualBoundedHTTPOutcome')
  if status is None:
   if type(transport_error)is not str or not 1<=len(transport_error)<=64:raise ValueError('ExplicitBoundedTransportOutcome')
  elif type(status)is not int or not 100<=status<=599:raise ValueError('ActualBoundedHTTPOutcome')
  self.probe1_done=finite(at)
  if self.probe1_done<self.receipts[0]['dispatched']:raise ValueError('Probe1ObservationClock')
  # Status deliberately does not select the next action.
  self.probe1_status=status;self.probe2_at=self.probe1_done+.25;self.state='probe1_observed'
 def dispatch_probe2(self,at):
  if self.state!='probe1_observed':raise ValueError('ExactlyOneProbe2AfterFirstObservation')
  actual=finite(at)
  if not self.probe2_at<=actual<=self.probe2_at+.35:raise ValueError('FixedProbe2ClockBoundary')
  self.receipts.append({'role':'probe2','session':SESSIONS['probe2'],'dispatched':actual});self.state='probe2'
 def snapshot(self):return{'state':self.state,'dispatch_receipts':list(self.receipts),'counter_evidence':list(self.counter_evidence),'profile_counter_used_for_dispatch':False,'status_used_to_skip_probe2':False,'execution_authorized':False}

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--policy',type=Path,default=Path(__file__).with_name('policy.json'));args=parser.parse_args()
 if EXECUTION_BLOCKED:raise RuntimeError('PlanOnlyRootReviewBeforeSourceOrNativeExecution')
 raise RuntimeError('OwnedComposeOriginAndReaderAdaptersMustQualifyBeforeActivation')
if __name__=='__main__':main()
