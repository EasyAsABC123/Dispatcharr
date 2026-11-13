"""Pure4s observation validity, with parity explicitly separate."""
import math
def hold(boundary,control_done,released):
 values=[boundary,control_done,released]
 if not all(type(v)in[int,float]and math.isfinite(v)for v in values):raise ValueError('ObservationClock')
 elapsed=released-boundary
 if not 0<=control_done-boundary<=3.1 or not 4<=elapsed<=4.35:raise ValueError('FourSecondHoldBound')
 return{'target_seconds':4,'maximum_seconds':4.35,'actual_seconds':elapsed,'overshoot_seconds':elapsed-4,'control_seconds':control_done-boundary}
def at_checkpoint(actual,boundary,offset):
 if not all(type(v)in[int,float]and math.isfinite(v)for v in[actual,boundary,offset]):raise ValueError('ObservationCheckpointClock')
 if not offset<=actual-boundary<=offset+.35:raise ValueError('ObservationCheckpointCadence')
 return{'offset_seconds':offset,'actual_offset_seconds':actual-boundary}
def compare(source,native):
 if not source.get('observation_valid')or not native.get('observation_valid'):return{'observation_valid':False,'parity_passed':False}
 if source.get('case')=='clientloss'or native.get('case')=='clientloss':
  if source.get('case')!=native.get('case'):return{'observation_valid':False,'parity_passed':False}
  same=client_loss(source)['matched']and client_loss(native)['matched']
  return{'observation_valid':True,'parity_passed':same,'scope':'real client FD/read-thread closure with no pre-close HTTP outcome; no server-handler cancellation or counter equality inference','full_M4_claim':False}
 s,n=source['target_outcome'],native['target_outcome']
 equal=s['status']==n['status']and s.get('error_body_hex')==n.get('error_body_hex')
 return{'observation_valid':True,'parity_passed':equal,'status_equal':s['status']==n['status'],'Source_status':s['status'],'native_status':n['status'],'full_M4_claim':False}

def finalize(report):
 remaining=report.get('remaining')
 inventory_valid=isinstance(remaining,dict)and set(remaining)=={'containers','networks','volumes'}and all(type(v)is str and v==''for v in remaining.values())
 cleanup_valid=(not report.get('physical_cleanup_errors')and not report.get('observation_finalization_errors')and inventory_valid and report.get('default_named_tables_sequences_unchanged')is True and report.get('Source_signal_terminal_valid')is True)
 for field in['proxy_terminal','Source_proxy_terminal']:
  value=report.get(field)
  if not isinstance(value,dict)or value.get('invalid')!=[]or type(value.get('connections'))is not int or value['connections']!=0 or type(value.get('seen'))is not int or value['seen']!=1:cleanup_valid=False
 valid=bool(report.get('observation_valid')and cleanup_valid and not report.get('error_type'))
 parity=bool(valid and report.get('parity_passed'))
 return{'observation_valid':valid,'parity_passed':parity,'passed':bool(valid and parity and report.get('Source_reference_expectation_matched'))}


def signal_evidence(status,rows,required_roles,validator):
 if status.get('invalid')!=[]or type(status.get('rows'))is not int or status['rows']!=len(rows):raise ValueError('SignalStatusJournalMismatch')
 return validator(rows,required_roles)


def client_loss(report):
 """Parity predicate, not a substitute for phase timing/cleanup validity."""
 close=report.get('target_client_loss',{});outcome=report.get('target_outcome',{})
 start=report.get('held_boundary_monotonic');at=close.get('close_begin');end=close.get('reader_joined_monotonic');done=outcome.get('completed')
 clocks=[start,at,end,done]
 valid_clocks=all(type(x)in[int,float]and math.isfinite(x)for x in clocks)
 matched=bool(valid_clocks and start<=at<=end<=start+3.1 and at<=done<=end and type(close.get('fd_after'))is int and close['fd_after']==-1 and close.get('local_shutdown')is True and close.get('reader_joined')is True and close.get('original_descriptor_identity_absent')is True and close.get('server_cancellation_inferred')is False and 'headers_received' not in outcome and 'status' not in outcome and outcome.get('transport_error')in['RemoteDisconnected','ConnectionResetError','ConnectionAbortedError','BrokenPipeError','OSError'])
 return{'matched':matched,'scope':'owned consumer TCP shutdown/read-thread join before release; no server cancellation inference'}
