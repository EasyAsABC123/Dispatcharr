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
