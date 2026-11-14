"""Socket-free stage decisions; no task or result-success authority."""
import datetime,math
def stage_wait(end,now,monotonic,command_started):
 if not(isinstance(end,datetime.datetime)and isinstance(now,datetime.datetime)and end.tzinfo is not None and now.tzinfo is not None):raise ValueError('AwareActualEndRequired')
 if not(all(type(v)in(int,float)and math.isfinite(v)for v in [monotonic,command_started])and monotonic>=command_started):raise ValueError('ActualFiniteOrderedCommandClocks')
 seconds=(end-now).total_seconds()
 if not(0<=seconds<=60 and monotonic-command_started+seconds+15+6.2+45<180):raise ValueError('BoundedStageWithinUnchanged180')
 return {'configured_end_utc':end.isoformat(),'observed_now_utc':now.isoformat(),'observed_monotonic':monotonic,'end_monotonic':monotonic+seconds,'target_probe_seconds':15,'task_seconds':180}
def settled_media(rows):
 if not(type(rows)is list and len(rows)==2):raise ValueError('ActualCaptureAndRemuxRequired')
 for row,code in zip(rows,[255,0]):
  if not(type(row.get('code'))is int and row['code']==code and row.get('settled_utc')is not None and row.get('signal')is None and row['stderr']['read_error']is None and all(row.get(k)is None for k in ['reader_error','command_error','wait_error','kill_error'])):raise ValueError('ActualWaitAndReadersSettledBeforeResultGate')
 return True
