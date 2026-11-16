"""Actual owned SQL lock/worker-budget/TERM drain; no Source calls or sleeps-only proof."""
import datetime,json,socket,time,urllib.error,urllib.request,uuid
from pathlib import Path

def run(f,op,seed,receiver,root,read,out):
 result={'passed':False,'scope':'functional physical-bound regression; no paired performance claim','held_rows':[],'physical_session_samples':[]};trigger=False;gate=False;terminate_sent=False
 def save():
  (out/'physical.json').write_text(json.dumps(result,indent=2)+'\n')
 def control(action):
  name='control-'+uuid.uuid4().hex
  f.python('import json;from pathlib import Path;Path('+repr(root+'/'+name+'.json')+').write_text('+repr(json.dumps({'action':action}))+')')
  return f.wait(lambda:read(name+'-receipt.json'),10,'ActualSQLGate:'+action)
 try:
  result['actual_gate_hold']=control('hold_sql');gate=True
  if result['actual_gate_hold'].get('actual')is not True:raise RuntimeError('ActualAdvisoryLockOwnershipRequired')
  result['actual_trigger']=op('trigger','dispatcharr_channels_channel','UPDATE',None,None,False,True);trigger=True
  # Four real families own four simultaneous SQL sessions. A fifth same-family
  # frame must remain at the actual FIFO head, with no invented task terminal.
  result['HTTP']=[];ids=[]
  for route,body in [('set-names-from-epg/',{'channel_ids':[seed['channel_ids'][4]]}),('set-tvg-ids-from-epg/',{'channel_ids':[seed['channel_ids'][5]]}),('set-logos-from-epg/',{'channel_ids':[seed['channel_ids'][3]]}),('from-stream/bulk/',{'stream_ids':[seed['stream_id']],'channel_profile_ids':[],'starting_channel_number':0})]:
   response=f.api('POST','/api/channels/channels/'+route,body);result['HTTP'].append(response)
   if response['status']!=(202 if route=='from-stream/bulk/'else 200):raise RuntimeError('ActualAPIStatusBeforeTaskID')
   ident=response['body']['task_id'];uuid.UUID(ident);ids.append(ident)
  result['task_ids']=ids
  def blocked():
   sample=op('pool_sessions',[result['actual_gate_hold']['PID']]);result['physical_session_samples'].append(sample)
   if sample['counted']>8:raise RuntimeError('ActualSharedPoolPhysicalConnectionsExceeded8')
   rows=op('held');result['held_rows'].append({'monotonic':time.monotonic(),'rows':rows});save()
   return rows if len(rows)==4 and all(r['state']=='active'and r['wait_event_type']=='Lock'and r['wait_event']=='advisory'for r in rows)else None
  result['actual_blocked_SQL']=f.wait(blocked,15,'FourActualWorkerSQLSessionsAdvisoryWait')
  if any(op('backend',ident)['metadata']is not None for ident in ids):raise RuntimeError('NoInventedTerminalWhileActualSQLHeld')
  fifth=f.api('POST','/api/channels/channels/set-names-from-epg/',{'channel_ids':[seed['channel_ids'][4]]});result['fifth_HTTP']=fifth
  if fifth['status']!=200:raise RuntimeError('FifthActualAPIStatus')
  fifth_id=fifth['body']['task_id'];result['fifth_id']=fifth_id;result['physical_session_samples'].append(op('pool_sessions',[result['actual_gate_hold']['PID']]));queue=op('queue');result['fifth_queue_before_TERM']=queue
  if not any(v['frame']['headers']['id']==fifth_id for v in queue)or op('backend',fifth_id)['metadata']is not None:raise RuntimeError('ActualFifthFrameMustRemainQueuedWithoutAttachment')
  # Existing Fixture inspection binds actual candidate and default8 setting.
  inspect=json.loads(f.command([f.docker,'inspect',f.native]))[0];env=dict(v.split('=',1)for v in inspect['Config']['Env']if '='in v)
  result['configured_shared_pool8']=env.get('RUST_DATABASE_POOL_SIZE')=='8';result['same_single_data_bind']=len(inspect['Mounts'])==1 and inspect['Mounts'][0]['Destination']=='/data'and Path(inspect['Mounts'][0]['Source']).resolve()==f.case_data.resolve()
  # Real TERM is asynchronous; docker CLI settles the delivered signal only.
  result['actual_TERM_command_started_monotonic']=time.monotonic();f.command([f.docker,'kill','--signal=TERM',f.native],10);result['actual_TERM_signal_CLI_receipt']=f.cli[-1];terminate_sent=True
  after=json.loads(f.command([f.docker,'inspect',f.native]))[0]
  result['actual_Main_still_running_while_SQL_held']=after['State']['Running']is True;result['actual_wait_retained_after_TERM']=op('held')
  # Report the actual listening boundary. A refused new TCP connection is not
  # relabeled as an HTTP503; the external contract may close the listener.
  try:
   try:r=urllib.request.urlopen(f.base_url+'/health/ready',timeout=f.remaining(2))
   except urllib.error.HTTPError as error:r=error
   with r:result['new_intake']={'kind':'HTTP','status':r.status,'bounded_body_base64':__import__('base64').b64encode(r.read(65537)).decode()}
  except (OSError,TimeoutError)as error:result['new_intake']={'kind':'transport_refusal','type':type(error).__name__}
  result['actual_gate_release']=control('release_sql');gate=False
  result['actual_terminal_after_release']={}
  for ident in ids:result['actual_terminal_after_release'][ident]=f.wait(lambda:(lambda value:value if value['metadata']and value['metadata']['status']=='SUCCESS'else None)(op('backend',ident)),30,'ActualSQLTaskSuccessAfterGateRelease')
  result['actual_Main_exit']=f.stop_native(expected=0,seconds=55)
  result['fifth_queue_after_Main_exit']=op('queue');result['fifth_backend_after_Main_exit']=op('backend',fifth_id)
  result['fifth_not_lost_or_claimed']=any(v['frame']['headers']['id']==fifth_id for v in result['fifth_queue_after_Main_exit'])and result['fifth_backend_after_Main_exit']['metadata']is None
  result['physical_shared_pool8_samples_passed']=bool(result['physical_session_samples'])and all(v['counted']<=8 for v in result['physical_session_samples'])
  result['passed']=result['physical_shared_pool8_samples_passed']and result['configured_shared_pool8']and result['same_single_data_bind']and result['fifth_not_lost_or_claimed']and result['actual_Main_still_running_while_SQL_held']and bool(result['actual_wait_retained_after_TERM'])and result['actual_Main_exit']['actual_exit']==0 and not result['actual_Main_exit']['oom_killed']and result['actual_gate_release'].get('actual_connection_closed')is True
 except BaseException as error:result['primary_error']={'type':type(error).__name__,'message':str(error)}
 finally:
  if f.functional_budget_expired():f.begin_cleanup()
  if gate:
   try:result['cleanup_release']=control('release_sql')
   except BaseException as error:result['cleanup_release_error']=str(error)
  if trigger:
   try:result['actual_trigger_removed']=op('trigger','dispatcharr_channels_channel',None,None,None,True,True)
   except BaseException as error:result['cleanup_trigger_error']=str(error)
  if terminate_sent and f.native:
   try:result['final_Main_settlement']=f.stop_native(expected=0,seconds=55)
   except BaseException as error:result['final_Main_error']=str(error)
  save()
 return result
