"""Full success needs phase predicates AND measured physical/cleanup coverage."""
import math,re
from validate import validate as phase
from bootstrap_context import validate as builtin_validate
STEPS=['discover_partial_role_ownership','stop_normal_Main','owned_backend_absence_Unix_observer','default_full_after_audit','stop_owned_origin','origin_listener_socket_handler_ack','stop_management_PG_Redis','compose_down_owned_volumes','fresh_project_and_exact_CID_absence','all_original_PID_start_wait_pipe_settlement']
def need(v,label):
 if not v:raise ValueError(label)
def validate(v):
 result=phase(v);builtin_validate(v['migration_builtin_context'],v['default_before']);need([r['step']for r in v['cleanup_steps']]==STEPS and all(r['settled']is True for r in v['cleanup_steps']),'TenUnconditionalMeasuredCleanupSteps')
 need(v['owned_backends_after_stop']==0 and v['fresh_project_cleanup']=={'container':[],'network':[],'volume':[]},'ActualBackendAndProjectAbsence')
 cids=v['fresh_exact_CID_cleanup'];need(len(cids)==4 and len({r['CID']for r in cids})==4 and {r['role']for r in cids}=={'candidate','origin','baseline','data-init'}and all(r['absent']is True and re.fullmatch('[a-f0-9]{64}',r['CID'])for r in cids),'AllFourActualCIDsAbsent')
 for name,limit in [('capture_phase_elapsed_seconds',40),('cleanup_elapsed_seconds',90),('host_elapsed_seconds',600)]:need(type(v[name])in(int,float)and math.isfinite(v[name])and 0<=v[name]<=limit,'ActualDeclaredClock:'+name)
 need(v['cleanup_within90']is True and v['host_within600']is True,'ClockFlagsMatchActualLimits')
 commands=v['commands'];need(type(commands)is list and len(commands)>=15,'ActualCLIReceiptCoverage')
 for row in commands:
  need(type(row['PID'])is int and row['PID']>1 and type(row['starttime'])is str and row['starttime'].isdigit()and row['physically_settled']is True and row['pipe_threads_settled']is True and row['collection_invalid']is None,'ActualCLIIdentityWaitPipes')
  need(all(type(row[k])in(int,float)and math.isfinite(row[k])for k in ['began_epoch','completed_epoch'])and row['began_epoch']<=row['completed_epoch']and row['actual_exit']in row['allowed_exit_codes'],'ActualCLIClockExit')
  if row['actual_exit']!=0:need(row['actual_exit']==1 and row['allowed_exit_codes']==[0,1]and row['argv'][-2:]==['/spa-tools/native_control.py','ready'],'OnlyPredispatchReadinessNonzero')
  for k in ['stdout_sha256','stderr_sha256']:need(type(row[k])is str and re.fullmatch('[a-f0-9]{64}',row[k]),'ActualSettledCLIStreamHash')
 need(len(v['original_host_identity_absence'])>=len(commands)+3 and all(r['current_starttime']is None or r['current_starttime']!=r['starttime']for r in v['original_host_identity_absence']),'EveryRecordedCLIAndContainerOriginalIdentityGone')
 return {**result,'full_measured_cleanup_qualified':True,'actual_command_count':len(commands),'default_full6259':True,'owned_request_phase_full6259':True,'Source_requests_replayed':0}
