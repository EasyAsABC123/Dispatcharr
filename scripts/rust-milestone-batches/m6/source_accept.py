"""Finite missing-domain Source assertions; never manufactures task status/code."""
import base64,json
from source_settlement import valid_worker
EXPECTED=['tvg-real-channel-signal','tvg-repeat','tvg-missing','tvg-dummy','tvg-unmapped','tvg-insert-error','version200-no-update','version200-update','version200-repeat','version200-malformed']
def need(v,label):
 if v is not True:raise ValueError(label)
def validate(report):
 need(report['capture_complete']is True and report['application_task_methods_replaced']is False,'RealCompleteSourceMethods')
 need([r['name']for r in report['cases']]==EXPECTED,'All10DeclaredSourceCases')
 ids=[]
 for row in report['cases']:
  ids.append(row['task_id']);actual=row['actual'];post=actual['postrun'];need(len(post)==1 and post[0]['id']==row['task_id'],'ActualPostrunIdentity')
  backend=actual['backend'];m=json.loads(base64.b64decode(backend['raw_base64'],validate=True));need(m==backend['metadata']and m['task_id']==row['task_id'],'ActualRawBackend')
  expected='FAILURE'if row['name']=='tvg-insert-error'else'SUCCESS';need(post[0]['state']==m['status']==expected,'ActualState:'+row['name'])
  if expected=='SUCCESS':need(m['result']==('EPG not found'if row['name']=='tvg-missing'else None),'ActualResult:'+row['name'])
  need(type(backend['ttl'])is int and 1<=backend['ttl']<=3600,'ActualBackendTTL')
  need(row['actual']['Original_AsyncResult']['state']==expected,'ActualOriginalConsumerState')
  if row['name']=='tvg-insert-error':need(row['before']['tables']['epg_programdata']==row['after']['tables']['epg_programdata'],'ActualAtomicRollbackPreservesOldRows')
  if row['name']in ['tvg-dummy','tvg-unmapped','tvg-missing']:need(row['before']['tables']['epg_programdata']==row['after']['tables']['epg_programdata'],'NoopRows:'+row['name'])
 need(len(set(ids))==10,'EveryDistinctTask')
 first=report['cases'][0]['after']['tables']['epg_programdata'];need(len(first)==1,'ActualLocalProgrammeImported')
 need([{k:v for k,v in r.items()if k!='id'}for r in first]==[{k:v for k,v in r.items()if k!='id'}for r in report['cases'][1]['after']['tables']['epg_programdata']],'ActualRepeatStableBusinessFields')
 for name in ['version200-update','version200-repeat','version200-malformed']:
  row=next(r for r in report['cases']if r['name']==name);notifications=row['after']['tables']['core_systemnotification'];selected=[r for r in notifications if r['notification_key']=='version-0.34.0'];need(len(selected)==1 and selected[0]['is_active']is selected[0]['admin_only']is True,'ActualVersionNotification:'+name)
 stop=report['worker_stop_actual'];need(valid_worker(stop),'ActualWorkerExit0AndPhysicalStop')
 need(stop['same_PID_namespace_as_observer']is True and all(v['PID_namespace']==stop['worker_PID_namespace']for v in report['cases'][-1]['actual']['observer']['postrun']),'ActualSamePIDNamespace')
 need(stop['starttime']is not None and stop['known_PID_generations']and all(r['original_absent']is True for r in stop['known_PID_generations']),'OriginalWorkerPIDGenerations')
 # Every started task must belong to this declared cohort, including failure;
 # do not discard an unknown started-but-unfinished prerun.
 observations=report['cases'][-1]['actual']['observer'];pre=observations['prerun'];post=observations['postrun'];need(len(pre)==len(post)==10 and {r['id']for r in pre}=={r['id']for r in post}==set(ids),'AllPrerunPostrunExactly10')
 sql=[r for r in observations['sql']if r.get('task_id')==report['cases'][5]['task_id']and r.get('exception_type')]
 need(sql!=[],'ActualInsertExceptionRetained')
 return {'passed':True,'Source_actual_tasks':10,'Source_observed_SQLSTATE_values':[r.get('actual_SQLSTATE')for r in sql],'captured_SQLSTATE_verified':any(r.get('actual_SQLSTATE')for r in sql),'declaration_is_not_measured_code':True,'Native_verified':False,'fullM6':False,'scope':'Source missing TVG/default-version branches inside full milestone batch; all actual raw fields retained'}


def validate_versions(report):
 need(report['capture_complete']is True and report['application_task_methods_replaced']is False,'ActualVersionOnlyCapture')
 need([r['name']for r in report['cases']]==EXPECTED[6:],'Exactly4UnmeasuredVersionCases')
 ids=[]
 for row in report['cases']:
  actual=row['actual'];ids.append(row['task_id']);metadata=json.loads(base64.b64decode(actual['backend']['raw_base64'],validate=True))
  need(metadata==actual['backend']['metadata']and metadata['task_id']==row['task_id'],'ActualVersionRawBackend')
  need(metadata['status']=='SUCCESS'and metadata['result']is None,'ActualVersionResult')
  need(actual['Original_AsyncResult']['state']=='SUCCESS'and actual['Original_AsyncResult']['value']is None,'ActualVersionConsumer')
  need(len(actual['postrun'])==1 and actual['postrun'][0]['state']=='SUCCESS','ActualVersionPostrun')
  if row['name']in ['version200-update','version200-repeat','version200-malformed']:
   selected=[r for r in row['after']['tables']['core_systemnotification']if r['notification_key']=='version-0.34.0']
   need(len(selected)==1 and selected[0]['is_active']is selected[0]['admin_only']is True,'ActualVersionNotification:'+row['name'])
 need(len(set(ids))==4,'DistinctVersionIDs')
 observations=report['cases'][-1]['actual']['observer'];need(len(observations['prerun'])==len(observations['postrun'])==4 and {r['id']for r in observations['postrun']}==set(ids),'ActualOnly4VersionWorkerTasks')
 need(len(observations['HTTP'])==4 and all(r['owned_dependency_route']is True for r in observations['HTTP']),'ActualFourOwnedHTTPDependencies')
 need(valid_worker(report['worker_stop_actual']),'ActualVersionWorkerPhysicalStop0')
 return {'passed':True,'Source_actual_tasks':4,'Source_actual_HTTP_requests':4,'Native_verified':False,'fullM6':False,'scope':'four previously unmeasured Source version200 cases; earlier six TVG task observations reused separately, first host failure remains failed'}


def validate_tvg_prefix(report):
 """Reuse the six completed cases from a host that later failed before version publication."""
 need(report['application_task_methods_replaced']is False,'OriginalTVGMethodsUnchanged')
 need([r['name']for r in report['cases']]==EXPECTED[:6],'ExactSixCompletedTVGCases')
 ids=[]
 for row in report['cases']:
  ids.append(row['task_id']);actual=row['actual'];metadata=json.loads(base64.b64decode(actual['backend']['raw_base64'],validate=True))
  need(metadata==actual['backend']['metadata']and metadata['task_id']==row['task_id'],'ActualTVGRawBackend')
  state='FAILURE'if row['name']=='tvg-insert-error'else'SUCCESS'
  need(metadata['status']==state and actual['Original_AsyncResult']['state']==state,'ActualTVGBackendConsumerState')
  need(len(actual['postrun'])==1 and actual['postrun'][0]['state']==state,'ActualTVGPostrun')
  if state=='SUCCESS':need(metadata['result']==('EPG not found'if row['name']=='tvg-missing'else None),'ActualTVGResult')
  if row['name']in ['tvg-missing','tvg-dummy','tvg-unmapped','tvg-insert-error']:
   need(row['before']['tables']['epg_programdata']==row['after']['tables']['epg_programdata'],'ActualTVGRetention:'+row['name'])
 first=report['cases'][0]['after']['tables']['epg_programdata'];repeat=report['cases'][1]['after']['tables']['epg_programdata']
 need(len(first)==1,'ActualTVGImportedProgramme')
 need([{k:v for k,v in r.items()if k!='id'}for r in first]==[{k:v for k,v in r.items()if k!='id'}for r in repeat],'ActualTVGRepeatFields')
 observations=report['cases'][-1]['actual']['observer'];need(len(set(ids))==len(observations['prerun'])==len(observations['postrun'])==6,'ActualOnlySixTVGTasks')
 need({r['id']for r in observations['prerun']}=={r['id']for r in observations['postrun']}==set(ids),'ActualTVGIdentities')
 errors=[r for r in observations['sql']if r.get('task_id')==ids[-1]and r.get('exception_type')]
 need(bool(errors)and any(r.get('actual_SQLSTATE')=='P0001'for r in errors),'ActualTVGInsertSQLSTATE')
 need(valid_worker(report['worker_stop_actual']),'ActualTVGWorkerPhysicalStop0')
 return {'passed_Source_subset':True,'Source_actual_tasks':6,'captured_SQLSTATE':'P0001','original_host_exit_remains':1,'version_cases_not_executed_in_this_epoch':True,'Native_verified':False,'fullM6':False}
