"""Exact pure predicates extracted from executed final controller; no dispatch."""
def require(ok,label):
 if not ok:raise ValueError(label)

def same(a,b):
 if type(a)is not type(b):return False
 if isinstance(a,dict):return a.keys()==b.keys()and all(same(v,b[k])for k,v in a.items())
 if isinstance(a,list):return len(a)==len(b)and all(same(x,y)for x,y in zip(a,b))
 return a==b

def validate_native_capture(value,case,database):
 import math
 require(type(case)is int and case in(1,2,3,4)and value['case']==case and type(value['case'])is int and value['database']==database,'ExactActualCaseDatabase')
 require(value['capture_complete']is True and value['capture_only']is True and value['passed']is False,'ActualCompleteCaptureNotFabricatedPassingReport')
 require(type(value['Source_runs'])is int and type(value['Main_runs'])is int and value['Source_runs']==value['Main_runs']==0,'NativeOnly')
 require(same(value['before'],value['after']) and same(value['before_credentials'],value['after_credentials']),'CapturedSQLAndCredentialEffectsEqual')
 require(value['request_slots_while_held']==3 and value['early_shutdown_returned']is False and value['final_shutdown_returned']is True,'ExistingFourSlotPhysicalDrain')
 duration=value['actual_physical_hold_seconds'];require(type(duration)in(int,float)and math.isfinite(duration)and 0<=duration<=25,'ActualPhysicalHold25')
 for name in ['cache_before_request','cache_after_request']:
  require(type(value[name]['raw_DUMP_PTTL'])is dict,'ActualCacheTypedDumpSnapshot')
 before={k:v['dump_hex']for k,v in value['cache_before_request']['raw_DUMP_PTTL'].items()};after={k:v['dump_hex']for k,v in value['cache_after_request']['raw_DUMP_PTTL'].items()};require(before==after,'ActualCacheValuesUnchangedNotTTLFreeze')
 if case in(1,2):require(value['caller_JoinHandle_abort_consumed']is True and value['HTTP_FD_loss_claimed']is False,'ActualCallerAbortNotInferredFDLoss')
 if case==2:
  prepared=value['actual_readonly_prepared_timeout_fixture'];require(type(prepared['subject_pool_size_before_lock'])is int and type(prepared['subject_pool_idle_before_lock'])is int and prepared['subject_pool_size_before_lock']==prepared['subject_pool_idle_before_lock']==1,'ActualSingleIdlePreparedSubjectBeforeLock');require(type(prepared['backend_pid'])is int and prepared['backend_pid']>0 and prepared['statement_prepared_before_lock']is True and prepared['held_reply_same_backend']is True and prepared['Source_business_request']is False and prepared['backend_pid']==value['actual_gate_reached']['held']['backend_pid'],'ActualPreparedThenTimedOutSameBackend')
 if case==3:
  require(value['actual_HTTP_FD_shutdown_and_drop']is True and value['handler_abort_claimed']is False and value['HTTP_server_graceful_wait0']is True,'ActualClientFDLossAndServerWait')
  fate=value['actual_HTTP_response_write_fate'];require(all(type(fate[k])is int for k in ['accepted','handler_started','handler_cancelled','bytes_read','bytes_written'])and 0<fate['bytes_read']<=65536 and 0<=fate['bytes_written']<=65536 and type(fate['read_eof'])is bool and type(fate['write_errors'])is list and len(fate['write_errors'])<=8 and all(type(x)is str and len(x)<=64 for x in fate['write_errors']),'TypedActualHTTPFate');require(fate['accepted']==1 and fate['handler_started']==1 and fate['socket_dropped']is True and fate['capacity_exceeded']is False,'ActualBoundedTransportFate')
  require(all(type(n)is int for n in fate['response_statuses'])and fate['response_statuses']in([],[401])and fate['handler_cancelled']+len(fate['response_statuses'])==1 and value['HTTP_write_failure_or_response_status_inferred_from_client_loss']is False,'ObservedHandlerAndWriteFateNoInference')
 if case==4:require(same(value['logical_response_before_physical_reply'],{'status':401})and value['later_pipeline_write_arrival_claimed']is False,'ActualLogical401AndDuplexReceipts')
 else:
  terminal=value['actual_target_endpoint_settlement'];require(terminal['original_socket_inodes_absent']is True and terminal['actual_backend_absent']==[] and len(terminal['actual_prior_client_FDs'])==1,'ActualTargetSocketAndPGBackendGone')
  require(value['same_SQL_pool_reuse_after_target_physical_close']is True,'ActualPoolReuseAfterClose')
 return {'capture_valid':True,'case':case,'no_Source_Main_or_fullM8_acceptance':True}
