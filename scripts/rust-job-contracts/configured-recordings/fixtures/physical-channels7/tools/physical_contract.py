"""Finite retained-evidence acceptor. No application/network/process on import.
These are Native resource/refusal controls, not Original physical-timing parity.
"""
import base64,json
PREFIX=('settings-cache-reply','programme-DataRow-CommandComplete-Ready','Clocked-marker-COMMIT-Ready','Periodic-COMMIT-Ready','artwork-LPUSH-reply','Channels-reply','result-SETEX-reply','result-reply-lost','result-SETEX-denied','oldSUCCESS-preserved','Recording-INSERT-P0001-Ready','snapshot-server-error-Ready')
REFUSALS=('foreign-family-refused','oversize-programme-refused')
DIRECT=('result-SETEX-denied','oldSUCCESS-preserved')+REFUSALS
PREACK=('settings-cache-reply','programme-DataRow-CommandComplete-Ready','snapshot-server-error-Ready')
OPERATIONAL=('Recording-INSERT-P0001-Ready','snapshot-server-error-Ready','result-SETEX-denied')
def require(value,label):
 if value is not True:raise ValueError(label)
def names(mode):
 if mode=='physical-channels-onward':return list(PREFIX[5:])
 if mode in REFUSALS:return[mode]
 raise ValueError('UnknownPhysicalMode')
def required_checks(label):
 checks=['fixture-endpoints-before-Native-child','actual-prepared-parent-route','normal-close-reply','actual-process-and-pipes0','all-default-children-untouched']
 if label not in DIRECT:
  checks+=['actual-target-reply-held','caller-admission-held-before-abort','aborted-caller-retains-physical-owner','drain-cannot-acknowledge-held-reply','physical-drain-after-release']
  if label not in ('result-SETEX-reply','result-reply-lost'):checks+=['no-early-terminal']
  if label in PREACK:checks+=['parent-still-unconsumed']
 if label in REFUSALS:checks+=['preACK-unsupported-frame-and-backend-preserved']
 if label=='oldSUCCESS-preserved':checks+=['oldSUCCESS-raw-and-TTL-preserved']
 if label in ('artwork-LPUSH-reply','Channels-reply','result-SETEX-reply','result-reply-lost','result-SETEX-denied'):checks+=['proxy-unused-before-Native-child']
 if label=='result-SETEX-denied':checks+=['actual-pinned-server-version','exact-ACL-nonmutating-preflight','operational-no-fabricated-metadata','actual-one-targeted-SETEX-denial']
 if label in ('Recording-INSERT-P0001-Ready','snapshot-server-error-Ready'):checks+=['actual-server-P0001-owned-error','operational-error-not-Source-normal-parity']
 if label=='Recording-INSERT-P0001-Ready':checks+=['injector-settled']
 if label not in REFUSALS+('snapshot-server-error-Ready',):checks+=['parent-ACK-observed']
 if label not in REFUSALS:checks+=['no-residual-owned-global-lease']
 if label=='result-reply-lost':checks+=['one-actual-write-no-replay']
 if label not in DIRECT+('Recording-INSERT-P0001-Ready','snapshot-server-error-Ready'):checks+=['actual-normal-result-and-two-children']
 return {label+'-'+value for value in checks}
def validate_report(exit_code,report,mode):
 require(type(exit_code)is int and exit_code==0,'ActualGuestExit0')
 require(report.get('passed')is True and report.get('Native_verified')is True,'ExplicitNativePhysicalPassed')
 require(report.get('performance_verified')is False,'NoPerformanceScope')
 require(report.get('mode')==mode and report.get('predeclared_modes')==names(mode),'ExactModeOrder')
 require(report.get('finally_errors')==[],'CleanupErrorsNotDiscarded')
 if mode=='physical-channels-onward':
  setup=report.get('setup_context',{});require(setup.get('Native_control_replay')is False and setup.get('Source_behavior_reference_capture')is False,'ExplicitSeedContextNotControlReplay')
  require(len(setup['before']['programmes'])==0 and len(setup['before']['recordings'])==0 and len(setup['after']['programmes'])==5 and len(setup['after']['recordings'])==5 and len(setup['after']['clocked'])-len(setup['before']['clocked'])==5 and len(setup['after']['periodic'])-len(setup['before']['periodic'])==5 and len(setup['after']['frames'])-len(setup['before']['frames'])==10,'ExactFivePrebookedProgrammesContext')
 cases=report.get('cases');require(type(cases)is list and[x.get('name')for x in cases]==names(mode),'EveryExactCasePresent')
 checks=report.get('checks');require(type(checks)is list and all(x.get('passed')is True for x in checks),'EveryActualCheckTrue')
 actual_names=[x.get('name')for x in checks];require(len(actual_names)==len(set(actual_names)),'NoDuplicateChecks')
 for c in cases:
  label=c['name'];require(required_checks(label).issubset(actual_names),'MissingRequiredCaseCheck:'+label)
  transport=c.get('transport_preflight',{});before_child=c.get('before_native_child',{});require(transport.get('broker_result_same_address')is True and transport.get('separate_broker_result_databases')==[8,9]and before_child.get('native_process_created')is False and before_child.get('action_commands_sent')==0,'ActualBeforeWorkerEndpointReceipt:'+label)
  if label in ('artwork-LPUSH-reply','Channels-reply','result-SETEX-reply','result-reply-lost','result-SETEX-denied'):require(before_child.get('proxy_commands')==0,'NoNativeProxyCommandBeforeChild:'+label)
  proc=c.get('native_process',{});require(proc.get('exit')==0 and proc.get('settled')is True and proc.get('pipes_settled')is True and proc.get('writer_settled')is True and proc.get('invalid')is None,'ActualPeerAndPipesSettled:'+label)
  require(type(proc.get('pid'))is int and type(proc.get('starttime'))is str,'ActualPeerIdentity:'+label)
  require(c.get('finished_utc')is not None and c.get('began_utc')is not None,'ActualCaseClockReceipt:'+label)
  require(type(c.get('published_parent'))is list and len(c['published_parent'])==1,'RawParentRetained:'+label)
  raw=json.loads(base64.b64decode(c['published_parent'][0],validate=True));require(raw['headers']['id']==c['id'],'RawParentIdentity:'+label)
  before_checks=c.get('before_assertions',[]);require(type(before_checks)is list and len(before_checks)>=len(required_checks(label)),'RawBeforeAssertions:'+label)
  for receipt in before_checks:require(type(receipt.get('raw'))is dict and 'backend'in receipt['raw']and'parent_queue'in receipt['raw'],'NoMissingRawAssertionReceipt:'+label)
  for proxy in ('PG','Redis'):
   if proxy in c:require(c[proxy].get('physical_close_verified')is True and c[proxy].get('errors')==[],'ActualProxySettlement:'+label)
  if label not in DIRECT:
   require(type(c.get('held_monotonic'))in(int,float)and type(c.get('actual_release_monotonic'))in(int,float)and c['actual_release_monotonic']-c['held_monotonic']>=6.2,'FixedPhysicalHold:'+label)
   require(c.get('drain_reply',{}).get('drained')is True and c['drain_reply'].get('caller_released')is True,'ActualDrainReceipt:'+label)
   require(type(c.get('held_observation'))is dict,'ActualHeldEffects:'+label)
   if label not in ('result-SETEX-reply','result-reply-lost'):require(c['held_observation']['backend']['metadata']is None,'NoEarlyTerminal:'+label)
  if label in OPERATIONAL:require(c['result']['metadata']is None,'NoInventedNormalTerminal:'+label)
  if label in REFUSALS:require(c['after_run']['parent_queue']==c['published_parent']and c['result']['metadata']is None,'RefusalPreservedFrame:'+label)
  if label=='programme-DataRow-CommandComplete-Ready':
   matching=[t for t in c['PG']['timeline']if t.get('stage')=='actual-matching-Ready-held'];require(len(matching)==1 and matching[0].get('matching_rows')==2 and matching[0].get('matching_commands_hex')==[b'SELECT 2\0'.hex()]and matching[0].get('executions')==1 and matching[0].get('frontend_Sync_or_simpleQuery_observed')is True,'ActualTwoRowSelectReadyIdentity')
 return {'passed':True,'cases':len(cases),'checks':len(checks),'scope':'Native physical/refusal only; exact retained records/processes, not Source timing parity or application/default-FIFO acceptance'}
