"""Offline validation of ONE historical instrumented Source cohort. No requests."""
import argparse,hashlib,importlib.util,json,math,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
BLOCKED='03763e2b3129a4b41baded659f7bb86cc1c16c85ba36fcce9718e22b96092848'
EXECUTION='9fc28c3b512413386ea6761451b92fecb9c4e831321191b788fcc80261f67f98'
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def verify_bundle():
 root=HERE/'integration4'
 if sha(root/'selection.json')!=BLOCKED:raise ValueError('BlockedSelectionHash')
 seen=set()
 for row in json.loads((root/'selection.json').read_text())['files']:
  name=Path(row['path'])
  if name.is_absolute()or'..'in name.parts or str(name)in seen:raise ValueError('UnsafeOrDuplicateSourcePath')
  seen.add(str(name))
  if sha(root/name)!=row['sha256']:raise ValueError('BlockedInputHash '+str(name))
 if len(seen)!=86 or sha(HERE/'reference-execution-selection.json')!=EXECUTION:raise ValueError('ExactReviewedClosure')
 return root

def load(module,path):
 spec=importlib.util.spec_from_file_location(module,path);result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result

def analyze(trace,wire,terminal,proxy,signals,phase,metadata):
 root=verify_bundle();sys.path.insert(0,str(root/'controller'))
 coverage=load('historical_causal_coverage',root/'controller/coverage.py');signal=load('historical_causal_signal',root/'controller/request_observer.py')
 independent=signal.validate(signals,['initial','target','follower','other']);result=coverage.validate(trace,wire,terminal,proxy,terminal['registry'],independent)
 if metadata.get('trace_rows')!=len(trace)or metadata.get('wire_rows')!=len(wire):raise ValueError('HistoricalRowCount')
 if any(phase.get(key)is not True for key in['HTTP_cohort_valid','causal_coverage_valid','request_finalization_valid','owned_cleanup_valid','actual_SQL_Bearer_user_verified','early_follower_valid','default_named_tables_sequences_unchanged']):raise ValueError('HTTPOrAuditLayer')
 hold=phase['hold_observation']
 if any(type(hold.get(key))not in[int,float]or not math.isfinite(hold[key])for key in['actual_seconds','control_seconds']):raise ValueError('FiniteHoldTiming')
 if phase['status']!=429 or phase['admin_stop_status']!=200 or not 4<=hold['actual_seconds']<=4.35 or not 0<=hold['control_seconds']<=3.1:raise ValueError('FixedHTTPContract')
 if phase['follower_status']!=200 or phase['follower_prefix_sha256']!='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f':raise ValueError('EarlyFollowerBody')
 early=phase['early_stop_witness']
 created=early.get('idle_created_at')
 if type(created)is not str or not 1<=len(created)<=64 or not math.isfinite(float(created))or float(created)<=0:raise ValueError('PresentGenerationMarker')
 if early.get('stop')!='true'or early.get('active_streams')!='2'or early.get('request_count')!='2'or early.get('idle_created_at')!=early.get('follower_created_at'):raise ValueError('LiteralEarlyStopOrMetadata')
 origin=phase['origin_preflight']
 if origin.get('passed')is not True or origin.get('status')!=206 or origin.get('fixture_sha256')!='4f3236e89fd6d9fee4f18bb1cd1e9759113a204dc64b74efa7a704fd4c6fc11c'or origin.get('prefix_sha256')!=phase['follower_prefix_sha256']:raise ValueError('OriginalFixtureOrigin')
 if set(phase['remaining'])!={'containers','networks','volumes'}or any(phase['remaining'].values()):raise ValueError('OwnedProjectNotEmpty')
 for row in phase['client_closes']:
  if type(row.get('fd_after'))is not int or row['fd_after']!=-1 or row.get('local_shutdown')is not True or row.get('server_cancellation_inferred')is not False:raise ValueError('PhysicalClientShutdown')
 observations=coverage.scalar_observations(trace,wire)
 zero=[row for row in wire if row['kind']=='actual_counter_commit'and type(row['value'])is int and row['value']==0]
 # This checks the recorded historical result. It is NOT a future zero filter.
 if len(zero)!=1 or not observations['zero_observed']:raise ValueError('HistoricalZeroResultChanged')
 matched=[row for row in result['attribution']['matched']if row['effect_sequence']==zero[0]['sequence']]
 if len(matched)!=1 or matched[0].get('role')!='follower'or matched[0].get('generator')!=11 or matched[0].get('callback')is not None or matched[0]['writer_firstline']!=321 or matched[0]['via']!='EXEC':raise ValueError('HistoricalZeroWriterIdentity')
 close=[row for row in phase['client_closes']if row['role']=='other']
 if len(close)!=1:raise ValueError('UniqueOtherClientShutdown')
 close_at=close[0]['close_begin'];progress=observations['raw_other_generator_yields_after_zero'];before=[row for row in progress if row['at']<close_at];after=[row for row in progress if row['at']>=close_at]
 if len(progress)!=57 or len(before)!=56 or len(after)!=1 or any(row['generator']!=4 for row in progress):raise ValueError('HistoricalGeneratorProgressBoundaries')
 if len(result['attribution']['matched'])!=6 or result['observed_callback_calls']!=3:raise ValueError('HistoricalWriteCallbackCounts')
 return{'valid':True,'scope':'historical instrumented Source cohort only, not prior untraced identity/Source bug/fullM4','HTTP_cohort_valid':True,'causal_coverage_valid':True,'typed_zero_observed':True,'zero_writer':matched[0],'counter_effects':6,'Source_rows':len(trace),'wire_rows':len(wire),'other_generator':4,'yields_after_zero':57,'before_client_shutdown':56,'after_client_shutdown_before_generator_finally':1,'client_shutdown_boundary':close_at,'callbacks_observed':3,'no_new_upstream_read_or_client_delivery_inferred':True,'no_Native_change':True}

def validate(directory):
 directory=Path(directory);seal=json.loads((directory/'seal.json').read_text());seen=set()
 for row in seal['files']:
  name=Path(row['path'])
  if name.is_absolute()or'..'in name.parts or str(name)in seen:raise ValueError('SafeUniqueEvidencePath')
  seen.add(str(name))
  if sha(directory/name)!=row['sha256']:raise ValueError('EvidenceHash '+str(name))
 required={'source-causal.jsonl','source-wire.jsonl','terminal.json','proxy.json','request-signals.jsonl','phase.json','metadata.json','independent-cleanup.json'}
 if seen!=required:raise ValueError('ExactEvidenceClosure')
 rows=lambda name:[json.loads(line)for line in(directory/name).read_text().splitlines()]
 obj=lambda name:json.loads((directory/name).read_text())
 if obj('independent-cleanup.json').get('passed')is not True or any(obj('independent-cleanup.json')['remaining'].values()):raise ValueError('IndependentCleanup')
 return analyze(rows('source-causal.jsonl'),rows('source-wire.jsonl'),obj('terminal.json'),obj('proxy.json'),rows('request-signals.jsonl'),obj('phase.json'),obj('metadata.json'))
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--evidence',type=Path,default=HERE/'evidence');args=parser.parse_args();print(json.dumps(validate(args.evidence),indent=2))
