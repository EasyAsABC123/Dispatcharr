"""Fixed Source-only characterization protocol; no operation on import.
Adapter owns actual warmed fixture/body/SQL/Origin; no Source outcome is assumed.
"""
import math,json
from pathlib import Path
PINNED=json.loads(Path(__file__).with_name("source-authority.json").read_text())["loaded_file_sha256_required"]
EXECUTION_BLOCKED=True
EXPECTED={'max_retries':3,'rotation_cooldown':60,'switch_timeout':20,'buffering_timeout':15,'stream_timeout':20,'failover_grace':20,'connection_timeout':10,'health_interval':5,'startup_timeout':8,'HTTP_connect_timeout':5,'HTTP_read_timeout':30,'HTTP_adapter_retries':0,'candidate_count':2}
def derive(settings):
 if settings!=EXPECTED:raise ValueError('EffectiveSettingsMismatchNoRetuning')
 backoff=sum(min(.25*n,3)for n in range(1,settings['max_retries']))
 single=max(settings['buffering_timeout'],settings['switch_timeout'],settings['stream_timeout']+settings['failover_grace'],settings['connection_timeout']+settings['health_interval'],settings['startup_timeout'],settings['HTTP_connect_timeout']+settings['HTTP_read_timeout'])
 return {'outage_seconds':math.ceil(settings['rotation_cooldown']+single+settings['candidate_count']*backoff),'restore_observation_seconds':settings['rotation_cooldown']+settings['switch_timeout']+settings['startup_timeout'],'single_path_horizon_seconds':single,'per_candidate_backoff_seconds':backoff}
def run(adapter,clock,sleep):
 """Actual adapter owns fixture and durable observations; dormant until reviewed."""
 if EXECUTION_BLOCKED:raise RuntimeError('RootReviewRequired')
 paths=adapter.exact_owned_candidate_paths()
 if paths!=['/live/0.ts','/live/100.ts']:raise ValueError('ExactlyTwoOwnedCandidates')
 result=None
 try:
  adapter.verify_warmed_original_body(seconds=15)
  settings,path_witness=adapter.effective_settings_and_path();schedule=derive(settings)
  if path_witness.get('loaded_Source_sha256')!=PINNED or path_witness.get('profile_name')!='Proxy'or path_witness.get('profile_locked')is not True or path_witness.get('metadata_profile_resolves_to_owned_locked_Proxy')is not True or path_witness.get('initial_owner_transcode')is not False or path_witness.get('actual_HTTP_reader_code_file')!='/app/apps/proxy/live_proxy/input/http_streamer.py':raise ValueError('ActualInputPathWitnessRequired')
  before=adapter.snapshot('before-outage');owned_input_ids=adapter.require_existing_owned_input(before,paths);acks=[]
  for path in paths:acks.append(adapter.origin_control(path,'fail'))
  physically_closed={i for ack in acks for i in ack['physical_close']['closed_ids']}
  if not set(owned_input_ids)<=physically_closed:raise RuntimeError('WarmedOriginInputNotPhysicallyClosed')
  probes=adapter.probe_all_unavailable(paths)
  if len(probes)!=2 or {x['path']for x in probes}!=set(paths)or any(x['status']!=503 for x in probes):raise RuntimeError('FixtureUnavailableNotEstablished')
  adapter.verify_origin_active_zero();start=clock();adapter.event('all-unavailable-start',start,acks=acks,probes=probes)
  while clock()<start+schedule['outage_seconds']:
   adapter.retain_original_body_outcome();sleep(min(.25,max(0,start+schedule['outage_seconds']-clock())))
  ending=adapter.probe_all_unavailable(paths)
  if len(ending)!=2 or {x['path']for x in ending}!=set(paths)or any(x['status']!=503 for x in ending):raise RuntimeError('FixtureUnavailableLost')
  ended=clock();adapter.event('all-unavailable-end',ended,probes=ending)
  before_restore=adapter.snapshot('before-restore');recover_receipt=adapter.origin_control(paths[0],'recover');restored=clock();adapter.event('primary-restored',restored,receipt=recover_receipt)
  while clock()<restored+schedule['restore_observation_seconds']:
   adapter.retain_original_body_outcome();sleep(min(.25,max(0,restored+schedule['restore_observation_seconds']-clock())))
  restore_observation_end=clock();adapter.event('restore-observation-end',restore_observation_end)
  old=adapter.freeze_and_physically_close_original_body()
  before_fresh=adapter.snapshot('before-fresh-request');fresh=adapter.one_fresh_postrestore_request()
  result={'schedule':schedule,'primary_recover_receipt':recover_receipt,'restored_monotonic':restored,'restore_observation_end_monotonic':restore_observation_end,'restore_observation_elapsed_seconds':restore_observation_end-restored,'path_witness':path_witness,'outage_elapsed_seconds':ended-start,'before':before,'before_restore':before_restore,'before_fresh':before_fresh,'original_body_outcome':old,'fresh_body_outcome':fresh,'Source_budget_acceptance':None,'resource_measurement':False}
  return result
 finally:
  errors=[]
  actions=[('recover:'+path,lambda path=path:adapter.origin_control(path,'recover'))for path in paths]
  actions += [('body-close-and-join',adapter.close_owned_bodies_and_join),('same-run-retention',lambda:adapter.same_run_retention(seconds=70))]
  for name,action in actions:
   try:
    value=action()
    if result is not None and name=='same-run-retention':result['retention_outcome']=value
   except BaseException as error:errors.append({'action':name,'error_type':type(error).__name__})
  if result is not None:result['cleanup_errors']=errors;result['fixture_cleanup_valid']=not errors
  adapter.retain_cleanup_result(errors)
