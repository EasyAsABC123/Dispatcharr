"""Recompute actual-negative and separate corrected-schema gates, offline only."""
import copy,hashlib,importlib.util,json
from pathlib import Path
from validate import preserved
HERE=Path(__file__).resolve().parent
HASHES={'grants.py':'534d1fbc7ef67ee46b46b6ace97c1c15c3bac72542a1451a2c538ed11858b855','acceptance.py':'89aca6270857b5ca056ad5ab227e833afb8b3f122293a0e56b519d918b16912a'}
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def original(name):
 path=HERE/'execution-inputs/tools'/name
 if digest(path)!=HASHES[name]:raise ValueError('ExactOriginalGateCode')
 spec=importlib.util.spec_from_file_location('normal_observation_'+name[:-3],path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def origin_coverage(origin,phase):
 connections=origin.get('connections');requests=origin.get('requests')
 if type(connections)is not list or not 1<=len(connections)<=128 or type(requests)is not list or not 1<=len(requests)<=64:raise ValueError('MandatoryOriginRecords')
 ids=[c.get('id')for c in connections];numbers=[r.get('id')for r in requests]
 if any(type(n)is not int or n<=0 for n in ids+numbers)or len(set(ids))!=len(ids)or len(set(numbers))!=len(numbers):raise ValueError('UniqueOriginRecords')
 if any(r.get('connection')not in ids for r in requests):raise ValueError('OriginRequestConnectionCoverage')
 if any(type(c.get('requests'))is not int or c['requests']<0 for c in connections):raise ValueError('OriginConnectionRequestCountType')
 if any(c.get('requests')!=sum(r['connection']==c['id']for r in requests)for c in connections):raise ValueError('OriginConnectionRequestCounts')
 if origin.get('accepted_TCP_total')!=len(connections)or origin.get('control_total')!=sum(r.get('kind')=='control'for r in requests)or origin.get('media_HEAD_total')!=sum(r.get('kind')=='media_HEAD'for r in requests)or origin.get('transient_TCP_without_request')!=sum(c['requests']==0 for c in connections):raise ValueError('OriginAggregateCoverage')
 required={'shared_movie','other_movie'}|{r['role']+'_movie'for r in phase['probe_outcomes']if r.get('status')==200}
 present={r.get('path_role')for r in requests if r.get('kind')=='stream_GET'and r.get('method')=='GET'and type(r.get('bytes_written'))is int and r['bytes_written']>0}
 if not required<=present:raise ValueError('MandatoryMediaRoleCoverage')
 terminal={r['id']:r for r in requests}
 for checkpoint in phase['checkpoints']:
  previous=checkpoint.get('origin',{}).get('requests')
  if type(previous)is not list or not previous:raise ValueError('MandatoryCheckpointOriginCoverage')
  for r in previous:
   current=terminal.get(r.get('id'))
   if current is None or any(current.get(k)!=r.get(k)for k in ['connection','method','path_role','kind','began'])or current.get('bytes_written',-1)<r.get('bytes_written',0):raise ValueError('OriginPastRequestCoverage')
 return True
def recompute(full,projected,cleanup):
 old=original('grants.py');gate=original('acceptance.py');summary=copy.deepcopy(full['summary']);actual={};corrected={};before=summary['backends']['native']['native_other_grant_before'];after=summary['backends']['native']['native_other_grant_after'];profile=projected['expected_profile'];content=projected['expected_content']
 if before!=projected['native_other_before']or after!=projected['native_other_after']:raise ValueError('RetainedProjectionAgreement')
 fixed=preserved(before,after,profile,content)
 for mode,phase in summary['backends'].items():
  work=copy.deepcopy(phase)
  if mode=='native':
   try:work['native_other_grant_preserved']=old.preserved(before,after,profile,content)
   except ValueError:work['native_other_grant_preserved']={'preserved':False,'error':'ValueError'}
   if work['native_other_grant_preserved']!=phase['native_other_grant_preserved']:raise ValueError('ActualGrantFailureReplay')
  actual[mode]=gate.phase(work)
  if any(phase.get(k)!=v for k,v in actual[mode].items()):raise ValueError('ActualPhaseGateReplay '+mode)
  if mode=='native':work['native_other_grant_preserved']=fixed
  corrected[mode]={**phase,**gate.phase(work)}
 # Independently derive physical inventory/default audit flags, do not trust them.
 if cleanup['project']!=summary['project']or any(cleanup['remaining'].values())or cleanup['matching_owned_tool_processes']:raise ValueError('IndependentPhysicalCleanup')
 clean=not summary['cleanup_errors']and not any(summary['remaining'].values())
 audit=summary['default_before']==summary['default_after']
 if summary['owned_cleanup_valid']!=clean or summary['default_named_tables_sequences_unchanged']!=audit:raise ValueError('ActualCleanupAuditReplay')
 origins={mode:summary[mode+'_origin_terminal']for mode in ['source','native']}
 for mode,origin in origins.items():origin_coverage(origin,summary['backends'][mode])
 if not all(not o['invalid']and o['actual_open_TCP']==0 and o['server_thread_settled']and all(c['socket_closed']and c['fd_after']==-1 for c in o['connections'])for o in origins.values()):raise ValueError('OriginPhysicalShutdown')
 if not summary['Source_status_and_main_listener_closed']or any(any(summary[mode+'_caps_after'].get(k)!=v for k,v in {'account_max_streams':2,'profile_max_streams':2,'profile_id':int(profile.split(':')[1]),'fixed_at_seed':True}.items())for mode in ['source','native']):raise ValueError('CompletedPhaseInputs')
 for mode in ['source','native']:
  phase=summary['backends'][mode]
  if not phase['actual_SQL_Bearer_verified']or any(phase[k]!={'account':2,'profile':2}for k in ['caps_before','caps_after','caps_terminal']):raise ValueError('ActualAuthMax2')
  if phase['admin_stop']['status']!=200 or phase['early_stop_literal']!='true':raise ValueError('AdminStopWitness')
  required={'other'}|{r['role']for r in phase['probe_outcomes']if r.get('status')==200}
  if type(phase.get('decoders'))is not dict or set(phase['decoders'])!=required or type(phase.get('readers_before_teardown'))is not dict or set(phase['readers_before_teardown'])!=required:raise ValueError('MandatoryDecoderReaderCoverage')
  if not all(type(d)is dict and d.get('passed')is True and type(d.get('frames'))is int and d['frames']>=50 and d['retained_bytes']==1048576 and d['retained_sha256']=='b4a9992aa94b69eaaebf85025ec5b55f3ebf6360b143196b0d056fcb04c6b097'and d['strict_audio_video_seconds']==3 and d.get('process_reaped')is True and d.get('pipes_settled')is True for d in phase['decoders'].values()):raise ValueError('ActualPrefixDecode')
 complete=set(summary['backends'])=={'source','native'}and summary['source_before']['dataset_sha256']==summary['native_before']['dataset_sha256']and all(summary['backends'][m]['mode']==m for m in ['source','native'])
 if summary['HTTP_pair_complete']!=complete:raise ValueError('ActualCompletedPairReplay')
 base={**summary,'HTTP_pair_complete':complete,'owned_cleanup_valid':clean,'default_named_tables_sequences_unchanged':audit}
 replay=gate.pair({**base,'backends':{m:{**summary['backends'][m],**actual[m]}for m in actual}})
 if any(summary.get(k)!=v for k,v in replay.items()):raise ValueError('ActualPairReplay')
 offline=gate.pair({**base,'backends':corrected})
 sharing={}
 for mode,origin in origins.items():
  groups={}
  for request in origin['requests']:
   key=request['method']+':'+str(request['path_role'])+':'+request['kind']
   row=groups.setdefault(key,{'requests':0,'bytes_written':0,'connection_ids':[]});row['requests']+=1;row['bytes_written']+=request['bytes_written'];row['connection_ids'].append(request['connection'])
  sharing[mode]={'groups':groups,'accepted_TCP_total':origin['accepted_TCP_total'],'control_total':origin['control_total'],'media_HEAD_total':origin['media_HEAD_total'],'transient_TCP_without_request':origin['transient_TCP_without_request'],'server_body_write_not_client_delivery':True,'one_TCP_per_viewer_inferred':False}
 counters={m:[{'label':c['label'],'value':c.get('literal_profile_counter'),'at':c['at']}for c in summary['backends'][m]['checkpoints']]for m in ['source','native']}
 return {'scope':'Offline raw-field recomputation only; actual exit/report preserved','actual_exit_code':full['actual_exit_code'],'actual_gate_recomputed':replay,'actual_phase_gate_recomputed':actual,'corrected_ownership':fixed,'separate_offline_gate_recomputed':offline,'same_statuses':{m:[r['status']for r in summary['backends'][m]['probe_outcomes']]for m in ['source','native']},'literal_counter_timeline':counters,'no_source_zero_at_recorded_checkpoints':all(c['value']!='0'for c in counters['source']),'owned_origin_request_groups':sharing,'physical_cleanup_and_default_audit_recomputed':clean and audit,'runtime_or_application_rerun':False,'full_M4_acceptance':False}
if __name__=='__main__':
 full=json.loads((HERE/'full-retained.json').read_text());projected=json.loads((HERE/'retained-input.json').read_text());cleanup_path=HERE/'independent-cleanup.json'
 if digest(cleanup_path)!=full['independent_cleanup_sha256']:raise ValueError('ExactIndependentCleanup')
 print(json.dumps(recompute(full,projected,json.loads(cleanup_path.read_text())),indent=2))
