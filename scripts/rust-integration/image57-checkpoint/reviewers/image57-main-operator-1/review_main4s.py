import sys,json,hashlib
from pathlib import Path
from collect_cleanup import collect
R=Path.cwd();O=R/'artifacts/rust/m4-vod-four-second-main-image57-1';C=R/'artifacts/rust/image57-vod-main4s-execution-1/tools';sys.path.insert(0,str(C));import observation
P=R/'artifacts/rust/root-image57-vod-main4s-actual-review-1';P.mkdir(exist_ok=False);s=json.loads((O/'summary.json').read_text());assert s['passed']and s['observation_valid']and s['parity_passed']and s['performance_verified']is False and [v['case']for v in s['cases']]==['success','busy','clientloss'];rows=[];pids=[]
for v in s['cases']:
 assert observation.finalize(v)=={k:v[k]for k in ['observation_valid','parity_passed','passed']};assert observation.compare(v['source'],v['native'])==v['comparison'];assert v['default_database_before']==v['default_database_after']and v['independent_DB_clones_verified']and v['Source_signal_terminal_valid']and v['actual_Main_image_id']=='sha256:fc5ea2aba28f86a3ebf3672f6bf8d874a6f79b2d9dd4c8a1efb8993de680375a';assert len(v['default_database_before']['tables'])==9 and len(v['default_database_before']['sequences'])==9;holds=[]
 for mode in ['source','native']:
  h=v[mode]['hold_observation'];assert 4<=h['actual_seconds']<=4.35 and 0<=h['control_seconds']<=3.1;holds.append({'mode':mode,**h})
 for q in (O/v['case']).glob('owned-*.json'):
  d=json.loads(q.read_text());assert type(d['pid'])is int and d['pid']>0 and type(d['starttime'])is str;pids.append({'case':v['case'],'role':d['role'],'container_namespace_pid':d['pid'],'record_sha256':hashlib.sha256(q.read_bytes()).hexdigest(),'scope':'controller-owned container PID identity, not a host PID; owned namespace disposal verified by fresh project inventory below'})
 rows.append({'case':v['case'],'passed':v['passed'],'hold_observations':holds,'scope':v['comparison'].get('scope','same status/error body only'),'full_counter_or_inner_generator_parity_accepted':False})
c=collect([v['project']for v in s['cases']],P/'independent-cleanup.json');assert c['passed'];review={'passed':True,'actual_host_session':54808,'actual_host_exit':0,'cases_recomputed':rows,'default_named9_tables_and9_sequences_structurally_exact':True,'retained_container_namespace_process_records':pids,'independent_all_owned_project_inventories_empty':True,'raw_summary_sha256':hashlib.sha256((O/'summary.json').read_bytes()).hexdigest(),'full_M4_or_performance_acceptance':False};(P/'SourceContract-review.json').write_text(json.dumps(review,indent=2)+'\n');print('Main57 three4-second cases recomputed, holds valid, exact default audits/process/project cleanup passed')
