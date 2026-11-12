import json,pathlib,hashlib,datetime
root=pathlib.Path(__file__).resolve().parents[3]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
scheduler=root/'artifacts/rust/image53-matrix-scheduler-2'
state=json.loads((scheduler/'state.json').read_text());plan=json.loads((scheduler/'inputs/plan.json').read_text())
assert len(state['nodes'])==28 and not state['halted']
assert all(v['status']=='passed-clean' for v in state['nodes'].values()),'All28 must pass semantic+cleanup review before Main'
assert all(v['node'] is None and v.get('reserved') is None for v in state['lanes'].values())
rows=[];events=[]
for name,item in state['nodes'].items():
 reviewpath=pathlib.Path(item['review_path']);assert sha(reviewpath)==item['review_sha256'];review=json.loads(reviewpath.read_text())
 resultpath=root/'artifacts/rust/image53-packaged-matrix-1/controller-logs'/(name+'.result.json');assert sha(resultpath)==item['controller_result_sha256']==review['controller_result_sha256'];actual=json.loads(resultpath.read_text())
 assert actual['returncode']==review['actual_process_exit']==0 and actual['process_settled'] is review['actual_process_settled'] is True
 assert review['name']==name and review['passed'] and review['semantic_review_complete'] and review['cleanup']['verified']
 for report in review['actual_reports']:assert sha(pathlib.Path(report['path']))==report['sha256']
 if name=='strict-typed154':
  assert review['cleanup']['scope']=='offline-no-resources' and review['cleanup']['project_ids']==[] and review['observed_default_audits']==[]
 else:
  query=review['cleanup']['query_receipt'];assert sha(pathlib.Path(query['path']))==query['sha256']
 assert all(not values for values in review['cleanup']['inventories'].values())
 node=next(v for v in plan['groups']if v['name']==name);assert actual['argv']==node['argv'] and actual['cwd']==node['cwd'] and actual['plan_sha256']==sha(scheduler/'inputs/plan.json')
 assert item['started_epoch']<=actual['completed_epoch']<=item['reviewed_epoch']
 events.extend([(item['started_epoch'],1,name),(item['reviewed_epoch'],-1,name)])
 rows.append({'name':name,'actual_exit':0,'process_settled':True,'review_sha256':item['review_sha256'],'actual_reports':review['actual_reports'],'observed_default_audits':review['observed_default_audits'],'audit_scope':review.get('audit_scope','Not present in retained review; no audit scope inferred'),'observed_counts_and_candidate_Source_scope':review['observed_counts_and_candidate_Source_scope'],'cleanup_verified':True})
active=set();peak=0
for stamp,change,name in sorted(events):
 if change==1:active.add(name)
 else:active.remove(name)
 peak=max(peak,len(active));assert len(active)<=3
 if any(x in active for x in ['sharedTS18','sharedTS6-all-down','retention100']):assert len(active)==1
assert not active
first=min(state['nodes'],key=lambda n:state['nodes'][n]['started_epoch']);assert first=='guide-grid-packaged';assert datetime.datetime.fromtimestamp(state['nodes'][first]['started_epoch'],datetime.timezone.utc).minute<=35
ordinary=[n for n in state['nodes']if n not in ['sharedTS18','sharedTS6-all-down','retention100']]
prior=max(state['nodes'][n]['reviewed_epoch']for n in ordinary)
for name in ['sharedTS18','sharedTS6-all-down','retention100']:
 assert state['nodes'][name]['started_epoch']>=prior;prior=state['nodes'][name]['reviewed_epoch']
s=state['nodes']['complete-refresh-original'];n=state['nodes']['complete-refresh'];assert s['lane']==n['lane'] and 0<=n['started_epoch']-s['reference_clock_epoch']<=900
value={'passed':True,'scope':'Independent28 actual controller/report/cleanup receipt bindings and finite schedule; retain each node scoped flags, no wholebackend/performance acceptance','nodes':rows,'maximum_owned_lanes':peak,'guide_first_valid_UTC_window':True,'SourceNative_refresh_same_lane_fresh':True,'three_heavy_solo_after_ordinary_fixed_order':True,'state_sha256':sha(scheduler/'state.json'),'plan_sha256':sha(scheduler/'inputs/plan.json'),'new_application_or_resource_runs':0,'all_milestones_complete':False}
p=pathlib.Path(__file__).with_name('Root-matrix-review.json');assert not p.exists();p.write_text(json.dumps(value,indent=2)+'\n');print(json.dumps({'passed':True,'nodes':len(rows),'maximum_lanes':peak,'receipt':str(p)}))
