"""Read-only future authority evaluator; accepts actual responses/frames only."""
import base64,hashlib,json,sys
from pathlib import Path
out=Path(sys.argv[1]);r=json.loads((out/'report.json').read_text());s=json.loads((out/'source-report.json').read_text());checks=[]
def check(name,value):checks.append({'name':name,'passed':bool(value)})
check('actual-controller-guest0',r['actual_controller_exit']==r['actual_guest_exit']==0)
check('default-unchanged',r['default_unchanged'])
check('exact-CID-project-cleanup',r['exact_owned_absence']and json.loads((out/'independent-cleanup.json').read_text())['exact_owned_absence'])
check('all-CLI-groups-settled',r['all_CLI_groups_settled'])
check('confirmed-HTTP-call-counter',s['Source_API_calls']==len([x for x in s['cases']if 'status'in x])==r['Source_API_calls'])
check('all-new-cells-attempted',len(s['cases'])==s['planned_cases']and s['all_production_HTTP_cells_attempted'])
check('no-task-body-replay',s['Source_task_body_calls']==0 and not s['task_worker_or_Beat_started']and s['backend_final_keys']==0 and s['task_processes_before_stop']==[])
check('unchanged-task-business-tables',s['before_business']==s['after_business'])
stop=s['web_role']['stop'];check('actual-web-normal0-pipes-generations',stop['actual_exit']==0 and stop['stdout_EOF']and stop['stderr_EOF']and stop['all_original_process_generations_absent']and not stop['forced'])
check('actual-role-owner-reaped0',s['web_role']['actual_owner_wait']['actual_exit']==0)
for case in s['cases']:
 name=case['definition']['name'];check(name+'-actual-cells',case.get('passed')and all(case.get('checks',{}).values()))
 check(name+'-raw-body-first',hashlib.sha256(base64.b64decode(case['raw_body_base64'])).hexdigest()==case['raw_body_sha256'])
 check(name+'-physical-HTTP-close',case['actual_physical_HTTP_close']['original_inode_absent']and case['actual_physical_HTTP_close']['timer_joined'])
 for frame in case['new_family_frames']:check(name+'-exact-frame-bytes',json.loads(base64.b64decode(frame['raw_base64']))==frame['frame'])
result={'passed':all(x['passed']for x in checks),'checks':checks,'scope':'Only new actual Source TCP producer/role/frame authority; reuses Source9 task outcomes without rerunning their bodies','Native_runs':0,'Source_task_body_calls':0,'full_rewrite':False};print(json.dumps(result,indent=2));raise SystemExit(0 if result['passed']else 1)
