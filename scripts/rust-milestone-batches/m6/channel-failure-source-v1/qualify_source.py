"""Read-only semantic qualification of new actual Source9; never dispatches apps."""
import base64,json,sys
from pathlib import Path
p=Path(sys.argv[1]);r=json.loads((p/'report.json').read_text());s=json.loads((p/'source-report.json').read_text());checks=[]
def check(name,value):checks.append({'name':name,'passed':bool(value)})
check('controller-actual0-guest0',r['actual_controller_exit']==r['actual_guest_exit']==0)
check('normal-and-independent-exact-cleanup',r['exact_owned_absence']and json.loads((p/'independent-cleanup.json').read_text())['exact_owned_absence'])
check('unchanged-default-database-sequences-redis7',r['default_unchanged'])
check('CLI-groups-and-pipes-settled',r['all_CLI_groups_settled']and all(x['process_group_settled']and x['stdout_EOF']and x['stderr_EOF']and not x['remaining_group_members']for x in r['commands']))
check('one-normal-worker-actualwait0-no-forcing-groupsgone',s['worker']['actual_wait_exit']==0 and not s['worker'].get('forced_kill')and s['worker']['remaining_group']==[]and s['worker']['leader_absent'])
check('Source-API0-unmeasured-context-only',s['Source_API_calls']==0 and not r['Source_case_replay'])
check('nine-cases-four-normal-five-failure',len(s['cases'])==9 and sum(x['metadata']['status']=='SUCCESS'for x in s['cases'])==4 and sum(x['metadata']['status']=='FAILURE'for x in s['cases'])==5)
expected={'names_P0001':'ProgrammingError','tvg_22001':'DataError','logos_23505':'IntegrityError','bulk_23514':'IntegrityError','logos_missing_arguments':'ValueError'}
for c in s['cases']:
 m=c['metadata'];check(c['name']+'-actual-configured-backend-bytes',json.loads(base64.b64decode(c['raw_backend_base64']))==m and m['task_id']==c['actual_task_id'])
 check(c['name']+'-terminalTTL',0<c['ttl']<=3600)
 check(c['name']+'-expected-status',c['expected_state_match'])
 decoded=c['Channels']['decoded']
 if c['name']in expected:
  check(c['name']+'-exception-class',m['result']['exc_type']==expected[c['name']])
  check(c['name']+'-module',m['result']['exc_module']==('builtins'if c['name']=='logos_missing_arguments'else'django.db.utils'))
  failed=[x['data']for x in decoded if x.get('data',{}).get('status')=='failed']
  check(c['name']+'-exact-failed-progress',len(failed)==1 and failed[0]['task_id']==c['actual_task_id']and failed[0]['progress']==0 and failed[0]['message']=='Task failed: '+m['result']['exc_message'][0]and failed[0]['error']==m['result']['exc_message'][0])
  check(c['name']+'-real-Python-traceback-not-fabricated',isinstance(m['traceback'],str)and '/app/apps/channels/tasks.py'in m['traceback'])
  check(c['name']+'-no-committed-channel-change',c['before']['dispatcharr_channels_channel']==c['after']['dispatcharr_channels_channel'])
  if c.get('controlled_owned_PG_error'):check(c['name']+'-actual-context-in-error','\nCONTEXT:  PL/pgSQL function owned_channel_failure() line 1 at RAISE'in m['result']['exc_message'][0])
 else:
  check(c['name']+'-completed-with-empty-errors',m['result']['status']=='completed'and m['result']['errors']==[]and m['traceback']is None)
check('ordinary-functional-wall-under600s',r['lease_ended_monotonic']-r['lease_started_monotonic']<600)
result={'passed':all(x['passed']for x in checks),'checks':checks,'scope':'Actual Source9 task-body/worker/backend/event/SQL authority only; no Native runtime acceptance or performance claim','Source_API_calls':0,'Original_worker_runs':1,'Original_guest_Django_processes':1,'Native_runs':0,'historical_failed_attempts_preserved':[1,2]};print(json.dumps(result,indent=2));raise SystemExit(0 if result['passed']else 1)
