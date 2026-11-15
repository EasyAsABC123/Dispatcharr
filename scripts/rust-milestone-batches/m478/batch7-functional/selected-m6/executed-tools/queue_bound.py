"""Functional Redis read bound for the actual unchanged future ETA FIFO head.
No CPU/RSS benchmark, synthetic workload or Source/application replay.
"""
import datetime,hashlib,json,math
from pathlib import Path

def epoch(value):return datetime.datetime.fromisoformat(value).timestamp()
def review(root,allow_incomplete=False):
 root=Path(root);policy=json.loads((root/'selected-queue-policy.json').read_text());rows=[json.loads(line)for line in(root/'redis-monitor.jsonl').read_text().splitlines()];capture=json.loads((root/'native-cohort.json').read_text());complete=capture['monitor_close']['terminal_barrier_observed']and capture['monitor_close'].get('terminal_barrier_fsynced',False)
 if not allow_incomplete and not complete:raise AssertionError('ActualDurableCompleteJournalRequiredForReadBound')
 queue=[];frames={};claimed=[];completions=[];windows={};head_since=None;peek_sha=policy['peek_script_sha1'];last=rows[-1]['time']if rows else 0
 for entry in rows:
  t=entry['time'];command=entry.get('command','');upper=command.upper()
  if upper.startswith('LPUSH CELERY '):
   frame=json.loads(command.split(' ',2)[2]);ident=frame['headers']['id'];frames[ident]={'frame':frame,'published':t};empty=not queue;queue.insert(0,ident)
   if empty:head_since=t
  elif upper=='RPOP CELERY':
   if not queue:raise AssertionError('ActualFIFOClaimWithoutPublication')
   ident=queue.pop();claimed.append((ident,t))
   if ident in windows:windows[ident]['actual_pop_epoch']=t
   head_since=t if queue else None
  elif upper.startswith('SETEX CELERY-TASKS:CELERY-TASK-META-'):
   ident=command.split(' ',2)[1].removeprefix('celery-tasks:celery-task-meta-');completions.append((ident,t))
  if queue:
   ident=queue[-1];value=frames[ident];eta=value['frame']['headers'].get('eta')
   if eta is not None and epoch(eta)>t:
    row=windows.setdefault(ident,{'task_id':ident,'task':value['frame']['headers']['task'],'head_since_epoch':head_since,'eta_epoch':epoch(eta),'actual_peek_times':[]})
    if upper.startswith('EVALSHA '+peek_sha.upper()+' ')and command.endswith(' celery'):row['actual_peek_times'].append(t)
 checks=[]
 for ident,value in windows.items():
  start=value['head_since_epoch'];end=min(value['eta_epoch'],value.get('actual_pop_epoch',last));duration=max(0,end-start);other_completed=sorted({key for key,t in completions if key!=ident and start<=t<end});prior_claimed=[key for key,t in claimed if t<start];boundary=prior_claimed[-policy['celery_maximum_concurrency']:]
  # Each poll turn may perform a lane peek, owned-worker admission peek and
  # one boundary reread. Actual already-claimed completions can wake the lane;
  # conservatively include same-wave boundary completions up to concurrency6.
  # No allowance is earned by arbitrary unclaimed NotDue completion loops.
  turns=math.ceil(duration/(policy['poll_ms']/1000))+1;named_wakes=len(other_completed)+len(boundary);bound=3*turns+2*named_wakes;actual=len([t for t in value['actual_peek_times']if t<end]);checks.append(value|{'interval_end_epoch':end,'pending_seconds':duration,'configured_poll_ms':policy['poll_ms'],'deadline_turns_including_initial_boundary':turns,'actual_other_backend_completion_ids_in_interval':other_completed,'actual_prior_claim_ids_for_boundary_overhang':boundary,'named_completion_wake_allowance':named_wakes,'maximum_peeks':bound,'actual_peeks':actual,'passed':actual<=bound})
 result={'qualified':complete and bool(checks)and all(c['passed']for c in checks),'diagnostic_only':not complete,'durable_journal_complete':complete,'pending_ETA_head_intervals':checks,'all_actual_peeks':sum(c['actual_peeks']for c in checks),'functional_resource_bound':'Configured100ms poll +lane/owned admission/boundary reads +actual named claimed-completion wakeups; no allowance for unclaimed NotDue spin','DVR_claimNotify_wakeups_in_this_Celery_only_window':0,'CPU_threshold_used':False,'paired_performance_gain_claimed':False,'Source_or_app_replays':0,'policy_sha256':hashlib.sha256((root/'selected-queue-policy.json').read_bytes()).hexdigest()};(root/'selected-ETA-read-bound.json').write_text(json.dumps(result,indent=2)+'\n');return result
