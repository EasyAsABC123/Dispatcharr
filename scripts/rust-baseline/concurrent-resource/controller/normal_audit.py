"""Read-only bounded terminal collector witnesses; not a Source task replacement."""
import json
import fcntl
from pathlib import Path
LIMIT=8388608
KINDS=('prerun','postrun','published')
def inspect(directory,collector):
 root=Path(directory)
 if(root/'worker-observer-capacity.json').exists():raise ValueError('PersistentWorkerObserverCapacity')
 budget_path=root/'worker-observer-budget.json'
 if budget_path.stat().st_size>256:raise ValueError('WorkerBudgetBound')
 with budget_path.open('rb')as ledger:
  fcntl.flock(ledger,fcntl.LOCK_SH)
  budget=json.loads(ledger.read())
  if any(type(budget[k])is not int for k in ['records','bytes'])or not 0<=budget['records']<=1024 or not 0<=budget['bytes']<=LIMIT:raise ValueError('WorkerBudget')
  paths=[root/('workflow-'+kind+'.jsonl')for kind in KINDS];size=sum(p.stat().st_size for p in paths)
  if size>LIMIT or size!=budget['bytes']:raise ValueError('WorkerJournalBytes')
  counts={}
  for kind,path in zip(KINDS,paths):
   lines=path.read_bytes().splitlines(keepends=True);counts[kind]=len(lines)
   for line in lines:
    if len(line)>65536 or not line.endswith(b'\n'):raise ValueError('WorkerRecordBound')
    json.loads(line)
  if sum(counts.values())!=budget['records']:raise ValueError('WorkerJournalRecords')
 pipes=[*collector.threads,*([collector.writer]if collector.writer else[])]
 if not collector.physically_settled or collector.process.poll()!=0 or any(t.is_alive()for t in pipes):raise ValueError('WorkflowPhysicalDrain')
 stdout=root/'workflow-controller-private.stdout';stderr=root/'workflow-controller-private.stderr';used=stdout.stat().st_size+stderr.stat().st_size
 if used!=collector.used or used>LIMIT or collector.invalid is not None:raise ValueError('WorkflowOutputBudget')
 return {'normal_capacity_marker_absent':True,'worker_journal_budget':budget,'worker_journal_counts':counts,'worker_aggregate_bytes':size,'stdout_bytes':stdout.stat().st_size,'stderr_bytes':stderr.stat().st_size,'combined_process_output_bytes':used,'process_returncode':0,'physical_process_and_pipes_settled':True,'resource_execution_blocked':True}
