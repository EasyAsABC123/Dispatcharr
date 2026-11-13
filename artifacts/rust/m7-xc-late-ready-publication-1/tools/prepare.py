"""Exact retained-data projection; never invokes a fixture."""
import copy,hashlib,json,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[4]
HERE=Path(__file__).resolve().parent.parent
OUT=HERE/'selected/scripts/rust-xc-contracts/late-ready-component'
COR=ROOT/'artifacts/rust/m7-xc-late-ready-offline-correction-1'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def digest(v):return hashlib.sha256(v.encode()).hexdigest()
def dump(p,v):p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(v,indent=2)+'\n')
EVENT_KEYS={'kind','connection','backend_pid','monotonic','sql','parameter_hex','arm_epoch','execute_armed','execute_generation','rows','errors','sync_seen','tx','execute_monotonic','up_joined'}
ROW_KEYS={'case','producer','mode','passed','live_identity','prelude_request_issued','prelude_began_monotonic','prelude','prelude_completed_monotonic','prelude_settled','warm_snapshot','warm_target','request_started_monotonic','cancelled_backend_pid','actual_SQL_sha256','held','observer_finished_seen_monotonic','while_held','shutdown_held','observer','settled','peer_stdout_sha256','peer_stderr_sha256','original_PID_absent','terminal','natural_proxy_drain','actual_exit','pipes_settled','proxy_terminal','observer_lock_cleanup','cleanup_steps','watchdog_expired','elapsed_seconds'}
def project(v,key=None):
 if key=='declared_first_parameter_hex':return {k:digest(x)for k,x in v.items()}
 if key=='parameter_hex':return None if v is None else [None if x is None else digest(x)for x in v]
 if key=='first_parameter_hex':return digest(v)
 if key=='database':return {'private_string_sha256':digest(v),'bytes':len(v.encode())}
 if key=='events':return [project({k:x[k]for k in EVENT_KEYS if k in x})for x in v]
 if key=='connections':return [{k:x[k]for k in('id','closed','up_joined')if k in x}for x in v]
 if isinstance(v,dict):return {k:project(x,k)for k,x in v.items()if k not in {'body_hex','header_hex','statement_hex','port'}}
 if isinstance(v,list):return [project(x)for x in v]
 return copy.deepcopy(v)
def faults(v,old=False):
 out=project({k:x for k,x in v.items()if k!='cases'})
 out['cases']=[project({k:r[k]for k in ROW_KEYS if k in r})for r in v['cases']]
 if old:out['cases'][1]={k:out['cases'][1][k]for k in('case','mode','passed')}
 return out
def main():
 bindings={};attempts=[]
 for n in(1,2,3):
  src=ROOT/f'artifacts/rust/m7-xc-late-ready-native-{n}';dest=OUT/f'retained/attempt-{n}'
  for name in('execution.json','faults-private.json','default-before-private.json','default-after-private.json','owned-backends-after-Native.json'):
   p=src/name;bindings[str(p.relative_to(ROOT))]=sha(p)
  e=json.loads((src/'execution.json').read_text());before=json.loads((src/'default-before-private.json').read_text());after=json.loads((src/'default-after-private.json').read_text())
  attempts.append({'attempt':n,'controller_exit':e['controller_exit'],'passed':e['passed'],'default_before':project(before),'default_after':project(after),'backend_after':json.loads((src/'owned-backends-after-Native.json').read_text()),'raw_capture_passed':json.loads((src/'faults-private.json').read_text())['passed']})
  if n==1:continue
  v=json.loads((src/'faults-private.json').read_text());dump(dest/'faults.json',faults(v,n==2))
  for row in v['cases'][:1] if n==2 else v['cases']:
   folder=f"{row['case']}-{row['mode']}"
   for stream in('stdout','stderr'):
    p=src/folder/f'peer.{stream}';t=dest/folder/p.name;t.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,t);bindings[str(p.relative_to(ROOT))]=sha(p)
 dump(OUT/'attempts.json',attempts);dump(OUT/'private-input-hashes.json',bindings)
 for name in('old1_validator.py','new9_validator.py','warm_contract.py'):
  shutil.copyfile(COR/name,OUT/name)
 for name in('schema.json','old-plan.json','plan.json','native-inputs.json'):shutil.copyfile(COR/'support'/name,OUT/'support'/name)
 shutil.copyfile(COR/'verification-final.json',OUT/'historical-agent-qualification.json')
 shutil.copyfile(COR/'Root-independent-review.json',OUT/'Root-independent-review.json')
 # Raw cleanup commands may contain owned nonce names. Publish only exact receipts
 # with output identities hashed; do not copy those identifying command strings.
 c=json.loads((COR/'fresh-cleanup-private.json').read_text())
 dump(OUT/'cleanup.json',{'attribution':'agent /root/stream_http, not Root','private_sha256':sha(COR/'fresh-cleanup-private.json'),'queries':[{k:x[k]for k in('attempt','kind','actual_exit','stdout_sha256','stderr_sha256','physically_settled','pipes_settled')}for x in c['queries']],'host_identity_absence':c['host_identity_absence']})
 dump(OUT/'projection.json',{'parameter_encoding':'SHA256 of original parameter hex text, including declared and warm-target equality tokens; not raw parameter bytes','omitted_fields':['unused backend body/header frame payloads','statement names','ports','unused case fields','failed second old case detail beyond identity/result'],'database_encoding':'SHA256 and byte length of original database identity','private_inputs':'private-input-hashes.json','Root_verification':'Exact deterministic projection verified privately; public verifier recomputes selected projected physical predicates, not hidden SQL parameter values or hidden database identities.'})
if __name__=='__main__':main()
