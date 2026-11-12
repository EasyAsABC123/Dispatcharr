"""Read-only final finite28 receipt validation; no workload dispatch.
Original Dispatcharr and dependency authors retain application/algorithm credit.
Counts stay per-node because contracts overlap and have different scopes.
"""
import argparse,hashlib,json,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
MATRIX=ROOT/'artifacts/rust/image53-packaged-matrix-1'
SCHED=ROOT/'artifacts/rust/image53-matrix-scheduler-2'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p):return json.loads(p.read_text())
def resolve(p):
 p=Path(p);return p if p.is_absolute()else ROOT/p
def bound(item):
 p=resolve(item['path']);assert sha(p)==item['sha256'],str(p);return p

def main():
 a=argparse.ArgumentParser(description=__doc__);a.add_argument('--output',required=True,type=Path);args=a.parse_args()
 state=load(SCHED/'state.json');plan=load(SCHED/'inputs/plan.json');assert not state['halted'] and len(state['nodes'])==28
 assert all(x['node']is None and x['reserved']is None for x in state['lanes'].values())
 rows=[]
 for node in plan['groups']:
  n=node['name'];s=state['nodes'][n];assert s['status']=='passed-clean' and s['returncode']==0 and s['process_settled']is True,n
  result=MATRIX/'controller-logs'/(n+'.result.json');assert sha(result)==s['controller_result_sha256'];r=load(result)
  assert r['name']==n and r['returncode']==0 and r['process_settled']is True and r['argv']==node['argv'] and r['cwd']==node['cwd']
  p=resolve(s['review_path']);assert sha(p)==s['review_sha256'];v=load(p)
  assert v['passed']is True and v['semantic_review_complete']is True and v['actual_process_settled']is True and v['actual_process_exit']==0 and v['controller_result_sha256']==sha(result)
  for b in v['actual_reports']:bound(b)
  c=v['cleanup'];assert c['verified']is True and c['inventories']=={'containers':[],'networks':[],'volumes':[]}
  if c['scope']=='offline-no-resources':assert n=='strict-typed154'
  else:
   assert c['scope']=='independent-owned-empty' and c['project_ids'];bound(c['query_receipt'])
  rows.append({'name':n,'kind':node['kind'],'schedule_class':node['schedule_class'],'process_exit':r['returncode'],'process_settled':True,'controller_result':{'path':str(result.relative_to(ROOT)),'sha256':sha(result)},'review':{'path':str(p.relative_to(ROOT)),'sha256':sha(p)},'actual_reports':v['actual_reports'],'observed_counts_and_scope':v['observed_counts_and_candidate_Source_scope'],'observed_default_audits':v['observed_default_audits'],'audit_scope':v.get('audit_scope'),'audit_scope_field_observed':('audit_scope'in v),'cleanup':c})
 out={'format_version':1,'scope':'Finite28 image53 scoped package/CLI/native-lifecycle gates; no new resource measurements, full application/milestone acceptance, or Main probes inferred','passed':True,'nodes':28,'actual_controller_exits_zero':28,'ordinary_nodes':25,'solo_heavy_nodes':3,'state_sha256':sha(SCHED/'state.json'),'plan_sha256':sha(SCHED/'inputs/plan.json'),'scheduler_execution_seal_sha256':sha(SCHED/'execution-seal.json'),'clearance_sha256':state['clearance_sha256'],'observed_epoch':time.time(),'per_node':rows,'no_cross_schema_count_sum':True,'missing_default_audits_not_inferred':True,'author_credit':'Original Dispatcharr/dependency authors retain application/algorithm/license credit'}
 assert not args.output.exists();args.output.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'passed':True,'nodes':28,'path':str(args.output),'sha256':sha(args.output)}))
if __name__=='__main__':main()
