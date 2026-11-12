#!/usr/bin/env python3
"""Validate static public M0 crosswalk attestations; never execute workloads.
Original Dispatcharr/dependency authors retain credit and licenses.
"""
import argparse,hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
CROSSWALK_SHA='4bb1f1e895874bb83d9d8f910e6806000ed81c52147a43cd9703d8fa60a46e71'
EXPECTED_PATHS={'docs/rust-contract-register.json','scripts/rust-baseline/idle-churn/policy.json','scripts/rust-baseline/idle-churn/verified-original-three-repetitions-corrected-2.json','scripts/rust-playback-baseline/verified-source-accounting-1.json','scripts/rust-playback-baseline/verified-source-finite-capacity-1.json','scripts/rust-playback-baseline/verified-source-finite-ten-1.json','scripts/rust-playback-baseline/verified-source-application-stream-change-1.json','scripts/rust-baseline/sustained/verified-source-sustained-accounting-1.json','scripts/rust-baseline/concurrent-import/verified-source-bounded-concurrent-1.json'}
def require(value,name):
 if not value:raise ValueError(name)
def digest(raw):return hashlib.sha256(raw).hexdigest()
def read(root,path):
 relative=Path(path);require(not relative.is_absolute()and'..'not in relative.parts,'RelativePublicPathRequired')
 target=(root/relative).resolve();require(target.is_relative_to(root.resolve()),'PublicPathEscape');return target.read_bytes()
def validate(root=None,directory=HERE,deep=False):
 root=Path(root or directory.parents[2]).resolve();directory=Path(directory)
 raw=(directory/'crosswalk.json').read_bytes();require(digest(raw)==CROSSWALK_SHA,'CrosswalkHistoricalBytesChanged');data=json.loads(raw);manifest=json.loads((directory/'public-authorities.json').read_text());require(manifest['crosswalk_sha256']==CROSSWALK_SHA,'CrosswalkBinding')
 authorities=manifest['authorities'];require(len(authorities)==len(EXPECTED_PATHS)and{r['path']for r in authorities}==EXPECTED_PATHS,'PublicAuthoritySet')
 documents={}
 for record in authorities:
  raw=read(root,record['path']);require(digest(raw)==record['sha256'],'PublicAuthorityHash:'+record['path']);documents[record['path']]=json.loads(raw)
 registry=documents['docs/rust-contract-register.json'];counts=registry['counts'];require(counts==data['registry']['counts'],'LiteralRegistryCountsChanged');require((counts['runtime_routes'],counts['runtime_operation_work_items'],counts['fully_verified_route_operations'])==(592,798,0),'NoCompletionFromCounts')
 require(next(r['sha256']for r in authorities if r['path']==data['registry']['path'])==data['registry']['sha256'],'RegistryBinding')
 operations={x['contract_id']:x for r in registry['runtime_routes']for x in r['operations']};candidates={r['candidate_id']:r for r in registry['non_HTTP_candidates']}
 for item in data['HTTP_observations']:
  contract=operations[item['literal_contract_id']];require(contract['method']==item['registered_method'],'RegisteredMethodChanged')
  require(item['registered_method']is None or item['registered_method']==item['observed_method'],'ObservedMethodCollapsed')
  require(item['registered_method']is not None or item['literal_contract_id'].endswith(':METHOD_UNRESOLVED'),'UnresolvedIDInvented')
  require(item['fully_verified']is False and item['Native_acceptance']is False and item['method_resolution_or_role_matrix_updated']is False,'HTTPOverclaim')
 for item in data['task_observations']:
  candidate=candidates[item['literal_candidate_id']];require(item['observed_task'].split('.')[-1]==candidate['source']['name'],'TaskCandidateChanged');require(item['fully_verified']is False and item['Native_acceptance']is False,'TaskOverclaim')
 require(len(data['HTTP_observations'])==12 and len(data['task_observations'])==3 and len(data['scenarios'])==8,'HistoricalScopeChanged')
 switch=[(x['observed_method'],x['statuses'])for x in data['HTTP_observations']if'change_stream'in x['observed_path']];require(switch==[('POST',[200]),('GET',[405]),('POST',[401])],'MethodRoleCollapse')
 require(data['M0_complete']is False and data['Native_acceptance']is False and data['fully_verified']is False,'M0Overclaim')
 policy=documents['scripts/rust-baseline/idle-churn/policy.json'];budgets=policy['budgets'];idle=data['scenarios'][0]
 require(all(budgets[k]==v for k,v in idle['literal_declared_growth_budgets'].items()),'LiteralGrowthBudgetChanged');require((budgets['post_idle_fd_growth'],budgets['post_idle_socket_growth'],budgets['post_idle_process_growth'])==(16,16,0),'LiteralFDNotZero')
 for scenario in data['scenarios'][1:-1]:
  require(all(scenario[k]is None for k in ['absolute_CPU_ceiling','absolute_memory_ceiling','growth_ceiling','Source_budget_acceptance']),'AbsentCeilingInvented')
 proposed=data['scenarios'][-1];require('HARD BLOCKED'in proposed['execution']and proposed['Source_budget_acceptance']is None,'ResourceDraftOverclaim')
 require(all(budgets[k]==v for k,v in proposed['proposed_budgets_not_yet_approved_for_four_roles'].items()),'InheritedProposedBudgetChanged')
 idle_public=documents['scripts/rust-baseline/idle-churn/verified-original-three-repetitions-corrected-2.json'];require(idle_public['observation_coverage_valid_all3']is True and idle_public['Source_budget_acceptance']is False,'SourceGrowthNegativeLost')
 for path,document in documents.items():
  if path.endswith('policy.json')or path=='docs/rust-contract-register.json':continue
  require(document.get('resource_acceptance',False)is False,'PublicResourceAcceptanceOverclaim')
  require(document.get('fully_verified',False)is False,'PublicEndpointOverclaim')
 worker=documents['scripts/rust-baseline/concurrent-import/verified-source-bounded-concurrent-1.json'];require(worker['real_worker_task_executions']==6 and worker['durable_checks']==44 and worker['resource_execution_hardblocked']is True,'WorkerReferenceScopeChanged')
 checked=0
 if deep:
  refs=[e for row in data['HTTP_observations']for e in row['Source_evidence']]+[x['evidence']for x in data['scenarios']]
  for ref in refs:require(digest(read(root,ref['path']))==ref['sha256'],'PrivateDeepEvidenceHash');checked+=1
 return {'passed':True,'validation_scope':'static committed/public summary attestation; not raw replay'if not deep else'static public plus original private hash verification; no execution','public_authorities':len(documents),'historical_crosswalk_sha256':CROSSWALK_SHA,'HTTP_method_role_observations':12,'task_candidate_observations':3,'scenarios':8,'registry_counts':{'routes':592,'operations':798,'fully_verified':0},'Source_growth_negative_preserved':True,'private_evidence_files_checked':checked,'M0_complete':False,'Native_acceptance':False,'resource_measurements_executed':False}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path);p.add_argument('--deep-evidence',action='store_true');a=p.parse_args();print(json.dumps(validate(a.repo,deep=a.deep_evidence),indent=2))
if __name__=='__main__':main()
