"""Literal executable-Source assignment/gap index; no applications or parity pass.
Original Dispatcharr authors retain all application/algorithm credit and licenses.
"""
import argparse,collections,hashlib,json,re
from pathlib import Path
HTTP={'GET','HEAD','POST','PUT','PATCH','DELETE','OPTIONS','TRACE'}
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def method_sites(value):
 methods=set()
 if isinstance(value,dict):
  for key,item in value.items():
   if key in('HTTP_method_observed','method')and isinstance(item,str)and item.upper()in HTTP:methods.add(item.upper())
   if key=='observed_request_methods'and isinstance(item,list):methods.update(v for v in item if v in HTTP)
   methods.update(method_sites(item))
 elif isinstance(value,list):
  for item in value:methods.update(method_sites(item))
 return methods

def generate(root,register_path=None,historical_log=None):
 root=Path(root);path=Path(register_path)if register_path else root/'docs/rust-contract-register.json';register=json.loads(path.read_bytes());manifest=json.loads((root/'scripts/rust-contracts/executable-source-inputs.json').read_bytes());types={};source_inputs=[]
 for reference in manifest['inputs']:
  source=root/reference['path'];assert sha(source)==reference['sha256'],'exact Source assignment input';binding=json.loads(source.read_bytes());source_inputs.append(reference)
  for contract in binding['contracts']:
   if contract['contract_id']in types and reference['kind']=='retained_original_management12_channelguide25_HTTP':continue
   assert contract['contract_id']not in types,'one primary Source definition per operation';types[contract['contract_id']]=(reference['kind'],contract)
 assigned=[];missing=[];unknown=[];seen=set()
 for route in register['runtime_routes']:
  for operation in route['operations']:
   identifier=operation['contract_id'];assert identifier not in seen;seen.add(identifier);definition=operation.get('executable_Source_contract');summary={'contract_id':identifier,'pattern_exact':route['pattern_exact'],'handler':route['source_handler'],'declared_method':operation['method'],'declared_action':operation['source_action'],'milestone':operation['owning_milestone']}
   assert operation['fully_verified']is False
   if definition is not None:
    assert identifier in types and types[identifier][1]==definition,'exact generated Source definition';assert definition['contract_id']==identifier and operation['setup_fixture']and operation['assertions']and operation['result'],'real setup assertions result required';assigned.append({**summary,'Source_definition_kind':types[identifier][0],'fixture_id':definition['fixture_id'],'fixture_method_sites':sorted(method_sites(definition)),'complete_role_matrix':False,'runtime_endpoint_or_Native_parity_claim':False})
   else:missing.append({**summary,'missing':'Executable Original setup/assertion/result assignment','related_Native_or_category_evidence_is_not_substitute':True})
   if operation['method']is None:unknown.append({**summary,'Source_fixture_assigned':definition is not None,'Source_fixture_method_sites':sorted(method_sites(definition))if definition else[],'runtime_handler_candidates':route.get('runtime_handler_candidates'),'declared_method_resolved':False,'site_scope':'Named successful framework workflow/static request sites or retained scoped capture; does not resolve router declaration or establish complete methods/roles'})
 nonhttp=[x for x in register['non_HTTP_candidates']if x.get('executable_Source_contract')is not None]
 summary=json.loads((root/'scripts/rust-contracts/framework-auth/full-suite-reference.json').read_bytes());assert summary['tests_passed']==3362 and summary['exit_code']==0 and summary['scope']=='unchanged-upstream-full-python-test-suite'
 suite={'recorded_passed_test_occurrences':3362,'distinct_test_label_count':None,'new_Source_executions':0,'qualified_reference_sha256':sha(root/'scripts/rust-contracts/framework-auth/full-suite-reference.json'),'full_log_sha256':summary['artifact_sha256']['tests.log'],'Native_coverage_increased':False}
 if historical_log is not None:
  raw=Path(historical_log).read_bytes();assert hashlib.sha256(raw).hexdigest()==suite['full_log_sha256'];labels=[v.decode()for v in re.findall(rb'(?m)^test_[^\n]+ \(([^\n]+)\)',raw)];assert len(labels)==3362;suite.update(distinct_test_label_count=len(set(labels)),duplicate_label_occurrences=len(labels)-len(set(labels)),full_log_read_only_checked=True)
 counts={'supplemental_Source_capture_operations':sum(bool(o.get('supplemental_executable_Source_captures'))for r in register['runtime_routes']for o in r['operations']),'runtime_routes':len(register['runtime_routes']),'runtime_operation_work_items':len(seen),'executable_Source_partial_operations':len(assigned),'operations_without_executable_Source_assignment':len(missing),'declared_method_unresolved_operations':len(unknown),'method_unresolved_with_Source_fixture':sum(x['Source_fixture_assigned']for x in unknown),'typed_non_HTTP_Source_partial_candidates':len(nonhttp),'non_HTTP_candidates':len(register['non_HTTP_candidates']),'fully_verified_operations':0}
 assert counts['runtime_routes']==592 and counts['runtime_operation_work_items']==798 and counts['declared_method_unresolved_operations']==322 and counts['non_HTTP_candidates']==250 and len(assigned)+len(missing)==798 and counts['typed_non_HTTP_Source_partial_candidates']<=250
 return {'scope':'Source definition assignment audit only; not M0 closure, HTTP/role completeness or Native runtime parity','Original_author_credit':'Original Dispatcharr authors retain full application/algorithm credit and licensing.','register_sha256':sha(path),'Source_inputs':source_inputs,'counts':counts,'qualified_Original_suite':suite,'assigned_Source_operations':assigned,'unassigned_Source_operations':missing,'method_unresolved_observation_crosswalk':unknown,'remaining_gates':['Root bounded M0 baseline disposition remains separate from later complete rewrite acceptance',f"{len(missing)} operation Source setup/assertion/result assignments remain absent; missing source authority cannot be filled with Native metadata or generic fixture text",f"322 declaration/method placeholders remain unresolved, including{counts['method_unresolved_with_Source_fixture']} with narrower Source fixture-method sites; observedGET does not resolve dispatch completeness",f"{250-len(nonhttp)} nonHTTP candidates lack typed Source assignment; static task inventory is not actual broker/worker graph acceptance",f"All{len(assigned)} assigned operations still have partial role/input/transport coverage and fully_verified false",'Core special Source51 has51 public framework observations over5unassignedoperations; its packet explicitly lacks complete transitive executed host/setup closure, so it is a follow-on authority candidate, not assigned executable Source here','58schema warnings and66graceful serializer-inference fallbacks remain discovery limitations'],'M0_complete':False,'Native_coverage_increased':False,'new_Source_executions':0,'performance_claims':False}

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[2]);p.add_argument('--register',type=Path);p.add_argument('--historical-log',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args();result=generate(a.root,a.register,a.historical_log);a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'counts':result['counts'],'qualified_Original_suite':result['qualified_Original_suite'],'M0_complete':False,'Source_or_Native_apps_started':0}))
if __name__=='__main__':main()
