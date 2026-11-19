"""Retained Original framework suite assignments; Original authors retain all credit.
No application/test module, resolver, network or database is executed.
"""
import ast,copy,hashlib,json
from pathlib import Path
KIND='retained_original_full_suite_asserted_framework'
BASE='scripts/rust-contracts/framework-suite/'
INPUT=BASE+'bindings.json'
INVENTORY_SHA='81e3071afe33a85e0e33ea53fb1d7ada2ad3b26f434839446faed70b48062a97'
sha=lambda b:hashlib.sha256(b).hexdigest()
def normalized_ast(node):
    # The retained dump omits empty optional fields. Explicit formatting gives
    # the same bytes in Bookworm's SDK Python and newer host interpreters.
    if isinstance(node, ast.AST):
        fields = []
        for name, value in ast.iter_fields(node):
            if isinstance(value, list) and not value:
                continue
            literal_none = name == 'value' and isinstance(node, (ast.Constant, ast.MatchSingleton))
            if value is None and not literal_none:
                continue
            fields.append(f'{name}={normalized_ast(value)}')
        return f'{type(node).__name__}({", ".join(fields)})'
    if isinstance(node, list):
        return f'[{", ".join(normalized_ast(value) for value in node)}]'
    return repr(node)

def fingerprint(node):
    return sha(normalized_ast(node).encode())
def digest(v):return sha(json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())
def checked(root,name,expected):
 p=Path(root)/name;assert p.is_file()and not p.is_symlink();data=p.read_bytes();assert sha(data)==expected,'exact framework authority bytes';return data
def inventory(root):return json.loads(checked(root,BASE+'inventory.json',INVENTORY_SHA))
def dependencies():
 root=Path(__file__).resolve().parents[2]
 return {INPUT,BASE+'inventory.json',*(BASE+n for n in inventory(root))}
def generate(root):
 inv=inventory(root);data={n:checked(root,BASE+n,h)for n,h in inv.items()};candidate=json.loads(data['candidates.json']);attestation=json.loads(data['selected-test-attestation.json'])
 assert candidate['baseline_HTTP_assignments']==163 and candidate['new_operation_candidates']==10
 assert attestation['suite_log_sha256']=='5ce7a671b204e8b1a444e2edbf51313363aaf385989e8268eee011ea1612ad35' and attestation['Source_suite_exit']==0 and attestation['total_recorded_test_occurrences']==3362
 attest={r['test_id']:r for r in attestation['selected_methods']};assert len(attest)==34
 files={}
 for name,h in candidate['source_files'].items():
  raw=data['original/'+name];assert sha(raw)==h;files[name]=(raw.decode(),ast.parse(raw))
 contracts=[];fixtures={};used=set();sites=0
 for key,rows in candidate['contracts'].items():
  first=rows[0];assert key==first['contract_id'];fixture='retained-suite-'+key
  for row in rows:
   sites+=1;used.add(row['test_id']);text,tree=files[row['source_path']]
   cls=next(n for n in tree.body if isinstance(n,ast.ClassDef)and n.name==row['class']);test=next(n for n in cls.body if isinstance(n,ast.FunctionDef)and n.name==row['test_method'])
   assert fingerprint(cls)==row['class_AST_sha256'] and fingerprint(test)==row['test_AST_sha256']
   assert row['test_id']==row['source_path'][:-3].replace('/','.')+'.'+cls.name+'.'+test.name
   container=test
   if row.get('helper_method'):
    container=next(n for n in cls.body if isinstance(n,ast.FunctionDef)and n.name==row['helper_method']);assert fingerprint(container)==row['helper_AST_sha256']
   calls=[n for n in ast.walk(container)if isinstance(n,ast.Call)];request=next(n for n in calls if fingerprint(n)==row['request_AST_sha256'])
   assert ast.get_source_segment(text,request)==row['request_expression'] and request.func.attr.upper()==row['observed_request_method']
   if row.get('explicit_as_view_expression'):
    view=next(n for n in calls if fingerprint(n)==row['explicit_as_view_AST_sha256']);assert ast.get_source_segment(text,view)==row['explicit_as_view_expression'];assert ast.literal_eval(view.args[0])=={row['observed_request_method'].lower():row['declared_action']};assert view.func.value.id==row['recorded_handler'].rsplit('.',1)[1]
   assertions={ast.get_source_segment(text,n)for n in ast.walk(test)if isinstance(n,ast.Call)};assert row['response_status_assertions']and set(row['response_status_assertions'])<=assertions
   record=attest[row['test_id']];assert record['passed_in_exact_unchanged_suite']is True and record['raw_block_sha256']==row['actual_retained_test_log_block_sha256']
   assert row['role_claim'].startswith('unresolved;')and row['new_Source_test_or_HTTP_execution']is False and row['Native_coverage_added']is False
   for k in ['contract_id','declared_method','declared_action','recorded_handler','pattern_exact','resolver_ordinal','owning_milestone']:assert row[k]==first[k]
  fixtures[fixture]={'baseline_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','setup':'Exact retained Original test class/setup/decorator/mock closure; framework client/factory only','suite_exit':0,'suite_tests':3362,'new_Source_execution':False,'effective_actor_roles_qualified':False,'Source_HTTP_transport_proven':False}
  contracts.append({'contract_id':key,'method':first['declared_method'],'source_action':first['declared_action'],'owning_milestone':first['owning_milestone'],'pattern_exact':first['pattern_exact'],'source_handler':first['recorded_handler'],'resolver_ordinal':first['resolver_ordinal'],'fixture_id':fixture,'source_role_specs':{},'observed_roles':[],'cases':copy.deepcopy(rows),'assertions':['Exact retained Original request and bound status/workflow assertions','Pinned Original class/helper/setup/decorators and actual successful suite attestation; no new HTTP/resolver execution'],'result':{'state':'retained_Original_framework_assertions','asserted_request_sites':len(rows),'new_Source_cases_this_task':0,'new_Source_execution':False,'Native_coverage_added':False,'fully_verified':False},'remaining':['Effective actor roles, real HTTP/runtime side effects and Native parity remain unqualified','All milestone and performance acceptance remain open']})
 assert sites==36 and used==set(attest)
 return {'format_version':1,'binding_kind':KIND,'credit':'Original Dispatcharr and dependency authors retain all application/algorithm credit.','contracts':contracts,'fixtures':fixtures,'operation_count':10,'request_sites':36,'historical_test_methods':34,'new_Source_execution':False,'fully_verified_added':0}
def validate(root,value,bindings=None):
 assert value==generate(root),'exact retained framework assignments'
 return {'new_assignments':10,'request_sites':36,'historical_test_methods':34,'fully_verified_added':0}
def restore_pre_framework(rows):
 root=Path(__file__).resolve().parents[2];packet=generate(root);proof=json.loads(checked(root,BASE+'preservation.json',inventory(root)['preservation.json']));selected={c['contract_id']:c for c in packet['contracts']}
 for route in rows:
  for op in route['operations']:
   key=op['contract_id']
   if key not in selected:continue
   c=selected[key];prior=proof['selected_original_unassigned_dicts'][key];expected=copy.deepcopy(prior);expected.update(setup_fixture=packet['fixtures'][c['fixture_id']],assertions=c['assertions'],result=c['result'],executable_Source_contract=c,coverage_state='partial_executable_Source_contract');expected['role_matrix'].update(observed_fixture_definitions={},complete=False,state='partial_Original_framework_test_assignment',observed_roles=[])
   assert op==expected,'only exact reviewed framework additions may be reversed';op.clear();op.update(copy.deepcopy(prior))
 return rows
