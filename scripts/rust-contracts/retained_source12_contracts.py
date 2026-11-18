"""Retained Source12 search and M3U observations with actual resolver custody.
Original Dispatcharr and dependency authors retain all application credit.
No application, resolver, network or database execution occurs here.
"""
import copy,hashlib,json
from pathlib import Path
KIND='retained_original_Source12_search_M3U_resolver_HTTP'
BASE='scripts/rust-contracts/retained-source12/'
INPUT=BASE+'bindings.json'
INVENTORY_SHA='950a8fa51abe8bac01a9708e40ab2252a6a65a891121915bff1f9d5d420999ba'
sha=lambda b:hashlib.sha256(b).hexdigest()
def digest(v):return sha(json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())
def checked(root,name,expected):
 p=Path(root)/name;assert p.is_file()and not p.is_symlink();raw=p.read_bytes();assert sha(raw)==expected,'exact Source12 authority bytes';return json.loads(raw)
def inventory(root):return checked(root,BASE+'inventory.json',INVENTORY_SHA)
def dependencies():
 root=Path(__file__).resolve().parents[2]
 return {INPUT,BASE+'inventory.json',*(BASE+n for n in inventory(root))}
def generate(root):
 inv=inventory(root);data={}
 for name,h in inv.items():
  raw=(Path(root)/BASE/name).read_bytes();assert sha(raw)==h,'exact Source12 retained input'
  if name.endswith('.json'):data[name]=json.loads(raw)
 c=data['candidate-input.json'];q=data['Source12-qualification.json'];terminal=data['resolver/Root-terminal.json'];report=data['resolver/UID-report.json'];observed=data['resolver/observations.json']
 assert c['Source_qualification_sha256']==inv['Source12-qualification.json']=='7c990cb60c721f99af20cd56fc062e87122bba67bf9cbaea8fa54ab1e6f1a686'
 assert q['actual_controller_exit']==0 and q['all_twelve_capture_qualified']and q['new_SD_search_captures_qualified']==9 and q['new_M3U_direct_captures_qualified']==3
 assert terminal['passed']and terminal['actual_exit']==0 and terminal['failure']is None and terminal['stdout_EOF']and terminal['stderr_EOF']and terminal['process_group_settled']and terminal['remaining_group_members']==[]and terminal['output_bytes_discarded']==0
 assert terminal['UID_report_sha256']==inv['resolver/UID-report.json']=='acce32f292a8cb23d82d8e02b6290c181d2090866df0c5eb5bcacc3b59d17a9b'
 assert report['passed']and report['control_actual_exit']==report['actual_exit']==0 and report['exact_CID_absent']and len(report['exact_CID'])==64 and report['Source_paths']==3 and report['resolver_routes']==592 and report['actual_source_files']==1211
 assert report['observation_sha256']==inv['resolver/observations.json']and report['prepared_closure_sha256']==inv['resolver/executed-control/closure.json']
 closure=data['resolver/executed-control/closure.json']
 for name,h in closure['files'].items():assert inv['resolver/executed-control/'+name]==h
 assert len(data['resolver/executed-control/source-files.json'])==1211 and len(data['resolver/executed-control/input.json']['routes'])==592
 assert observed['passed']and observed['actual_UID']==65532 and observed['all592_resolver_identities_order_actions_exact']and observed['Source_HTTP_business_calls']==report['Source_HTTP_business_calls']==0
 assert observed['counts']==report['actual_guard_counts']=={'SQL_connection_or_cursor_attempts':0,'network_attempts':1,'thread_attempts':0,'child_attempts':0,'handler_invocations':0}
 assert observed['qualified_blocked_import_probe_count']==report['qualified_blocked_import_probe_count']==1
 denied,=observed['denied_operations'];assert denied['stage']=='django_setup'and denied['operation']=='socket.socket.bind'and denied['caller']['module']=='urllib3.util.connection'and denied['caller']['function']=='_has_ipv6'and denied['caller_file_sha256']=='2633bbdb69731e5ccb5cf4e4afd65605d86c7979cc5633126f50c92d5ad74a74'and denied['IPv6_loopback_ephemeral_bind']and denied['original_operation_executed']is False
 first=data['first-failure/Root-terminal.json'];failed=data['first-failure/UID-report.json'];commands=data['first-failure/commands.json'];fix=data['first-failure/successor-controls.json'];assert first['actual_exit']==1 and not first['passed']and failed['error_type']=='IndexError'and failed['exact_CID_absent']and fix['both_observer_cardinality_assertions_match3']
 assert any('line 25'in row['stderr']and "len(request['cases']) == 4"in row['stderr']and 'AssertionError'in row['stderr']for row in commands)
 witnesses={row['path']:row for row in observed['observations']};assert len(witnesses)==3
 contracts=[];fixtures={}
 for group,key,ordinal,method,action,handler,pattern in [('new_search_POST',c['new_search_contract_id'],157,'POST','sd_lineups_search','apps.epg.api_views.EPGSourceViewSet','api/epg/^sources/(?P<pk>[^/.]+)/sd-lineups/search/$'),('existing_M3U_METHOD_UNRESOLVED_supplement',c['existing_supplement_contract_id'],407,None,None,'apps.output.views.xc_get','get.php')]:
  cases=copy.deepcopy(c['cases'][group]);assert len(cases)==(9 if ordinal==157 else 3)
  for case in cases:
   w=witnesses[case['resolver_path_without_query']];assert w['actual_resolver_ordinal']==ordinal and w['actual_handler']==handler and w['actual_pattern']==pattern
   assert w['actual_action_map']==({'post':'sd_lineups_search'}if ordinal==157 else None)
   assert w['actual_kwargs']==({'pk':case['resolver_path_without_query'].split('/')[4]}if ordinal==157 else{})
   actual=next(row for row in w['retained_observations']if row['label']==case['label']);assert actual['private_wire_sha256']==case['raw_wire_sha256']and actual['HTTP_status']==case['actual_status']and actual['method']==case['observed_method']
   path='/home/justin/github/Dispatcharr/'+actual['private_wire_path'];assert q['evidence'][path]['sha256']==case['raw_wire_sha256']and q['evidence'][str(Path(path).parent/'wire-provenance.json')]['sha256']==case['wire_provenance_sha256']
   assert not case['effective_role_qualified']and not case['query_or_token_or_body_content_projected'];case['actual_resolver_witness_sha256']=digest(w)
  fixture='retained-Source12:'+key;fixtures[fixture]={'baseline_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','Source_image':report['image_id'],'actual_Source_controller_exit':0,'actual_resolver_exit':0,'Source_HTTP_replayed':False,'setup':'Actual qualified Source12 independent model/DB/Redis/media scopes with full SQL/cache/jobs/WS/provider captures','query_credentials_projected':False,'effective_role_matrix_qualified':False}
  contracts.append({'contract_id':key,'pattern_exact':pattern,'source_handler':handler,'resolver_ordinal':ordinal,'method':method,'source_action':action,'owning_milestone':'M5'if ordinal==157 else'M7','fixture_id':fixture,'cases':cases,'source_role_specs':{},'observed_roles':[],'assertions':['Exact retained qualified Source12 response status/body digest and private wire/provenance custody','Actual full592 resolver forest plus successful chain/ordinal/handler/actions/kwargs for all3 paths','Full retained Source SQL/sequences/cache/jobs/WebSocket/provider qualification; no Native parity acceptance'],'result':{'state':'retained_Source12_HTTP_with_actual_resolver_custody','new_Source_cases_this_task':0,'Source_observations_qualified':len(cases),'Native_coverage_increased':False,'fully_verified':False},'remaining':['Effective actor roles, format aliases, further inputs and Native parity remain unqualified','All milestones and performance acceptance remain open']})
 return {'format_version':1,'binding_kind':KIND,'credit':'Original Dispatcharr and dependency authors retain all application/algorithm credit.','contracts':contracts,'fixtures':fixtures,'new_assignments':1,'supplement_existing':1,'Source_observations':12,'method_aliases_added':0,'new_Source_HTTP_execution':0,'fully_verified_added':0}
def validate(root,value,bindings=None):
 assert value==generate(root),'exact Source12 assignments and actual resolver custody'
 return {'new_assignments':1,'supplement_existing':1,'Source_observations':12,'fully_verified_added':0}
def restore_pre_Source12(rows):
 root=Path(__file__).resolve().parents[2];packet=generate(root);proof=checked(root,BASE+'preservation.json',inventory(root)['preservation.json']);selected={c['contract_id']:c for c in packet['contracts']}
 for route in rows:
  for op in route['operations']:
   key=op['contract_id']
   if key not in selected:continue
   c=selected[key]
   if c['method']is None:
    captures=op.pop('supplemental_executable_Source_captures');assert captures==[{'definition':c,'setup_fixture':packet['fixtures'][c['fixture_id']],'input':{'path':INPUT,'sha256':sha((root/INPUT).read_bytes()),'kind':KIND}}];assert digest(op)==proof['existing_supplement_target_whole_dict_sha256']
   else:
    prior=proof['new_selected_original_unassigned_dict'];expected=copy.deepcopy(prior);expected.update(setup_fixture=packet['fixtures'][c['fixture_id']],assertions=c['assertions'],result=c['result'],executable_Source_contract=c,coverage_state='partial_executable_Source_contract');expected['role_matrix'].update(observed_fixture_definitions={},complete=False,state='partial_Original_real_HTTP_capture_assignment',observed_roles=[]);assert op==expected;op.clear();op.update(copy.deepcopy(prior))
 return rows
