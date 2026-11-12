"""Three exact partial Original Source48 detail-method assignments.
Original Dispatcharr/dependency authors retain application/algorithm credit.
Real-resolver forced-active-actor framework capture, NOT suite/TCP/Native parity.
"""
import copy,hashlib,importlib.util,json,re
from pathlib import Path
PREFIX='scripts/rust-contracts/notification-mutations-source/'
INPUT='scripts/rust-contracts/notification-mutation-bindings.json'
KIND='retained_original_notification_mutations_Source48_framework_capture'
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
RAW='68542935c46471505dfbc9e1442e45fa9cbe498ef2c587fe0c257fb2ebd8b1bd'
ROUTE='route-06ad6f49ab1bfa0a7ea3'
PATTERN='api/core/^notifications/(?P<pk>[^/.]+)/$'
INDICES={'PUT':[0,1,2,15,30,44],'PATCH':[3,4,5,9,10,12,13,14,16,17,18,19,20,21,22,23,24,25,26,27,28,29,31,32,33,34,35,36,37,38,39,43,46,47],'DELETE':[6,7,8,11,40,41,42,45]}
ACTIONS={'PUT':'update','PATCH':'partial_update','DELETE':'destroy'}
# This explicit projection is not a replacement for the exact raw case authority.
# The full case canonical SHA binds omitted process/runtime detail too.
FIELDS=('index','name','role','method','path','target_id','request_payload','request_body_encoding','resolver','client_raise_request_exception','transport_kind','actual_status','response_headers','body_hex','body_sha256','body_bytes','json','framework_exception','began_utc','completed_utc','before','after','before_named_audit','after_named_audit','before_cache','after_cache','before_condition_cache','after_condition_cache','capture_complete')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def read(root,p):return json.loads((Path(root)/p).read_text())
def ref(root,p):return {'path':p,'sha256':sha(Path(root)/p)}
def dependencies():
 root=Path(__file__).resolve().parents[2]
 return [INPUT,'scripts/rust-contracts/notification-mutation-preservation.json','scripts/rust-contracts/notification-mutations-source-review.json']+[str(p.relative_to(root))for p in sorted((root/PREFIX).rglob('*'))if p.is_file()and'__pycache__'not in p.parts]
def packet(root):
 p=Path(root)/PREFIX/'validate.py';spec=importlib.util.spec_from_file_location('_notification_mutations_Source48_validator',p);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def generate(root,registry=None):
 root=Path(root);validator=packet(root);proof=validator.validate(root/PREFIX);actual=validator.load_capture(root/PREFIX)
 assert proof['Source2_requests']==48 and proof['recomputed_effect_checks']==1449 and proof['Source2_shell_and_controller_exit']==0 and proof['Source1_prefailure_exit_preserved']==1
 assert proof['Source_reruns']==proof['Native_runs']==proof['operation_registrations']==0 and proof['whole_endpoint_TCP_or_credential_or_performance_acceptance']is False
 assert proof['raw_Source2_capture_sha256']==RAW and actual['capture_valid']is True and actual['actual_request_count']==48 and actual['forced_auth_framework_only']is True and actual['credentials_or_TCP_verified']is False
 review=read(root,'scripts/rust-contracts/notification-mutations-source-review.json');assert review['passed']is True and review['Root_recomputed_effect_checks']==1449 and review['raw_Source2_capture_sha256']==RAW and review['Source1_negative_preserved']is True and review['operation_assignments_added']==0
 execution=read(root,PREFIX+'raw/Source2/execution.json');assert execution['passed']is True and execution['controller_exit']==execution['Source_process_result']['exit']==0 and execution['actual_request_count']==48
 registry=registry or read(root,'docs/rust-contract-register.json');index={o['contract_id']:(r,o)for r in registry['runtime_routes']for o in r['operations']}
 users={u['username'].split('_')[1]:u for u in actual['seed_users']};assert set(users)=={'admin','standard','adminDenied'}
 roles={'anonymous':{'actor':'APIClient without force_authenticate','credential_backend_bypassed':False,'observed_denial_status':401}}
 for name,user in users.items():
  assert user['is_active']is True and user['is_superuser']is False
  roles[name]={'actor':'persisted owned active actor passed to APIClient.force_authenticate','credential_backend_bypassed':True,'actual_actor':{k:copy.deepcopy(user[k])for k in ['id','username','is_active','is_superuser','user_level','custom_properties','date_joined']},'raw_actor_sha256':digest(user),'raw_actor_pointer':'/seed_users/'+str(actual['seed_users'].index(user))}
 assert roles['admin']['actual_actor']['user_level']==roles['adminDenied']['actual_actor']['user_level']==10 and roles['standard']['actual_actor']['user_level']==1
 fixture_id='retained-Original-notification-mutations-Source2-48'
 fixture={'id':fixture_id,'baseline_revision':REV,'baseline_image':'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f','evidence_kind':KIND,'scope':'Real resolver Django APIClient forced-active actors and raw framework exceptions; no full suite, TCP/WSGI, credential acceptance, Native or whole endpoint coverage','roles':roles,'seed':{'actual_aliases':actual['seed_aliases'],'actual_notification_dismissal_rows':actual['seed_rows'],'actual_actors':{k:v for k,v in roles.items()if k!='anonymous'},'seed_binding':actual['seed_binding'],'raw_seed_audit_sha256':digest(actual['seed_audit']),'raw_cache_after_setup_sha256':digest(actual['cache_after_setup'])},'ownership':{'actual_owned_database':actual['database'],'owned_copied_database_only':True,'default_named_tables':11,'default_named_sequences':11,'both_Source_attempts_default11_exact_and_owned_CID_PID_cleanup':True},'setup':{'plan':ref(root,PREFIX+'executed/executor3/inputs/plan.json'),'controller':ref(root,PREFIX+'executed/executor3/controller/run.py'),'fixture':ref(root,PREFIX+'executed/executor3/controller/source_fixture.py'),'exact_executor3_seal':ref(root,PREFIX+'executed/executor3/seal.json'),'materializer':ref(root,PREFIX+'materialize_inputs.py'),'exact_input_count':44,'reconstruct_argv':['python3',PREFIX+'materialize_inputs.py','--repo','<local repository containing pinned Original>','--destination','<new owned input directory>','--executor','3'],'future_replay_requires_new_separate_authorization':True,'new_Source_runs_this_mapping':0},'assertions':{'whole_sequential_fixture_shared_checks':1449,'checks_are_not_repeated_or_summed_per_method':True,'unchanged_executed_effects':ref(root,PREFIX+'executed/executor3/controller/effects.py'),'actual_effect_projection':ref(root,PREFIX+'raw/Source2/effect-validation.json')},'execution_result':{'actual_Source2_shell_exit':0,'actual_Source2_controller_exit':0,'actual_Source2_execution':ref(root,PREFIX+'raw/Source2/execution.json'),'raw_Source2_gzip':ref(root,PREFIX+'raw/Source2/notification-source-private.json.gz'),'raw_Source2_uncompressed_sha256':RAW,'raw_capture_passed_field_preserved':actual['passed'],'raw_effect_qualification_field_preserved':actual['effect_qualification'],'offline_1449_effects_passed':True,'Source1_preseed_negative_exit':1,'Source1_negative_execution':ref(root,PREFIX+'raw/Source1/execution.json'),'independent_Source2_cleanup':ref(root,PREFIX+'raw/Source2/independent-cleanup-query-receipt.json'),'Root_review':ref(root,'scripts/rust-contracts/notification-mutations-source-review.json'),'new_Source_runs':0}}
 contracts=[]
 for method,numbers in INDICES.items():
  identifier=ROUTE+':'+method;route,op=index[identifier];action=ACTIONS[method]
  assert route['pattern_exact']==PATTERN and route['source_handler']=='core.api_views.SystemNotificationViewSet' and route['resolver_ordinal']==244
  assert op['method']==method and op['source_action']==action and op['owning_milestone']=='M5' and op['fully_verified']is False
  assert [r['index']for r in actual['requests']if r['method']==method]==numbers
  cases=[]
  for i in numbers:
   row=actual['requests'][i];assert row['index']==i and row['method']==method and row['capture_complete']is True and row['transport_kind']=='http_response'
   assert row['resolver']['class']==route['source_handler'] and row['resolver']['actions'][method.lower()]==action and row['resolver']['url_name']=='systemnotification-detail' and row['resolver']['namespaces']==['api','core']
   assert re.fullmatch(PATTERN.replace('/^','/'),row['path'].lstrip('/'))
   assert row['resolver']['kwargs']=={'pk':row['path'].rstrip('/').rsplit('/',1)[-1]}
   raw=bytes.fromhex(row['body_hex']);assert len(raw)==row['body_bytes']and hashlib.sha256(raw).hexdigest()==row['body_sha256']
   cases.append({'source_index':i,'label':row['name'],'method':method,'path':row['path'],'role':row['role'],'Source_authority':{'gzip':ref(root,PREFIX+'raw/Source2/notification-source-private.json.gz'),'uncompressed_sha256':RAW,'json_pointer':'/requests/'+str(i),'full_record_canonical_sha256':digest(row)},'actual_framework_observation':{k:copy.deepcopy(row[k])for k in FIELDS},'projection_policy':'Explicit response/request/resolver/SQL/cache/clock fields retained; omitted raw runtime/process fields stay bound by full-record SHA and untouched raw capture; no field normalization','assertion_scope':'Exact immutable whole-fixture1449 checks, shared sequential context; not an independent method execution'})
  observed=sorted({c['role']for c in cases})
  contracts.append({'contract_id':identifier,'pattern_exact':PATTERN,'source_handler':route['source_handler'],'method':method,'source_action':action,'owning_milestone':'M5','fixture_id':fixture_id,'observed_roles':observed,'source_role_specs':{role:copy.deepcopy(roles[role])for role in observed},'cases':cases,'assertions':[{'kind':'exact_actual_resolver_method_action_and_alias','resolver_ordinal':244,'case_indices':numbers},{'kind':'actual_request_raw_response_SQL_cache_own_clocks','case_indices':numbers},{'kind':'whole_sequential_fixture_shared1449_effect_checks','reference':ref(root,PREFIX+'executed/executor3/controller/effects.py'),'not_per_method_check_count':True},{'kind':'actual_Source2_exit0_with_Source1_negative_separately_preserved','Source2_execution':ref(root,PREFIX+'raw/Source2/execution.json'),'Root_review':ref(root,'scripts/rust-contracts/notification-mutations-source-review.json')}],'result':{'state':'retained_Original_Source48_framework_effects_passed_Source2_exit0','historical_captured_cases':len(cases),'unique_fixture_observations':48,'whole_fixture_shared_effect_checks':1449,'new_Source_cases_this_task':0,'new_Source_runs_this_mapping':0,'Source2_shell_exit':0,'Source2_controller_exit':0,'Source1_preseed_negative_exit_preserved':1,'fully_verified':False},'remaining':['Credentials/backend and deployed TCP/WSGI/browser behavior are not captured here','Finite forced-active-actor cases do not close whole role/input matrix or serializer grammar','No format/no-slash aliases, other methods, live signal effects, broker/WebSocket delivery or concurrency inferred','No Native/performance/full endpoint/M0/M1/milestone acceptance'],'fully_verified':False})
 assert sorted(case['source_index']for contract in contracts for case in contract['cases'])==list(range(48))
 return {'format_version':1,'binding_kind':KIND,'Original_revision':REV,'operation_count':3,'unique_actual_Source_observations':48,'method_case_counts':{k:len(v)for k,v in INDICES.items()},'new_Source_executions':0,'Source2_shell_and_controller_exit':0,'Source1_negative_preserved':True,'fully_verified_added':0,'Native_coverage_increased':False,'fixtures':{fixture_id:fixture},'contracts':contracts,'non_HTTP_contracts':[]}

def validate(root,value,bindings=None):
 assert value==generate(root),'exact Source48 method/resolver/actor/body/seed/SQL/cache/clock/assertion/result binding'
 return {'passed':True,'operations':3,'retained_actual_Source_cases':48,'shared_fixture_effect_checks':1449,'Source2_exit':0,'new_Source_runs':0,'fully_verified_added':0}
if __name__=='__main__':
 import argparse
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--write-bindings',action='store_true');a=p.parse_args();root=Path(__file__).resolve().parents[2];value=generate(root)
 if a.write_bindings:(root/INPUT).write_text(json.dumps(value,indent=2)+'\n')
 else:validate(root,read(root,INPUT))
 print(json.dumps({'passed':True,'operations':3,'retained_Source48':48,'Source_runs':0,'wrote_bindings':a.write_bindings}))
