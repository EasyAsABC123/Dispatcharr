"""Exact retained correct-route Original framework capture, not a fresh run.
Original Dispatcharr contributors retain application/algorithm credit/licensing.
Only actual resolved GET-list/POST-create/POST-dismiss observations are assigned.
"""
import copy,hashlib,importlib.util,json,re,sys
from pathlib import Path
PREFIX='scripts/rust-contracts/notifications-source/'
INPUT='scripts/rust-contracts/notification-source-bindings.json'
IDENTITIES={
 'route-025c0d74fa99218d4728:GET':{'pattern_exact':'api/core/^notifications/$','source_handler':'core.api_views.SystemNotificationViewSet','method':'GET','source_action':'list','owning_milestone':'M5','case_indices':[0,1,5,6]},
 'route-025c0d74fa99218d4728:POST':{'pattern_exact':'api/core/^notifications/$','source_handler':'core.api_views.SystemNotificationViewSet','method':'POST','source_action':'create','owning_milestone':'M5','case_indices':[2,7]},
 'route-1577c7cc68b36d7ee6f4:POST':{'pattern_exact':'api/core/^notifications/(?P<pk>[^/.]+)/dismiss/$','source_handler':'core.api_views.SystemNotificationViewSet','method':'POST','source_action':'dismiss','owning_milestone':'M5','case_indices':[3,4]},
}
CASE_NAMES=('anonymous-list','standard-list','standard-create-denied','standard-dismiss','standard-dismiss-repeat','standard-list-after-dismiss','admin-list-independent','admin-create-positive')
SEAL='2004c823a728d3ecdb7f7750867b0b0d3385fdac5779b4a074afb899b892ba59'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(root,name):return json.loads((Path(root)/name).read_text())
def reference(root,name):return {'path':name,'sha256':sha(Path(root)/name)}
def packet_modules(root):
    folder=Path(root)/PREFIX;sys.path.insert(0,str(folder));saved={key:sys.modules.pop(key,None)for key in('effects','fixture_guards')}
    try:
        modules=[]
        for filename in('validate.py','materialize_inputs.py'):
            spec=importlib.util.spec_from_file_location('_notification_source_'+filename.replace('.','_'),folder/filename);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);modules.append(module)
        return modules
    finally:
        sys.path.remove(str(folder))
        for key,value in saved.items():
            sys.modules.pop(key,None)
            if value is not None:sys.modules[key]=value

def dependencies():
    root=Path(__file__).resolve().parents[2];folder=root/PREFIX
    # Every public packet byte used by closure, Source/URL definitions and raw
    # capture/effect/negative/cleanup validation belongs to the portable closure.
    return [INPUT]+[str(p.relative_to(root))for p in sorted(folder.rglob('*'))if p.is_file()and'__pycache__'not in p.parts]

def generate(root,registry=None):
    root=Path(root);registry=registry or read(root,'docs/rust-contract-register.json');packet,closure=packet_modules(root);proof=packet.validate(root/PREFIX);seal=closure.validate(root/PREFIX)
    assert proof['passed']is True and proof['requests']==8 and proof['effect_checks']==213 and proof['controller_exit_preserved']==1 and proof['registered_operations']==0
    assert len(seal['files'])==29 and sha(root/(PREFIX+'raw/executed-seal.json'))==SEAL
    reconstruction=read(root,PREFIX+'closure-proof.json');assert reconstruction['reconstructed_exact_original_inputs']==29 and reconstruction['Source_reruns']==0
    assert reconstruction['materialization']['archive_sha256']==ARCHIVE and reconstruction['materialization']['Source_application_or_request_executed']is False
    assert reconstruction['original_controller_readonly']['Source_executed']is False and reconstruction['original_controller_readonly']['requests_executed']==0
    observed=read(root,PREFIX+'raw/source-observations.json');assert tuple(c['name']for c in observed['requests'])==CASE_NAMES
    assert [c['actual_status']for c in observed['requests']]==[401,200,403,200,200,200,200,201]
    assert observed['passed']is False and observed['capture_valid']is True and observed['effect_qualification']=='PENDING_OFFLINE_VALIDATOR'
    index={o['contract_id']:(r,o)for r in registry['runtime_routes']for o in r['operations']}
    roles={'anonymous':{'actor':'APIClient without force_authenticate','credential_backend_bypassed':False,'authentication_result_observed':401},'standard':{'actor':'real owned user with force_authenticate','user':observed['seed_users'][0],'credential_backend_bypassed':True,'Source_user_level':1},'admin':{'actor':'real owned user with force_authenticate','user':observed['seed_users'][1],'credential_backend_bypassed':True,'Source_user_level':10}}
    assert roles['standard']['user']['user_level']==1 and roles['admin']['user']['user_level']==10 and all(roles[k]['user']['is_active']and not roles[k]['user']['is_superuser']for k in('standard','admin'))
    fixture_id='retained-notification-correct-resolver-Source8'
    fixture={'id':fixture_id,'baseline_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','baseline_image':'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f','scope':'Original management-shell real-resolver APIClient, forced users; not HTTP TCP or credential/backend/production-startup acceptance','ownership':{'actual_owned_database':observed['database'],'guarded_owned_copy_only':True,'default_named_tables':11,'default_named_sequences':11,'default_unchanged_and_cleanup_independently_recovered':True},'roles':roles,'seed':{'actual_notification_and_dismissal_rows':observed['seed_rows'],'actual_seed_users':observed['seed_users'],'actual_network_settings':observed['network_access_settings_observed'],'network_policy_modified':False},'setup':{'executed_controller':reference(root,PREFIX+'executed/controller/run.py'),'executed_fixture':reference(root,PREFIX+'executed/controller/source_fixture.py'),'executed_bootstrap':reference(root,PREFIX+'executed/controller/start.sh'),'exact_executed_seal':reference(root,PREFIX+'raw/executed-seal.json'),'sealed_inputs':29,'materializer':reference(root,PREFIX+'materialize_inputs.py'),'archive_recipe':{'pinned_git_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','bytes':16609280,'sha256':ARCHIVE,'Source_execution_required_for_materialization':False},'materialize_argv':['python3',PREFIX+'materialize_inputs.py','--repo','<local repository containing pinned Original>','--destination','<new owned input directory>'],'future_Source_entrypoint':['python3','<materialized>/controller/run.py','--execute-reviewed-seal',SEAL,'--output','<new owned output>'],'future_Source_replay_requires_separate_authorization':True,'mapping_task_Source_runs':0},'assertion_definition':{'comparer':reference(root,PREFIX+'executed/controller/effects.py'),'pure_guards':reference(root,PREFIX+'executed/controller/fixture_guards.py'),'actual_whole_fixture_effect_checks':213},'execution_result':{'actual_observations':8,'Source_controller_passed':False,'Source_controller_exit':1,'Source_controller_cleanup_error':'ExactOwnedContainerAbsent (Docker capitalization)','offline_effect_comparison_passed':True,'independent_cleanup_recovered':True,'raw_observations':reference(root,PREFIX+'raw/source-observations.json'),'negative_controller':reference(root,PREFIX+'raw/controller-execution.json'),'actual_effects':reference(root,PREFIX+'raw/executed-effect-validation.json'),'recovered_cleanup':reference(root,PREFIX+'raw/execution-recovery.json'),'new_Source_runs_for_mapping':0}}
    contracts=[]
    for identifier,identity in IDENTITIES.items():
        route,operation=index[identifier]
        assert {k:route[k]for k in('pattern_exact','source_handler')}=={k:identity[k]for k in('pattern_exact','source_handler')}
        assert all(operation[k]==identity[k]for k in('method','source_action','owning_milestone'))and operation['fully_verified']is False
        cases=[]
        for number in identity['case_indices']:
            captured=observed['requests'][number];assert captured['method']==identity['method']and captured['resolver_action']==identity['source_action']and captured['follow_redirects']is False
            assert re.fullmatch(identity['pattern_exact'].replace('/^','/'),captured['path'].lstrip('/')),'actual exact route, never format/wrong-path alias'
            cases.append({'source_index':number,'label':captured['name'],'method':captured['method'],'path':captured['path'],'role':captured['role'],'Source_authority':{'path':PREFIX+'raw/source-observations.json','sha256':sha(root/(PREFIX+'raw/source-observations.json')),'json_pointer':'/requests/'+str(number)},'actual_observation':copy.deepcopy(captured),'assertion_scope':'Per-request response/SQL/cache/clock capture plus exact whole-fixture213 effect checks; retained sequential context, not a new independent execution'})
        observed_roles=sorted({c['role']for c in cases})
        contracts.append({'contract_id':identifier,**{k:identity[k]for k in('pattern_exact','source_handler','method','source_action','owning_milestone')},'fixture_id':fixture_id,'observed_roles':observed_roles,'source_role_specs':{k:copy.deepcopy(roles[k])for k in observed_roles},'cases':cases,'assertions':[{'kind':'actual_resolver_and_method','action':identity['source_action'],'exact_pattern':identity['pattern_exact']},{'kind':'actual_response_bytes_status_JSON_SQL_sequences_cache_clocks','case_indices':identity['case_indices']},{'kind':'whole_unchanged_executed_effect_definition','checks':213,'reference':reference(root,PREFIX+'executed/controller/effects.py')},{'kind':'separate_negative_execution_and_recovered_cleanup','controller_exit':1,'default11_audits_exact':True,'cleanup_recovered':True}],'result':{'state':'retained_Original_framework_capture_effects_passed_controller_exit1_cleanup_recovered','historical_captured_cases':len(cases),'unique_fixture_observations':8,'new_Source_cases_this_task':0,'new_Source_runs_this_mapping':0,'Source_controller_passed':False,'Source_controller_exit':1,'offline_effect_comparison_passed':True,'offline_whole_fixture_effect_checks':213,'independent_cleanup_recovered':True,'fully_verified':False},'remaining':['other role slices and credentials/backend authentication','HTTP TCP/browser and production startup','other CRUD/actions/format aliases and developer/expiry/admin-only branches','full role matrix/endpoint/Native/performance/M0/M1 acceptance'],'fully_verified':False})
    return {'format_version':1,'binding_kind':'retained_Original_real_resolver_framework_capture','Original_revision':fixture['baseline_revision'],'operation_count':3,'unique_actual_Source_observations':8,'new_Source_executions':0,'Source_controller_exit_preserved':1,'fully_verified_added':0,'Native_coverage_increased':False,'fixtures':{fixture_id:fixture},'contracts':contracts,'non_HTTP_contracts':[]}

def validate(root,value,bindings=None):
    assert value==generate(root),'exact bounded retained notification Source setup/assertion/result/actor/route/observation binding'
    return {'passed':True,'operations':3,'actual_retained_observations':8,'Source_effect_checks':213,'new_Source_runs':0,'fully_verified_added':0}
if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--write-bindings',action='store_true');args=parser.parse_args();root=Path(__file__).resolve().parents[2];value=generate(root)
    if args.write_bindings:(root/INPUT).write_text(json.dumps(value,indent=2)+'\n')
    else:validate(root,read(root,INPUT))
    print(json.dumps({'passed':True,'operations':3,'retained_actual_Source_cases':8,'Source_runs':0,'wrote_bindings':args.write_bindings}))
