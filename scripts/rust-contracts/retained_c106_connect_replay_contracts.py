"""Bind the exact Original Connect methods proven in the Candidate106 replay."""
import re

from source_contracts import checked

KIND = 'retained_original_C106_Connect_exact_canonical_and_JSON_HTTP'
INPUT = 'scripts/rust-contracts/retained-c106-connect-replay/bindings.json'
INPUT_SHA = 'b71c2547db4de3ffd263b9733869f7d2eeed5acff0aac843232e96db8d9bb16a'
RECEIPT = 'scripts/rust-integration/milestone-batch106/connect-replay-register.json'
RECEIPT_SHA = '6d2c4d77ee3d26f1908af621e2236fb53866e9ee665c4c323687c010046a67b5'
FIXTURE = 'retained-C106-Connect-exact-image-replay'
SOURCE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
SOURCE_IMAGE = 'sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
NATIVE = 'd58a400ad8bbec4fa7fc7546dca8525430afa47c'
NATIVE_IMAGE = 'sha256:3d200c0a1334fa5fec7927d7978441ec475d3e8a9254ffc7479f82608ea6a895'
SEAL = 'f9a3549115c54101ae0905eaa549a2fd78f748f8b34a7773915915f8a0c13ea1'
PLAN = 'd62d1628cc6f6360e4acfb554bf1055516c6c029145f96b717e0fa3801122cbb'
HISTORICAL = '37c4bb0fcb526d7f7d89e145005ca30c42a2a6215a004ecff949387c4c802700'
BUILD = '600ef2153fe473e7f91922a8b8b893ef9334ff4d1505f696a9ef98f3d47c69bf'
BODY = 'fd62a53afdd49595ccd111b6ac06466a1690e7c8da4aeb0e884b5e0f4e937624'
EMPTY_BODY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
ROLES = ('admin_api_key','admin_jwt','anonymous','inactive_admin','standard','viewer')
DENIAL = {'anonymous':401,'inactive_admin':401,'standard':403,'viewer':403}
INTEGRATION = '/api/connect/integrations/{integration}/'
SUBSCRIPTIONS = '/api/connect/subscriptions/'
DETAIL = SUBSCRIPTIONS+'{subscription}/'
DECLARATIONS = (
    ('integration-get',334,'GET',INTEGRATION,'retrieve','IntegrationViewSet','integration-detail',200),
    ('nested-subscriptions',336,'GET',INTEGRATION+'subscriptions/','list_subscriptions','IntegrationViewSet','integration-list-subscriptions',200),
    ('subscriptions-list',342,'GET',SUBSCRIPTIONS,'list','EventSubscriptionViewSet','subscription-list',200),
    ('subscription-detail',344,'GET',DETAIL,'retrieve','EventSubscriptionViewSet','subscription-detail',200),
    ('log-detail',348,'GET','/api/connect/logs/{log}/','retrieve','DeliveryLogViewSet','delivery-log-detail',200),
    ('integration-patch',334,'PATCH',INTEGRATION,'partial_update','IntegrationViewSet','integration-detail',200),
    ('subscription-create',342,'POST',SUBSCRIPTIONS,'create','EventSubscriptionViewSet','subscription-list',201),
    ('subscription-put',344,'PUT',DETAIL,'update','EventSubscriptionViewSet','subscription-detail',200),
    ('subscription-patch',344,'PATCH',DETAIL,'partial_update','EventSubscriptionViewSet','subscription-detail',200),
    ('subscription-delete',344,'DELETE',DETAIL,'destroy','EventSubscriptionViewSet','subscription-detail',204),
)


def dependencies():
    return {INPUT,RECEIPT}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def qualification(receipt):
    assert receipt['scope']=='retained_C106_exact_Connect_JSON_and_canonical_replay'
    assert (receipt['source_revision'],receipt['source_image'],receipt['candidate_revision'],receipt['native_image'])==(
        SOURCE,SOURCE_IMAGE,NATIVE,NATIVE_IMAGE)
    assert (receipt['fixture_sha256'],receipt['matrix_plan_sha256'],receipt['historical_projection_sha256'],
            receipt['build_manifest_sha256'])==(SEAL,PLAN,HISTORICAL,BUILD)
    assert (receipt['outer_actual_exit'],receipt['inner_actual_exit'],receipt['historical_actual_exit'])==(0,0,1)
    assert all(receipt[field] is True for field in ('settled_output_and_process_group','exact_owned_cleanup',
                'all_phases_exact_sql_and_sequences','zero_owned_delivery'))
    assert receipt['owned_roles_retired']==['baseline','candidate','data-init','origin']
    assert (receipt['matched_pairs'],receipt['physical_http_cells'],receipt['scoped_legacy_error_pairs'])==(120,240,2)
    assert len(receipt['operations'])==20 and len({row['contract_id'] for row in receipt['operations']})==20
    assert all(receipt[field] is False for field in ('whole_operation_verified','performance_verified','milestone_accepted'))
    assert set(receipt['raw_fingerprints'])=={'binding.json','receipt.json','execution.json','execution.stdout',
          'execution.stderr','independent-cleanup.json','evidence/integration.json',
          'evidence/owned-before-compose-down.json','outer-state.json','outer-replay.log'}
    assert all(re.fullmatch('[0-9a-f]{64}',value) for value in receipt['raw_fingerprints'].values())


def observed(row, declaration, variant):
    key,bare,method,path,action,handler,name,positive = declaration
    ordinal,literal = (bare,path) if variant=='bare' else (bare+1,path.removesuffix('/')+'.json')
    assert (row['key'],row['variant'],row['resolver_ordinal'],row['method'],row['name'],row['path_template'])==(
        key,variant,ordinal,method,'api:connect:'+name,literal)
    assert [cell['role'] for cell in row['role_cases']]==list(ROLES)
    accepted = []
    for cell in row['role_cases']:
        role = cell['role']
        expected = DENIAL.get(role,positive)
        legacy = ordinal==337 and role not in DENIAL
        status = 500 if legacy else expected
        source,native = cell['Source'],cell['Native']
        for observation in (source,native):
            assert (observation['role'],observation['actual_status'],observation['expected_scoped_status'],
                    observation['expected_stock_status'])==(role,status,status,expected)
            assert observation['exact_observed_legacy_error_match'] is legacy
            assert observation['owned_sql_rows_match'] is observation['scope_response_match'] is True
            assert observation['authentication_challenge_present'] is (status==401)
            assert re.fullmatch('[0-9a-f]{64}',observation['raw_response_sha256'])
            if legacy:
                assert observation['mime']=='text/html' and observation['raw_response_sha256']==BODY
            elif status==204:
                assert observation['mime']=='' and observation['raw_response_sha256']==EMPTY_BODY
            else:
                assert observation['mime']=='application/json'
            if role in DENIAL:
                assert re.fullmatch('[0-9a-f]{64}',observation['denial_json_sha256'])
            else:
                assert observation['denial_json_sha256'] is None
        assert all(source[field]==native[field] for field in ('actual_status','mime',
                       'authentication_challenge_present','denial_json_sha256'))
        accepted.append({'method':method,'literal_path_template':literal,**source})
    return ordinal,method,action,'apps.connect.api_views.'+handler,accepted


def build(receipt):
    qualification(receipt)
    contracts = []
    selected = [(declaration,variant) for declaration in DECLARATIONS for variant in ('bare','json')]
    for row,(declaration,variant) in zip(receipt['operations'],selected,strict=True):
        ordinal,method,action,handler,cases = observed(row,declaration,variant)
        assertions = ['Matched selected request, status, authorization challenge and response contract.',
                      'Exact scoped SQL transitions and sequences, unrelated rows preserved and zero owned delivery.']
        if ordinal==337:
            assertions.append('The two authorized formatted nested reads retain the exact observed legacy HTML 500. All four denied cells keep their ordinary statuses.')
        contracts.append({
            'contract_id':row['contract_id'],'fixture_id':FIXTURE,'resolver_ordinal':ordinal,
            'resolver_name':row['name'],'pattern_exact':row['pattern_exact'],'source_handler':handler,
            'owning_milestone':'M5','method':method,'source_action':action,'observed_methods':[method],
            'observed_roles':list(ROLES),'source_role_specs':{
                role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in ROLES},
            'assertions':assertions,'fully_verified':False,'cases':cases,
            'result':{'Source_case_observations':6,'new_Source_cases_this_task':0,
                'Native_coverage_increased':False,'paired_representative_scope_verified':True,
                'known_legacy_error_cells':2 if ordinal==337 else 0},
        })
    fixture = {'id':FIXTURE,'source_revision':SOURCE,'source_image':SOURCE_IMAGE,'paired_candidate_revision':NATIVE,
               'paired_candidate_image':NATIVE_IMAGE,'actual_controller_exit':0,'historical_actual_exit':1,
               'exact_owned_namespace_cleanup':True,'public_artifacts':[{'path':RECEIPT,'sha256':RECEIPT_SHA}]}
    return {'binding_kind':KIND,'operation_count':len(contracts),'fully_verified_operations':0,
            'Native_coverage_increased':False,'performance_verified':False,'credit':receipt['credit'],
            'fixtures':{FIXTURE:fixture},'contracts':contracts}


def generate(receipt,register):
    result = build(receipt)
    previous = {op['contract_id']:(route,op) for route in register['runtime_routes'] for op in route['operations']}
    for row in result['contracts']:
        route,op = previous[row['contract_id']]
        assert 'executable_Source_contract' not in op
        assert (route['resolver_ordinal'],route['name'],route['pattern_exact'],route['source_handler'])==tuple(
            row[key] for key in ('resolver_ordinal','resolver_name','pattern_exact','source_handler'))
        assert all(op[key]==row[key] for key in ('owning_milestone','method','source_action'))
    return result


def validate(root,packet):
    assert packet==load(root)
    assert packet==build(checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA}))


def restore_pre_c106_replay(root,rows):
    selected = {row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            key = operation['contract_id']
            if key in selected:
                assert operation.pop('executable_Source_contract')==selected[key] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}
