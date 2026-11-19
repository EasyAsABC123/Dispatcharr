"""Validate retained Original Core JSON-suffix HTTP evidence."""
from collections import Counter
import re

from source_contracts import checked

KIND = 'retained_original_Core_profile_literal_JSON_HTTP'
INPUT = 'scripts/rust-contracts/retained-core-profile-json/bindings.json'
INPUT_SHA = '1419d535cff3043830de20f24c953083ed86181f441b2c2d0f210657500b182c'
RECEIPT = 'scripts/rust-integration/retained-core-profile-json/receipt.json'
RECEIPT_SHA = '117e1fb0428056285d483b7adbbb25a48d415da7d21f98d0f0aa935d546d575a'
FIXTURE = 'retained-Candidate105-core-profile-literal-json'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
CANDIDATE = 'fc2c81c2d52bbc17299d6900534d9564432d8e89'
ROLES = ('admin_api_key','admin_jwt','anonymous','inactive_admin','standard','viewer')
OPERATIONS = (
    (221,'GET','useragents','list',200),(221,'POST','useragents','create',201),
    (223,'GET','useragents','retrieve',200),(223,'PATCH','useragents','partial_update',200),
    (223,'PUT','useragents','update',200),(223,'DELETE','useragents','destroy',204),
    (225,'GET','streamprofiles','list',200),(225,'POST','streamprofiles','create',201),
    (227,'GET','streamprofiles','retrieve',200),(227,'PUT','streamprofiles','update',200),
    (229,'GET','outputprofiles','list',200),(229,'POST','outputprofiles','create',201),
    (231,'GET','outputprofiles','retrieve',200),
)
HANDLERS = {'useragents':'core.api_views.UserAgentViewSet','streamprofiles':'core.api_views.StreamProfileViewSet',
            'outputprofiles':'core.api_views.OutputProfileViewSet'}


def dependencies():
    return {INPUT,RECEIPT}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def fixture(receipt):
    return {'id':FIXTURE,'source_revision':ORIGINAL,'paired_candidate_revision':CANDIDATE,
            'paired_image':receipt['candidate_image'],'actual_controller_exit':1,
            'Source_78_selected_cases_matched':True,'Native_78_literal_404':True,
            'exact_owned_namespace_cleanup':True,'public_artifacts':[{'path':RECEIPT,'sha256':RECEIPT_SHA}]}


def status(method, role, authorized):
    return 401 if role in ('anonymous','inactive_admin') else (
        403 if role=='viewer' or (role=='standard' and method!='GET') else authorized)


def case_rows(receipt, ordinal, method, family, action, authorized):
    selected = [row for row in receipt['rows'] if (row['resolver_ordinal'],row['method'])==(ordinal,method)]
    assert [row['role'] for row in selected]==list(ROLES)
    detail = action not in ('list','create')
    literal = '/api/core/'+family+('/{owned_pk}' if detail else '')+'.json'
    for row in selected:
        role = row['role']
        assert row['literal_request_template']==literal
        for side in ('Source','Native'):
            result=row[side]
            assert (result['method'],result['resolver_ordinal'],result['role'],result['path_template'])==(method,ordinal,role,literal)
            assert result['expected_status']==status(method,role,authorized) and result['transport_error'] is None
            assert re.fullmatch('[0-9a-f]{64}',result['response_sha256'])
        source, native = row['Source'],row['Native']
        assert source['status']==status(method,role,authorized)
        assert source['selected_response_match'] is source['sql_and_cache_match'] is True
        assert native['status']==404 and native['challenge_present'] is native['selected_response_match'] is False
    return [dict(row['Source'],case=row['case']) for row in selected]


def assertion(family, method):
    if method=='GET':
        return ['Exact selected JSON fields and owned collection membership or detail, with no read or denial writes.']
    text=['Accepted Administrator JWT and API-key writes match the owned SQL row transition, with unrelated rows preserved.']
    if family=='useragents':
        text.append('Accepted UserAgent writes invalidate default value and advance the cache generation.')
        if method=='DELETE':
            text.append('Deletion clears the deliberately linked StreamProfile foreign key.')
    else:
        text.append('Writes preserve both UserAgent cache observations.')
    return text


def validate(root,packet):
    assert packet==load(root)
    receipt = checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA})
    assert receipt['source_revision']==ORIGINAL and receipt['candidate_revision']==CANDIDATE
    assert receipt['actual_controller_exit']==1 and receipt['settled_process_group_and_EOF'] and receipt['exact_owned_cleanup']
    assert receipt['owned_container_generations']==4 and receipt['source_matched_cases']==78 and receipt['native_matched_cases']==0
    assert receipt['native_status_counts']=={'404':78} and len(receipt['rows'])==78 and receipt['fully_verified_operations']==0
    assert {row['Native']['status'] for row in receipt['rows']}=={404}
    assert len({row['Native']['response_sha256'] for row in receipt['rows']})==1
    assert packet['binding_kind']==KIND and packet['operation_count']==len(packet['contracts'])==13
    assert packet['fully_verified_operations']==packet['new_Source_executions']==0
    assert packet['Native_coverage_increased'] is packet['performance_verified'] is False
    assert packet['fixtures']=={FIXTURE:fixture(receipt)}
    for contract,op in zip(packet['contracts'],OPERATIONS,strict=True):
        ordinal,method,family,action,authorized = op
        assert (contract['resolver_ordinal'],contract['method'],contract['owning_milestone'],
                contract['source_action'],contract['source_handler'])==(ordinal,method,'M5',action,HANDLERS[family])
        cases=case_rows(receipt,*op)
        assert contract['cases']==cases and contract['assertions']==assertion(family,method)
        ids={row['contract_id'] for row in receipt['rows'] if (row['resolver_ordinal'],row['method'])==(ordinal,method)}
        assert ids=={contract['contract_id']}
        assert contract['observed_roles']==list(ROLES) and contract['observed_methods']==[method]
        assert len({row['case'] for row in cases})==len(ROLES)
        assert contract['source_role_specs']=={role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in ROLES}
        assert contract['fully_verified'] is False


def generate(receipt,register):
    contracts=[]
    indexed={route['resolver_ordinal']:route for route in register['runtime_routes']}
    for op in OPERATIONS:
        ordinal,method,family,action,authorized = op
        route=indexed[ordinal]
        [old]=[item for item in route['operations'] if item['method']==method]
        assert 'executable_Source_contract' not in old and old['source_action']==action
        assert route['source_handler']==HANDLERS[family] and '\\.(?P<format>' in route['pattern_exact']
        contracts.append({
            'contract_id':old['contract_id'],'fixture_id':FIXTURE,'pattern_exact':route['pattern_exact'],
            'source_handler':route['source_handler'],'resolver_ordinal':ordinal,'resolver_name':route['name'],
            'owning_milestone':old['owning_milestone'],'method':method,'source_action':action,
            'observed_methods':[method],'observed_roles':list(ROLES),
            'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in ROLES},
            'assertions':assertion(family,method),'result':{'Source_case_observations':6,
                'new_Source_cases_this_task':0,'Native_coverage_increased':False},
            'fully_verified':False,'cases':case_rows(receipt,*op),
        })
    return {'binding_kind':KIND,'operation_count':len(contracts),'new_Source_executions':0,
            'credit':receipt['credit'],'fully_verified_operations':0,'Native_coverage_increased':False,
            'performance_verified':False,'fixtures':{FIXTURE:fixture(receipt)},'contracts':contracts}


def restore_pre_core_profiles(root,rows):
    selected={row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            key=operation['contract_id']
            if key in selected:
                assert operation.pop('executable_Source_contract')==selected[key] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}
