"""Bind the eight canonical Image57 M3U mutation controls."""
import re

from retained_image57_accounts_contracts import AUTH
from source_contracts import checked

KIND = 'retained_original_Image57_M3U_canonical_HTTP'
INPUT = 'scripts/rust-contracts/retained-image57-m3u/bindings.json'
INPUT_SHA = '9f98e95b6af397845cfdc71412ca45419ae0d6a338fb5a517f653acb68a3d631'
RECEIPT = 'scripts/rust-integration/image57-checkpoint/selected-m3u-Original.json'
RECEIPT_SHA = 'e32c214563057bc585a28ab5b1bc96d13e13bbc3d1f5ac83579b6b3fb0f12a45'
FIXTURE = 'retained-Original-Image57-canonical-M3U'
METHODS = {'POST':'create','DELETE':'destroy','PATCH':'partial_update','PUT':'update'}
ROUTES = {
    202: ('m3u-filters-list',r'^accounts/(?P<account_id>\d+)/filters/$','M3UFilterViewSet',{'POST':4}),
    204: ('m3u-filters-detail',r'^accounts/(?P<account_id>\d+)/filters/(?P<pk>[^/.]+)/$','M3UFilterViewSet',
          {'DELETE':5,'PATCH':8,'PUT':3}),
    206: ('server-group-list',r'^server-groups/$','ServerGroupViewSet',{'POST':5}),
    208: ('server-group-detail',r'^server-groups/(?P<pk>[^/.]+)/$','ServerGroupViewSet',{'DELETE':4,'PATCH':5,'PUT':3}),
}


def dependencies():
    return {INPUT,RECEIPT,*AUTH.values()}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def validate(root,packet):
    assert packet==load(root)
    receipt=checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA})
    refs=receipt['historical_public_authorities']
    assert {key:ref['path'] for key,ref in refs.items()}==AUTH
    actual={key:checked(root,ref) for key,ref in refs.items()}
    report=actual['report']
    assert report['passed'] is True and report['cleanup']=='project removed' and not report['resources_retained']
    assert actual['controller']['returncode']==receipt['actual_controller_exit']==0
    assert actual['controller']['process_settled'] and receipt['controller_process_settled']
    assert actual['cleanup']['passed'] and receipt['owned_namespace_cleanup_verified']
    assert report['source_baseline']==receipt['source_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert all(report['tool_source_sha256'][path]==sha for path,sha in receipt['tools_sha256'].items())
    assert receipt['credential_values_exported'] is receipt['Native_parity_assigned'] is receipt['whole_operation_accepted'] is False
    assert packet['binding_kind']==KIND and packet['operation_count']==len(packet['contracts'])==8
    assert packet['new_Source_executions']==packet['fully_verified_operations']==0
    assert packet['Native_coverage_increased'] is packet['performance_verified'] is False
    published={row['name']:row for row in report['checks']}
    seen=set()
    for contract in packet['contracts']:
        ordinal,method=contract['resolver_ordinal'],contract['method']
        name,pattern,handler,methods=ROUTES[ordinal]
        assert contract['source_action']==METHODS[method] and contract['owning_milestone']=='M5'
        assert contract['resolver_name']=='api:m3u:'+name and contract['pattern_exact']=='api/m3u/'+pattern
        assert contract['source_handler']=='apps.m3u.api_views.'+handler
        assert contract['fully_verified'] is False and contract['fixture_id']==FIXTURE
        cases=[row for row in receipt['selected_Original_observations'] if row['ordinal']==ordinal and row['method']==method]
        assert cases==contract['cases'] and len(cases)==methods[method]
        roles=list(dict.fromkeys(row['role'] for row in cases))
        assert contract['observed_roles']==roles and contract['observed_methods']==[method]
        assert contract['source_role_specs']=={role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles}
        assert contract['result']=={'Source_case_observations':len(cases),'new_Source_cases_this_task':0,
                                    'Native_coverage_increased':False}
        for row in cases:
            assert set(row)=={'ordinal','method','role','name','path_template','status','response_projection_sha256'}
            path=row['path_template'].replace('{account_id}','11').replace('{other_account_id}','12').replace('{owned_pk}','13')
            assert re.fullmatch('/api/m3u/'+pattern.removeprefix('^'),path)
            record=row['name']
            assert record not in seen
            expected_role=record.rsplit('-',1)[-1] if ':denied-' in record else 'admin'
            assert row['role']==expected_role
            actual_row=published[record]
            assert actual_row['passed'] and actual_row['details']['python_status']==row['status']
            assert row['response_projection_sha256']==actual_row['details']['python_body_sha256']
            seen.add(record)
    assert len(seen)==37
    witnesses={
        'server-group-crud:set-null-preserves-account':200,'server-group-crud:read-deleted':404,
        'filter-crud:following-put-get':200,'filter-crud:read-deleted':404,
    }
    assert set(receipt['related_stored_state_witnesses'])==witnesses.keys()
    assert all(published[name]['passed'] and published[name]['details']['python_status']==status
               for name,status in witnesses.items())
    fixture=packet['fixtures'][FIXTURE]
    assert fixture['actual_controller_exit']==0 and fixture['exact_owned_namespace_cleanup']
    assert fixture['source_revision']==receipt['source_revision']
    assert fixture['credential_material_exported'] is False


def restore_pre_image57_m3u(root,rows):
    contracts={row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            identifier=operation['contract_id']
            if identifier in contracts:
                assert operation.pop('executable_Source_contract')==contracts[identifier] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}


def generate(receipt,register):
    fixture={
        'id':FIXTURE,'source_revision':receipt['source_revision'],
        'source_archive_sha256':receipt['source_archive_sha256'],'actual_controller_exit':receipt['actual_controller_exit'],
        'exact_owned_namespace_cleanup':receipt['owned_namespace_cleanup_verified'],
        'credential_material_exported':False,'public_artifacts':[{'path':RECEIPT,'sha256':RECEIPT_SHA}],
    }
    contracts=[]
    for ordinal,(name,pattern,handler,methods) in ROUTES.items():
        [route]=[row for row in register['runtime_routes'] if row['resolver_ordinal']==ordinal]
        assert route['name']=='api:m3u:'+name and route['pattern_exact']=='api/m3u/'+pattern
        assert route['source_handler']=='apps.m3u.api_views.'+handler
        for method,count in methods.items():
            [operation]=[row for row in route['operations'] if row['method']==method]
            assert 'executable_Source_contract' not in operation and operation['source_action']==METHODS[method]
            cases=[row for row in receipt['selected_Original_observations'] if row['ordinal']==ordinal and row['method']==method]
            assert len(cases)==count
            roles=list(dict.fromkeys(row['role'] for row in cases))
            contracts.append({
                'contract_id':operation['contract_id'],'fixture_id':FIXTURE,'pattern_exact':route['pattern_exact'],
                'source_handler':route['source_handler'],'resolver_ordinal':ordinal,'resolver_name':route['name'],
                'owning_milestone':'M5','method':method,'source_action':operation['source_action'],
                'observed_methods':[method],'observed_roles':roles,'source_role_specs':{
                    role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles},
                'assertions':[f'{method} {name} matches the selected Original owned-fixture role and status cases'],
                'result':{'Source_case_observations':len(cases),'new_Source_cases_this_task':0,'Native_coverage_increased':False},
                'fully_verified':False,'cases':cases,
            })
    return {'binding_kind':KIND,'operation_count':len(contracts),'new_Source_executions':0,
            'credit':receipt['credit'],'fully_verified_operations':0,'Native_coverage_increased':False,
            'performance_verified':False,'fixtures':{FIXTURE:fixture},'contracts':contracts}
