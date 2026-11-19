"""Validate three retained Original provider and core administrative bindings."""
import hashlib
import json
import re

from retained_image57_accounts_contracts import AUTH as IMAGE57
from source_contracts import checked

KIND = 'retained_original_provider_and_core_admin_HTTP'
INPUT = 'scripts/rust-contracts/retained-provider-admin/bindings.json'
INPUT_SHA = 'e109934cd592947120aefc733ea6d3494554ba38baca45cd9740a4e2d0e067e1'
RECEIPT = 'scripts/rust-integration/retained-provider-admin/receipt.json'
RECEIPT_SHA = '88119db18eedbd54b6d81a2fb934dbb89678c5be79e5a7a8a755ba87056293a0'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
PROFILE = 'scripts/rust-integration/m4-credential-pool-source1/'
CHECKPOINT = 'scripts/rust-integration/image57-checkpoint/evidence/matrix/'
AUTH = {
    **{f'tz_{key}': path for key,path in IMAGE57.items()},
    'ua_report':CHECKPOINT+'reports/user-agent/0-integration.json.json',
    'ua_checks':CHECKPOINT+'reports/user-agent/1-packaged-checks.json.json',
    'ua_controller':CHECKPOINT+'controllers/user-agent.json', 'ua_cleanup':CHECKPOINT+'cleanup/user-agent.json',
    'profile_identity':PROFILE+'identity.json','profile_terminal':PROFILE+'terminal.json',
    'profile_cleanup':PROFILE+'Root-independent-cleanup.json',
    **{f'profile_{label}':PROFILE+f'control/{label}-profile-save.json' for label in 'ABCD'},
}
SCOPES = {
    'profile': (200,'PUT','update','PUT','api:m3u:m3u-account-profiles-detail',
                r'api/m3u/^accounts/(?P<account_id>\d+)/profiles/(?P<pk>[^/.]+)/$',
                'apps.m3u.api_views.M3UAccountProfileViewSet',4,
                'Four owned administrator PUT saves return the exact account and profile identities'),
    'timezone': (215,None,None,'GET','api:core:timezones','api/core/timezones/',
                 'core.api_views.TimezoneListView',6,'Original timezone collection GET and ignored-query checks for scoped roles'),
    'useragent': (222,'DELETE','destroy','DELETE','api:core:useragent-detail',
                  r'api/core/^useragents/(?P<pk>[^/.]+)/$','core.api_views.UserAgentViewSet',3,
                  'Two owned UserAgent deletions and the missing-id control, with cache and FK observations'),
}
EXITS = {'profile':1,'timezone':0,'useragent':0}


def dependencies():
    return {INPUT,RECEIPT,*AUTH.values()}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def projection_sha(body):
    return hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def observed_path(row):
    path=row['path_template']
    return path.replace('{account_id}','11').replace('{owned_pk}','21').replace('{missing_pk}','999999')


def fixture_id(scope):
    return 'retained-Original-provider-core-'+scope


def validate(root,packet):
    assert packet==load(root)
    receipt=checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA})
    assert receipt['source_revision']==ORIGINAL and receipt['exact_owned_namespace_cleanup']
    assert receipt['new_Source_executions']==receipt['full_operations_accepted']==0
    assert receipt['credential_values_exported'] is receipt['Native_coverage_increased'] is False
    refs=receipt['historical_public_authorities']
    assert {key:ref['path'] for key,ref in refs.items()}==AUTH
    actual={key:checked(root,ref) for key,ref in refs.items()}
    for prefix in ('tz','ua'):
        assert actual[prefix+'_report']['passed'] and actual[prefix+'_report']['cleanup']=='project removed'
        assert not actual[prefix+'_report']['resources_retained']
        assert actual[prefix+'_controller']['returncode']==0 and actual[prefix+'_controller']['process_settled']
        clean=actual[prefix+'_cleanup']
        assert clean['passed'] and not any(clean['inventories'].values())
    assert actual['tz_report']['source_baseline']==actual['profile_identity']['Original_revision']==ORIGINAL
    terminal=actual['profile_terminal']
    assert terminal['actual_exit']==actual['profile_identity']['actual_Source_controller_exit']==1
    assert terminal['stdout_EOF'] and terminal['stderr_EOF'] and terminal['process_group_settled']
    assert actual['profile_cleanup']['passed']
    tz={row['name']:row for row in actual['tz_report']['checks']}
    ua={row['name']:row for row in actual['ua_checks']['checks']}
    assert actual['ua_report']['packaged_checks_sha256']==refs['ua_checks']['sha256']
    assert packet['binding_kind']==KIND and packet['operation_count']==len(packet['contracts'])==3
    assert packet['new_Source_executions']==packet['fully_verified_operations']==0
    assert packet['Native_coverage_increased'] is packet['performance_verified'] is False
    scopes=list(SCOPES.items())
    for contract,(scope,spec) in zip(packet['contracts'],scopes,strict=True):
        ordinal,method,action,observed,name,pattern,handler,count,assertion=spec
        assert contract['capture_case']==scope and contract['fixture_id']==fixture_id(scope)
        assert (contract['resolver_ordinal'],contract['method'],contract['source_action'],contract['owning_milestone'])==(
            ordinal,method,action,'M5')
        assert contract['resolver_name']==name and contract['pattern_exact']==pattern and contract['source_handler']==handler
        assert contract['assertions']==[assertion] and contract['fully_verified'] is False
        fixture=packet['fixtures'][fixture_id(scope)]
        assert fixture['actual_controller_exit']==EXITS[scope] and fixture['exact_owned_namespace_cleanup']
        assert fixture['source_revision']==ORIGINAL and fixture['credential_material_exported'] is False
        cases=[row for row in receipt['selected_Original_observations'] if row['scope']==scope]
        assert cases==contract['cases'] and len(cases)==count
        roles=list(dict.fromkeys(row['role'] for row in cases))
        assert contract['observed_roles']==roles and contract['observed_methods']==[observed]
        assert contract['source_role_specs']=={role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles}
        assert contract['result']=={'Source_case_observations':len(cases),'new_Source_cases_this_task':0,
                                    'Native_coverage_increased':False}
        for row in cases:
            case_name=row['name']
            assert row['method']==observed
            if scope=='timezone':
                req='/api/core/timezones/'
                if case_name.endswith(':query-ignored'):
                    req+='?limit=1&search=invalid'
                assert row['path_template']==req and tz[case_name]['passed']
                details=tz[case_name]['details']
                assert row['status']==details['python_status'] and row['response_projection_sha256']==details['python_body_sha256']
            elif scope=='useragent':
                assert re.fullmatch(r'/api/core/useragents/[^/.]+/',observed_path(row))
                assert ua[case_name]['passed'] and not ua[case_name]['mismatches']
                status,removed={'delete-default':(204,True),'delete-other':(204,True),'lookup-DELETE-61':(404,False)}[case_name]
                assert row['status']==status and row['role']=='admin' and row['db_row_removed']==removed
            else:
                source=actual['profile_'+case_name]
                assert re.fullmatch(r'/api/m3u/accounts/\d+/profiles/[^/.]+/',observed_path(row))
                assert row['status']==source['status']==200 and row['role']==source['credential_role']=='admin'
                assert source['method']=='PUT' and row['account_and_profile_identity_preserved']
                assert row['response_projection_sha256']==projection_sha(source['body'])


def restore_pre_provider_admin(root,rows):
    contracts={row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            identifier=operation['contract_id']
            if identifier in contracts:
                assert operation.pop('executable_Source_contract')==contracts[identifier] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}


def generate(receipt,register):
    contracts=[]
    fixtures={}
    for scope,(ordinal,method,action,observed,name,pattern,handler,count,assertion) in SCOPES.items():
        [route]=[row for row in register['runtime_routes'] if row['resolver_ordinal']==ordinal]
        assert (route['name'],route['pattern_exact'],route['source_handler'])==(name,pattern,handler)
        [operation]=[row for row in route['operations'] if row['method']==method]
        assert 'executable_Source_contract' not in operation and operation['source_action']==action
        cases=[row for row in receipt['selected_Original_observations'] if row['scope']==scope]
        assert len(cases)==count
        roles=list(dict.fromkeys(row['role'] for row in cases))
        fixture=fixture_id(scope)
        fixtures[fixture]={'id':fixture,'source_revision':ORIGINAL,'actual_controller_exit':EXITS[scope],
                           'exact_owned_namespace_cleanup':True,'credential_material_exported':False,
                           'public_artifacts':[{'path':RECEIPT,'sha256':RECEIPT_SHA}]}
        contracts.append({
            'contract_id':operation['contract_id'],'fixture_id':fixture,'pattern_exact':pattern,
            'source_handler':handler,'resolver_ordinal':ordinal,'resolver_name':name,'owning_milestone':'M5',
            'method':method,'source_action':action,'observed_methods':[observed],'observed_roles':roles,
            'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles},
            'assertions':[assertion],'result':{'Source_case_observations':count,'new_Source_cases_this_task':0,
                                            'Native_coverage_increased':False},'capture_case':scope,'fully_verified':False,
            'cases':cases,
        })
    return {'binding_kind':KIND,'operation_count':len(contracts),'new_Source_executions':0,
            'credit':receipt['credit'],'fully_verified_operations':0,'Native_coverage_increased':False,
            'performance_verified':False,'fixtures':fixtures,'contracts':contracts}
