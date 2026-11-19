"""Bind only the actual Image57 User, Group and API-key canonical requests."""
import re
from source_contracts import checked

KIND = 'retained_original_Image57_accounts_canonical_HTTP'
INPUT = 'scripts/rust-contracts/retained-image57-accounts/bindings.json'
INPUT_SHA = '510a6d658af1a0be636be8e897d583661ad29074c0a522535cd3936505c835c8'
RECEIPT = 'scripts/rust-integration/image57-checkpoint/selected-accounts-Original.json'
RECEIPT_SHA = '1ed2520de570108d203c95cb4b3edac8d3abff5e7c30a7307a3499579d02b308'
FIXTURE = 'retained-Original-Image57-canonical-Accounts'
EVIDENCE = 'scripts/rust-integration/image57-checkpoint/evidence/matrix/'
AUTH = {
    'report':EVIDENCE+'reports/baseline-all/0-integration.json.json',
    'cleanup':EVIDENCE+'cleanup/baseline-all.json',
    'controller':EVIDENCE+'controllers/baseline-all.json',
}
ROWS = {
    'user_delete':(15,'DELETE','destroy','user-detail',r'^users/(?P<pk>[^/.]+)/$','UserViewSet',
                   [('delete','admin',204),('delete-missing','admin',404),('viewer-denied-delete','viewer',403)]),
    'user_get':(15,'GET','retrieve','user-detail',r'^users/(?P<pk>[^/.]+)/$','UserViewSet',
                [('read','admin',200),('read-deleted','admin',404)]),
    'group_delete':(19,'DELETE','destroy','group-detail',r'^groups/(?P<pk>[^/.]+)/$','GroupViewSet',
                   [('delete','admin',204),('delete-missing','admin',404),('viewer-denied-delete','viewer',403)]),
    'group_get':(19,'GET','retrieve','group-detail',r'^groups/(?P<pk>[^/.]+)/$','GroupViewSet',
                [('read','admin',200),('read-deleted','admin',404)]),
    'key_list':(21,'GET','list','api-key-list',r'^api-keys/$','APIKeyViewSet',
                 [('list-admin','admin',200),('list-viewer','viewer',200)]),
    'key_generate':(23,'POST','generate','api-key-generate',r'^api-keys/generate/$','APIKeyViewSet',
                    [('own-empty-object','viewer',201),('other-user','viewer',403),('explicit-self','viewer',403),
                     ('malformed-user-id','admin',404),('anonymous','anonymous',401)]),
    'key_revoke':(25,'POST','revoke','api-key-revoke',r'^api-keys/revoke/$','APIKeyViewSet',
                  [('own-empty-object','viewer',200),('other-user','viewer',403),('explicit-self','viewer',403),
                   ('malformed-user-id','admin',404),('anonymous','anonymous',401)]),
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
    assert all(ref['sha256']==report['tool_source_sha256'][ref['path']] for ref in receipt['actual_fixture_tool_digests'].values())
    assert receipt['credential_values_exported'] is receipt['Native_parity_assigned'] is receipt['whole_operation_accepted'] is False
    assert packet['binding_kind']==KIND and packet['operation_count']==len(packet['contracts'])==7
    assert packet['new_Source_executions']==packet['fully_verified_operations']==0
    assert packet['Native_coverage_increased'] is packet['performance_verified'] is False
    published={row['name']:row for row in report['checks']}
    seen=set()
    for contract,(name,(ordinal,method,action,view_name,pattern,handler,expect)) in zip(packet['contracts'],ROWS.items(),strict=True):
        assert contract['capture_case']==name
        assert (contract['resolver_ordinal'],contract['method'],contract['source_action'],contract['owning_milestone'])==(ordinal,method,action,'M5')
        assert contract['resolver_name']=='api:accounts:'+view_name
        assert contract['pattern_exact']=='api/accounts/'+pattern
        assert contract['source_handler']=='apps.accounts.api_views.'+handler
        assert contract['fully_verified'] is False and contract['fixture_id']==FIXTURE
        cases=[row for row in receipt['selected_Original_observations'] if row['operation']==name]
        assert cases==contract['cases']
        assert [(r['case'],r['role'],r['status']) for r in cases]==expect
        roles=list(dict.fromkeys(row['role'] for row in cases))
        assert contract['observed_roles']==roles and contract['observed_methods']==[method]
        assert contract['source_role_specs']=={role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles}
        assert contract['result']=={'Source_case_observations':len(cases),'new_Source_cases_this_task':0,
                                    'Native_coverage_increased':False}
        for row in cases:
            assert row['method']==method and set(row)<={'operation','case','role','method','path_template','status',
                                  'actual_record_name','actual_status_field','response_projection_sha256'}
            path=row['path_template']
            assert re.fullmatch('/api/accounts/'+pattern.removeprefix('^'),path.replace('{owned_pk}','37'))
            record=row['actual_record_name']
            assert record not in seen
            actual_row=published[record]
            assert actual_row['passed'] and actual_row['details'][row['actual_status_field']]==row['status']
            body=actual_row['details'].get('python_body_sha256') or actual_row['details'].get('body_sha256')
            assert row.get('response_projection_sha256')==body
            seen.add(record)
    assert len(seen)==22
    state=receipt['related_Owned_key_state']
    assert state=={'seed_owned_key_authenticated_with_Original_HTTP':True,
                   'same_Owned_key_rejected_after_Original_revoke':True,
                   'wrong_role_actions_preserved_key_state':True}
    for name,status in [('accounts:key-cross-auth:python:python',200),
                        ('accounts:revoked-key-rejected:python:python',401)]:
        assert published[name]['passed'] and published[name]['details']['status']==status
    denied=published['accounts:denied-key-actions-preserve-other-user']
    assert denied['passed'] and denied['details']['before_sha256']==denied['details']['after_sha256']
    fixture=packet['fixtures'][FIXTURE]
    assert fixture['actual_controller_exit']==0 and fixture['exact_owned_namespace_cleanup']
    assert fixture['source_revision']==receipt['source_revision']
    assert fixture['credential_material_exported'] is False


def restore_pre_image57_accounts(root,rows):
    contracts={row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            identifier=operation['contract_id']
            if identifier in contracts:
                assert operation.pop('executable_Source_contract')==contracts[identifier] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}


def generate(receipt, register):
    messages = {
        'user_delete': 'Owned user delete and missing repeat, with a viewer delete denial',
        'user_get': 'Owned user detail before and after Original deletion',
        'group_delete': 'Owned group delete and missing repeat, with a viewer delete denial',
        'group_get': 'Owned group detail before and after Original deletion',
        'key_list': 'API-key list GET as administrator and viewer',
        'key_generate': 'Viewer generates an own key, with separate explicit-target, malformed-target and anonymous controls',
        'key_revoke': 'Viewer revokes the own key, with separate explicit-target, malformed-target and anonymous controls',
    }
    fixture = {
        'id': FIXTURE, 'source_revision': receipt['source_revision'],
        'source_archive_sha256': receipt['source_archive_sha256'],
        'actual_controller_exit': receipt['actual_controller_exit'],
        'exact_owned_namespace_cleanup': receipt['owned_namespace_cleanup_verified'],
        'credential_material_exported': False,
        'public_artifacts': [{'path': RECEIPT, 'sha256': RECEIPT_SHA}],
    }
    contracts=[]
    for name,(ordinal,method,action,view_name,pattern,handler,expected) in ROWS.items():
        [route]=[row for row in register['runtime_routes'] if row['resolver_ordinal']==ordinal]
        [operation]=[row for row in route['operations'] if row['method']==method]
        assert 'executable_Source_contract' not in operation
        assert route['pattern_exact']=='api/accounts/'+pattern
        assert operation['source_action']==action
        cases=[row for row in receipt['selected_Original_observations'] if row['operation']==name]
        roles=list(dict.fromkeys(row['role'] for row in cases))
        contracts.append({
            'contract_id':operation['contract_id'],'fixture_id':FIXTURE,
            'pattern_exact':route['pattern_exact'],'source_handler':'apps.accounts.api_views.'+handler,
            'resolver_ordinal':ordinal,'resolver_name':'api:accounts:'+view_name,'owning_milestone':'M5',
            'method':method,'source_action':action,'observed_methods':[method],'observed_roles':roles,
            'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles},
            'assertions':[messages[name]],'result':{'Source_case_observations':len(cases),
                'new_Source_cases_this_task':0,'Native_coverage_increased':False},
            'fully_verified':False,'capture_case':name,'cases':cases,
        })
    return {'binding_kind':KIND,'operation_count':len(contracts),'new_Source_executions':0,
            'credit':receipt['credit'],'fully_verified_operations':0,'Native_coverage_increased':False,
            'performance_verified':False,'fixtures':{FIXTURE:fixture},'contracts':contracts}
