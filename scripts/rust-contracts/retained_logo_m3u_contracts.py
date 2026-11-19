"""Validate the independent Original logo and M3U child HTTP captures."""
import re

from source_contracts import checked

KIND = 'retained_original_logo_library_and_M3U_child_literal_HTTP'
INPUT = 'scripts/rust-contracts/retained-logo-m3u/bindings.json'
INPUT_SHA = '76190ae3b6ef921da514e9fdac5fee993d8622d125578dfb9a92b2f127026881'
RECEIPT = 'scripts/rust-integration/retained-logo-m3u/receipt.json'
RECEIPT_SHA = '29232943ccb1a30b5e20edaac2aefdf4d43021bc590c812fa3a642157b8200f1'
SOURCE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
IMAGE = 'sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
EMPTY_BODY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
LEGACY_BODY = 'fd62a53afdd49595ccd111b6ac06466a1690e7c8da4aeb0e884b5e0f4e937624'
ROLES = ('anonymous','viewer','standard','admin')
SOURCE_SCOPE = {
    'logo':('40cd4acf782a3ffad3bbe7f9fab184220068c229',
            'b2a744d5a9c5e26e4bb9f23e30c388121aee3fb64aaa1be405f70a815dce9811',8,
            'selected_owned_files_match','retained-Original-channel-logo-library'),
    'm3u':('a2f4a0b1e055422b9cb373ccf5b435b53e8f83ef',
           '3f51b84f56502616c32cce8c216efe42926a0eda0a9a8d6fdae7c148f6a6c3cd',19,
           'selected_broker_and_schedules_match','retained-Original-M3U-child-management'),
}
LOGO_PATH = '/api/channels/logos'
LOGOS = (
    ('bulk_delete',31,None,'DELETE',LOGO_PATH+'/bulk-delete/',None,'BulkDeleteLogosAPIView',204),
    ('cleanup',32,None,'POST',LOGO_PATH+'/cleanup/',None,'CleanupUnusedLogosAPIView',200),
    ('list_json',108,'GET','GET',LOGO_PATH+'.json','list','LogoViewSet',200),
    ('create_json',108,'POST','POST',LOGO_PATH+'.json','create','LogoViewSet',201),
    ('upload_json',110,'POST','POST',LOGO_PATH+'/upload.json','upload','LogoViewSet',500),
    ('delete_json',112,'DELETE','DELETE',LOGO_PATH+'/{pk}.json','destroy','LogoViewSet',204),
    ('patch_json',112,'PATCH','PATCH',LOGO_PATH+'/{pk}.json','partial_update','LogoViewSet',200),
    ('replace_json',112,'PUT','PUT',LOGO_PATH+'/{pk}.json','update','LogoViewSet',200),
)
M3U_PATH = '/api/m3u/accounts/{account}'
M3U_VIEWS = (
    ('profiles',199,201,M3U_PATH+'/profiles','M3UAccountProfileViewSet'),
    ('filters',203,205,M3U_PATH+'/filters','M3UFilterViewSet'),
    ('groups',207,209,'/api/m3u/server-groups','ServerGroupViewSet'),
)
METHODS = (('list','GET',200),('create','POST',201),('retrieve','GET',200),
           ('partial_update','PATCH',200),('update','PUT',200),('destroy','DELETE',204))
RAW_FILES = {'receipt.json','probe.stdout','probe-execution.json','independent-cleanup.json',
             'owned-roles.json','startup-execution.json','cleanup-execution.json','project-before.json',
             'outer-state.json','outer-selection.json','outer.log'}


def dependencies():
    return {INPUT,RECEIPT}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def scopes(kind):
    if kind=='logo':
        return sorted(LOGOS)
    rows = []
    for family,collection,detail,path,handler in M3U_VIEWS:
        for action,method,status in METHODS:
            listed = action in ('list','create')
            rows.append((f'{family}_{action}_json',collection if listed else detail,method,method,
                         path+('.json' if listed else '/{pk}.json'),action,handler,status))
    rows.append(('profiles_partial_update_canonical',200,'PATCH','PATCH',M3U_PATH+'/profiles/{pk}/',
                 'partial_update','M3UAccountProfileViewSet',200))
    return sorted(rows)


def check_cohort(kind,cohort):
    base,seal,count,_,_ = SOURCE_SCOPE[kind]
    assert (cohort['scope_base'],cohort['fixture_sha256'],cohort['selected_operations'],
            len(cohort['observations']),cohort['selected_cases'])==(base,seal,count,count,4*count)
    assert cohort['outer_actual_exit']==0 and cohort['physical_cleanup'] is True
    assert cohort['owned_roles_retired']==['baseline','origin']
    assert set(cohort['actual_child_results'])=={'startup','probe','cleanup'}
    assert all(value=={'actual_exit':0,'stdout_EOF':True,'stderr_EOF':True,'process_group_settled':True}
               for value in cohort['actual_child_results'].values())
    assert set(cohort['raw_fingerprints'])==RAW_FILES
    assert all(re.fullmatch('[0-9a-f]{64}',value) for value in cohort['raw_fingerprints'].values())


def check_operation(kind,op,scope):
    name,ordinal,declared,method,path,action,handler,positive = scope
    app = 'channels' if kind=='logo' else 'm3u'
    assert (op['name'],op['declaration'],op['observed_method'],op['path_template'])==(name,declared,method,path)
    assert (op['resolver']['ordinal'],op['resolver']['action'],op['resolver']['handler'])==(
        ordinal,action,f'apps.{app}.api_views.{handler}')
    pattern = re.escape(path).replace(r'\{pk\}',r'[1-9][0-9]*').replace(r'\{account\}',r'[1-9][0-9]*')
    assert re.fullmatch(pattern,op['actual_path'])
    proof = op['proof']
    assert proof['selected_sql_state_matches'] is proof[SOURCE_SCOPE[kind][3]] is proof['role_denials_preserved_state'] is True
    assert (proof['before_sha256']!=proof['after_sha256']) is (method!='GET' and positive!=500)
    assert all(re.fullmatch('[0-9a-f]{64}',proof[key]) for key in ('before_sha256','after_sha256'))
    assert proof.get('known_legacy_error',False) is (name=='upload_json')
    assert [row['role'] for row in op['cases']]==list(ROLES)
    for row,expected in zip(op['cases'],(401,403,200 if method=='GET' else 403,positive),strict=True):
        assert (row['actual_status'],row['method'],row['path'])==(expected,method,op['actual_path'])
        assert row['authentication_challenge_present'] is (expected==401)
        assert re.fullmatch('[0-9a-f]{64}',row['actual_body_sha256'])
        assert (row['actual_body_bytes']==0) is (expected==204)
        if expected==500:
            assert name=='upload_json' and row['role']=='admin'
            assert (row['actual_content_type'],row['actual_body_sha256'],row['actual_body_bytes'])==('text/html',LEGACY_BODY,145)
        elif expected==204:
            assert row['actual_body_sha256']==EMPTY_BODY
        else:
            assert row['actual_content_type']=='application/json'


def describe(kind,name):
    if kind=='m3u':
        details = ['Exact selected JSON or empty-delete response and account-scoped rows.',
                   'Unrelated rows, notifications, periodic tasks and observed broker queue length stayed unchanged.']
        if name=='profiles_destroy_json':
            details.append('Delete removed only the target profile ID from the owned viewer allowlist.')
        if name=='groups_destroy_json':
            details.append('Delete cleared the account foreign key to the selected server group and preserved the peer link.')
        return details
    if name=='upload_json':
        return ['The authorized formatted multipart upload returned the exact retained Original HTML 500.',
                'Denied roles retained their selected authorization responses. The scoped SQL and owned files stayed unchanged.']
    if name=='bulk_delete':
        return ['Selected logo rows and files were removed, the relevant channel link was cleared, and the owned survivor was preserved.']
    if name=='cleanup':
        return ['Only the unused logo rows were removed. Their files and the linked logo were preserved.']
    return ['Exact selected response, SQL and owned logo files, with lower-role denials preserving scoped state.']


def build(receipt):
    assert receipt['scope']=='retained_Original_channel_logo_and_M3U_child_HTTP'
    assert (receipt['source_revision'],receipt['source_archive_sha256'],receipt['image_id'])==(SOURCE,ARCHIVE,IMAGE)
    assert all(receipt[key] is False for key in ('Native_verified','whole_operation_verified','performance_verified'))
    assert set(receipt['cohorts'])==SOURCE_SCOPE.keys()
    fixtures,contracts = {},[]
    for kind in SOURCE_SCOPE:
        cohort = receipt['cohorts'][kind]
        check_cohort(kind,cohort)
        fixture = SOURCE_SCOPE[kind][-1]
        fixtures[fixture] = {'id':fixture,'source_revision':SOURCE,'source_image':IMAGE,
             'actual_controller_exit':0,'scope_base':cohort['scope_base'],'fixture_sha256':cohort['fixture_sha256'],
             'selected_Original_HTTP_cases':cohort['selected_cases'],
             'exact_owned_namespace_cleanup':True,'public_artifacts':[{'path':RECEIPT,'sha256':RECEIPT_SHA}]}
        for op,scope in zip(cohort['observations'],scopes(kind),strict=True):
            check_operation(kind,op,scope)
            name,ordinal,declared,method,_,action,_,status = scope
            resolver = op['resolver']
            contracts.append({
                'contract_id':op['contract_id'],'fixture_id':fixture,'resolver_ordinal':ordinal,
                'resolver_name':resolver['view_name'],'pattern_exact':resolver['pattern'],
                'source_handler':resolver['handler'],'owning_milestone':'M5','method':declared,'source_action':action,
                'observed_methods':[method],'observed_roles':list(ROLES),
                'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in ROLES},
                'assertions':describe(kind,name),'fully_verified':False,'cases':op['cases'],
                'result':{'Source_case_observations':4,'new_Source_cases_this_task':0,'Native_coverage_increased':False,
                          'known_Original_legacy_error_cells':int(status==500)},
            })
    assert len({row['contract_id'] for row in contracts})==27
    return {'binding_kind':KIND,'operation_count':27,'fully_verified_operations':0,
            'Native_coverage_increased':False,'performance_verified':False,'credit':receipt['credit'],
            'fixtures':fixtures,'contracts':contracts}


def generate(receipt,register):
    result = build(receipt)
    previous = {op['contract_id']:(route,op) for route in register['runtime_routes'] for op in route['operations']}
    for row in result['contracts']:
        route,op = previous[row['contract_id']]
        assert 'executable_Source_contract' not in op and not op['related_evidence']
        assert (route['resolver_ordinal'],route['name'],route['pattern_exact'],route['source_handler'])==tuple(
            row[key] for key in ('resolver_ordinal','resolver_name','pattern_exact','source_handler'))
        assert all(op[key]==row[key] for key in ('owning_milestone','method','source_action'))
    return result


def validate(root,packet):
    assert packet==load(root)
    assert packet==build(checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA}))


def restore_pre_logo_m3u(root,rows):
    selected = {row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            key = operation['contract_id']
            if key in selected:
                assert operation.pop('executable_Source_contract')==selected[key] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}
