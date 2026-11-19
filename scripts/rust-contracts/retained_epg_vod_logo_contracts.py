"""Validate the independent Original EPG and VOD logo HTTP captures."""
import re

from source_contracts import checked

KIND = 'retained_original_EPG_library_and_VOD_logo_literal_HTTP'
INPUT = 'scripts/rust-contracts/retained-epg-vod-logo/bindings.json'
INPUT_SHA = '4d76b842a9b095425fb90484ab3de67b712aafc28d54b49fe13ff656162c5923'
RECEIPT = 'scripts/rust-integration/retained-epg-vod-logo/receipt.json'
RECEIPT_SHA = '23fd50385a7c3320a6ac4988a16135287d36e936036f59b44a27ea21b6c07330'
SOURCE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
IMAGE = 'sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
EMPTY_BODY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
LEGACY_BODY = 'fd62a53afdd49595ccd111b6ac06466a1690e7c8da4aeb0e884b5e0f4e937624'
ROLES = ('anonymous','viewer','standard','admin')
SOURCE_SCOPE = {
    'epg':('47162474e954fd79c0e2ddb2b1740a80dc1ab884','9a376010703aa89b0606a7256c22a117d4da35a9f7c1d42962a77bae95afa2ae',15,'selected_schedule_and_queue_match','retained-Original-epg-library'),
    'vod':('af55f5d60c7e80ac28af1fe5d11480d98a3504f9','3b2c386512a2ef42a6dd353542c5b436899ab58a4ce5fe0039402b838afba057',12,'selected_owned_files_match','retained-Original-vod-library'),
}
OPERATIONS = {
    'epg': (
        ('epgdata_list', 168, 'GET', '/api/epg/epgdata.json', 'list', 'apps.epg.api_views.EPGDataViewSet', (401, 403, 200, 200)),
        ('epgdata_retrieve', 170, 'GET', '/api/epg/epgdata/{pk}.json', 'retrieve', 'apps.epg.api_views.EPGDataViewSet', (401, 403, 200, 200)),
        ('poster_json', 166, 'GET', '/api/epg/programs/{pk}/poster.json', 'poster', 'apps.epg.api_views.ProgramViewSet', (500, 500, 500, 500)),
        ('poster_no_slash', 148, 'GET', '/api/epg/programs/{pk}/poster', 'poster', 'apps.epg.api_views.ProgramViewSet', (404, 404, 404, 404)),
        ('programs_create', 160, 'POST', '/api/epg/programs.json', 'create', 'apps.epg.api_views.ProgramViewSet', (401, 403, 403, 500)),
        ('programs_destroy', 164, 'DELETE', '/api/epg/programs/{pk}.json', 'destroy', 'apps.epg.api_views.ProgramViewSet', (401, 403, 403, 204)),
        ('programs_list', 160, 'GET', '/api/epg/programs.json', 'list', 'apps.epg.api_views.ProgramViewSet', (401, 403, 200, 200)),
        ('programs_partial_update', 164, 'PATCH', '/api/epg/programs/{pk}.json', 'partial_update', 'apps.epg.api_views.ProgramViewSet', (401, 403, 403, 200)),
        ('programs_retrieve', 164, 'GET', '/api/epg/programs/{pk}.json', 'retrieve', 'apps.epg.api_views.ProgramViewSet', (401, 403, 200, 200)),
        ('programs_update', 164, 'PUT', '/api/epg/programs/{pk}.json', 'update', 'apps.epg.api_views.ProgramViewSet', (401, 403, 403, 200)),
        ('sources_destroy', 154, 'DELETE', '/api/epg/sources/{pk}.json', 'destroy', 'apps.epg.api_views.EPGSourceViewSet', (401, 403, 403, 204)),
        ('sources_list', 150, 'GET', '/api/epg/sources.json', 'list', 'apps.epg.api_views.EPGSourceViewSet', (401, 403, 200, 200)),
        ('sources_partial_update', 154, 'PATCH', '/api/epg/sources/{pk}.json', 'partial_update', 'apps.epg.api_views.EPGSourceViewSet', (401, 403, 403, 200)),
        ('sources_retrieve', 154, 'GET', '/api/epg/sources/{pk}.json', 'retrieve', 'apps.epg.api_views.EPGSourceViewSet', (401, 403, 200, 200)),
        ('sources_update', 154, 'PUT', '/api/epg/sources/{pk}.json', 'update', 'apps.epg.api_views.EPGSourceViewSet', (401, 403, 403, 200)),
    ),
    'vod': (
        ('canonical_create', 306, 'POST', '/api/vod/vodlogos/', 'create', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 403, 201)),
        ('canonical_destroy', 312, 'DELETE', '/api/vod/vodlogos/{pk}/', 'destroy', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 403, 204)),
        ('canonical_list', 306, 'GET', '/api/vod/vodlogos/', 'list', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 200, 200)),
        ('canonical_partial_update', 312, 'PATCH', '/api/vod/vodlogos/{pk}/', 'partial_update', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 403, 200)),
        ('canonical_retrieve', 312, 'GET', '/api/vod/vodlogos/{pk}/', 'retrieve', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 200, 200)),
        ('canonical_update', 312, 'PUT', '/api/vod/vodlogos/{pk}/', 'update', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 403, 200)),
        ('json_create', 307, 'POST', '/api/vod/vodlogos.json', 'create', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 403, 201)),
        ('json_destroy', 313, 'DELETE', '/api/vod/vodlogos/{pk}.json', 'destroy', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 403, 204)),
        ('json_list', 307, 'GET', '/api/vod/vodlogos.json?used=movies&no_pagination=true', 'list', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 200, 200)),
        ('json_partial_update', 313, 'PATCH', '/api/vod/vodlogos/{pk}.json', 'partial_update', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 403, 200)),
        ('json_retrieve', 313, 'GET', '/api/vod/vodlogos/{pk}.json', 'retrieve', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 200, 200)),
        ('json_update', 313, 'PUT', '/api/vod/vodlogos/{pk}.json', 'update', 'apps.vod.api_views.VODLogoViewSet', (401, 403, 403, 200)),
    ),
}
RAW_FILES = {'receipt.json','probe.stdout','probe-execution.json','independent-cleanup.json',
             'owned-roles.json','startup-execution.json','cleanup-execution.json','project-before.json',
             'outer-state.json','outer-launcher.py','outer.log'}


def dependencies():
    return {INPUT,RECEIPT}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def scopes(kind):
    return OPERATIONS[kind]


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
    name,ordinal,method,path,action,handler,statuses = scope
    assert (op['name'],op['declaration'],op['observed_method'],op['path_template'])==(name,method,method,path)
    assert (op['resolver']['ordinal'],op['resolver']['action'],op['resolver']['handler'])==(ordinal,action,handler)
    assert re.fullmatch(re.escape(path).replace(r'\{pk\}',r'[1-9][0-9]*'),op['actual_path'])
    proof = op['proof']
    role_proof = 'role_checks_have_expected_state' if kind=='epg' else 'role_denials_preserved_state'
    assert proof['selected_sql_state_matches'] is proof[SOURCE_SCOPE[kind][3]] is proof[role_proof] is True
    assert (proof['before_sha256']!=proof['after_sha256']) is (method!='GET' and statuses[-1]!=500)
    assert all(re.fullmatch('[0-9a-f]{64}',proof[key]) for key in ('before_sha256','after_sha256'))
    assert proof.get('known_legacy_error',False) is (name in ('poster_json','programs_create'))
    assert [row['role'] for row in op['cases']]==list(ROLES)
    for row,expected in zip(op['cases'],statuses,strict=True):
        assert (row['actual_status'],row['method'],row['path'])==(expected,method,op['actual_path'])
        assert row['authentication_challenge_present'] is (expected==401)
        assert re.fullmatch('[0-9a-f]{64}',row['actual_body_sha256'])
        assert (row['actual_body_bytes']==0) is (expected in (204,404))
        if expected==500:
            assert name=='poster_json' or (name=='programs_create' and row['role']=='admin')
            assert (row['actual_content_type'],row['actual_body_sha256'],row['actual_body_bytes'])==('text/html',LEGACY_BODY,145)
        elif expected in (204,404):
            assert row['actual_body_sha256']==EMPTY_BODY and row['actual_content_type']==''
        else:
            assert row['actual_content_type']=='application/json'


def describe(kind,name):
    if name=='poster_json':
        return ['The exact Original formatted poster produced HTML 500 in all four tested roles and changed no selected SQL, schedule or queue state.']
    if name=='poster_no_slash':
        return ['The seeded programme without an image returned an empty 404 in all four tested roles.']
    if name=='programs_create':
        return ['The exact authorized formatted Programme POST returned Original HTML 500 with scoped SQL, schedule and queues unchanged.']
    if kind=='epg':
        return ['Literal formatted request, response metadata and selected EPG SQL changes or preservation with owned role, schedule and queue assertions.']
    if name=='json_list':
        return ['The literal formatted movies-linked no-pagination query filtered results to the owned movies-linked logo.']
    return ['Literal VOD logo request with owned SQL, logo files and role-denial effects recorded.']


def build(receipt):
    assert receipt['scope']=='retained_Original_EPG_library_and_VOD_logo_HTTP'
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
            name,ordinal,method,_,action,_,statuses = scope
            resolver = op['resolver']
            contracts.append({
                'contract_id':op['contract_id'],'fixture_id':fixture,'resolver_ordinal':ordinal,
                'resolver_name':resolver['view_name'],'pattern_exact':resolver['pattern'],
                'source_handler':resolver['handler'],'owning_milestone':'M5','method':method,'source_action':action,
                'observed_methods':[method],'observed_roles':list(ROLES),
                'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in ROLES},
                'assertions':describe(kind,name),'fully_verified':False,'cases':op['cases'],
                'result':{'Source_case_observations':4,'new_Source_cases_this_task':0,'Native_coverage_increased':False,
                          'known_Original_legacy_error_cells':statuses.count(500)},
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


def restore_pre_epg_vod_logo(root,rows):
    selected = {row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            key = operation['contract_id']
            if key in selected:
                assert operation.pop('executable_Source_contract')==selected[key] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}
