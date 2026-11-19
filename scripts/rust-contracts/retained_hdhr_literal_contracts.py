"""Validate the retained Original literal HDHomeRun HTTP cohort."""
import re

from source_contracts import checked

KIND = 'retained_original_HDHomeRun_literal_devices_and_discovery_HTTP'
INPUT = 'scripts/rust-contracts/retained-hdhr-literal/bindings.json'
INPUT_SHA = 'a0e02bb16edf8e189e8890c515e088e192412db7b42ea17f7d511ee574f46c0c'
RECEIPT = 'scripts/rust-integration/retained-hdhr-literal/receipt.json'
RECEIPT_SHA = '4ed813daa6dce65d795888184883ec5bf6448577f8f2a0c9f048e7334e950115'
FIXTURE = 'retained-Original-HDHomeRun-literal-devices-and-discovery'
SOURCE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
SEAL = '6e0ae51cc877c22498eb795e172fc95416c93cdd62cc698930e248c2c658d606'
IMAGE = 'sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
GROUPS = (
    ('api_canonical',177,179,'/api/hdhr/devices/','/api/hdhr/devices/{pk}/'),
    ('api_json_suffix',178,180,'/api/hdhr/devices.json','/api/hdhr/devices/{pk}.json'),
    ('public_json_suffix',379,381,'/hdhr/devices.json','/hdhr/devices/{pk}.json'),
)
WRITES = (('DELETE','destroy',204), ('PATCH','partial_update',200), ('POST','create',201), ('PUT','update',200))
PUBLIC = (
    ('profile_output_discovery',365,'/hdhr/{profile}/output_profile/{output}/discover.json','DiscoverAPIView'),
    ('profile_output_lineup_status',367,'/hdhr/{profile}/output_profile/{output}/lineup_status.json','LineupStatusAPIView'),
    ('output_lineup_status',370,'/hdhr/output_profile/{output}/lineup_status.json','LineupStatusAPIView'),
)


def dependencies():
    return {INPUT,RECEIPT}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def scopes():
    selected = []
    for family,collection,detail,list_path,detail_path in GROUPS:
        for method,action,positive in WRITES:
            ordinal,path = (collection,list_path) if method=='POST' else (detail,detail_path)
            selected.append((family+'_'+method.lower(),ordinal,method,method,path,action,'HDHRDeviceViewSet',positive))
    for name,ordinal,path,handler in PUBLIC:
        selected.append((name,ordinal,None,'GET',path,None,handler,200))
    return sorted(selected)


def qualification(receipt):
    assert receipt['scope']=='retained_Original_HDHomeRun_literal_device_and_discovery_HTTP'
    assert (receipt['source_revision'],receipt['source_archive_sha256'],receipt['fixture_sha256'],receipt['image_id'])==(SOURCE,ARCHIVE,SEAL,IMAGE)
    assert receipt['outer_actual_exit']==0 and receipt['physical_cleanup'] is True
    assert receipt['owned_roles_retired']==['baseline','origin']
    assert set(receipt['actual_child_results'])=={'startup','probe','cleanup'}
    for row in receipt['actual_child_results'].values():
        assert row=={'actual_exit':0,'stdout_EOF':True,'stderr_EOF':True,'process_group_settled':True}
    assert receipt['selected_operations']==len(receipt['rows'])==15 and receipt['selected_cases']==57
    assert sum(len(row['cases']) for row in receipt['rows'])==57
    assert receipt['fully_verified_operations']==0
    assert all(receipt[key] is False for key in ('Native_verified','full_role_matrix','performance_verified'))
    assert all(re.fullmatch('[0-9a-f]{64}',value) for value in receipt['raw_fingerprints'].values())


def observed(row, scope):
    name,ordinal,declared,method,template,action,handler,status = scope
    assert (row['name'],row['declaration'],row['observed_method'],row['literal_path_template'])==(name,declared,method,template)
    identity = row['resolver']
    assert (identity['ordinal'],identity['action'],identity['handler'])==(ordinal,action,'apps.hdhr.api_views.'+handler)
    pattern = re.escape(template).replace(r'\{pk\}',r'[1-9][0-9]*').replace(r'\{output\}',r'[1-9][0-9]*')
    pattern = pattern.replace(r'\{profile\}','owned-m0-hdhr-residual')
    assert re.fullmatch(pattern,row['actual_path'])
    public = declared is None
    expected = {'allowed_network':('anonymous',200),'allowed_network_standard':('standard',200),
                'denied_network':('anonymous',403)} if public else {
        'admin':('admin',status),'anonymous':('anonymous',401),'standard':('standard',403),'viewer':('viewer',403)}
    assert len(row['cases'])==len(expected) and {case['label'] for case in row['cases']}==expected.keys()
    for case in row['cases']:
        assert (case['role'],case['actual_status'])==expected[case['label']]
        assert (case['path'],case['method'])==(row['actual_path'],method)
        assert re.fullmatch('[0-9a-f]{64}',case['actual_body_sha256'])
        assert (case['actual_body_bytes']==0) is (case['actual_status']==204)
        if case['actual_status']!=204:
            assert case['actual_content_type']=='application/json'
    proof = row['proof']
    assert proof['selected_sql_state_matches'] is proof['role_denials_preserved_state'] is True
    assert (proof['before_sha256']==proof['after_sha256']) is public
    assert all(re.fullmatch('[0-9a-f]{64}',proof[key]) for key in ('before_sha256','after_sha256'))
    return sorted({role for role,_ in expected.values()})


def build(receipt, rows):
    qualification(receipt)
    by_ordinal = {row['resolver_ordinal']:row for row in rows}
    contracts = []
    for row,scope in zip(receipt['rows'],scopes(),strict=True):
        roles = observed(row,scope)
        name,ordinal,declared,method,path,action,_,_ = scope
        route = by_ordinal[ordinal]
        [operation] = [op for op in route['operations'] if op['method']==declared]
        assert (operation['contract_id'],operation['source_action'],operation['owning_milestone'])==(row['contract_id'],action,'M7')
        assert (route['pattern_exact'],route['name'],route['source_handler'])==tuple(
            row['resolver'][key] for key in ('pattern','view_name','handler'))
        if declared:
            assertions = ['Exact selected Administrator response and owned HDHomeRun device SQL transition.',
                          'Unauthenticated and lower-role requests preserved all scoped rows.']
        else:
            assertion = ('Exact owned device projection, tuner field and discovery URLs.' if name=='profile_output_discovery'
                         else 'Exact owned lineup status response.')
            assertions = [assertion,'Anonymous and standard loopback allow, anonymous network denial, and restored scoped SQL.']
        contracts.append({
            'contract_id':operation['contract_id'],'fixture_id':FIXTURE,'resolver_ordinal':ordinal,
            'resolver_name':route['name'],'pattern_exact':route['pattern_exact'],
            'source_handler':route['source_handler'],'owning_milestone':'M7','method':declared,'source_action':action,
            'observed_roles':roles,'observed_methods':[method],
            'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles},
            'assertions':assertions,'fully_verified':False,'cases':row['cases'],
            'result':{'Source_case_observations':len(row['cases']),'new_Source_cases_this_task':0,
                      'Native_coverage_increased':False},
        })
    fixture = {'id':FIXTURE,'source_revision':SOURCE,'source_image':IMAGE,'actual_controller_exit':0,
               'exact_owned_namespace_cleanup':True,'public_artifacts':[{'path':RECEIPT,'sha256':RECEIPT_SHA}]}
    return {'binding_kind':KIND,'operation_count':len(contracts),'fully_verified_operations':0,
            'Native_coverage_increased':False,'performance_verified':False,'credit':receipt['credit'],
            'fixtures':{FIXTURE:fixture},'contracts':contracts}


def generate(receipt,register):
    result = build(receipt,register['runtime_routes'])
    previous = {op['contract_id']:op for row in register['runtime_routes'] for op in row['operations']}
    assert all('executable_Source_contract' not in previous[row['contract_id']] for row in result['contracts'])
    return result


def validate(root,packet):
    assert packet==load(root)
    receipt = checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA})
    # Rebuild the projected contract shape independently from the captured route identities.
    from itertools import groupby
    rows = []
    for ordinal, group in groupby(sorted(receipt['rows'],key=lambda row:row['resolver']['ordinal']),
                                  key=lambda row:row['resolver']['ordinal']):
        inputs = list(group)
        identity = inputs[0]['resolver']
        assert all(tuple(item['resolver'][k] for k in ('pattern','view_name','handler'))==
                   tuple(identity[k] for k in ('pattern','view_name','handler')) for item in inputs)
        rows.append({'resolver_ordinal':ordinal,'pattern_exact':identity['pattern'],
                     'name':identity['view_name'],'source_handler':identity['handler'],
                     'operations':[{'contract_id':item['contract_id'],'method':item['declaration'],
                                    'source_action':item['resolver']['action'],'owning_milestone':'M7'} for item in inputs]})
    assert packet==build(receipt,rows)


def restore_pre_literal_hdhr(root,rows):
    selected = {row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            key = operation['contract_id']
            if key in selected:
                assert operation.pop('executable_Source_contract')==selected[key] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}
