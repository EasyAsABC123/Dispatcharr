"""Bind five exact retained C98 Original HDHomeRun and React resolver rows."""
from source_contracts import checked

KIND = 'retained_original_C98_HDHomeRun_ingress_HTTP'
INPUT = 'scripts/rust-contracts/retained-c98-hdhr/bindings.json'
INPUT_SHA = 'fce9f713bc1d4176ca7b0329fbf2365483e694d93806aeff4f07b669f912698e'
RECEIPT = 'scripts/rust-integration/retained-c98-hdhr/receipt.json'
RECEIPT_SHA = 'cc27d4d608bcd4526e3d1f844d0d0b05086565690669e6f3553cd74797aa89c9'
AUTH = {'cohort':'scripts/rust-integration/milestone-batch98/acceptance.json'}
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
CANDIDATE = '452a7130dce1b96733dfcb06f04b7b7088688bbd'
FIXTURE = 'retained-Candidate98-paired-HDHR-and-React'
INVALID = 'presented_invalid_bearer'
HDHR = 'apps.hdhr.api_views.HDHRDeviceViewSet'
OPERATIONS = {
    177: ('GET','list','M7','api/hdhr/^devices/$','api:hdhr:hdhr-device-list',HDHR,
          'Standard credential returns the exact owned canonical device list'),
    179: ('GET','retrieve','M7',r'api/hdhr/^devices/(?P<pk>[^/.]+)/$','api:hdhr:hdhr-device-detail',HDHR,
          'Standard credential receives the exact owned detail and missing-device 404'),
    180: ('GET','retrieve','M7',r'api/hdhr/^devices/(?P<pk>[^/.]+)\.(?P<format>[a-z0-9]+)/?$','api:hdhr:hdhr-device-detail',HDHR,
          'Standard credential receives the exact literal JSON-suffix device detail'),
    364: (None,None,'M8','hdhr','','django.views.generic.base.RedirectView',
          'Bare /hdhr GET redirects permanently to /hdhr/ and drops the fixture query'),
    590: (None,None,'M8','<path:unused_path>','','django.views.generic.base.TemplateView',
          'Six exact slashless HTTP requests reach the unchanged SPA catch-all with the supplied invalid bearer'),
}
CASE_ROWS = (
    (177,'canonical-collection','GET','standard',200,'/api/hdhr/devices/'),
    (179,'canonical-detail','GET','standard',200,'/api/hdhr/devices/{owned_pk}/'),
    (179,'canonical-missing-detail','GET','standard',404,'/api/hdhr/devices/999999/'),
    (180,'json-suffix-detail','GET','standard',200,'/api/hdhr/devices/{owned_pk}.json'),
    (364,'explicit-public-root-redirect','GET','standard',301,'/hdhr?fixture=ignored'),
    (590,'api-root-slashless','GET',INVALID,200,'/api/hdhr?format=unknown'),
    (590,'public-device-list-slashless','GET',INVALID,200,'/hdhr/devices'),
    (590,'api-device-slashless','GET',INVALID,200,'/api/hdhr/devices/{owned_pk}?format=json&fixture=owned'),
    (590,'api-missing-device-slashless','GET',INVALID,200,'/api/hdhr/devices/owned-missing'),
    (590,'api-device-slashless-head','HEAD',INVALID,200,'/api/hdhr/devices/{owned_pk}'),
    (590,'api-device-slashless-range','GET',INVALID,200,'/api/hdhr/devices/{owned_pk}'),
)


def dependencies():
    return {INPUT,RECEIPT,*AUTH.values()}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def validate(root,packet):
    assert packet==load(root)
    receipt=checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA})
    ref=receipt['public_cohort_authority']
    assert ref['path']==AUTH['cohort']
    actual=checked(root,ref)
    scope=actual['scope_results']['hdhr-react']
    assert scope['scope_passed'] and scope['exact_cleanup'] and scope['checks_observed']==42 and not scope['failed_checks']
    assert actual['candidate_revision']==receipt['candidate_revision']==CANDIDATE
    assert actual['image_id']==receipt['candidate_image']
    assert receipt['source_revision']==ORIGINAL and receipt['settled_process_group_and_EOF']
    assert receipt['exact_owned_cleanup'] and receipt['owned_container_generations']==4
    assert scope['actual_exit']==scope['actual_child_exit']==receipt['actual_controller_exit']==receipt['actual_child_exit']==0
    assert scope['evidence_sha256']==receipt['captured_report_sha256']
    assert receipt['new_Source_or_Native_executions']==receipt['fully_verified_operations']==0
    for row,(ordinal,name,method,role,status,path) in zip(receipt['rows'],CASE_ROWS,strict=True):
        assert (row['resolver_ordinal'],row['name'],row['method'],row['role'],row['path_template'])==(ordinal,name,method,role,path)
        assert row['selected_wire_and_sql_unchanged'] and row['source']==row['native']
        assert row['source']['status']==status and len(row['source']['body_sha256'])==64
    assert packet['binding_kind']==KIND and packet['operation_count']==len(packet['contracts'])==5
    assert packet['fully_verified_operations']==packet['new_Source_executions']==0
    assert packet['Native_coverage_increased'] is packet['performance_verified'] is False
    for contract,(ordinal,(method,action,owner,pattern,name,handler,assertion)) in zip(packet['contracts'],OPERATIONS.items(),strict=True):
        assert (contract['resolver_ordinal'],contract['method'],contract['source_action'],
                contract['owning_milestone'])==(ordinal,method,action,owner)
        assert (contract['resolver_name'],contract['pattern_exact'],contract['source_handler'])==(name,pattern,handler)
        cases=[row for row in receipt['rows'] if row['resolver_ordinal']==ordinal]
        assert cases==contract['cases'] and contract['assertions']==[assertion]
        roles=list(dict.fromkeys(row['role'] for row in cases))
        methods=list(dict.fromkeys(row['method'] for row in cases))
        assert contract['observed_roles']==roles and contract['observed_methods']==methods
        assert contract['source_role_specs']=={role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles}
        assert contract['fully_verified'] is False and contract['fixture_id']==FIXTURE
        assert contract['result']=={'Source_case_observations':len(cases),'retained_paired_HTTP_case_observations':len(cases),
                                    'new_Source_cases_this_task':0,'Native_coverage_increased':False}


def restore_pre_c98_hdhr(root,rows):
    contracts={row['contract_id']:row for row in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            identifier=operation['contract_id']
            if identifier in contracts:
                assert operation.pop('executable_Source_contract')==contracts[identifier] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}


def generate(receipt,register):
    fixture={'id':FIXTURE,'source_revision':ORIGINAL,'paired_candidate_revision':CANDIDATE,
             'paired_image':receipt['candidate_image'],'actual_controller_exit':0,'exact_owned_namespace_cleanup':True,
             'public_artifacts':[{'path':RECEIPT,'sha256':RECEIPT_SHA}]}
    contracts=[]
    for ordinal,(method,action,owner,pattern,name,handler,assertion) in OPERATIONS.items():
        [route]=[row for row in register['runtime_routes'] if row['resolver_ordinal']==ordinal]
        assert (route['name'],route['pattern_exact'],route['source_handler'])==(name,pattern,handler)
        [operation]=[row for row in route['operations'] if row['method']==method]
        assert 'executable_Source_contract' not in operation and operation['source_action']==action
        cases=[row for row in receipt['rows'] if row['resolver_ordinal']==ordinal]
        roles=list(dict.fromkeys(row['role'] for row in cases))
        contracts.append({
            'contract_id':operation['contract_id'],'fixture_id':FIXTURE,'pattern_exact':pattern,
            'source_handler':handler,'resolver_ordinal':ordinal,'resolver_name':name,'owning_milestone':owner,
            'method':method,'source_action':action,'observed_methods':list(dict.fromkeys(row['method'] for row in cases)),
            'observed_roles':roles,'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False}
                                                       for role in roles},
            'assertions':[assertion],'result':{'Source_case_observations':len(cases),
                'retained_paired_HTTP_case_observations':len(cases),'new_Source_cases_this_task':0,
                'Native_coverage_increased':False},'fully_verified':False,'cases':cases,
        })
    return {'binding_kind':KIND,'operation_count':len(contracts),'new_Source_executions':0,
            'credit':receipt['credit'],'fully_verified_operations':0,'Native_coverage_increased':False,
            'performance_verified':False,'fixtures':{FIXTURE:fixture},'contracts':contracts}
