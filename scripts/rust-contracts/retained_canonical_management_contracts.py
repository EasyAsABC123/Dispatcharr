"""Bind exact retained Original canonical Channel, DVR and Programme HTTP."""
import re

from source_contracts import checked

KIND = 'retained_original_canonical_management_HTTP'
INPUT = 'scripts/rust-contracts/retained-canonical-management/bindings.json'
INPUT_SHA = 'a3d92e61c9a74e1ec76e31c48ab82c3b125dc2d991c5ab61edfa8006bb050ef5'
RECEIPT = 'scripts/rust-integration/canonical-management-original/receipt.json'
RECEIPT_SHA = 'd32795ab9485bf1bdb67fc8b907faf415f82255f0b9372c8e17e714ebc479835'
FIXTURE = 'Original-owned-canonical-management-sixteen'
ROWS = {
    'stream_delete': (57,'DELETE','/api/channels/streams/{id}/','admin','standard',204),
    'channel_regex': (75,'POST','/api/channels/channels/edit/bulk-regex/','admin','standard',200),
    'logo_list': (107,'GET','/api/channels/logos/','standard','viewer',200),
    'logo_upload': (109,'POST','/api/channels/logos/upload/','admin','standard',201),
    'logo_delete': (111,'DELETE','/api/channels/logos/{id}/','admin','standard',204),
    'profile_list': (115,'GET','/api/channels/profiles/','standard','viewer',200),
    'profile_detail': (117,'GET','/api/channels/profiles/{id}/','standard','viewer',200),
    'profile_delete': (117,'DELETE','/api/channels/profiles/{id}/','admin','standard',204),
    'recurring_detail': (141,'GET','/api/channels/recurring-rules/{id}/','dvr_manager','standard',200),
    'recurring_patch': (141,'PATCH','/api/channels/recurring-rules/{id}/','dvr_manager','standard',200),
    'recurring_put': (141,'PUT','/api/channels/recurring-rules/{id}/','dvr_manager','standard',200),
    'recurring_delete': (141,'DELETE','/api/channels/recurring-rules/{id}/','dvr_manager','standard',204),
    'program_create_defect': (159,'POST','/api/epg/programs/','admin','standard',500),
    'program_patch': (163,'PATCH','/api/epg/programs/{id}/','admin','standard',200),
    'program_put': (163,'PUT','/api/epg/programs/{id}/','admin','standard',200),
    'program_delete': (163,'DELETE','/api/epg/programs/{id}/','admin','standard',204),
}
DELTAS = {
    'stream_delete': {'streams','links'},'channel_regex': {'channels'},
    'logo_upload': {'uploaded_file','uploaded_logo'},'logo_delete': {'uploaded_file','uploaded_logo'},
    'profile_delete': {'memberships','profiles','user_profiles'},
    **dict.fromkeys(('recurring_patch','recurring_put','recurring_delete'),{'rules'}),
    **dict.fromkeys(('program_patch','program_put','program_delete'),{'programs'}),
}


def dependencies():
    return {INPUT,RECEIPT}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def validate(root, packet):
    assert packet==load(root)
    receipt = checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA})
    assert packet['binding_kind']==KIND and packet['operation_count']==len(packet['contracts'])==16
    assert packet['new_Source_executions']==packet['fully_verified_operations']==0
    assert packet['Native_coverage_increased'] is packet['performance_verified'] is False
    fixture = packet['fixtures'][FIXTURE]
    assert fixture['source_revision']==receipt['source_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert fixture['source_image_id']==receipt['source_image_id']
    assert fixture['actual_launcher_exit']==receipt['actual_launcher_exit']==0
    assert fixture['exact_cleanup'] is receipt['exact_cleanup'] is True
    assert all(fixture[k] is True for k in ('stdout_EOF','stderr_EOF','process_groups_settled','exact_host_generation_cleanup'))
    assert fixture['public_artifacts']==[{'path':RECEIPT,'sha256':RECEIPT_SHA}]
    assert fixture['raw_fingerprints']==receipt['evidence_sha256']
    assert receipt['routes_qualified']==16 and set(receipt['cases'])==set(ROWS)
    assert receipt['candidate_comparison_performed'] is receipt['milestone_accepted'] is False
    assert receipt['known_source_failure']=='Program POST did not commit a row'
    assert len({c['contract_id'] for c in packet['contracts']})==16
    seen_cases=0
    for contract,(name,(ordinal,method,template,positive,denied,status)) in zip(packet['contracts'],ROWS.items(),strict=True):
        published = receipt['cases'][name]
        assert contract['batch_case']==name and contract['contract_id']==published['contract_id']
        assert contract['resolver_ordinal']==ordinal and contract['method']==method
        assert contract['pattern_exact']==published['runtime_route'] and 'format' not in contract['pattern_exact']
        assert contract['owning_milestone']=='M5' and contract['fixture_id']==FIXTURE
        assert contract['fully_verified'] is False
        observed = list(dict.fromkeys(('anonymous',denied,positive)))
        assert contract['observed_roles']==observed and contract['observed_methods']==[method]
        assert contract['source_role_specs']=={role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in observed}
        desired = {'anonymous':('anonymous',401),'role_denial':(denied,403),'owned_business_case':(positive,status)}
        if name=='profile_detail':
            desired['unassigned_profile']=('standard',404)
        if name=='program_create_defect':
            desired['serializer_required_fields']=('admin',400)
        assert len(contract['cases'])==len(desired)
        assert {case['case'] for case in contract['cases']}==set(desired)==set(published['actual'])
        assert published['owned_state_verified'] and published['qualification']=='passed'
        seen_cases += len(desired)
        pattern = re.escape(template).replace(r'\{id\}',r'[1-9][0-9]*')
        business_path = next(case['literal_path'] for case in contract['cases'] if case['case']=='owned_business_case')
        for case in contract['cases']:
            assert re.fullmatch(pattern,case['literal_path'].split('?',1)[0])
            if case['case']=='unassigned_profile':
                assert case['literal_path']!=business_path
            else:
                assert case['literal_path']==business_path
            assert case['method']==method and (case['role'],case['status'])==desired[case['case']]
            assert {('actual_status' if k=='status' else k):case[k] for k in
                    ('actual_body_bytes','actual_body_sha256','status','method','role')}==published['actual'][case['case']]
        proof = contract['owned_state']
        assert proof['role_denials_preserved_state']
        assert set(proof['changed_sections'])==DELTAS.get(name,set())
        assert (proof['before_sha256']!=proof['after_sha256'])==bool(DELTAS.get(name))
        result = contract['result']
        defect = name=='program_create_defect'
        assert result['Source_case_observations']==len(desired) and result['new_Source_cases_this_task']==0
        assert result['Native_coverage_increased'] is False and result['known_Source_failure'] is defect
        assert result['actual_business_status']==status and result['Source_business_success'] is (not defect)
        if defect:
            assert proof['committed_create_row'] is False
    assert seen_cases==50


def restore_pre_management(root, rows):
    contracts = {contract['contract_id']:contract for contract in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            identifier = operation['contract_id']
            if identifier in contracts:
                assert operation.pop('executable_Source_contract')==contracts[identifier] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}
