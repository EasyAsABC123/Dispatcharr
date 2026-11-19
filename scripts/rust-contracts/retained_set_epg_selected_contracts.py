"""Validate the two retained single Set EPG controls from packaged Candidate85."""
import re

from source_contracts import checked

KIND = 'retained_original_selected_single_Set_EPG_HTTP'
INPUT = 'scripts/rust-contracts/retained-set-epg-selected/bindings.json'
SHA256 = 'c88dc997a23bc1ee627afabd3c9a7f4ef532ca81b7d28575d23bed45261c06c3'
CONTRACT = 'route-d52bb764a9f93bf486e8:POST'
PATTERN = 'api/channels/^channels/(?P<pk>[^/.]+)/set-epg/$'
PATH = '/api/channels/channels/{fixture_channel_id}/set-epg/'
ROUTE_ORDINAL = 105
SELECTED_PUBLISHED = 'scripts/rust-integration/milestone-batch85/selected-guide-compose.json'
BATCH_PUBLISHED = 'scripts/rust-integration/milestone-batch91/acceptance.json'


def dependencies():
    return {INPUT, BATCH_INPUT, SELECTED_PUBLISHED, BATCH_PUBLISHED}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def digest(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def validate(root, value):
    assert value==load(root) and value['binding_kind']==KIND
    assert value['operation_count']==len(value['fixtures'])==len(value['contracts'])==1
    assert value['new_Source_executions']==value['new_Native_executions']==value['fully_verified_operations']==0
    assert value['Native_coverage_increased'] is value['performance_verified'] is False
    [contract]=value['contracts']
    fixture=value['fixtures'][contract['fixture_id']]
    assert fixture['Original_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert fixture['Native_revision']=='85d7db9b316bb2ad5de79014a9e5321f77579c96'
    assert fixture['Native_image']=='sha256:5ff38d1002aa58ee18ca7dad36aeaa42dd17bdec36af5fc77e0c6fe4fc5c885f'
    assert fixture['actual_runner_exit']==0 and fixture['exact_owned_containers_absent']==5
    assert all(fixture[key] is True for key in ('both_pipes_closed','process_group_settled','exact_cleanup_passed','selected_producer_scope'))
    assert fixture['endpoint_authority']=={'actual_monitor_sha256':'39a686ce270e89113db9f68e9faf687714d58f1c0a019d4f123a79f5145c9ac0',
                                           'resolver_ordinal':ROUTE_ORDINAL,'method':'POST'}
    published=fixture['exact_packaged_receipt']
    assert published['path']==SELECTED_PUBLISHED
    paired=checked(root,published)
    assert paired['scope']=='paired_set_epg_selected_xml_and_orphan_producer'
    assert paired['candidate_revision']==fixture['Native_revision'] and paired['candidate_image']==fixture['Native_image']
    assert paired['actual_exit']==fixture['actual_runner_exit']==0
    assert paired['receipt_sha256']==published['retained_receipt_sha256']=='d38fc2273505aab515c7baffb23b3bafe299421101225777a3931b664bf0e8a3'
    assert paired['independent_cleanup_sha256']==fixture['independent_cleanup_sha256']=='bcf836073c3ab16b72d317f7a3b6255c0809a29689266d45ffa8bf24dff65564'
    assert paired['owned_containers_absent']==fixture['exact_owned_containers_absent']==5
    assert paired['cleanup_verified'] is paired['loopback_version_origin_verified'] is paired['sql_preservation_verified'] is True
    assert paired['application_check_count']==4 and paired['all_captured_runner_checks']==22
    assert paired['original_and_native_xml_status']==200 and paired['original_and_native_orphan_status']==400
    assert fixture['integration_report_sha256']=='6f144f0e7a5a8f8495fcae795586ca689e5a0dc39ad0c7cce5e7f72fcb8a8aaa'
    assert all(digest(item) for item in (published['sha256'],fixture['execution_sha256']))
    assert (contract['contract_id'],contract['method'],contract['source_action'],contract['owning_milestone'])==(CONTRACT,'POST','set_epg','M5')
    assert contract['pattern_exact']==PATTERN and contract['source_handler']=='apps.channels.api_views.ChannelViewSet'
    assert contract['observed_methods']==['POST'] and contract['observed_roles']==['admin']
    assert contract['source_role_specs']=={'admin':{'fixture_role_or_supplied_credential':'admin','complete':False}}
    assert contract['fully_verified'] is False
    assert contract['result']=={'Source_case_observations':2,'Native_scoped_controls_matched':2,
                                'new_Source_cases_this_task':0,'Native_coverage_increased':False}
    expected=[('xmltv',200,18,20),('orphan',400,19,21)]
    for case,(name,status,original,native) in zip(contract['cases'],expected,strict=True):
        assert case['case']==name and case['status']==status and case['role']=='admin' and case['method']=='POST'
        assert case['path_template']==PATH and case['literal_numeric_path_recorded'] is False
        assert case['Original_evidence_pointer']==f'/checks/{original}/details'
        assert case['Native_evidence_pointer']==f'/checks/{native}/details'
        assert all(digest(case[f'{side}_{kind}_sha256']) for side in ('Original','Native')
                   for kind in ('body','publication_frame'))
        assert (case['Original_body_sha256']==case['Native_body_sha256']) is (name=='orphan')
        assert case['Original_publication_frame_sha256']!=case['Native_publication_frame_sha256']
        assert case['scoped_observations']=={
            'only_target_fk_changed_and_sequences_preserved':True,
            'exact_redis_publication':True,'cache_increment_precedes_publication':True,
            'monitor_fences_and_thread_verified':True,'response_matches_normal_contract':True,
            'publication_observed_count':1,
        }


def restore_pre_set_epg_selected(root, rows):
    """Reverse only the new primary assignment for historical preservation tests."""
    [operation]=[op for route in rows for op in route['operations'] if op['contract_id']==CONTRACT]
    [contract]=load(root)['contracts']
    assert operation.pop('executable_Source_contract')==contract and not operation['related_evidence']
    operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
    operation['role_matrix']={'state':'unmapped','required_roles':[]}


BATCH_KIND = 'retained_original_batch_Set_EPG_HTTP'
BATCH_INPUT = 'scripts/rust-contracts/retained-set-epg-batch/bindings.json'
BATCH_SHA256 = 'eeb42868ab4b8c0d28461c21d2ddee560692a095051bb609e3270d809c39dca0'
BATCH_CONTRACT = 'route-d76a10f6c622ba6fa4ad:POST'
BATCH_PATTERN = 'api/channels/^channels/batch-set-epg/$'
BATCH_PATH = '/api/channels/channels/batch-set-epg/'
BATCH_CASES = (
    ('anonymous-before-parse','anonymous',401),('standard','standard',403),('viewer','streamer',403),
    ('inactive-admin','admin',401),('malformed-json','admin',400),('missing-associations','admin',400),
    ('empty-associations','admin',400),('null-associations','admin',400),('ignore-unknown','admin',200),
    ('assign-two-with-unknown','admin',200),('repeat-assignment','admin',200),('last-association-wins','admin',200),
    ('missing-guide-clears','admin',200),('collection-ignores-filter','admin',200),
    ('assign-across-original-batches','admin',200),('clear-across-original-batches','admin',200),
)


def load_batch(root):
    return checked(root, {'path': BATCH_INPUT, 'sha256': BATCH_SHA256})


def validate_batch(root, value):
    assert value==load_batch(root) and value['binding_kind']==BATCH_KIND
    assert value['operation_count']==len(value['fixtures'])==len(value['contracts'])==1
    assert value['new_Source_executions']==value['new_Native_executions']==value['fully_verified_operations']==0
    assert value['Native_coverage_increased'] is value['performance_verified'] is False
    [contract]=value['contracts']
    fixture=value['fixtures'][contract['fixture_id']]
    assert fixture['Original_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert fixture['Native_revision']=='695779244a9dffb477ecb885ebbb19791d3a5021'
    assert fixture['Native_image']=='sha256:4a287f12d9d0831ccb4b89eddcedcb8a037fa02930641356daa58713373d18fa'
    assert fixture['endpoint_authority']=={'actual_probe_sha256':'a82d7093d62e731cdc9676f4f7e4f2b1a2e01acf515502f21730e6512125d607',
                                           'resolver_ordinal':73,'method':'POST'}
    assert fixture['receipt_sha256']=='9b707e858c7f969618dd3a8b189cc61fd220c74da7cd05c26f028873e67a084d'
    assert fixture['cleanup_sha256']=='e205c1db5e89a420853b26fecbf92a203b8b969b618a32af999abbd2fb191cd5'
    assert all(digest(fixture[key]) for key in ('exact_build_sha256','exact_fixture_inputs_sha256','binding_sha256',
                                               'execution_sha256','integration_report_sha256'))
    assert fixture['actual_runner_exit']==0 and fixture['exact_owned_containers_absent']==4
    assert fixture['foundation_checks_excluded']==18
    assert all(fixture[key] is True for key in ('both_pipes_closed','process_group_settled','exact_cleanup_passed'))
    reference=fixture['exact_published_acceptance']
    assert reference['path']==BATCH_PUBLISHED
    acceptance=checked(root,reference)
    assert acceptance['original_revision']==fixture['Original_revision']
    assert acceptance['candidate_revision']==fixture['Native_revision']
    assert acceptance['package']['status']=='passed' and acceptance['package']['actual_exit']==0
    assert acceptance['package']['image_id']==fixture['Native_image']
    assert acceptance['package']['manifest_sha256']==fixture['exact_build_sha256']
    assert acceptance['full_m5_accepted'] is acceptance['performance_accepted'] is False
    evidence=acceptance['M5']
    assert evidence['status']=='passed' and evidence['actual_exit']==fixture['actual_runner_exit']==0
    assert evidence['cleanup_verified'] is evidence['process_settled'] is True
    assert evidence['retired_container_generations']==fixture['exact_owned_containers_absent']
    assert evidence['foundation_checks']==fixture['foundation_checks_excluded']
    assert evidence['matched_requests']==2*len(BATCH_CASES)
    assert evidence['sources_sha256']=={
        'binding.json':fixture['binding_sha256'],'evidence/integration.json':fixture['integration_report_sha256'],
        'execution.json':fixture['execution_sha256'],'independent-cleanup.json':fixture['cleanup_sha256'],
        'receipt.json':fixture['receipt_sha256'],
    }
    public_cases=evidence['case_results']
    assert len(public_cases)==34
    guard=fixture['Native_publisher_absent_guard']
    assert guard['case']=='publisher-absent-atomic-guard' and guard['status']==501
    assert guard['role']=='admin' and guard['method']=='POST' and guard['path']==BATCH_PATH
    assert guard['Native_evidence_pointer']=='/checks/51/details' and digest(guard['Native_body_sha256'])
    assert guard['sql_cache_broker_unchanged'] is True and guard['Native_guide_publication_verified'] is False
    assert guard['status']==evidence['native_atomic_unsupported_batch_guard_status']
    assert guard['Native_guide_publication_verified'] is evidence['native_batch_guide_publication_verified'] is False
    assert public_cases['native:publisher-absent-atomic-guard']=={
        'actual_status':guard['status'],'actual_body_sha256':guard['Native_body_sha256'],'sql_cache_broker_match':True,
    }
    assert (contract['contract_id'],contract['method'],contract['source_action'],contract['owning_milestone'])==(BATCH_CONTRACT,'POST','batch_set_epg','M5')
    assert contract['pattern_exact']==BATCH_PATTERN and contract['source_handler']=='apps.channels.api_views.ChannelViewSet'
    roles=['anonymous','streamer','standard','admin']
    assert contract['observed_methods']==['POST'] and contract['observed_roles']==roles
    assert contract['source_role_specs']=={role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles}
    assert contract['fully_verified'] is False
    assert contract['result']=={'Source_case_observations':17,'Native_scoped_controls_matched':16,
                                'new_Source_cases_this_task':0,'Native_coverage_increased':False}
    assert len(contract['cases'])==17
    shared=contract['cases'][:-1]
    for index,(case,(name,role,status)) in enumerate(zip(shared,BATCH_CASES,strict=True)):
        assert (case['case'],case['role'],case['status'])==(name,role,status)
        assert case['method']=='POST' and case['path']==BATCH_PATH and case['sql_cache_broker_match'] is True
        assert case['Original_evidence_pointer']==f'/checks/{18+index}/details'
        assert case['Native_evidence_pointer']==f'/checks/{35+index}/details'
        assert digest(case['Original_response_and_Native_response_sha256'])
        expected={'actual_status':status,'actual_body_sha256':case['Original_response_and_Native_response_sha256'],
                  'sql_cache_broker_match':case['sql_cache_broker_match']}
        assert public_cases[f'source:{name}']==public_cases[f'native:{name}']==expected
        if name=='collection-ignores-filter':
            assert case['request_query_template']=='channel_profile_id={fixture_profile_id}'
        else:
            assert 'request_query_template' not in case
    original=contract['cases'][-1]
    assert original['case']=='source-xmltv-publication' and original['status']==200 and original['role']=='admin'
    assert original['method']=='POST' and original['path']==BATCH_PATH and original['Original_evidence_pointer']=='/checks/34/details'
    assert original['Original_exact_FK_changes_and_single_task_checked'] is True and digest(original['Original_body_sha256'])
    assert not any(key.startswith('Native_') for key in original)
    assert original['status']==evidence['original_only_xmltv_publication_status']
    assert public_cases['source:source-xmltv-publication']=={
        'actual_status':original['status'],'actual_body_sha256':original['Original_body_sha256'],'sql_cache_broker_match':True,
    }


def restore_pre_epg_assignments(root, rows):
    restore_pre_set_epg_selected(root, rows)
    [operation]=[op for route in rows for op in route['operations'] if op['contract_id']==BATCH_CONTRACT]
    [contract]=load_batch(root)['contracts']
    assert operation.pop('executable_Source_contract')==contract and not operation['related_evidence']
    operation.update(setup_fixture=None, assertions=[], result=None, coverage_state='not_yet_mapped')
    operation['role_matrix']={'state':'unmapped','required_roles':[]}
