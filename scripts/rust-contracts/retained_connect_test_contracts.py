"""Validate one retained, paired saved Connect webhook Test operation."""
import re

from source_contracts import checked

INPUT = 'scripts/rust-contracts/retained-connect-test/bindings.json'
SHA256 = 'a6b410fd131bce638c0ce4e05f590173d293ddcafb52c68cc38e66b246e1ebbd'
KIND = 'retained_original_Connect_saved_webhook_test_HTTP'
CONTRACT = 'route-dc9e8fb9408ad091d048:POST'
ROUTE_NAME = 'api:connect:integration-test'
PATH = '/api/connect/integrations/{fixture_integration_id}/test/'
PATTERN = 'api/connect/^integrations/(?P<pk>[^/.]+)/test/$'
ORIGINAL = 'scripts/rust-integration/milestone-batch84/packaged.json'
NATIVE = 'scripts/rust-integration/milestone-batch89/packaged.json'
ROLES = ['anonymous', 'streamer', 'standard', 'admin']
CONTROLS = {
    'anonymous': ('anonymous', 'owned_accepted_webhook', 401),
    'standard': ('standard', 'owned_accepted_webhook', 403),
    'viewer': ('streamer', 'owned_accepted_webhook', 403),
    'inactive-admin': ('admin', 'owned_accepted_webhook', 401),
    'role-before-lookup': ('standard', 'missing_integration', 403),
    'missing': ('admin', 'missing_integration', 404),
    'accepted': ('admin', 'owned_accepted_webhook', 200),
    'rejected': ('admin', 'owned_rejected_webhook', 200),
}


def dependencies():
    return {INPUT, ORIGINAL, NATIVE}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def is_sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def validate(root, value):
    assert value == load(root) and value['binding_kind'] == KIND
    assert value['operation_count'] == len(value['fixtures']) == len(value['contracts']) == 1
    assert value['retained_scoped_checks_per_backend'] == 9 and value['retained_HTTP_observations_per_backend'] == 8
    assert value['fully_verified_operations'] == value['new_Source_executions'] == value['new_Native_executions'] == 0
    assert value['Native_coverage_increased'] is value['performance_verified'] is False
    assert [ref['path'] for ref in value['public_authorities']] == [ORIGINAL, NATIVE]
    original_record, native = (checked(root, ref) for ref in value['public_authorities'])
    original = original_record['original_connect_test']
    assert original['probe_exit'] == original['compose_down_exit'] == native['native_probe_exit'] == native['compose_down_exit'] == 0
    assert original['internal_network'] and original['cleanup_verified']
    assert native['internal_network_verified'] and native['exact_cleanup_verified'] and native['source_calls_replayed'] == 0
    assert original['checks_passed'] == 9 and original['owned_receiver_requests'] == 2
    assert native['matched_original_controls'] == dict.fromkeys([*CONTROLS, 'owned-receiver-total'], True)

    [contract] = value['contracts']
    fixture = value['fixtures'][contract['fixture_id']]
    assert fixture['Original_revision'] == original['revision'] == native['original_revision']
    assert fixture['Native_revision'] == native['candidate_revision'] and fixture['Native_image'] == native['candidate_image']
    assert fixture['Original_terminal']['receipt_sha256'] == original['receipt_sha256'] == native['original_authority_sha256']
    assert fixture['Native_terminal']['receipt_sha256'] == native['native_receipt_sha256']
    for reference, count in (('Original_terminal', 2), ('Native_terminal', 4)):
        terminal = fixture[reference]
        assert terminal['actual_probe_exit'] == terminal['actual_compose_down_exit'] == 0
        assert terminal['exact_cleanup_verified'] is terminal['internal_network_verified'] is True
        assert terminal['owned_CID_and_generation_absence_count'] == count
        assert all(is_sha(terminal[field]) for field in ('cleanup_sha256', 'isolation_sha256'))
    assert fixture['Native_original_web_stopped_verified'] and fixture['Original_replayed'] is False
    assert fixture['retained_scoped_checks'] == 9 and fixture['aggregate_owned_receiver_requests_per_backend'] == 2
    endpoint = fixture['endpoint_authority']
    assert endpoint['canonical_path_template'] == PATH and endpoint['method'] == 'POST'
    assert endpoint['resolver_ordinal'] == 340 and endpoint['resolver_name'] == ROUTE_NAME
    assert endpoint['probe_only_backend_base_URL_changed'] is True
    assert all(is_sha(endpoint[f'{backend}_{asset}_sha256'])
               for backend in ('original', 'native') for asset in ('probe', 'seal'))

    assert contract['contract_id'] == CONTRACT and contract['method'] == 'POST' and contract['source_action'] == 'test'
    assert contract['pattern_exact'] == PATTERN and contract['source_handler'] == 'apps.connect.api_views.IntegrationViewSet'
    assert contract['owning_milestone'] == 'M5' and contract['fully_verified'] is False
    assert contract['observed_methods'] == ['POST'] and contract['observed_roles'] == ROLES
    assert contract['source_role_specs'] == {
        role: {'fixture_role_or_supplied_credential': role, 'complete': False} for role in ROLES
    }
    assert contract['result'] == {'Source_case_observations': 8, 'Native_scoped_controls_matched': 9,
                                  'new_Source_cases_this_task': 0, 'Native_coverage_increased': False}
    assert [case['case'] for case in contract['cases']] == list(CONTROLS)
    for index, case in enumerate(contract['cases']):
        role, resource, status = CONTROLS[case['case']]
        assert case['role'] == role and case['method'] == 'POST' and case['status'] == status
        assert case['path_template'] == PATH.replace('fixture_integration_id', f'fixture_{resource}_id')
        assert case['literal_numeric_path_recorded'] is False
        assert case['sanitized_original_receipt_pointer'] == case['sanitized_native_receipt_pointer'] == f'/checks/{index}/details'
        original_hash, native_hash = case['Original_body_sha256'], case['Native_body_sha256']
        assert is_sha(original_hash) and is_sha(native_hash)
        effects = case['scoped_observations']
        if status == 200:
            assert original_hash != native_hash
            assert is_sha(case['Original_receiver_body_sha256']) and is_sha(case['Native_receiver_body_sha256'])
            assert case['Original_receiver_body_sha256'] != case['Native_receiver_body_sha256']
            content_type = 'application/x-www-form-urlencoded' if case['case'] == 'accepted' else 'application/json'
            assert effects == {'sql_and_sequences_unchanged': True, 'receiver_increment': 1,
                               'dummy_payload_exact_in_request_window': True,
                               'one_form_encoded_dispatch_with_expected_headers': True,
                               'receiver_content_type': content_type,
                               'returned_exact_receiver_status_body_and_boolean': True}
        else:
            assert original_hash == native_hash
            assert effects == {'sql_and_sequences_unchanged': True, 'owned_receiver_empty': True}


def restore_pre_connect_test(root, rows):
    """Remove only the primary Test assignment before historical comparisons."""
    [operation] = [o for route in rows for o in route['operations'] if o['contract_id'] == CONTRACT]
    [contract] = load(root)['contracts']
    assert operation.pop('executable_Source_contract') == contract and not operation['related_evidence']
    operation.update(setup_fixture=None, assertions=[], result=None, coverage_state='not_yet_mapped')
    operation['role_matrix'] = {'state': 'unmapped', 'required_roles': []}
