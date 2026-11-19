"""Compile a Source-bound, reviewable acceptance requirement proposal."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile

from materialize import ROOT, load_recipe, verify

MATRIX_SEAL='ba55511baad9857507254324e8784e64371dfb9b54871fbf513af4f08d29aeb6'
MATRIX_PLAN='d62d1628cc6f6360e4acfb554bf1055516c6c029145f96b717e0fa3801122cbb'

AUTHORITY_FILES=(
    'apps/connect/api_urls.py','apps/connect/api_views.py','apps/connect/models.py',
    'apps/connect/serializers.py','apps/accounts/models.py','apps/accounts/permissions.py',
    'apps/accounts/authentication.py','dispatcharr/settings.py',
)

EXTRA_AUTH=(
    'inactive_api_key_before_failure_rate_limit',
    'viewer_x_api_key','standard_x_api_key','admin_authorization_apikey',
    'unknown_api_key_before_failure_rate_limit','expired_bearer',
    'invalid_bearer_precedes_valid_api_key',
    'allowed_admin_on_denied_ui_network',
)

COMMON_PHYSICAL=(
    'exact_original_archive_and_native_image',
    'actual_settled_runner_with_complete_stdout_and_stderr',
    'owned_request_targets_and_authoritative_response_projection',
    'no_owned_receiver_delivery',
    'only_declared_SQL_rows_reference_changes_and_sequence_delta',
    'denied_requests_never_change_Connect_tables',
    'independent_owned_network_container_volume_and_process_generation_cleanup',
)

REQUEST_CONTROL={
    'integration-get':['owned_object','owned_missing_object_404','immutable_store'],
    'nested-subscriptions':['exact_child_roster','other_integration_excluded',
                            'missing_numeric_integration_empty_200','immutable_store'],
    'subscriptions-list':['complete_membership_and_fields','irrelevant_query_and_collection_order_scope','immutable_store'],
    'subscription-detail':['owned_object','owned_missing_object_404','immutable_store'],
    'log-detail':['owned_nested_log_fields','owned_missing_object_404','integration_and_type_filters','immutable_store'],
    'integration-patch':['partial_omissions_preserved','invalid_type_config_400','missing_object_404',
                         'unchanged_subscriptions_and_logs','unchanged_sequence'],
    'subscription-create':['valid_event_and_integration','duplicate_event_model_behavior','invalid_event_choice_400',
                           'missing_foreign_key_400','defaults_and_one_sequence_advance'],
    'subscription-put':['required_event_and_integration','omitted_optional_fields_preserved','exact_reassignment',
                        'missing_object_404','invalid_choice_no_write','no_sequence_advance'],
    'subscription-patch':['partial_omissions_preserved','permitted_foreign_key_and_event_update',
                          'missing_object_404','invalid_choice_no_write','no_sequence_advance'],
    'subscription-delete':['owned_target_and_owned_log_cascade','repeat_missing_delete_404','no_sequence_advance'],
}

FORMAT_CONTROL=('json_suffix_optional_final_slash','Browsable_API_renderer_if_negotiated','unsupported_format_response',
                'Accept_header_precedence_separately_observed')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def compile_requirements(matrix,repository=ROOT):
    _,sealed=load_recipe()
    verify(matrix,sealed)
    freeze=(matrix/'freeze.json').read_bytes()
    plan_raw=(matrix/'matrix-plan.json').read_bytes()
    assert sha(freeze)==MATRIX_SEAL and sha(plan_raw)==MATRIX_PLAN
    plan=json.loads(plan_raw)
    revision=plan['base_register_commit']
    register=subprocess.check_output(['git','show',f'{revision}:docs/rust-contract-register.json'],cwd=repository)
    assert sha(register)==plan['base_register_sha256']
    ledger=json.loads(register)
    routes={row['resolver_ordinal']:row for row in ledger['runtime_routes']}
    original=matrix/'copied/original-source.tar'
    assert sha(original.read_bytes())==plan['original_archive_sha256']
    with tarfile.open(original) as archive:
        evidence={path:archive.extractfile(path).read() for path in AUTHORITY_FILES}
    permissions=evidence['apps/accounts/permissions.py']
    authentication=evidence['apps/accounts/authentication.py']
    settings=evidence['dispatcharr/settings.py']
    assert b'return request.user.user_level >= 10' in permissions
    assert b'network_access_allowed(request, "UI", user)' in permissions
    assert b'"apps.accounts.permissions.IsAdmin"' in settings
    assert settings.index(b'JWTAuthentication')<settings.index(b'apps.accounts.authentication.ApiKeyAuthentication')
    assert all(bit in authentication for bit in (b'HTTP_X_API_KEY',b'keyword = "ApiKey"',b'if not user.is_active:'))
    assert all(bit in evidence['apps/accounts/models.py'] for bit in (b'STREAMER = 0',b'STANDARD = 1',b'ADMIN = 10'))
    assert b'def list_subscriptions(self, request, pk=None):' in evidence['apps/connect/api_views.py']
    assert b'EventSubscription.objects.filter(integration_id=pk)' in evidence['apps/connect/api_views.py']
    assert b'on_delete=models.CASCADE' in evidence['apps/connect/models.py']
    records=[]
    for case in plan['cases']:
        route=routes[case['resolver_ordinal']]
        [operation]=[op for op in route['operations'] if op['contract_id']==case['contract_id']]
        assert route['default_permission_candidates']==['apps.accounts.permissions.IsAdmin']
        assert route['pattern_exact']==case['pattern_exact'] and case['method']==operation['method']
        assert len(case['role_cases'])==6 and 'executable_Source_contract' not in operation
        records.append({
            'contract_id':operation['contract_id'],'resolver_ordinal':route['resolver_ordinal'],
            'resolver_name':route['name'],'pattern_exact':route['pattern_exact'],'source_handler':route['source_handler'],
            'method':operation['method'],'source_action':operation['source_action'],'source_permission':'IsAdmin',
            'variant':case['variant'],'case_key':case['key'],'path_template':case['path_template'],
            'representative_role_cells':case['role_cases'],'required_physical_witnesses':COMMON_PHYSICAL,
            'required_beyond_representative_capture':{
                'credential_and_UI_policy':EXTRA_AUTH,
                'request_equivalence_classes':REQUEST_CONTROL[case['key']],
                'format_and_content_negotiation':FORMAT_CONTROL if case['variant']=='json' else (
                    'ordinary_JSON_and_source_browsable_renderer_or_exact_renderer_rule',),
                'additional_effect_authorities':['exact_SQL_and_sequence_witness_for_each_new_mutation_class',
                                                 'broker_publish_witness_if_the_operation_contract_requires_it']
                    if case['method']!='GET' else ['unchanged_SQL_and_receiver_for_each_new_read_class'],
            },
            'legacy_defect_needs_own_review':case['expected_existing_source_format_failure_candidate'],
            'whole_operation_policy_approved':False,
        })
    assert len(records)==20 and len({row['contract_id'] for row in records})==20
    return {'schema_version':1,'base_register_revision':revision,'base_register_sha256':sha(register),
            'original_revision':plan['source_revision'],'original_archive_sha256':plan['original_archive_sha256'],
            'source_files_sha256':{path:sha(content) for path,content in evidence.items()},
            'matrix_plan_sha256':MATRIX_PLAN,'matrix_fixture_seal_sha256':MATRIX_SEAL,
            'role_authority':'Original IsAdmin and production JWT/API-key order, active-account and UI network predicates',
            'representative_required_controls':120,'whole_operation_policy_approved':False,
            'full_milestone_accepted':False,'operations':records,
            'credit':'Original Dispatcharr authors and contributors retain full application and behavior credit.'}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture',type=Path,required=True)
    print(json.dumps(compile_requirements(parser.parse_args().fixture.resolve()),indent=2,sort_keys=True))
