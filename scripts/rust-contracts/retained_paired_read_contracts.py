"""Bind exact retained packaged GET observations with incomplete role coverage."""
import re
from urllib.parse import quote

from source_contracts import checked

KIND = 'retained_original_System_Logs_and_regex_preview_GET'
INPUT = 'scripts/rust-contracts/retained-paired-reads/bindings.json'
SHA256 = '17304fa362b0b3a3cfb1728c66da71795441e6ac9526f7d6b71ac52c5721943d'
SOURCE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
LOG_AUTHORITY = 'scripts/rust-integration/milestone-batch92/acceptance.json'
REGEX_AUTHORITY = 'scripts/rust-integration/milestone-batch94/acceptance.json'
LOG_CAPTURE = 'C92'
REGEX_CAPTURE = 'C94'
REGEX = 'route-1e6515342e6335fab078:GET'
DOWNLOAD = 'route-54a5519b46ae18bea548:METHOD_UNRESOLVED'
TAIL = 'route-209e806f3f1d6d576eec:METHOD_UNRESOLVED'
IDS = {REGEX, DOWNLOAD, TAIL}
SUPPLEMENTS = {DOWNLOAD, TAIL}
ENDPOINTS = {
    REGEX: (55, 'api:channels:stream-regex-preview', 'api/channels/^streams/regex-preview/$', 'apps.channels.api_views.StreamViewSet', 'GET', 'regex_preview'),
    DOWNLOAD: (218, 'api:core:log_file_download', 'api/core/logs/<str:name>/download/', 'core.log_files.WrappedAPIView', None, None),
    TAIL: (219, 'api:core:log_file', 'api/core/logs/<str:name>/', 'core.log_files.WrappedAPIView', None, None),
}
REGEX_PATH = '/api/channels/streams/regex-preview/'
LOG_PREFIX = '/api/core/logs/'
LOG_NAME = 'dispatcharr.log-tail'
REGEX_CASES = (
    ('anonymous', 'anonymous', 401), ('viewer', 'streamer', 403),
    ('missing-group', 'admin', 400), ('invalid-account', 'admin', 400), ('pattern-length', 'admin', 400),
    ('count-only', 'standard', 200), ('account-isolation', 'standard', 200),
    ('other-account', 'admin', 200), ('whole-group', 'admin', 200), ('empty-group', 'admin', 200),
    ('case-sensitive', 'admin', 200), ('invalid-pattern', 'admin', 200), ('name-length', 'admin', 200),
    ('last-value-and-clamp', 'admin', 200), ('full-scan-cap', 'admin', 200),
)
TAIL_CASES = (
    ('tail-initial', 'admin', 200, LOG_NAME), ('tail-no-complete-append', 'admin', 200, LOG_NAME),
    ('tail-delta', 'admin', 200, LOG_NAME), ('tail-reset-mid-line', 'admin', 200, LOG_NAME),
    ('tail-reset-malformed', 'admin', 200, LOG_NAME), ('tail-reset-past-end', 'admin', 200, LOG_NAME),
    ('tail-reset-after-rotation', 'admin', 200, LOG_NAME), ('tail-safe-alias', 'admin', 200, LOG_NAME + '.2'),
    ('tail-bounded', 'admin', 200, LOG_NAME + '.3'), ('tail-missing', 'admin', 404, LOG_NAME + '.404'),
    ('tail-excluded-alias', 'admin', 404, 'dispatcharr.log.8'),
    ('tail-anonymous', 'anonymous', 401, LOG_NAME), ('tail-standard', 'standard', 403, LOG_NAME),
)
DOWNLOAD_CASES = (
    ('download-attachment', 'admin', 200, LOG_NAME), ('download-empty', 'admin', 200, LOG_NAME + '.5'),
    ('download-safe-alias', 'admin', 200, LOG_NAME + '.2'),
    ('download-unicode-base64', 'admin', 200, 'dispatcharr.log.١'),
    ('download-unicode-quoted-printable', 'admin', 200, 'dispatcharr.log-web0123456789abc.١'),
    ('download-excluded-alias', 'admin', 404, 'dispatcharr.log.8'),
    ('download-anonymous', 'anonymous', 401, LOG_NAME), ('download-standard', 'standard', 403, LOG_NAME),
)
REGEX_PREDICATES = ('expected_body_match', 'literal_match', 'sql_broker_unchanged', 'wire_match')
LOG_PREDICATES = ('exact_expected_body', 'role_and_representation_headers_match', 'same_read_only_input_set')


def dependencies():
    return {INPUT, LOG_AUTHORITY, REGEX_AUTHORITY}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def is_sha(value):
    return isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def endpoint(identifier):
    ordinal, name, pattern, handler, method, action = ENDPOINTS[identifier]
    return {'resolver_ordinal': ordinal, 'name': name, 'pattern_exact': pattern,
            'source_handler': handler, 'declared_method': method, 'source_action': action}


def expected(identifier):
    if identifier == REGEX:
        return [(case, role, status, REGEX_PATH) for case, role, status in REGEX_CASES]
    controls = TAIL_CASES if identifier == TAIL else DOWNLOAD_CASES
    suffix = '/' if identifier == TAIL else '/download/'
    return [(case, role, status, LOG_PREFIX + quote(file) + suffix) for case, role, status, file in controls]


def pointer(side, label, identifier):
    if identifier == REGEX:
        return f'/scopes/regex_preview/cases/stream-regex-preview-{side}:{label}'
    names = [case[0] for case in TAIL_CASES + DOWNLOAD_CASES]
    return f'/checks/{35 + 2 * names.index(label) + (side == "native")}/details'


def fixture_binding(fixture, expected_path, receipt):
    assert fixture['reference']['path'] == expected_path
    assert fixture['source_revision'] == receipt['original_revision'] == SOURCE
    assert fixture['exact_CIDs_and_generations_retired'] == 4
    assert fixture['actual_controller_exit'] == 0
    assert fixture['both_pipes_and_process_group_settled'] is fixture['owned_resources_empty'] is True
    assert fixture['observed_methods'] == ['GET'] and fixture['Source_replayed'] is False
    assert is_sha(fixture['probe_sha256'])


def validate(root, value):
    assert value == load(root)
    assert value['binding_kind'] == KIND and set(value['fixtures']) == {REGEX_CAPTURE, LOG_CAPTURE}
    assert value['operation_count'] == len(value['contracts']) == 3
    assert value['primary_count'] == 1 and value['supplement_count'] == 2
    assert value['fully_verified_operations'] == value['new_Source_executions'] == value['new_Native_executions'] == 0
    assert value['Native_coverage_increased'] is value['performance_verified'] is False
    logs, regex = (value['fixtures'][key] for key in (LOG_CAPTURE, REGEX_CAPTURE))
    c92, c94 = (checked(root, fixture['reference']) for fixture in (logs, regex))
    fixture_binding(logs, LOG_AUTHORITY, c92)
    fixture_binding(regex, REGEX_AUTHORITY, c94)
    assert logs['native_revision'] == c92['candidate_revision'] == 'bae33e83801096c8037c0e41d05fa2b25f32dbc6'
    assert logs['native_image'] == c92['candidate_image']
    assert logs['raw_artifact_sha256'] == {name: c92['artifact_sha256'][name] for name in ('execution', 'integration', 'cleanup', 'receipt')}
    assert c92['actual_build_exit'] == c92['actual_compose_exit'] == 0
    assert c92['all_streams_and_process_group_settled'] and c92['owned_resources_empty']
    assert c92['owned_generations_retired'] == logs['exact_CIDs_and_generations_retired']
    assert c92['paired_cases_per_backend'] == {'list': 8, 'tail': 13, 'download': 8}
    assert c92['passed_mount_identity_checks'] == 1 and c92['foundation_checks'] == 18
    assert logs['probe_sha256'] == 'ce2363757593d4cf7f9a76ef2100c0f868a34cb32684b176955f21949c89ab98'
    assert regex['native_revision'] == c94['candidate_revision'] == 'fc5562a37d91ed6673e6ee9a740be4e0dd6c7c5f'
    assert regex['native_image'] == c94['package_image']
    published = c94['scopes']['regex_preview']
    assert published['raw_fingerprints'] == regex['raw_artifact_sha256']
    assert c94['build_actual_exit'] == published['actual_exit'] == 0
    assert published['pipes_and_process_group_settled'] and published['owned_resource_inventories_empty']
    assert published['owned_generations_removed'] == regex['exact_CIDs_and_generations_retired']
    assert published['business_checks'] == len(published['cases']) == 30 and published['foundation_checks'] == 18
    assert regex['probe_sha256'] == '4f103a10edc720260d1cabfa6688c64e55b51711d1e72b24c2af582e8efd0cd3'
    assert set(contract['contract_id'] for contract in value['contracts']) == IDS
    assert c92['full_M8_accepted'] is c94['full_milestone_passed'] is False
    for contract in value['contracts']:
        identifier = contract['contract_id']
        target = endpoint(identifier)
        assert contract['endpoint_authority'] == target
        assert (contract['pattern_exact'], contract['source_handler'], contract['method'], contract['source_action']) == (
            target['pattern_exact'], target['source_handler'], target['declared_method'], target['source_action'])
        is_regex = identifier == REGEX
        assert contract['binding'] == ('primary' if is_regex else 'supplemental')
        assert contract['fixture_id'] == (REGEX_CAPTURE if is_regex else LOG_CAPTURE)
        assert contract['owning_milestone'] == 'M5' and contract['observed_methods'] == ['GET']
        assert contract['fully_verified'] is False
        roles = ['anonymous', 'streamer', 'standard', 'admin'] if is_regex else ['anonymous', 'standard', 'admin']
        assert contract['observed_roles'] == roles
        assert contract['source_role_specs'] == {
            role: {'fixture_role_or_supplied_credential': role, 'complete': False} for role in roles
        }
        cases = expected(identifier)
        assert contract['result'] == {
            'Source_case_observations': len(cases), 'Native_scoped_controls_matched': len(cases),
            'new_Source_cases_this_task': 0, 'Native_coverage_increased': False,
        }
        assert len(contract['cases']) == len(cases)
        for row, (label, role, status, path) in zip(contract['cases'], cases, strict=True):
            assert (row['case'], row['role'], row['status'], row['path']) == (label, role, status, path)
            assert row['method'] == 'GET'
            assert row['Original_body_sha256'] == row['Native_body_sha256'] and is_sha(row['Original_body_sha256'])
            for side, key in (('source', 'Original'), ('native', 'Native')):
                assert row[f'{key}_evidence_pointer'] == pointer(side, label, identifier)
                if is_regex:
                    observed = published['cases'][f'stream-regex-preview-{side}:{label}']
                    assert observed['actual_status'] == row['status']
                    assert observed['actual_body_sha256'] == row[f'{key}_body_sha256']
                    assert observed['response_bytes'] == row['response_bytes']
                    assert all(observed[name] is True for name in REGEX_PREDICATES)
            assert row['scoped_observations'] == dict.fromkeys(REGEX_PREDICATES if is_regex else LOG_PREDICATES, True)
            if label.startswith('download-unicode-'):
                assert row['wire_content_disposition'] == c92['matched_unicode_attachment_wire_headers'][label]


def restore_pre_paired_reads(root, rows):
    """Reverse the exact three additions for historical checkpoint assertions."""
    by_id = {contract['contract_id']: contract for contract in load(root)['contracts']}
    operations = {item['contract_id']: item for route in rows for item in route['operations']}
    for identifier in SUPPLEMENTS:
        captures = operations[identifier]['supplemental_executable_Source_captures']
        matching = [capture for capture in captures if capture['input']['kind'] == KIND]
        assert len(matching) == 1 and matching[0]['definition'] == by_id[identifier]
        captures.remove(matching[0])
        if not captures:
            operations[identifier].pop('supplemental_executable_Source_captures')
    primary = operations[REGEX]
    assert primary.pop('executable_Source_contract') == by_id[REGEX] and not primary['related_evidence']
    primary.update(setup_fixture=None, assertions=[], result=None, coverage_state='not_yet_mapped')
    primary['role_matrix'] = {'state': 'unmapped', 'required_roles': []}
