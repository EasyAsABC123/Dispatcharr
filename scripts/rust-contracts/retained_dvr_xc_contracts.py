"""Validate the retained Original DVR and XC response projection.

The original Dispatcharr authors retain all application and algorithm credit.
"""
from collections import Counter
import re

from source_contracts import checked

KIND = 'retained_original_DVR_XC_playback_HTTP'
INPUT = 'scripts/rust-contracts/retained-dvr-xc/bindings.json'
SHA256 = '556f9bb313ed642dd75a05bedb5120008f4ba50784cb000df74add3cb9ed1a94'


def dependencies():
    return {INPUT}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': SHA256})


def validate(root, value):
    assert load(root) == value and value['binding_kind'] == KIND
    assert value['operation_count'] == len(value['contracts']) == 5
    assert value['unique_Source_cases'] == 49
    assert value['fully_verified_operations'] == value['new_Source_executions'] == 0
    assert value['Native_coverage_increased'] is False
    fixtures = value['fixtures']
    for name in ('DVR-HLS', 'DVR-authorization'):
        assert fixtures[name]['outer_controller_exit'] == 1
        assert fixtures[name]['exact_Original_cleanup'] is True
    assert fixtures['XC-live-entry-paired']['positive_media_qualified'] is False
    assert fixtures['Original-manual-Comskip']['global_producer_graph_qualified'] is False
    scopes = Counter()
    seen = set()
    for definition in value['contracts']:
        fixture = definition['fixture_id']
        assert definition['fully_verified'] is False
        pattern = definition['pattern_exact'].replace('/^', '/')
        pattern = re.sub(r'<path:[^>]+>', r'.+', pattern)
        pattern = re.sub(r'<int:[^>]+>', r'[0-9]+', pattern)
        pattern = re.sub(r'<str:[^>]+>', r'[^/]+', pattern)
        cases = definition['cases']
        assert definition['observed_roles'] == sorted({row['role'] for row in cases})
        assert definition['observed_methods'] == sorted({row['method'] for row in cases})
        assert definition['result']['Source_case_observations'] == len(cases)
        for row in cases:
            identity = (fixture, row['source_index'])
            assert identity not in seen
            seen.add(identity)
            assert re.fullmatch(pattern, row['path'].lstrip('/'))
            assert definition['method'] is None or row['method'] == definition['method']
            assert 100 <= row['HTTP_status'] <= 599
            assert re.fullmatch(r'[0-9a-f]{64}', row['response_projection_sha256'])
            if fixture == 'XC-live-entry-paired':
                assert definition['method'] is None and definition['source_action'] is None
                assert row['credential_path_components_omitted']
                assert row['fixture_role_is_auth_success'] is False
            scopes[fixture] += 1
    assert scopes == {'DVR-HLS': 8, 'DVR-authorization': 1,
                       'Original-manual-Comskip': 4, 'XC-live-entry-paired': 36}


def restore_pre_dvr_xc(root, rows):
    """Reverse this retained binding while proving historical register identities."""
    value = load(root)
    operations = {op['contract_id']: op for route in rows for op in route['operations']}
    for contract in value['contracts']:
        operation = operations[contract['contract_id']]
        assert operation.pop('executable_Source_contract') == contract
        operation.update(setup_fixture=None, assertions=[], result=None,
                         coverage_state='scoped_evidence_needs_case_mapping'
                         if operation['related_evidence'] else 'not_yet_mapped')
        for key in ('observed_fixture_definitions', 'complete', 'observed_roles'):
            operation['role_matrix'].pop(key)
        operation['role_matrix']['state'] = 'unmapped'
