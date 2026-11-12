"""Apply explicitly scoped executable Source definitions to stable register rows.
Original Dispatcharr authors retain application/algorithm credit and licenses.
No operation, method, role completeness or Native verification is invented.
"""
import copy
import hashlib
import json
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def checked(root, reference):
    path = root / reference['path']
    assert path.is_file(), 'Source authority absent'
    value = json.loads(path.read_text())
    if 'canonical_sha256' in reference:
        assert digest(value) == reference['canonical_sha256'], 'Source canonical authority'
    else:
        assert hashlib.sha256(path.read_bytes()).hexdigest() == reference['sha256'], 'Source authority hash'
    return value


def validate_captured(root, value, bindings):
    for reference in value['authorities']:
        checked(root, reference)
    indexed = {b['contract_id']: b for b in bindings['bindings']}
    attestation = json.loads((root / 'scripts/rust-contracts/datasets/image44-scoped-cases.json').read_text())
    seen = set()
    for item in value['contracts']:
        b = indexed[item['contract_id']]
        assert item['contract_id'] not in seen, 'duplicate Source contract'
        seen.add(item['contract_id'])
        fixture = value['fixtures'][item['fixture_id']]
        assert fixture['id'] == item['fixture_id']
        assert fixture['baseline_revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
        assert fixture['baseline_image'].endswith('sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f')
        assert fixture['ownership'] and len(fixture['setup']) == 5
        assert fixture['seed']['sha256'] and fixture['oracle']['historical_executed_sha256']
        assert fixture['execution']['entrypoint'] == 'scripts/rust-contracts/replay_source_contract.py'
        assert item['observed_roles'] == sorted({c['role'] or 'anonymous' for c in b['cases']})
        assert item['source_role_specs'] == {role: fixture['roles'][role] for role in item['observed_roles']}
        assert all(item[k] == b[k] for k in ['contract_id', 'source_handler', 'pattern_exact', 'method'])
        assert item['fully_verified'] is False and item['remaining'] and len(item['assertions']) == 4
        assert item['result']['historical_captured_cases'] == len(b['cases']) == len(item['cases'])
        for declared, old in zip(item['cases'], b['cases']):
            assert declared['existing_case_binding_sha256'] == digest(old), 'Source case binding'
            assert all(declared[k] == old[k] for k in ['source_index', 'method', 'path', 'role'])
            response = declared['response']
            assert response['status'] == old['status'] and response['body_sha256'] == old['body_sha256']
            assert type(response['body_bytes']) is int and response['body_bytes'] >= 0
            if 'evidence_dataset_id' in b:
                row = next(r for r in attestation['case_projection']
                           if r['dataset_id'] == b['evidence_dataset_id'] and r['source_index'] == old['source_index'])
                assert response == row['source_response'], 'Source response projection'
                assert declared['Original_authority'] == row['source_authority']
                assert declared['label'] == row['label']
            else:
                # Legacy source JSON is already attested by the existing summary;
                # the six predeclared fresh authorities retain their own raw hash.
                assert declared['Original_authority'] == ('predeclared_fresh_same_state' if old['fresh_predeclared_Source_tie_authority'] else 'Original_public_source4')
        if item['fixture_id'] == 'provider150':
            assert item['result']['new_Source_cases_this_task'] == len(item['cases'])
            assert item['result']['state'] == 'fresh_Original_scoped_assertions_passed_with_historical_attestation'
            authority = item['result']['fresh_Original_authority']
            assert authority['public_input'] == 'scripts/rust-contracts/executable-source-bindings.json'
            assert authority['json_pointer'] == '/fresh_Original_qualification'
            assert authority['wire_projection_sha256'] == value['fresh_Original_qualification']['wire_projection_sha256']
            assert authority['raw_capture'] in value['fresh_Original_qualification']['raw_authorities']
        else:
            assert item['result']['new_Source_cases_this_task'] == 0
            assert item['result']['state'] == 'historical_Original_observations_attested_not_new_execution'
    assert len(seen) == value['operation_count'] == 11
    assert sum(len(c['cases']) for c in value['contracts']) == value['historical_case_count'] == 403
    fresh = value['fresh_Original_qualification']
    assert fresh['passed'] is True and fresh['GET_cases'] == len(fresh['wire_projection']) == 78
    assert digest(fresh['wire_projection']) == fresh['wire_projection_sha256']
    case_index = {(c['contract_id'], x['source_index']): x for c in value['contracts'] for x in c['cases']}
    for wire in fresh['wire_projection']:
        expected = case_index[(wire['contract_id'], wire['source_index'])]
        assert all(wire[k] == expected[k] for k in ['path', 'role', 'method'])
        assert wire['status'] == expected['response']['status']
        assert {k: v for k, v in wire['headers'].items() if k != 'Content-Length'} == {k: v for k, v in expected['response']['headers'].items() if k != 'Content-Length'}
        assert wire['headers']['Content-Length'] in [None, str(wire['body_bytes'])]
        assert len(wire['body_sha256']) == 64 and wire['body_bytes'] >= 0
    assert fresh['owned18_tables_restored'] and fresh['provider_trap_attempts'] == 0 and fresh['owned_cleanup_empty']
    assert sum(c['result']['new_Source_cases_this_task'] for c in value['contracts']) == 78
    assert value['fully_verified_operations'] == 0 and value['Native_coverage_increased'] is False


def apply(root, rows, bindings):
    root = Path(root)
    manifest_path = root / 'scripts/rust-contracts/executable-source-inputs.json'
    assert manifest_path.is_file(), 'Source input manifest absent'
    # Historical checkouts use their historical generator; this generator fails
    # closed if its declared additive inputs are missing.
    manifest = json.loads(manifest_path.read_text())
    assert manifest['format_version'] == 1
    index = {o['contract_id']: (r, o) for r in rows for o in r['operations']}
    seen = set()
    loaded = []
    for reference in manifest['inputs']:
        value = checked(root, reference)
        if reference['kind'] == 'captured_HTTP_Source_contracts':
            validate_captured(root, value, bindings)
        elif reference['kind'] == 'pinned_original_framework_tests':
            import framework_auth_contracts
            framework_auth_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'pinned_original_account_settings_tests':
            import account_settings_contracts
            account_settings_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'captured_original_scanner_task_observations':
            import scanner_task_contracts
            scanner_task_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'pinned_original_channel_guide_tests':
            import channel_guide_contracts
            channel_guide_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'pinned_original_dvr_import_tests':
            import dvr_import_contracts
            dvr_import_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'pinned_original_provider_output_consumer_tests':
            import provider_output_contracts
            provider_output_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'pinned_original_admin_rest_tests':
            import admin_rest_contracts
            admin_rest_contracts.validate(root, value, bindings)
        else:
            raise AssertionError('unsupported Source input kind; add a reviewed validator')
        for contract in value['contracts']:
            identifier = contract['contract_id']
            assert identifier not in seen, 'conflicting Source contract inputs'
            seen.add(identifier)
            route, operation = index[identifier]
            assert contract['pattern_exact'] == route['pattern_exact']
            assert contract['source_handler'] == route['source_handler']
            assert all(contract[k] == operation[k] for k in ['method', 'source_action', 'owning_milestone']), 'stable operation schema/owner'
            assert operation['fully_verified'] is False
            operation['setup_fixture'] = copy.deepcopy(value['fixtures'][contract['fixture_id']])
            operation['assertions'] = copy.deepcopy(contract['assertions'])
            operation['result'] = copy.deepcopy(contract['result'])
            operation['executable_Source_contract'] = copy.deepcopy(contract)
            operation['role_matrix']['observed_fixture_definitions'] = copy.deepcopy(contract['source_role_specs'])
            operation['role_matrix']['complete'] = False
            if reference['kind'] in ['pinned_original_framework_tests', 'pinned_original_account_settings_tests', 'pinned_original_channel_guide_tests', 'pinned_original_dvr_import_tests', 'pinned_original_provider_output_consumer_tests', 'pinned_original_admin_rest_tests']:
                operation['role_matrix']['state'] = 'partial_Original_framework_test_assignment'
                operation['role_matrix']['observed_roles'] = copy.deepcopy(contract['observed_roles'])
            operation['coverage_state'] = 'partial_executable_Source_contract'
        loaded.append({'path': reference['path'], 'sha256': reference['sha256'], 'kind': reference['kind']})
    return {'state': 'partial executable Source definitions, not endpoint verification',
            'operations': len(seen), 'inputs': loaded, 'fully_verified_added': 0}


def apply_non_HTTP(root, entries):
    """Only reviewed exact model/task/consumer candidates; no arbitrary importer."""
    root = Path(root)
    manifest = json.loads((root / 'scripts/rust-contracts/executable-source-inputs.json').read_text())
    index = {entry['candidate_id']: entry for entry in entries}
    seen = set()
    for reference in manifest['inputs']:
        if reference['kind'] not in ['pinned_original_account_settings_tests', 'captured_original_scanner_task_observations', 'pinned_original_provider_output_consumer_tests']:
            continue
        value = checked(root, reference)
        if reference['kind'] == 'pinned_original_account_settings_tests':
            import account_settings_contracts
            account_settings_contracts.validate(root, value)
            source_kind = 'model_candidate'
        elif reference['kind'] == 'captured_original_scanner_task_observations':
            import scanner_task_contracts
            scanner_task_contracts.validate(root, value)
            source_kind = 'task_candidate'
        else:
            import provider_output_contracts
            provider_output_contracts.validate(root, value)
            source_kind = 'websocket_consumer_candidate'
        for contract in value['non_HTTP_contracts']:
            identifier = contract['candidate_id']
            assert identifier not in seen, 'conflicting nonHTTP Source inputs'
            seen.add(identifier)
            entry = index[identifier]
            assert entry['source'] == contract['source'], 'exact nonHTTP source identity'
            assert entry['source']['kind'] == source_kind
            assert entry['fully_verified'] is False and contract['fully_verified'] is False
            entry['setup_fixture'] = (copy.deepcopy(value['fixtures'][contract['fixture_id']]) if source_kind in ['model_candidate', 'websocket_consumer_candidate']
                                      else {key: copy.deepcopy(value['fixtures'][key]) for key in contract['fixture_ids']})
            entry['assertions'] = copy.deepcopy(contract['assertions'])
            entry['result'] = copy.deepcopy(contract['result'])
            entry['executable_Source_contract'] = copy.deepcopy(contract)
            entry['state'] = ('partial_executable_Original_model_tests' if source_kind == 'model_candidate'
                              else 'partial_executable_Original_task_observations' if source_kind == 'task_candidate'
                              else 'partial_executable_Original_synthetic_consumer_method_tests')
    return {'state': 'typed partial Original model/task/synthetic-consumer assertions, not HTTP or Native verification',
            'candidates': len(seen), 'fully_verified_added': 0}
