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


def validate_matching_source51(root, value):
    """Map only the two captured admin POST operations, never Native parity."""
    assert value['binding_kind'] == 'captured_original_Source51_matching_admin_HTTP'
    assert value['operation_count'] == 2 and value['unique_Source_cases'] == 14
    assert value['Source_only'] is True and value['Native_coverage_increased'] is False
    assert value['fully_verified_operations'] == 0
    for reference in value['authorities']:
        checked(root, reference)
    qualified = checked(root, value['qualification'])
    cases = checked(root, value['case_projection'])['cases']
    assert qualified['state'] == 'ACTUAL_SOURCE51_SELECTED_CHARACTERIZATION_PASSED'
    assert qualified['actual_exit'] == 0 and qualified['actual_cleanup_passed']
    assert qualified['stdout_EOF'] and qualified['stderr_EOF'] and qualified['process_group_settled']
    assert qualified['actual_HTTP_statuses'] == {'202': 14} and len(cases) == 14
    assert qualified['Native_application_qualified'] is False and qualified['performance_verified'] is False
    assert qualified['Source_supported_config_domain'] == {'DISABLE_ML_DOWNLOADS': 'true', 'actual_MiniLM_cache_present': False, 'default_ML_availability_parity': False}
    seen = set()
    for contract in value['contracts']:
        assert contract['fully_verified'] is False and contract['observed_roles'] == ['admin']
        assert contract['method'] == 'POST' and contract['source_handler'] == 'apps.channels.api_views.ChannelViewSet'
        fixture = value['fixtures'][contract['fixture_id']]
        assert fixture['baseline_revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
        assert contract['source_role_specs'] == fixture['role']
        assert contract['result']['Native_application_qualified'] is False and contract['result']['complete_role_matrix'] is False
        assert contract['result']['Source_cases_passed'] == len(contract['cases']) and contract['result']['new_Source_cases_this_task'] == 0
        for case in contract['cases']:
            index = case['source_index'];assert index not in seen;seen.add(index)
            actual = cases[index]
            assert case['record_json_pointer'] == f'/cases/{index}' and case['record_sha256'] == digest(actual)
            assert case['name'] == actual['name'] and case['method'] == 'POST' and case['role'] == 'admin'
            assert case['actual_HTTP_status'] == actual['actual_HTTP_status'] == 202 and actual['status'] == 'passed'
            assert case['actual_response_body_sha256'] == actual['actual_response_body_sha256']
            assert case['actual_root_task'] == actual['root_task'] and case['backend_status'] == actual['backend_status']
            assert actual['DB1_journal_terminal_durable'] and actual['actual_child_parent_and_root_match']
            single = actual['root_task'] == 'apps.channels.tasks.match_single_channel_epg'
            assert single == (contract['source_action'] == 'match_channel_epg')
            assert contract['source_action'] in ['match_epg', 'match_channel_epg']
            expected = 'FAILURE' if actual['name'] == 'selected-SQL-failure-and-following-continuation' else 'SUCCESS'
            assert actual['backend_status'] == expected
            assert case['result'] == 'retained_Source51_case_passed_Native_unmeasured'
    assert seen == set(range(14))


def validate_station_aggregate_source53_55(root, value):
    """Retain eight captured admin cases without resolving unknown methods."""
    assert value['binding_kind'] == 'captured_original_Source53_stations_Source55_aggregate_admin_HTTP'
    assert value['operation_count'] == 3 and value['unique_Source_cases'] == 8
    assert value['Source_only'] is True and value['Native_coverage_increased'] is False
    assert value['fully_verified_operations'] == 0
    for reference in value['authorities']:
        checked(root, reference)
    station = json.loads((root / 'scripts/rust-integration/channel-stations-source53/metadata.json').read_text())
    assert station['combined_selected_workflow_passed'] and station['Native_HTTP_producers_qualified'] is False
    assert station['Source_HTTP_create_status'] == 201 and station['Source_HTTP_lineup_add_status'] == 200
    aggregate = json.loads((root / 'scripts/rust-integration/milestone-batch38/Source55/qualification.json').read_text())
    assert aggregate['actual_exit'] == 0 and aggregate['actual_cleanup_passed']
    assert aggregate['actual_Source_phases_passed'] == 4 and aggregate['Native_application_qualified'] is False
    expected = {
        'route-e8763dfe777286fc6b57:POST': ('Source53', [0, 2, 3], 'create'),
        'route-7e6941c073eccaa2b6dd:POST': ('Source53', [1], 'sd_lineups'),
        'route-c991f2de07ac41a62a0a:METHOD_UNRESOLVED': ('Source55', [0, 1, 2, 3], None),
    }
    assert {c['contract_id'] for c in value['contracts']} == set(expected)
    seen = set()
    for contract in value['contracts']:
        dataset, indices, action = expected[contract['contract_id']]
        assert contract['source_action'] == action and contract['observed_methods'] == ['POST']
        assert contract['method'] == (None if dataset == 'Source55' else 'POST')
        assert contract['observed_roles'] == ['admin'] and contract['fully_verified'] is False
        fixture = value['fixtures'][contract['fixture_id']]
        assert fixture['baseline_revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
        assert contract['source_role_specs'] == fixture['role']
        execution = checked(root, fixture['execution_authority'])
        assert execution['actual_exit'] == 0 and execution['stdout_EOF'] and execution['stderr_EOF'] and execution['process_group_settled']
        assert checked(root, fixture['cleanup_authority'])['all_owned_resources_absent']
        result = contract['result']
        assert result['Source_cases_passed'] == len(indices) and result['new_Source_cases_this_task'] == 0
        assert result['Native_application_qualified'] is False and result['complete_role_matrix'] is False
        assert result['fully_verified'] is False and result['performance_verified'] is False
        assert [case['source_index'] for case in contract['cases']] == indices
        for case in contract['cases']:
            index = case['source_index']; key = (dataset, index)
            assert key not in seen; seen.add(key)
            actual = checked(root, case['record_authority'])['cases'][index]
            assert case['source_dataset'] == dataset and case['record_json_pointer'] == f'/cases/{index}'
            assert case['record_sha256'] == digest(actual) and case['name'] == actual['name'] and actual['status'] == 'passed'
            assert case['method'] == 'POST' and case['role'] == 'admin'
            assert case['result'] == 'retained_Source_case_passed_Native_unmeasured'
            assert len(case['actual_response_body_sha256']) == 64
            if dataset == 'Source53':
                frame = checked(root, case['actual_task_frame']); backend = checked(root, case['actual_backend'])
                headers = frame['headers']
                assert headers['task'] == case['actual_root_task'] == 'apps.epg.tasks.fetch_schedules_direct_stations'
                assert headers['id'] == headers['root_id'] == backend['task_id'] and headers['parent_id'] is None
                assert backend['status'] == case['backend_status'] == 'SUCCESS' and backend['result'] is case['backend_result'] is None
                assert headers['timelimit'] == [None, None] and backend['children'] == []
                assert case['actual_HTTP_status'] == (200 if index == 1 else 201)
                assert case['actual_ETA_present'] == (headers['eta'] is not None) == (index != 1)
            else:
                assert contract['operation_method_discovery_resolved'] is False and result['operation_method_discovery_resolved'] is False
                assert case['actual_path'] == '/api/m3u/refresh/' and case['actual_HTTP_status'] == actual['actual_HTTP_status'] == 202
                assert case['actual_response_body_sha256'] == digest(actual['actual_HTTP_body'])
                assert case['actual_root_task'] == 'apps.m3u.tasks.refresh_m3u_accounts'
                assert case['backend_status'] == actual['actual_parent_backend_status'] == 'SUCCESS'
                assert case['backend_result'] == actual['actual_parent_result']
                assert case['actual_direct_children'] == actual['actual_direct_children'] == [0, 2, 1, 2][index]
                assert actual['actual_root_parent_and_child_lineage_assertions_passed'] and actual['parent_children_trail_matches_actual_publication_order']
                assert all(state == 'SUCCESS' for state in actual['actual_child_backend_statuses'])
    assert seen == {('Source53', i) for i in range(4)} | {('Source55', i) for i in range(4)}


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
    primary_kinds = {}
    supplemented = set()
    unmapped_method_observations = []
    loaded = []
    for reference in manifest['inputs']:
        value = checked(root, reference)
        if reference['kind'] == 'retained_original_range_Source10_resolver_HTTP':
            import retained_range_source10_contracts
            retained_range_source10_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_xc_Source11_single_refresh_HTTP':
            import retained_xc_refresh_contracts
            retained_xc_refresh_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_Source12_search_M3U_resolver_HTTP':
            import retained_source12_contracts
            retained_source12_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_full_suite_asserted_framework':
            import framework_suite_contracts
            framework_suite_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_SD25_resolver_scoped_HTTP':
            import retained_sd_contracts
            retained_sd_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_selected_single_Set_EPG_HTTP':
            import retained_set_epg_selected_contracts
            retained_set_epg_selected_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_batch_Set_EPG_HTTP':
            import retained_set_epg_selected_contracts
            retained_set_epg_selected_contracts.validate_batch(root, value)
        elif reference['kind'] == 'retained_original_System_Logs_and_regex_preview_GET':
            import retained_paired_read_contracts
            retained_paired_read_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Live_TS_global_stop_HTTP':
            import retained_live_stop_contracts
            retained_live_stop_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Live_TS_detail_and_targeted_stop_HTTP':
            import retained_ts_controls_contracts
            retained_ts_controls_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_canonical_management_HTTP':
            import retained_canonical_management_contracts
            retained_canonical_management_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_exact_VOD_split_HTTP':
            import retained_exact_vod_contracts
            retained_exact_vod_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Image57_accounts_canonical_HTTP':
            import retained_image57_accounts_contracts
            retained_image57_accounts_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Image57_M3U_canonical_HTTP':
            import retained_image57_m3u_contracts
            retained_image57_m3u_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Connect_saved_webhook_test_HTTP':
            import retained_connect_test_contracts
            retained_connect_test_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Connect_detail_PUT_DELETE_HTTP':
            import retained_connect_detail_contracts
            retained_connect_detail_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_alias_remainder_HTTP':
            import retained_alias_remainder_contracts
            retained_alias_remainder_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_alias19_anonymous_HTTP':
            import retained_alias19_contracts
            retained_alias19_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Connect_log_read_and_subscription_set_HTTP':
            import retained_connect_actions_contracts
            retained_connect_actions_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Connect_collection_HTTP':
            import retained_connect_contracts
            retained_connect_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_DVR_XC_playback_HTTP':
            import retained_dvr_xc_contracts
            retained_dvr_xc_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_admin_model_TCP':
            import retained_admin_model_contracts
            retained_admin_model_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_repository_socket_switch':
            import retained_repository_socket_switch_contracts
            retained_repository_socket_switch_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_Image22_TS_stream_group_HTTP':
            import retained_image22_controls_contracts
            retained_image22_controls_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_playback_VOD_XC_TCP':
            import retained_playback_contracts
            retained_playback_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_HDHR_Image27_TCP':
            import retained_hdhr_contracts
            retained_hdhr_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_provider_channel_vod_HTTP':
            import retained_provider_channel_vod_contracts
            retained_provider_channel_vod_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_image22_serializer_null_TCP':
            import retained_serializer_null_contracts
            retained_serializer_null_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_image22_core_reads_TCP':
            import retained_core_read_contracts
            retained_core_read_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_stored_epg_104_framework':
            import retained_epg_reads_contracts
            retained_epg_reads_contracts.validate(root, value)
        elif reference['kind'] == 'retained_original_auth51_plugin35_resolver_scoped_HTTP':
            import retained_auth_plugin_contracts
            retained_auth_plugin_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_core_settings_Source32_framework':
            import core_settings_source_contracts
            core_settings_source_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_management12_channelguide25_HTTP':
            import retained_management_contracts
            retained_management_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'captured_HTTP_Source_contracts':
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
        elif reference['kind'] == 'retained_original_clocked_recording_tasks':
            import retained_clocked_recording_contracts
            retained_clocked_recording_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_comskip_task_observations':
            import retained_comskip_task_contracts
            retained_comskip_task_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_task_operator7_observations':
            import retained_task_operator_contracts
            retained_task_operator_contracts.validate(root, value, bindings)
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
        elif reference['kind'] == 'pinned_original_proxy_plugin_tests':
            import proxy_plugin_contracts
            proxy_plugin_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'pinned_original_schema_cache_tests':
            import schema_cache_contracts
            schema_cache_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'pinned_original_vod_access_tests':
            import vod_access_contracts
            vod_access_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_notification_framework_capture':
            import notification_source_contracts
            notification_source_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_notification_mutations_Source48_framework_capture':
            import notification_mutation_contracts
            notification_mutation_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'retained_original_notification_Source20_real_credentials_framework':
            import notification_credential_contracts
            notification_credential_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'pinned_original_logo_path_tests':
            import logo_source_contracts
            logo_source_contracts.validate(root, value, bindings)
        elif reference['kind'] == 'captured_original_Source51_matching_admin_HTTP':
            validate_matching_source51(root, value)
        elif reference['kind'] == 'captured_original_Source53_stations_Source55_aggregate_admin_HTTP':
            validate_station_aggregate_source53_55(root, value)
        else:
            raise AssertionError('unsupported Source input kind; add a reviewed validator')
        if reference['kind'] in ['retained_original_auth51_plugin35_resolver_scoped_HTTP', 'retained_original_SD25_resolver_scoped_HTTP', 'retained_original_stored_epg_104_framework']:
            route_index = {route['route_id']: route for route in rows}
            for observation in value['unmapped_method_Source_observations']:
                route = route_index[observation['route_id']]
                assert observation['contract_id'] is None and observation['fully_verified'] is False
                assert observation['pattern_exact'] == route['pattern_exact'] and observation['source_handler'] == route['source_handler']
                assert observation['observed_method'] not in {op['method'] for op in route['operations']}
                unmapped_method_observations.append(copy.deepcopy(observation))
        for contract in value['contracts']:
            identifier = contract['contract_id']
            supplemental = (reference['kind'] in ['retained_original_range_Source10_resolver_HTTP', 'retained_original_Source12_search_M3U_resolver_HTTP', 'retained_original_management12_channelguide25_HTTP', 'retained_original_auth51_plugin35_resolver_scoped_HTTP']
                            and identifier in seen
                            and primary_kinds[identifier] != reference['kind']
                            and identifier not in supplemented)
            if reference['kind'] == 'retained_original_alias_remainder_HTTP':
                supplemental = identifier in seen
                decision = value['mapping_decisions'][identifier]
                assert decision['binding'] == ('supplemental' if supplemental else 'primary')
                resolver_ordinal = index[identifier][0]['resolver_ordinal']
                assert all(case['resolver_ordinal'] == resolver_ordinal for case in contract['cases'])
                if supplemental:
                    assert primary_kinds[identifier] == 'retained_original_alias19_anonymous_HTTP'
            if reference['kind'] == 'retained_original_System_Logs_and_regex_preview_GET':
                supplemental = identifier in retained_paired_read_contracts.SUPPLEMENTS
                if supplemental:
                    assert primary_kinds[identifier] == 'pinned_original_admin_rest_tests'
            assert identifier not in seen or supplemental, 'conflicting Source contract inputs'
            seen.add(identifier)
            primary_kinds.setdefault(identifier, reference['kind'])
            if supplemental:
                supplemented.add(identifier)
            route, operation = index[identifier]
            if reference['kind'] in ('retained_original_Live_TS_global_stop_HTTP', 'retained_original_Live_TS_detail_and_targeted_stop_HTTP', 'retained_original_canonical_management_HTTP', 'retained_original_exact_VOD_split_HTTP', 'retained_original_Image57_accounts_canonical_HTTP', 'retained_original_Image57_M3U_canonical_HTTP'):
                assert route['resolver_ordinal'] == contract['resolver_ordinal']
                assert route['name'] == contract['resolver_name']
            if reference['kind'] == 'retained_original_System_Logs_and_regex_preview_GET':
                anchor = contract['endpoint_authority']
                assert (route['resolver_ordinal'], route['name']) == (anchor['resolver_ordinal'], anchor['name'])
            if reference['kind'] == 'retained_original_Connect_saved_webhook_test_HTTP':
                endpoint = value['fixtures'][contract['fixture_id']]['endpoint_authority']
                assert route['resolver_ordinal'] == endpoint['resolver_ordinal'] and route['name'] == endpoint['resolver_name']
            if reference['kind'] in ('retained_original_selected_single_Set_EPG_HTTP', 'retained_original_batch_Set_EPG_HTTP'):
                assert route['resolver_ordinal'] == value['fixtures'][contract['fixture_id']]['endpoint_authority']['resolver_ordinal']
            assert contract['pattern_exact'] == route['pattern_exact']
            assert contract['source_handler'] == route['source_handler']
            assert all(contract[k] == operation[k] for k in ['method', 'source_action', 'owning_milestone']), 'stable operation schema/owner'
            assert operation['fully_verified'] is False
            if supplemental:
                # Keep the original Source setup/assertions/result byte-for-byte.
                assert operation.get('executable_Source_contract') is not None
                capture = {
                    'definition': copy.deepcopy(contract),
                    'setup_fixture': copy.deepcopy(value['fixtures'][contract['fixture_id']]),
                    'input': copy.deepcopy(reference),
                }
                captures = operation.setdefault('supplemental_executable_Source_captures', [])
                if capture not in captures:
                    captures.append(capture)
                continue
            operation['setup_fixture'] = copy.deepcopy(value['fixtures'][contract['fixture_id']])
            operation['assertions'] = copy.deepcopy(contract['assertions'])
            operation['result'] = copy.deepcopy(contract['result'])
            operation['executable_Source_contract'] = copy.deepcopy(contract)
            operation['role_matrix']['observed_fixture_definitions'] = copy.deepcopy(contract['source_role_specs'])
            operation['role_matrix']['complete'] = False
            if reference['kind'] in ['retained_original_full_suite_asserted_framework', 'pinned_original_framework_tests', 'pinned_original_account_settings_tests', 'pinned_original_channel_guide_tests', 'pinned_original_dvr_import_tests', 'pinned_original_provider_output_consumer_tests', 'pinned_original_admin_rest_tests', 'pinned_original_proxy_plugin_tests', 'pinned_original_schema_cache_tests', 'pinned_original_vod_access_tests', 'pinned_original_logo_path_tests']:
                operation['role_matrix']['state'] = 'partial_Original_framework_test_assignment'
                operation['role_matrix']['observed_roles'] = copy.deepcopy(contract['observed_roles'])
            if reference['kind'] in ['retained_original_notification_framework_capture', 'retained_original_notification_mutations_Source48_framework_capture', 'retained_original_notification_Source20_real_credentials_framework', 'retained_original_core_settings_Source32_framework', 'retained_original_stored_epg_104_framework']:
                operation['role_matrix']['state'] = 'partial_Original_framework_capture_assignment'
                operation['role_matrix']['observed_roles'] = copy.deepcopy(contract['observed_roles'])
            if reference['kind'] in ['retained_original_xc_Source11_single_refresh_HTTP', 'retained_original_Source12_search_M3U_resolver_HTTP', 'captured_original_Source51_matching_admin_HTTP', 'captured_original_Source53_stations_Source55_aggregate_admin_HTTP', 'retained_original_management12_channelguide25_HTTP', 'retained_original_auth51_plugin35_resolver_scoped_HTTP', 'retained_original_SD25_resolver_scoped_HTTP', 'retained_original_image22_core_reads_TCP', 'retained_original_image22_serializer_null_TCP', 'retained_original_provider_channel_vod_HTTP', 'retained_original_HDHR_Image27_TCP', 'retained_original_playback_VOD_XC_TCP', 'retained_original_Image22_TS_stream_group_HTTP']:
                operation['role_matrix']['state'] = 'partial_Original_real_HTTP_capture_assignment'
                operation['role_matrix']['observed_roles'] = copy.deepcopy(contract['observed_roles'])
            if reference['kind'] == 'retained_original_repository_socket_switch':
                operation['role_matrix']['state'] = 'partial_Original_retained_transport_observations'
                operation['role_matrix']['observed_roles'] = copy.deepcopy(contract['observed_roles'])
            if reference['kind'] == 'retained_original_admin_model_TCP':
                operation['role_matrix']['state'] = 'partial_Original_real_HTTP_capture_assignment'
                operation['role_matrix']['observed_roles'] = copy.deepcopy(contract['observed_roles'])
            if reference['kind'] in ('retained_original_DVR_XC_playback_HTTP', 'retained_original_Connect_collection_HTTP',
                                     'retained_original_Connect_log_read_and_subscription_set_HTTP', 'retained_original_alias19_anonymous_HTTP',
                                     'retained_original_Connect_detail_PUT_DELETE_HTTP', 'retained_original_alias_remainder_HTTP',
                                     'retained_original_Connect_saved_webhook_test_HTTP', 'retained_original_selected_single_Set_EPG_HTTP', 'retained_original_batch_Set_EPG_HTTP', 'retained_original_System_Logs_and_regex_preview_GET', 'retained_original_Live_TS_global_stop_HTTP', 'retained_original_Live_TS_detail_and_targeted_stop_HTTP', 'retained_original_canonical_management_HTTP', 'retained_original_exact_VOD_split_HTTP', 'retained_original_Image57_accounts_canonical_HTTP', 'retained_original_Image57_M3U_canonical_HTTP'):
                operation['role_matrix']['state'] = 'partial_Original_real_HTTP_capture_assignment'
                operation['role_matrix']['observed_roles'] = copy.deepcopy(contract['observed_roles'])
            operation['coverage_state'] = 'partial_executable_Source_contract'
        loaded.append({'path': reference['path'], 'sha256': reference['sha256'], 'kind': reference['kind']})
    return {'state': 'partial executable Source definitions, not endpoint verification',
            'operations': len(seen), 'inputs': loaded, 'fully_verified_added': 0,
            **({'unmapped_method_Source_observations': unmapped_method_observations} if unmapped_method_observations else {})}


def apply_non_HTTP(root, entries):
    """Only reviewed exact model/task/consumer candidates; no arbitrary importer."""
    root = Path(root)
    manifest = json.loads((root / 'scripts/rust-contracts/executable-source-inputs.json').read_text())
    index = {entry['candidate_id']: entry for entry in entries}
    seen = set()
    for reference in manifest['inputs']:
        if reference['kind'] not in [
            'pinned_original_account_settings_tests', 'captured_original_scanner_task_observations',
            'pinned_original_provider_output_consumer_tests', 'retained_original_task_operator7_observations',
            'retained_original_comskip_task_observations', 'retained_original_clocked_recording_tasks',
        ]:
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
        elif reference['kind'] == 'retained_original_task_operator7_observations':
            import retained_task_operator_contracts
            retained_task_operator_contracts.validate(root, value)
            source_kind = None  # Only the seven validator-pinned task/command identities.
        elif reference['kind'] == 'retained_original_comskip_task_observations':
            import retained_comskip_task_contracts
            retained_comskip_task_contracts.validate(root, value)
            source_kind = 'task_candidate'
        elif reference['kind'] == 'retained_original_clocked_recording_tasks':
            import retained_clocked_recording_contracts
            retained_clocked_recording_contracts.validate(root, value)
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
            current_kind = contract['source']['kind'] if source_kind is None else source_kind
            if source_kind is None:
                assert current_kind in ['task_candidate', 'management_command']
            assert entry['source']['kind'] == current_kind
            assert entry['fully_verified'] is False and contract['fully_verified'] is False
            entry['setup_fixture'] = (copy.deepcopy(value['fixtures'][contract['fixture_id']]) if current_kind in ['model_candidate', 'websocket_consumer_candidate']
                                      else {key: copy.deepcopy(value['fixtures'][key]) for key in contract['fixture_ids']})
            if reference['kind'] in ('retained_original_task_operator7_observations',
                                     'retained_original_comskip_task_observations', 'retained_original_clocked_recording_tasks'):
                entry['owning_milestone'] = contract['owning_milestone']
            entry['assertions'] = copy.deepcopy(contract['assertions'])
            entry['result'] = copy.deepcopy(contract['result'])
            entry['executable_Source_contract'] = copy.deepcopy(contract)
            entry['state'] = ('partial_executable_Original_model_tests' if current_kind == 'model_candidate'
                              else 'partial_executable_Original_task_observations' if current_kind == 'task_candidate'
                              else 'partial_executable_Original_management_subprocess_observations' if current_kind == 'management_command'
                              else 'partial_executable_Original_synthetic_consumer_method_tests')
    return {'state': 'typed partial Original model/task/management/synthetic-consumer assertions, not HTTP or Native verification',
            'candidates': len(seen), 'fully_verified_added': 0}
