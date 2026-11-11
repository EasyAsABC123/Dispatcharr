"""Compare narrowly captured actual idle branches; full M4 stays unaccepted.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
"""
import json

# Exact Source-observed spellings from distinct Original processes286/287.
# This is not generic token sorting or permission/method inference.
ALLOW_OBSERVED = ['GET, OPTIONS', 'OPTIONS, GET']
SELECTED = ['content-type', 'content-length', 'cache-control', 'pragma', 'accept-ranges',
            'x-content-type-options', 'x-frame-options', 'referrer-policy',
            'cross-origin-opener-policy', 'vary']


def compare(source, native, outer, busy):
    checks = {}
    remaining_headers = {}
    for role, record in [('Source', source), ('native', native)]:
        idle = record.get('controlled_idle_entry', {})
        before, after, wire = (idle.get(key, {}) for key in ['before', 'after', 'wire'])
        expected = 429 if busy else 200
        checks[role + '_capture_complete'] = record.get('capture_complete') is True
        checks[role + '_actual_initial_HTTP'] = record.get('initial_status') == 200
        viewer = record.get('viewer_auth')
        initial = next((item.get('metadata', {}) for item in record.get('observations', []) if item.get('name') == 'active-before-loss'), {})
        checks[role + '_initial_identity_matches_capture'] = initial.get('user_id') == (str(viewer['user_id']) if viewer else 'unknown')
        ids = record.get('redis_run_ids', [])
        checks[role + '_actual_Redis_restart_headerloss'] = len(ids) == 2 and ids[0] != ids[1] and record.get('authorization_omitted_after_loss') is True
        checks[role + '_independent_idle_qualification'] = idle.get('qualified') is True and idle.get('attempted') is True and before.get('active_streams') == '0' and idle.get('provider_before') == ('1' if busy else '0')
        checks[role + '_same_generation_identity_preserved'] = before.get('created_at') is not None and all(before.get(key) == after.get(key) for key in ['created_at', 'user_id', 'content_uuid', 'content_obj_type']) and before.get('user_id') == 'unknown'
        checks[role + '_wire_status'] = wire.get('status') == expected and wire.get('streaming') is (not busy)
        checks[role + '_request_count'] = before.get('request_count') == '2' and after.get('request_count') == ('2' if busy else '3')
        checks[role + '_provider_and_active_after'] = idle.get('provider_after') == '1' and after.get('active_streams') == ('0' if busy else '1')
        checks[role + '_body_prefixes'] = record.get('body_prefix_matches_origin') is True and record.get('younger_after_old_close', {}).get('ended') is False and record.get('younger_after_old_close', {}).get('chunks_read') == 103
        headers = wire.get('response_headers', {})
        pairs = wire.get('response_header_pairs', [])
        checks[role + '_lossless_header_pairs_present'] = isinstance(pairs, list) and bool(pairs)
        checks[role + '_single_Source_Vary_field'] = [value for name, value in pairs if name.lower() == 'vary'] == ['Accept, origin']
        checks[role + '_Allow_is_exact_observed_Source_variant'] = headers.get('allow') in ALLOW_OBSERVED
        checks[role + '_no_Retry_After'] = 'retry-after' not in headers
        if busy:
            policy = record.get('idle_busy_fixture', {})
            checks[role + '_actual_owned_ORM_2_to_1'] = policy.get('initial_profile_max_streams') == 2 and policy.get('initial_account_max_streams') == 2 and policy.get('final_profile_max_streams') == 1 and policy.get('final_account_max_streams') == 1 and policy.get('policy_lowered_before_target_close') is True
            other = record.get('owned_idle_contender', {})
            observed = record.get('owned_idle_contender_admission', {})
            checks[role + '_other_real_HTTP_movie_preserved'] = other.get('status') == 200 and other.get('streaming') is True and other.get('first_bytes') == 8192 and other.get('provider_count') == '1' and observed.get('provider_count') == '2' and observed.get('active_streams') == '1' and observed.get('admitted_under_limit') == 2
            checks[role + '_bare37_HTML'] = wire.get('error_body') == 'Connection limit exceeded for profile' and headers.get('content-type') == 'text/html; charset=utf-8' and headers.get('content-length') == '37'
        else:
            checks[role + '_valid_first8192'] = wire.get('first_bytes') == 8192 and wire.get('first_sha256') == record.get('same_id_reopen', {}).get('first_sha256')
            timer = next((item for item in record.get('observations', []) if item['name'] == 'controlled-idle-entry-after-prior-cleanup-timer'), {})
            checks[role + '_survives_prior_timer'] = bool(timer.get('metadata')) and timer.get('provider_count') == '1'
        remaining_headers[role] = {key: [value for name, value in pairs if name.lower() == key] for key in ['allow', 'x-worker-id', 'connection', 'date', 'server']}
    left = source['controlled_idle_entry']['wire'].get('response_header_pairs', [])
    right = native['controlled_idle_entry']['wire'].get('response_header_pairs', [])
    # Field names are case-insensitive; preserve every value/line and its order.
    # No sorting/coalescing of values hides duplicate or missing field lines.
    checks['selected_headers_exact'] = all([v for n, v in left if n.lower() == key] == [v for n, v in right if n.lower() == key] for key in SELECTED)
    checks['manifest_bound_candidate_process_preserved'] = outer.get('active_candidate_process_preserved') is True and bool(outer.get('active_candidate_provenance'))
    checks['runner_completed_without_failure_and_cleaned'] = outer.get('passed') is True and not outer.get('failure') and outer.get('cleanup_passed') is True
    return {'passed': all(checks.values()), 'scope': 'Actual packaged Source/native normal-idle or dynamic-profile refusal branch only; no fullM4/default/worker/hop-header/Source270 concurrent acceptance.', 'busy': busy, 'checks': checks, 'selected_header_names': SELECTED, 'Source_observed_Allow_exact_variants': ALLOW_OBSERVED, 'remaining_headers_raw': remaining_headers, 'full_m4_verified': False, 'default_recovery_enabled': False}
