#!/usr/bin/env python3
"""Verify and project Candidate91’s scoped M4/M5 acceptance without private payloads."""
import argparse
import hashlib
import json
from pathlib import Path
import re

CANDIDATE = '695779244a9dffb477ecb885ebbb19791d3a5021'
PREDECESSOR = '3a1b0f1cb8b86724037ee2840b089dbe3b9a5b6c'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ORIGINAL_ARCHIVE = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
SHA = re.compile('[0-9a-f]{64}')
M4 = ('dvr-list', 'watch-completed-with-react-query', 'file-head', 'closed-seek', 'open-seek',
      'hls-playlist-bearer', 'hls-segment-bearer', 'hls-heartbeat', 'sql-unchanged')
M4_MEDIA_CODES = {'watch-completed-with-react-query': 200, 'file-head': 200, 'closed-seek': 206,
                  'open-seek': 206, 'hls-playlist-bearer': 200, 'hls-segment-bearer': 200}
M5 = {'anonymous-before-parse': 401, 'standard': 403, 'viewer': 403, 'inactive-admin': 401,
      **dict.fromkeys(('malformed-json', 'missing-associations', 'empty-associations', 'null-associations'), 400),
      **dict.fromkeys(('ignore-unknown', 'assign-two-with-unknown', 'repeat-assignment', 'last-association-wins',
                       'missing-guide-clears', 'collection-ignores-filter', 'assign-across-original-batches',
                       'clear-across-original-batches'), 200)}


def small(path):
    with path.open('rb') as source:
        data = source.read(4 * 1024**2 + 1)
    if len(data) > 4 * 1024**2:
        raise ValueError('An evidence summary exceeds the 4 MiB limit')
    return data


def load(path):
    return json.loads(small(path))


def sha(path):
    return hashlib.sha256(small(path)).hexdigest()


def counts(log):
    match = re.search(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
    if match is None:
        raise ValueError('No terminal Rust test counts')
    return dict(zip(('passed', 'failed', 'ignored'), map(int, match.groups()), strict=True))


def focused(before, after):
    assert (before/'revision').read_text().strip() == PREDECESSOR and (after/'revision').read_text().strip() == CANDIDATE
    pre, sb, rerun = load(before/'exit.json'), load(after/'exit.json'), load(after/'qualified-exit.json')
    assert pre == {'fmt': 0, 'store': 101, 'clippy': 101}
    assert all(b'error[E0432]: unresolved import `crate::connect_create`' in small(before/(name+'.log'))
               for name in ('store', 'clippy'))
    sandbox, working = (small(after/name) for name in ('store.log', 'store-loopback.log'))
    assert sb == {'fmt': 0, 'store': 101, 'clippy': 0} and rerun == {'fmt': 0, 'store': 0, 'clippy': 0}
    assert sandbox.count(b'kind: PermissionDenied, message: "Operation not permitted"') == 8
    assert counts(sandbox) == {'passed': 526, 'failed': 8, 'ignored': 79}
    assert counts(working) == {'passed': 534, 'failed': 0, 'ignored': 79}
    witness = (b'channels::batch_epg::tests::keeps_last_association_and_treats_absent_guide_as_clear',
               b'channels::batch_epg::tests::supports_bigautofield_ids_and_declines_non_ui_identifier_shapes',
               b'recording_playback::writer_admission_tests::ten_viewers_wait_in_order_without_increasing_four_writer_limit',
               b'recording_playback::writer_admission_tests::cancelled_and_timed_out_waiters_do_not_consume_a_writer')
    assert all(b'test '+name+b' ... ok' in working for name in witness)
    return [
        {'revision': PREDECESSOR, 'stage': 'initial compile', 'actual_exits': pre, 'failure': 'E0432 unresolved Connect module import',
         'sources_sha256': {name: sha(before/name) for name in ('exit.json','store.log','clippy.log')}},
        {'revision': CANDIDATE, 'stage': 'sandbox Store tests', 'actual_exits': sb, 'store': counts(sandbox),
         'failure': 'Eight loopback socket operations refused by the sandbox',
         'sources_sha256': {name: sha(after/name) for name in ('exit.json','store.log','clippy.log','fmt.log')}},
        {'revision': CANDIDATE, 'stage': 'qualified loopback Store rerun', 'actual_exits': rerun, 'store': counts(working),
         'm4_writer_tests_passed': 2, 'm5_batch_assignment_tests_passed': 2,
         'sources_sha256': {name: sha(after/name) for name in ('qualified-exit.json','store-loopback.log')}},
    ]


def fixture(directory, manifest, manifest_sha, scope):
    if directory is None:
        return {'status': 'not_observed'}
    names = ['receipt.json', 'binding.json', 'execution.json', 'independent-cleanup.json']
    receipt, binding, execution, cleanup = (load(directory/name) for name in names)
    report_name = 'evidence/integration.json'
    integration = load(directory/report_name) if (directory/report_name).is_file() else None
    if integration is not None:
        names.append(report_name)
    expected = 'paired_ordinary_dvr_playback' if scope == 'M4' else 'paired_batch_epg_stock_react'
    assert receipt['scope'] == binding['scope'] == expected
    assert receipt['source_revision'] == ORIGINAL and receipt['candidate_revision'] == binding['candidate_revision'] == CANDIDATE
    assert receipt['image_id'] == binding['image_id'] == manifest['image_id']
    assert binding['build_sha256'] == manifest_sha and binding['source_archive_sha256'] == manifest['source_archive_sha256']
    assert receipt['project'] == binding['project'] == cleanup['project']
    assert integration is None or integration['project'] == receipt['project']
    assert execution['actual_exit'] == receipt['tracked_runner_actual_exit']
    settled = (execution['stdout_EOF'] and execution['stderr_EOF'] and execution['process_group_settled']
               and not execution['remaining_group_members'])
    roles = cleanup.get('owned_container_generations', [])
    retired = sum(r['container_absent'] and r['host_generation_absent'] for r in roles)
    if not receipt['passed']:
        assert not receipt.get('performance_verified')
        return {'status': 'failed', 'actual_exit': execution['actual_exit'], 'process_settled': settled,
                'cleanup_verified': cleanup['passed'], 'retired_container_generations': retired,
                'business_cases_observed': len(integration['checks']) if integration else 0,
                'sources_sha256': {name: sha(directory/name) for name in names}}
    assert integration is not None
    assert execution['actual_exit'] == 0 and execution['failure'] is None and settled and cleanup['passed']
    assert sorted(r['service'] for r in roles) == ['baseline', 'candidate', 'data-init', 'origin'] and retired == 4
    assert not any(cleanup['inventories'].values()) and integration['cleanup'] == 'project removed'
    identity = integration['candidate_image_binding']
    assert identity['manifest_verified'] and identity['image_id'] == manifest['image_id']
    assert identity['source_tree_sha256'] == manifest['source_tree_sha256'] and integration['baseline_archive_sha256'] == ORIGINAL_ARCHIVE
    assert integration['passed'] and receipt['foundation']['passed'] and not receipt['performance_verified']
    checks = integration['checks']
    assert all(item['passed'] for item in checks)
    if scope == 'M4':
        case_prefix = 'recording-read-'
        expected_m4 = {f'{case_prefix}{backend}:{label}' for backend in ('source', 'native') for label in M4}
        cases = [item for item in checks if item['name'].startswith(case_prefix)]
        assert {item['name'] for item in cases} == expected_m4 and len(cases) == len(expected_m4)
        assert integration['contract_suites'] == ['recording_read'] and receipt['ordinary_dvr_scope_passed']
        projected_cases = {}
        for item in cases:
            name = item['name'].removeprefix(case_prefix)
            _, label = name.split(':')
            raw = item['details']
            if label in M4_MEDIA_CODES:
                assert raw['actual_status'] == M4_MEDIA_CODES[label] and raw['headers_match']
                assert raw['expected_media_or_manifest_exact'] and SHA.fullmatch(raw['response_sha256'])
                witnesses = ('actual_status','headers_match','expected_media_or_manifest_exact','response_bytes','response_sha256')
            elif label == 'dvr-list':
                assert raw['actual_status'] == 200 and raw['owned_urls_and_rows_match'] and SHA.fullmatch(raw['json_sha256'])
                witnesses = ('actual_status','owned_urls_and_rows_match','json_sha256')
            elif label == 'hls-heartbeat':
                assert raw['owned_heartbeat_active'] and 0 < raw['actual_ttl_seconds'] <= 20
                witnesses = ('owned_heartbeat_active','actual_ttl_seconds')
            else:
                assert label == 'sql-unchanged' and raw['all_recording_rows_and_sequence_unchanged']
                witnesses = ('all_recording_rows_and_sequence_unchanged',)
            projected_cases[name] = {key: raw[key] for key in witnesses}
        for label in ('watch-completed-with-react-query','file-head','closed-seek','open-seek','hls-segment-bearer'):
            assert projected_cases['source:'+label] == projected_cases['native:'+label]
        details = {'contract': 'ordinary DVR list, completed read/seek/HEAD and active HLS read', 'requests_verified': len(cases)}
    else:
        case_prefix = 'channel-batch-epg-'
        expected_m5 = {f'{case_prefix}{backend}:{label}': value for backend in ('source','native') for label,value in M5.items()}
        expected_m5.update({case_prefix+'source:source-xmltv-publication': 200, case_prefix+'native:publisher-absent-atomic-guard': 501})
        cases = [item for item in checks if item['name'].startswith(case_prefix)]
        assert {item['name'] for item in cases} == set(expected_m5) and len(cases) == len(expected_m5)
        assert all(item['details']['actual_status'] == expected_m5[item['name']] and item['details']['sql_cache_broker_match']
                   and SHA.fullmatch(item['details']['actual_body_sha256']) for item in cases)
        assert integration['contract_suites'] == ['channel_batch_epg'] and receipt['batch_epg_ui_scope_passed']
        assert receipt['native_batch_guide_publication_verified'] is False
        projected_cases = {item['name'].removeprefix(case_prefix): {key: item['details'][key] for key in
                           ('actual_status', 'actual_body_sha256', 'sql_cache_broker_match')} for item in cases}
        assert all(projected_cases['source:'+label] == projected_cases['native:'+label] for label in M5)
        details = {'contract': 'stock React batch dummy assignment and clear', 'matched_requests': 2*len(M5),
                   'original_only_xmltv_publication_status': 200, 'native_atomic_unsupported_batch_guard_status': 501,
                   'native_batch_guide_publication_verified': False}
    return {'status': 'passed', 'actual_exit': 0, 'process_settled': True, 'cleanup_verified': True,
            'retired_container_generations': retired, 'foundation_checks': len(checks)-len(cases),
            'case_results': projected_cases, **details, 'sources_sha256': {name: sha(directory/name) for name in names}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('candidate90-focused', 'candidate91-focused'):
        parser.add_argument('--'+name, required=True, type=Path)
    for name in ('manifest','m4-dir','m5-dir'):
        parser.add_argument('--'+name, type=Path)
    args = parser.parse_args()
    if not __debug__:
        parser.error('Optimized assertion removal is unsupported')
    if (args.m4_dir or args.m5_dir) and not args.manifest:
        parser.error('Paired receipts require the exact package manifest')
    report = {'credit':'Original Dispatcharr authors and contributors retain full application and algorithm credit.',
              'candidate_revision':CANDIDATE, 'original_revision':ORIGINAL, 'performance_accepted':False,
              'full_m4_accepted':False, 'full_m5_accepted':False,
              'focused_chronology':focused(args.candidate90_focused,args.candidate91_focused)}
    manifest = load(args.manifest) if args.manifest else None
    if manifest is not None:
        assert manifest['git_revision'] == CANDIDATE
        assert manifest['passed'] == (manifest['exit_code'] == 0)
    if manifest and manifest['passed']:
        assert manifest['source_input_mode'] == 'committed-revision-snapshot' and manifest['runtime_packages_frozen']
        assert re.fullmatch('sha256:[0-9a-f]{64}',manifest['image_id'])
    if (args.m4_dir or args.m5_dir) and not manifest['passed']:
        parser.error('Paired receipts require a successful exact package')
    digest = sha(args.manifest) if manifest else None
    package = {'status': 'not_observed'}
    if manifest is not None:
        package = {'manifest_sha256': digest, 'actual_exit': manifest['exit_code'],
                   'status': 'passed' if manifest['passed'] else 'failed'}
        if manifest['passed']:
            package.update(image_id=manifest['image_id'], source_tree_sha256=manifest['source_tree_sha256'])
    report['package'] = package
    report['M4'] = fixture(args.m4_dir,manifest,digest,'M4')
    report['M5'] = fixture(args.m5_dir,manifest,digest,'M5')
    print(json.dumps(report,sort_keys=True,indent=2))


if __name__ == '__main__':
    main()
