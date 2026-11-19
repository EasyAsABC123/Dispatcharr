"""Qualify the three scoped Docker Compose fixtures on the exact C94 package."""
import argparse
import hashlib
import json
from pathlib import Path
import re

REVISION = 'fc5562a37d91ed6673e6ee9a740be4e0dd6c7c5f'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
SCOPES = {
    'selected_guide': ('batch-epg-refresh-', 29, 10),
    'regex_preview': ('stream-regex-preview-', 48, 30),
    'default_guide': ('channel-batch-epg-', 52, 34),
}
FIELDS = {'actual_status', 'actual_body_sha256', 'channels_changed', 'exact_redis_publication',
          'monitor_fences_and_thread_verified', 'no_request_cache_increment_before_publish',
          'only_target_fk_changed_and_sequences_preserved', 'publication_frame_sha256',
          'publication_observed_count', 'response_matches_normal_contract', 'programme_count',
          'success_with_exact_programmes', 'worker_backend_and_claim_retired',
          'expected_body_match', 'literal_match', 'response_bytes', 'sql_broker_unchanged',
          'wire_match', 'sql_cache_broker_match'}


def load(path):
    return json.loads(path.read_bytes())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def qualify(directory, expected, image, build_hash):
    files = {name: directory / leaf for name, leaf in (
        ('execution', 'execution.json'), ('receipt', 'receipt.json'),
        ('integration', 'evidence/integration.json'), ('cleanup', 'independent-cleanup.json'))}
    execution, receipt, report, cleanup = (load(path) for path in files.values())
    assert receipt['candidate_revision'] == REVISION and receipt['source_revision'] == ORIGINAL
    assert image == receipt['image_id'] == report['candidate_image_binding']['image_id']
    assert report['candidate_image_binding']['manifest_verified'] and report['build_manifest_sha256'] == build_hash
    assert execution['actual_exit'] == receipt['tracked_runner_actual_exit'] == 0
    assert execution['stdout_EOF'] and execution['stderr_EOF'] and execution['process_group_settled']
    assert execution['failure'] is None and not execution['remaining_group_members']
    assert receipt['passed'] and report['passed'] and cleanup['passed'] and report['cleanup'] == 'project removed'
    owned = cleanup['owned_container_generations']
    assert len(owned) == 4 and not any(cleanup['inventories'].values())
    assert all(row['container_absent'] and row['host_generation_absent'] for row in owned)
    prefix, check_count, business_count = expected
    checks = report['checks']
    assert len(checks) == check_count and all(row['passed'] for row in checks)
    cases = {}
    for row in checks:
        if row['name'].startswith(prefix):
            assert row['name'] not in cases
            detail = {name: value for name, value in row['details'].items() if name in FIELDS}
            if 'actual_status' in detail:
                assert re.fullmatch('[0-9a-f]{64}', detail['actual_body_sha256'])
            cases[row['name']] = detail
    assert len(cases) == business_count
    return {'actual_exit': 0, 'pipes_and_process_group_settled': True,
            'owned_generations_removed': len(owned), 'owned_resource_inventories_empty': True,
            'business_checks': business_count, 'foundation_checks': check_count-business_count,
            'cases': cases, 'raw_fingerprints': {name: sha(path) for name, path in files.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('build', 'reference-build', 'focus', 'namespace-refusal', *SCOPES):
        parser.add_argument('--'+name.replace('_','-'), required=True, type=Path)
    args = parser.parse_args()
    if not __debug__:
        parser.error('Run with contract assertions enabled')
    build = load(args.build)
    baseline = load(args.reference_build)
    credit_inputs = {name: digest for name, digest in build['source_sha256'].items()
                     if name.startswith('frontend/') or name in {'ATTRIBUTION.md','LICENSE'}}
    assert len(credit_inputs) == 497 and all(baseline['source_sha256'][k] == v for k,v in credit_inputs.items())
    assert build['git_revision'] == REVISION and build['passed'] and build['exit_code'] == 0
    build_hash = sha(args.build)
    focus = args.focus
    terminal = focus/'exit.json'
    assert load(terminal) == {'fmt':0,'store':0,'main':0,'clippy':0}
    assert '544 passed; 0 failed; 79 ignored' in (focus/'store.log').read_text()
    assert '35 passed; 0 failed; 1 ignored' in (focus/'main.log').read_text()
    runtime = {scope: qualify(getattr(args,scope), expected, build['image_id'], build_hash)
               for scope, expected in SCOPES.items()}
    failure = load(args.namespace_refusal)
    assert failure['actual_runner_exit']==2 and failure['blocked_before_own_project_creation']
    assert not any(failure['physical_owned_resource_counts'].values()) and failure['product_checks_executed']==0
    report = {'candidate_revision': REVISION, 'original_revision': ORIGINAL, 'build_actual_exit': 0,
              'package_image': build['image_id'], 'package_manifest_sha256': build_hash,
              'react_license_attribution_inputs_unchanged': len(credit_inputs),
              'focused_rust': {'strict_clippy': True, 'fmt': True, 'store_passed':544, 'main_passed':35,
                               'store_fixture_ignored':79, 'main_fixture_ignored':1,
                               'hashes':{path.name: sha(path) for path in (
                                   terminal, focus/'store.log', focus/'main.log', focus/'clippy.log')}},
              'initial_selected_guide_namespace_refusal': failure, 'scopes': runtime,
              'full_milestone_passed': False, 'performance_verified': False,
              'credit': 'Original Dispatcharr authors and contributors retain full application and algorithm credit.'}
    print(json.dumps(report, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
