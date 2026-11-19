"""Produce a sanitized receipt from the actual C98 package and independent cleanup."""
import argparse
import hashlib
import json
from pathlib import Path

LANES = {
    'channel-sole-guide': 'sole_guide_override_scope_passed',
    'hdhr-react': 'hdhr_react_read_scope_passed',
    'admin-xc': 'admin_ingress_alias_scope_passed',
}


def load(path):
    return json.loads(path.read_text())


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sanitize_lane(run, name, benchmark, manifest, admin_retry):
    entry = benchmark['cases'][name]
    if entry['preflight_actual_exit'] != 0:
        require(entry['application_started'] is False, 'fixture was not admitted')
        return {'application_started': False, 'preflight_actual_exit': entry['preflight_actual_exit']}
    retry = admin_retry and name == 'admin-xc'
    folder = run / f"{name}-compose{'-retry2' if retry else ''}"
    paths = {key: folder / target for key, target in (
        ('receipt', 'receipt.json'), ('execution', 'execution.json'), ('cleanup', 'independent-cleanup.json'),
        ('report', 'evidence/integration.json'))}
    report = load(paths['report'])
    proof = load(paths['receipt'])
    execution = load(paths['execution'])
    cleanup = load(paths['cleanup'])
    actual = int((run / f"{name}{'-retry2' if retry else ''}.actual-exit").read_text())
    require(actual == entry['compose_actual_exit'] and execution['actual_exit'] == proof['tracked_runner_actual_exit'],
            'literal independent launcher and child statuses')
    require(all(execution[key] is True for key in ('process_group_settled', 'stdout_EOF', 'stderr_EOF')),
            'owned runtime output and process group are settled')
    require(execution['failure'] is None and cleanup['passed'] and entry['exact_cleanup'], 'owned runtime teardown')
    require(report['project'] == proof['project'] and cleanup['project'] == proof['project'], 'isolated fixture identity')
    require(proof['candidate_revision'] == manifest['git_revision'] and proof['image_id'] == manifest['image_id'],
            'immutable Native package and commit identity')
    require(proof[LANES[name]] is (actual == 0), 'actual result matches the qualified scope')
    checks = report['checks']
    failed = [row['name'] for row in checks if not row['passed']]
    foundation = proof['foundation']
    qualified = proof[LANES[name]]
    return {'selected_scope': proof['scope'], 'actual_exit': actual, 'actual_child_exit': execution['actual_exit'],
            'checks_observed': len(checks), 'failed_checks': failed, 'actual_report_error_type': report.get('failure'),
            'foundation': foundation, 'scope_passed': qualified, 'exact_cleanup': cleanup['passed'],
            'native_public_redirect_verified': proof.get('native_public_redirect_verified'),
            'evidence_sha256': {key: sha(path) for key, path in paths.items()}}


def repaired_cohort(run, manifest):
    first = run / 'admin-xc-compose'
    cleanup_path = first / 'independent-cleanup.json'
    execution_path = first / 'execution.json'
    report_path = first / 'evidence/integration.json'
    cleanup, execution, report = map(load, (cleanup_path, execution_path, report_path))
    actual = int((run / 'admin-xc.actual-exit').read_text())
    observed = cleanup['owned_container_generations']
    require(actual == execution['actual_exit'] == 1 and execution['process_group_settled']
            and execution['stdout_EOF'] and execution['stderr_EOF'], 'original fixture exit must stay explicit')
    require(report['failure'] == 'TimeoutError' and report['cleanup'] == 'project removed'
            and sorted(row['service'] for row in observed) == ['baseline', 'origin']
            and all(row['container_absent'] and row['host_generation_absent'] for row in observed)
            and not any(cleanup['inventories'].values()), 'exact partial-start cleanup')
    lanes = {}
    for name in LANES:
        ending = '-retry2' if name == 'admin-xc' else ''
        preflight = int((run / f'{name}-preflight.actual-exit').read_text())
        actual = int((run / f'{name}{ending}.actual-exit').read_text())
        retired = load(run / f'{name}-compose{ending}/independent-cleanup.json')['passed']
        lanes[name] = {'application_started': True, 'preflight_actual_exit': preflight,
                       'compose_actual_exit': actual, 'exact_cleanup': retired}
    return {'cases': lanes, 'revision': manifest['git_revision'], 'image_id': manifest['image_id']}, {
        'actual_exit': 1, 'observed_roles_retired': ['baseline', 'origin'], 'Native_started': False,
        'remaining_owned_resources': 0, 'failed_phase': 'Original nginx startup',
        'evidence_sha256': {'cleanup': sha(cleanup_path), 'execution': sha(execution_path), 'report': sha(report_path)},
    }


def verify(run, manifest_path, baseline_path):
    manifest = load(manifest_path)
    state = load(run / 'state.json')
    admin_retry = state['state'] == 'queue-stopped'
    if admin_retry:
        require(state['failure'] == 'admin-xc retained named Compose resources', 'preserve original controller diagnosis')
        benchmark, first = repaired_cohort(run, manifest)
    else:
        benchmark = load(run / 'cohort.json')
        require(state['state'] == 'compose-cohort-settled' and state['cases'] == benchmark['cases'], 'complete literal cohort')
        first = None
    require(manifest['passed'] and manifest['exit_code'] == 0
            and benchmark['revision'] == manifest['git_revision'] and benchmark['image_id'] == manifest['image_id'],
            'successfully built exact package')
    selected = lambda d: {name: h for name, h in d['source_sha256'].items()
                          if name.startswith('frontend/') or name in ('LICENSE', 'ATTRIBUTION.md')}
    unchanged = selected(manifest)
    require(len(unchanged) == 497 and unchanged == selected(load(baseline_path)), 'unchanged React, license and attribution inputs')
    scopes = {name: sanitize_lane(run, name, benchmark, manifest, admin_retry) for name in LANES}
    return {'candidate_revision': manifest['git_revision'], 'image_id': manifest['image_id'],
            'build_manifest_sha256': sha(manifest_path), 'rust_test_actual_exits': load(run / 'exit.json'),
            'react_license_attribution_inputs': len(unchanged),
            'initial_admin_fixture_failure': first, 'cohort_scopes_passed': all(row.get('scope_passed') for row in scopes.values()),
            'scope_results': scopes, 'milestone_acceptance': False, 'performance_verified': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.run, args.build, args.baseline), indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
