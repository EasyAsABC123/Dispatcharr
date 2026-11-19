"""Reproduce the safe scoped System Logs proof from retained Compose evidence."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

REVISION = 'bae33e83801096c8037c0e41d05fa2b25f32dbc6'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
PREFIX = 'persisted-log-list-'
PREDICATES = ('exact_expected_body', 'role_and_representation_headers_match', 'same_read_only_input_set')


def load(path):
    return json.loads(path.read_bytes())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(run_dir, manifest_path, reference_path, focus_dir):
    manifest = load(manifest_path)
    reference = load(reference_path)
    def ui_inputs(value):
        return {name: digest for name, digest in value['source_sha256'].items()
                if name.startswith('frontend/') or name in {'LICENSE', 'ATTRIBUTION.md'}}
    assets = ui_inputs(manifest)
    assert len(assets) == 497 and assets == ui_inputs(reference)
    terminal_path = run_dir / 'execution.json'
    cleanup_path = run_dir / 'independent-cleanup.json'
    receipt_path = run_dir / 'receipt.json'
    report_path = run_dir / 'evidence/integration.json'
    terminal, cleanup, receipt, report = map(load, (terminal_path, cleanup_path, receipt_path, report_path))
    assert manifest['git_revision'] == receipt['candidate_revision'] == REVISION
    assert receipt['source_revision'] == ORIGINAL and manifest['passed'] and manifest['exit_code'] == 0
    assert manifest['image_id'] == receipt['image_id'] == report['candidate_image_binding']['image_id']
    assert report['candidate_image_binding']['manifest_verified'] and receipt['with_download']
    assert report['build_manifest_sha256'] == sha(manifest_path)
    assert terminal['actual_exit'] == receipt['tracked_runner_actual_exit'] == 0
    assert terminal['stdout_EOF'] and terminal['stderr_EOF'] and terminal['process_group_settled']
    assert not terminal['remaining_group_members'] and terminal['failure'] is None
    entries = cleanup['owned_container_generations']
    assert cleanup['passed'] and not any(cleanup['inventories'].values())
    assert len(entries) == 4 and all(row['container_absent'] and row['host_generation_absent'] for row in entries)
    checks = report['checks']
    related = {}
    for row in checks:
        assert row['passed'] is True
        name = row['name']
        if name.startswith(PREFIX):
            backend, name = name[len(PREFIX):].split(':')
            details = row['details']
            assert backend in ('source', 'native')
            assert re.fullmatch(r'[a-f0-9]{64}', details['actual_body_sha256'])
            assert all(details[key] is True for key in PREDICATES)
            cases = related.setdefault(name, {})
            assert backend not in cases
            cases[backend] = details
    assert report['passed'] and receipt['passed'] and report['cleanup'] == 'project removed'
    [mount] = [row for row in checks if row['name'] == 'paired-same-read-only-log-set']
    assert mount['details']['same_fixture_mount'] is True
    counts = Counter()
    media = {}
    for name, cases in related.items():
        assert cases['source'] == cases['native']
        counts['tail' if name.startswith('tail-') else
               'download' if name.startswith('download-') else 'list'] += 1
        if name.startswith('download-unicode-'):
            media[name] = cases['source']['actual_content_disposition']
    assert len(checks) == 77 and counts == {'list': 8, 'tail': 13, 'download': 8}
    focus = focus_dir / 'exit.json'
    assert load(focus) == {'fmt': 0, 'clippy': 0, 'store': 0}
    tests = focus_dir / 'store.log'
    assert '542 passed; 0 failed; 79 ignored' in tests.read_text()
    return {'candidate_revision': REVISION, 'original_revision': ORIGINAL,
            'candidate_image': manifest['image_id'], 'actual_build_exit': 0, 'actual_compose_exit': 0,
            'all_streams_and_process_group_settled': True, 'foundation_checks': 18,
            'paired_cases_per_backend': dict(counts), 'passed_mount_identity_checks': 1,
            'matched_unicode_attachment_wire_headers': media, 'owned_generations_retired': 4,
            'owned_resources_empty': True, 'react_license_attribution_inputs_unchanged': len(assets),
            'store_tests_passed': 542, 'external_fixture_tests_ignored': 79,
            'focused_format_and_strict_clippy_passed': True, 'full_M8_accepted': False,
            'performance_accepted': False,
            'artifact_sha256': {name: sha(path) for name, path in
                                (('manifest', manifest_path), ('reference_manifest', reference_path), ('execution', terminal_path),
                                 ('receipt', receipt_path), ('cleanup', cleanup_path),
                                 ('integration', report_path), ('focus_exit', focus), ('store_log', tests))}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--reference-manifest', type=Path, required=True)
    parser.add_argument('--focus-dir', type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        parser.error('Run with contract assertions enabled')
    print(json.dumps(run(args.run_dir, args.manifest, args.reference_manifest, args.focus_dir), sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
